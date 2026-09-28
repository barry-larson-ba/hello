import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd


def write_delta_history_report(
    spark,
    dbutils,
    table_fqn: str,
    output_path: str,
    cadence_label: str,
    *,
    mlflow_runs_df: "pd.DataFrame | None" = None,
    mlflow_delta_version_col: str = "tags_delta_version",
    timezone_name: str = "America/Los_Angeles",
    tz_label: str = "PT",
    history_limit: int = 25,
    recent_display: int = 5,
    redact_users: bool = False,
    redact_paths: bool = False,
) -> str:
    """
    Write a GitHub-flavored Markdown report for a Delta table's schema and history.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session.
    dbutils : DBUtils
        Databricks utilities handle (used for notebook context / URLs).
    table_fqn : str
        Fully qualified table name, "catalog.schema.table".
    output_path : str
        Absolute path (typically under /Workspace/...) to write the .md file.
    cadence_label : str
        Human-readable release cadence, e.g.
        "Quarterly (10th of the month after quarter close)".
    mlflow_runs_df : pandas.DataFrame, optional
        Result of ``mlflow.search_runs()`` with dots replaced by underscores
        in column names. When provided, each Delta version is left-joined
        against its MLflow run so Recent history also shows report_date,
        source file, and row counts from the run.
    mlflow_delta_version_col : str
        Column in ``mlflow_runs_df`` holding the Delta version tagged by
        the run (defaults to ``tags_delta_version``, matching the tag set
        immediately after the Delta write).
    timezone_name : str
        IANA timezone for all displayed timestamps.
    tz_label : str
        Short label appended to timestamps (e.g. "PT").
    history_limit : int
        Number of recent Delta versions to scan for schema changes and to
        list in the collapsed history block.
    recent_display : int
        Number of versions shown uncollapsed at the top of Recent history.
    redact_users : bool
        When True, user emails in Schema changes and Recent history are
        masked (``first.last@example.com`` → ``f***@example.com``). Use
        for reports that will be shared outside the immediate team.
    redact_paths : bool
        When True, the source-file cell shows just the basename with no
        hyperlink, so the full ``/Volumes/catalog/schema/volume/...`` path
        never appears in the rendered file (not in the display text, not
        in the underlying link target).

    Returns
    -------
    str
        The rendered Markdown (also written to `output_path`).

    Examples
    --------
    With MLflow enrichment:

    >>> import mlflow
    >>> exp = mlflow.get_experiment_by_name(
    ...     "/Workspace/Users/.../orders_history"
    ... )
    >>> runs_pdf = mlflow.search_runs(experiment_ids=[exp.experiment_id])
    >>> runs_pdf.columns = [c.replace(".", "_") for c in runs_pdf.columns]
    >>> write_delta_history_report(
    ...     spark,
    ...     dbutils,
    ...     table_fqn="main.silver.orders_history",
    ...     output_path="/Workspace/Users/.../orders_history.md",
    ...     cadence_label="Quarterly",
    ...     mlflow_runs_df=runs_pdf,
    ... )

    Without MLflow (falls back to Delta-only columns):

    >>> write_delta_history_report(
    ...     spark,
    ...     dbutils,
    ...     table_fqn="your.silver.file",
    ...     output_path="/Workspace/Shared/reports/lookup_group_history.md",
    ...     cadence_label="Quarterly (10th of the month after quarter close)",
    ... )
    """
    catalog, schema_name, table = table_fqn.split(".")
    tz = ZoneInfo(timezone_name)

    # ------------------------------------------------------------------
    # Notebook context & URLs
    # ------------------------------------------------------------------
    ctx = (
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
    )

    def _opt(o):
        """Unwrap a Scala Option to a Python value or None."""
        try:
            return o.get() if o.isDefined() else None
        except Exception:
            return None

    host = (
        _opt(ctx.browserHostName())
        or spark.conf.get("spark.databricks.workspaceUrl", None)
        or ""
    )
    notebook_path = _opt(ctx.notebookPath()) or ""

    notebook_url = f"https://{host}/#workspace{notebook_path}"
    table_url = f"https://{host}/explore/data/{catalog}/{schema_name}/{table}"
    history_url = f"{table_url}?activeTab=history"

    def version_url(v):
        return f"{table_url}?activeTab=history&version={v}"

    def volume_url(path):
        """
        Turn a Unity Catalog volume path like
        ``/Volumes/catalog/schema/volume/sub/path/file.ext`` into a
        Catalog Explorer URL. Returns None for missing values or for
        non-volume paths (dbfs:/, s3://, /mnt/..., workspace paths).
        """
        if path is None or (isinstance(path, float) and pd.isna(path)):
            return None
        p = str(path)
        if not p.startswith("/Volumes/"):
            return None
        return f"https://{host}/explore/data/volumes/{p[len('/Volumes/'):]}"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _isna(x):
        return x is None or (isinstance(x, float) and pd.isna(x))

    def esc(x):
        """Escape a cell value for a GFM table. Empty stays empty (not '—')."""
        if _isna(x):
            return ""
        return str(x).replace("|", "\\|").replace("\n", " ").replace("\r", " ")

    def render_table(headers, rows):
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in rows:
            lines.append("| " + " | ".join(esc(c) for c in row) + " |")
        return "\n".join(lines)

    def fmt_ts(dt, seconds=False):
        fmt = f"%Y-%m-%d %H:%M:%S {tz_label}" if seconds else f"%Y-%m-%d %H:%M {tz_label}"
        return dt.strftime(fmt)

    # ---- MLflow-column helpers -----------------------------------------
    def short_md5(x, n=7):
        return "" if _isna(x) else str(x)[:n]

    def source_file_cell(path):
        """
        Render source_file as a markdown link to the file in Catalog
        Explorer when it's a Unity Catalog volume path; otherwise just
        the basename as plain text. When ``redact_paths`` is True, no
        URL is emitted regardless of the path shape.
        """
        if _isna(path):
            return ""
        name = os.path.basename(str(path))
        if redact_paths:
            return name
        url = volume_url(path)
        return f"[{name}]({url})" if url else name

    def _mask_email(s):
        """'first.last@domain' -> 'f***@domain'. Non-emails: first char + '***'."""
        text = str(s)
        if "@" not in text:
            return (text[0] + "***") if text else ""
        local, _, domain = text.partition("@")
        return f"{local[0]}***@{domain}" if local else f"@{domain}"

    def render_user(name):
        if _isna(name):
            return ""
        return _mask_email(name) if redact_users else str(name)

    def fmt_int(x):
        return "" if _isna(x) else f"{int(x):,}"

    def fmt_date(x):
        return "" if _isna(x) else str(x)

    # ------------------------------------------------------------------
    # Delta history
    # ------------------------------------------------------------------
    hist_full = (
        spark.sql(f"DESCRIBE HISTORY {table_fqn}")
        .select("version", "timestamp", "userName", "operation", "operationMetrics")
        .orderBy("version", ascending=False)
        .limit(history_limit)
        .toPandas()
    )

    if len(hist_full) > 0:
        ts = hist_full["timestamp"]
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        hist_full["timestamp"] = ts.dt.tz_convert(tz)

    # Pull rows_written out of operationMetrics (a dict column)
    def _rows_written(m):
        if not m:
            return None
        v = m.get("numOutputRows")
        return int(v) if v is not None else None

    hist_full["rows_written"] = hist_full["operationMetrics"].apply(_rows_written)

    # ------------------------------------------------------------------
    # Merge MLflow run metadata (optional)
    # ------------------------------------------------------------------
    # Map raw underscore-form column names -> report-friendly names.
    ML_COLS_MAP = {
        "params_report_date":       "report_date",
        "tags_input_path":          "source_file",
        "tags_input_md5":           "source_md5",
        "metrics_input_row_count":  "source_rows_read",
    }

    if mlflow_runs_df is not None and len(mlflow_runs_df) > 0:
        if mlflow_delta_version_col not in mlflow_runs_df.columns:
            raise KeyError(
                f"mlflow_runs_df is missing the join column "
                f"'{mlflow_delta_version_col}'. Did the write cell tag "
                f"the run with mlflow_set_tag('delta_version', ...)?"
            )

        ml = mlflow_runs_df.copy()
        # tags come back as strings; coerce to a nullable int for the join
        ml["_delta_version"] = pd.to_numeric(
            ml[mlflow_delta_version_col], errors="coerce"
        ).astype("Int64")

        keep = ["_delta_version"] + [c for c in ML_COLS_MAP if c in ml.columns]
        ml_slim = (
            ml[keep]
            .dropna(subset=["_delta_version"])
            .drop_duplicates("_delta_version", keep="last")
            .rename(columns=ML_COLS_MAP)
        )

        hist_full = hist_full.merge(
            ml_slim, left_on="version", right_on="_delta_version", how="left"
        )
    else:
        # Guarantee the columns exist so hist_row() works uniformly
        for c in ML_COLS_MAP.values():
            hist_full[c] = None

    # ------------------------------------------------------------------
    # Summary + links
    # ------------------------------------------------------------------
    if len(hist_full) > 0:
        last_write_str = fmt_ts(hist_full["timestamp"].iloc[0].to_pydatetime())
    else:
        last_write_str = "—"

    summary_table = render_table(["Last write"], [[last_write_str]])

    links_block = (
        f":closed_book: [Databricks Notebook]({notebook_url})\\\n"
        f":file_cabinet: [Unity Catalog Location of Table]({table_url})\\\n"
        f":rewind: [Version History]({history_url})\\\n"
        f":spiral_calendar: Cadence: {cadence_label}"
    )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    desc_rows = spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()
    columns = []
    for r in desc_rows:
        name = r["col_name"]
        if not name or name.startswith("#"):
            break
        columns.append((name, r["data_type"], r["comment"] or ""))

    detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0].asDict()
    partition_cols = detail.get("partitionColumns") or []
    num_files = detail.get("numFiles")
    size_bytes = detail.get("sizeInBytes")
    size_str = f"{size_bytes / (1024**2):,.1f} MB" if size_bytes is not None else "—"

    partition_display = (
        "`" + "`, `".join(partition_cols) + "`" if partition_cols else "—"
    )

    layout_line = (
        f"**Partitioned by** {partition_display} · "
        f"**Files** {num_files if num_files is not None else '—'} · "
        f"**Size** {size_str}"
    )

    if columns:
        schema_rows = [(f"`{n}`", f"`{d}`", c) for n, d, c in columns]
        schema_table = render_table(["Column", "Type", "Comment"], schema_rows)
    else:
        schema_table = "No schema information."

    # ------------------------------------------------------------------
    # Schema changes
    # ------------------------------------------------------------------
    SCHEMA_OPS = {
        "ADD COLUMNS", "CHANGE COLUMN", "RENAME COLUMN", "DROP COLUMNS",
        "REPLACE COLUMNS", "UPDATE COLUMN METADATA",
        "CREATE TABLE", "CREATE TABLE AS SELECT",
        "REPLACE TABLE", "REPLACE TABLE AS SELECT",
        "CREATE OR REPLACE TABLE",
    }

    schema_changes = (
        hist_full[hist_full["operation"].isin(SCHEMA_OPS)]
        if len(hist_full) > 0 else hist_full
    )

    if len(schema_changes) > 0:
        n = len(schema_changes)
        s = "" if n == 1 else "s"
        sc_alert = (
            f"> [!WARNING]\n"
            f"> {n} schema-changing operation{s} in the last {history_limit} versions."
        )
        sc_rows = [
            [
                f"[v{row['version']}]({version_url(row['version'])})",
                f"`{row['operation']}`",
                render_user(row["userName"]),
                fmt_ts(row["timestamp"]),
            ]
            for _, row in schema_changes.iterrows()
        ]
        sc_table = render_table(["Version", "Operation", "By", "When"], sc_rows)
        schema_changes_block = f"{sc_alert}\n\n{sc_table}"
    else:
        schema_changes_block = (
            f"No schema-changing operations in the last {history_limit} versions."
        )

    # ------------------------------------------------------------------
    # Recent history  (now with MLflow-enriched columns)
    # ------------------------------------------------------------------
    RECENT_HEADERS = [
        "Version", "Timestamp", "Operation", "User",
        "Report date", "Source file", "Rows in", "Rows written",
    ]

    def hist_row(row):
        v = row["version"]
        return [
            f"[{v}]({version_url(v)})",
            fmt_ts(row["timestamp"], seconds=True),
            f"`{row['operation']}`",
            render_user(row["userName"]),
            fmt_date(row.get("report_date")),
            source_file_cell(row.get("source_file")),
            fmt_int(row.get("source_rows_read")),
            fmt_int(row.get("rows_written")),
        ]

    if len(hist_full) == 0:
        recent_history_block = "No history."
    else:
        top_rows = [hist_row(r) for _, r in hist_full.head(recent_display).iterrows()]
        recent_table = render_table(RECENT_HEADERS, top_rows)

        if len(hist_full) > recent_display:
            all_rows = [hist_row(r) for _, r in hist_full.iterrows()]
            all_table = render_table(RECENT_HEADERS, all_rows)
            details = (
                f"<details>\n"
                f"<summary>All {len(hist_full)} versions</summary>\n\n"
                f"{all_table}\n\n"
                f"</details>"
            )
            recent_history_block = f"{recent_table}\n\n{details}"
        else:
            recent_history_block = recent_table

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------
    gen_time = fmt_ts(datetime.now(tz))
    footer = (
        f"---\n\n"
        f"<sub>`{table_fqn}` · generated {gen_time} by "
        f"[notebook]({notebook_url})</sub>"
    )

    # ------------------------------------------------------------------
    # Assemble & write
    # ------------------------------------------------------------------
    md = f"""# `{table}`

{summary_table}

{links_block}

## Schema

{layout_line}

{schema_table}

## Schema changes

{schema_changes_block}

## Recent history

{recent_history_block}

{footer}
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)

    return md
