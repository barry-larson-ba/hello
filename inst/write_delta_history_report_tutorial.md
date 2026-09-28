# `write_delta_history_report` — Tutorial

A reusable helper that produces a GitHub-flavored Markdown report documenting a Delta table's schema, size, schema-change log, and recent write history. When paired with MLflow tracking, each row of the history table also carries the report date, source file, and row counts of the run that produced it.

Use it after your write cells to generate a durable, human-readable audit trail that lives alongside the notebook.

---

## What the report contains

Every `.md` file the function writes has the same structure:

1. **H1 with the table name** (e.g. `` `orders_history` ``)
2. **Summary card** — a one-cell table showing the timestamp of the last write
3. **Links block** — clickable links to the notebook, the Unity Catalog table page, its version history, and the cadence label
4. **Schema** — partition columns, file count, on-disk size, followed by a `Column | Type | Comment` table pulled straight from `DESCRIBE TABLE`
5. **Schema changes** — a warning callout and a table listing any schema-changing operations in the last N versions (`ADD COLUMNS`, `RENAME COLUMN`, etc.); a plain "No schema-changing operations" line when there are none
6. **Recent history** — the top N versions uncollapsed, then all N inside a `<details>` fold. When MLflow is wired in, each row also shows the report date, source file, source row count, and rows written
7. **Footer** — the fully qualified table name, generation time, and a link back to the notebook

The report is designed to be **read in the Databricks workspace file viewer** (which renders `.md` natively) or checked into a Git folder.

---

## Prerequisites

### Where the function lives

Save `write_delta_history_report.py` somewhere importable. Two common patterns:

**Option A — a `%run`-able notebook.** Paste the function into a notebook at `/Workspace/Shared/lib/write_delta_history_report`, then in any consuming notebook:

```python
%run /Workspace/Shared/lib/write_delta_history_report
```

**Option B — a proper Python module.** Save it to `/Workspace/Shared/lib/write_delta_history_report.py`, then:

```python
import sys
LIB_DIR = "/Workspace/Shared/lib"
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

from write_delta_history_report import write_delta_history_report
```

Option B is preferred for anything that will be tested or packaged. Option A is fine for one-off notebooks.

### For the MLflow-enrichment path

Your write notebook must already do these two things (covered in the earlier tracking discussion):

1. **Start an MLflow run** and log the inputs and outputs you care about (report date, source file path, row counts).
2. **Tag the run with the Delta version it produced**, immediately after the write:

   ```python
   %r
   hist <- SparkR::sql("DESCRIBE HISTORY main.silver.orders_history")
   latest_version <- SparkR::collect(SparkR::limit(hist, 1))$version
   mlflow_set_tag("delta_version", as.character(latest_version))
   ```

Without the `delta_version` tag, the function has nothing to join on and the MLflow columns will render empty for every row.

---

## Quick start

### Delta only (no MLflow)

The simplest possible call, useful for tables that don't have an associated MLflow experiment:

```python
write_delta_history_report(
    spark,
    dbutils,
    table_fqn="your.silver.your_table",
    output_path="/Workspace/Shared/reports/your_table_history.md",
    cadence_label="Daily",
)
```

The MLflow columns (Report date, Source file, Rows in) will render as empty cells. Version, Timestamp, Operation, User, and Rows written come from `DESCRIBE HISTORY` alone.

### With MLflow enrichment

```python
import mlflow

# 1. Pull every MLflow run for this notebook's experiment.
#    Databricks auto-assigns experiment name = notebook path.
EXPERIMENT_PATH = (
    "/Workspace/Users/first.last@example.com/"
    "reports/extracts/orders_history/orders_history"
)
exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
runs_pdf = mlflow.search_runs(experiment_ids=[exp.experiment_id])

# 2. Rename dotted columns to underscore form (SQL/pandas-friendly)
runs_pdf.columns = [c.replace(".", "_") for c in runs_pdf.columns]

# 3. Render the report
write_delta_history_report(
    spark,
    dbutils,
    table_fqn="main.silver.orders_history",
    output_path=(
        "/Workspace/Users/first.last@example.com/"
        "reports/extracts/orders_history/orders_history.md"
    ),
    cadence_label="Quarterly",
    mlflow_runs_df=runs_pdf,
)
```

The returned string is also the report contents, so you can `print()` it in the notebook to double-check without opening the file.

---

## Parameter reference

| Parameter | Type | Default | Purpose |
| --- | --- | --- | --- |
| `spark` | SparkSession | — | Active Spark session. Pass in from the notebook. |
| `dbutils` | DBUtils | — | Databricks utilities handle. Used to build notebook/table URLs. |
| `table_fqn` | str | — | Fully qualified table name, `catalog.schema.table`. |
| `output_path` | str | — | Absolute path to write the `.md` file. |
| `cadence_label` | str | — | Human-readable release cadence, shown in the links block. |
| `mlflow_runs_df` | DataFrame | `None` | Pandas DataFrame from `mlflow.search_runs()` (with dots→underscores). Enables the enriched columns. |
| `mlflow_delta_version_col` | str | `"tags_delta_version"` | Column in `mlflow_runs_df` that holds the Delta version tagged by each run. |
| `timezone_name` | str | `"America/Los_Angeles"` | IANA timezone for all displayed timestamps. |
| `tz_label` | str | `"PT"` | Short label appended to timestamps. |
| `history_limit` | int | `25` | Number of recent Delta versions to scan for schema changes and list in the collapsed history. |
| `recent_display` | int | `5` | Number of versions shown uncollapsed at the top of Recent history. |
| `redact_users` | bool | `False` | Masks user emails: `first.last@example.com` → `f***@example.com`. |
| `redact_paths` | bool | `False` | Renders source-file cells as plain basenames with no hyperlink and no volume path anywhere in the file. |

---

## Recipes

### Change the timezone

For an Eastern-time team:

```python
write_delta_history_report(
    spark, dbutils,
    table_fqn="...",
    output_path="...",
    cadence_label="Hourly",
    timezone_name="America/New_York",
    tz_label="ET",
)
```

### Show more versions uncollapsed

For a low-frequency table where you want to see the last 10 writes at a glance:

```python
write_delta_history_report(
    spark, dbutils,
    table_fqn="...",
    output_path="...",
    cadence_label="Monthly",
    recent_display=10,
)
```

### Look further back for schema changes

Default is 25 versions. For a stable table where you want to catch schema drift over a longer window:

```python
write_delta_history_report(
    spark, dbutils,
    table_fqn="...",
    output_path="...",
    cadence_label="Weekly",
    history_limit=100,
    recent_display=10,
)
```

Note: `recent_display` should always be `<= history_limit`.

### Generate a redacted version for external sharing

For a compliance package or vendor deliverable:

```python
write_delta_history_report(
    spark, dbutils,
    table_fqn="main.silver.orders_history",
    output_path=".../orders_history_redacted.md",
    cadence_label="Quarterly",
    mlflow_runs_df=runs_pdf,
    redact_users=True,
    redact_paths=True,
)
```

This produces a parallel report with emails masked and volume paths reduced to basenames — safe(r) for wider distribution. Column names from `DESCRIBE TABLE` are **not** redacted; if the schema itself is sensitive, generate the report on a view that omits those columns.

### Regenerate after every write

Add this as the last cell of your write notebook so the report always reflects the latest state:

```python
# Cell N — Regenerate history report
write_delta_history_report(
    spark, dbutils,
    table_fqn=TARGET_TABLE,
    output_path=REPORT_PATH,
    cadence_label=CADENCE,
    mlflow_runs_df=runs_pdf,
)
print(f"Wrote history report to {REPORT_PATH}")
```

---

## Sample output walkthrough

Here's how a real row from the Recent history section reads, using the enriched columns:

| Version | Timestamp | Operation | User | Report date | Source file | Rows in | Rows written |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [26](…&version=26) | 2026-09-28 10:28:05 PT | `WRITE` | first.last@example.com | 2026-01-12 | source_extract_20260112.sas7bdat | 1,247,893 | 1,247,893 |
| [25](…&version=25) | 2026-09-28 10:12:46 PT | `WRITE` | first.last@example.com | 2026-01-12 | source_extract_20260112.sas7bdat | 1,247,893 | 1,247,893 |
| [22](…&version=22) | 2026-09-28 08:45:56 PT | `WRITE` | first.last@example.com |   |   |   | 1,247,893 |

What this tells you:

- **v26 and v25 wrote the same file on the same report date with the same row count.** That's the re-run pattern — same source, run twice. Fine, or a duplicate load; you decide.
- **v22 has `Rows written` but empty MLflow columns.** That version was written outside the tracked notebook (a repair, a `RESTORE`, an ad-hoc SQL statement). If you see this a lot, someone's touching the table without going through the standard pipeline.
- **`Rows in` and `Rows written` are equal** for these WRITE ops. When they diverge, the transformation dropped or added rows — expected for `MERGE`, worth an eyeball for straight `WRITE`.

---

## Handling sensitive information

The report is metadata about the table, not the data itself, but it does include some identifiers by default. Here's what lands in the file and how to control it:

| Item | Default | How to remove |
| --- | --- | --- |
| User emails in history | Shown | Pass `redact_users=True` |
| Full `/Volumes/...` paths (embedded in source-file link) | Shown | Pass `redact_paths=True` |
| Workspace host in URLs | Shown | Don't call the function on a workspace whose URL is sensitive; there's no toggle |
| Notebook path (may contain user email) | Shown in the "Notebook" link | Same as above — reorganize the notebook to a shared path if needed |
| Column names from `DESCRIBE TABLE` | Shown | Generate the report against a view that omits sensitive columns |
| Row counts | Shown | No toggle; drop the enriched columns by passing `mlflow_runs_df=None` if aggregate counts are sensitive |

By default the report is written to a Databricks Workspace path, which is private to you and workspace admins. It becomes shareable only when you explicitly copy it out, publish it, or check it into a public Git folder. Treat the `output_path` as the security boundary.

---

## Troubleshooting

**All MLflow columns are empty in every row.**
Your MLflow runs aren't tagged with `delta_version`, or the tag column has a different name. Confirm by running:

```python
runs_pdf["tags_delta_version"].dropna().head()
```

If that column is missing, check your write cell — it should call `mlflow_set_tag("delta_version", ...)` right after the Delta write. If the tag lives under a different name in your runs, pass it explicitly:

```python
write_delta_history_report(
    ...,
    mlflow_runs_df=runs_pdf,
    mlflow_delta_version_col="tags_my_custom_version_tag",
)
```

**`KeyError` on `tags_delta_version`.**
Same root cause, but louder — the function raises when the join column is entirely missing. Fix the write-side tagging.

**MLflow columns are populated for old versions but empty for new ones.**
The write cell isn't tagging every run. Check that `mlflow_set_tag("delta_version", ...)` runs unconditionally, not inside an `if` that's sometimes false.

**Duplicate rows in the report.**
Multiple MLflow runs tagged the same `delta_version`. The function calls `drop_duplicates("_delta_version", keep="last")`, so the most recent duplicate wins — but you'll want to figure out why two runs claim the same version.

**Report generation is slow.**
`DESCRIBE HISTORY` is the usual culprit on very large tables. Lower `history_limit`; you rarely need more than the last 25–50 versions in a human-readable report. `mlflow.search_runs` can also be slow for old experiments — filter the search to recent runs when the experiment has years of history:

```python
runs_pdf = mlflow.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string="attributes.start_time > 1704067200000",  # unix ms; adjust
)
```

**Timestamps look wrong.**
`DESCRIBE HISTORY` returns UTC timestamps. The function converts to `timezone_name`. If the times look off by a fixed offset, you've passed the wrong IANA name. `America/Los_Angeles` handles DST automatically; don't use `PST` (which is a fixed offset).

**Source file cell shows the basename but isn't linked.**
The path in `source_file` doesn't start with `/Volumes/`. Legacy DBFS paths (`dbfs:/mnt/...`), S3 URIs (`s3://...`), and workspace paths don't have a Catalog Explorer equivalent, so the function falls back to plain text. Migrate the source to a Unity Catalog volume to get the link back.

**The report never appears in the workspace file browser.**
Databricks Workspace file browser doesn't auto-refresh. Click the refresh icon on the folder, or navigate away and back.
