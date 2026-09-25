# Tracking Notebook Inputs in Databricks

*A tutorial on making a scheduled notebook self-documenting, so that months later you can answer: "which source file produced this row?"*

---

## 1. Why bother

When a Databricks notebook runs on a schedule and lands data in a Delta table, three questions come up eventually:

1. **Which source file fed this specific version of the table?**
2. **Did anything about the input change between last run and this one?**
3. **When something looks wrong downstream, who ran what, when, with which parameters?**

Databricks captures *some* of this for you automatically, but there are gaps you need to fill by hand. The gaps are what this tutorial is about.

### What you get for free

| Layer | Captures | Notes |
|---|---|---|
| Job run metadata | Widget values, start/end time, user, cluster | Visible in the Jobs UI; queryable via `system.workflow.*` |
| `system.access.audit` | Every file access on a Volume, every table read/write | Governance-level audit trail |
| Unity Catalog lineage | Table-to-table dependencies from Spark plans | Only fires when the read goes through Spark |
| Delta history | Every commit to a Delta table, with row counts and user | `DESCRIBE HISTORY table_name` |

### What you have to log yourself

If you read a file with `haven::read_sas()`, `pandas.read_sas()`, `readr::read_csv()`, or any other non-Spark reader, UC lineage will *not* connect that file to the Delta table you eventually write. The read happens on the driver, outside the Spark plan.

**MLflow tags close that gap.** Every notebook run — scheduled or interactive — starts an MLflow run, tags it with the input file's identity, and (optionally) tags it with the Delta version it wrote. A small SQL view joins MLflow tags to Delta history, and you have a complete audit trail.

---

## 2. The pattern

Every notebook that reads a file and writes a Delta table follows this shape:

```
┌─────────────────────────────────────────────┐
│ 1. Setup (libraries, parameters)            │
│ 2. Start MLflow run                         │  ← new
│ 3. Log input file identity                  │  ← new
│ 4. Read the file                            │
│ 5. Log input row count                      │  ← new
│ 6. Transform                                │
│ 7. Write to Delta table                     │
│ 8. Log Delta version written                │  ← new
│ 9. Log output row count                     │  ← new
│ 10. End MLflow run                          │  ← new
└─────────────────────────────────────────────┘
```

Everything with "← new" is a one- to three-line addition. None of it changes your existing transformation logic.

### What gets logged

- **Params** — inputs that define what this run was *supposed* to do (e.g. the report date). Set once, never overwritten.
- **Tags** — descriptive metadata about the run (input file path, md5 checksum, target table, Delta version written). Overwritable.
- **Metrics** — numeric values that describe what happened (row counts, elapsed times). Timestamped.

MLflow adds its own auto-tags when running inside Databricks: `mlflow.databricks.jobID`, `mlflow.databricks.jobRunID`, `mlflow.databricks.notebookID`, `mlflow.user`, `mlflow.source.name`. These are your join keys back to `system.workflow.*`.

---

## 3. Template — R version

This is a full notebook skeleton. Placeholders in `{braces}` should be replaced with real values. Each cell heading corresponds to what you'd type in the notebook's cell title.

### Cell 1 — Restart Python (housekeeping)

```python
%python
dbutils.library.restartPython()
```

### Cell 2 — Load helper functions

```r
%r
# Load any project-specific R functions
source("/Workspace/Users/{your-user}/{project-path}/functions.R")
```

### Cell 3 — Load libraries

```r
%r
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(haven)      # read_sas
  library(lubridate)
  library(stringr)
  library(mlflow)     # ← input tracking
  library(digest)     # ← md5 checksum (optional)
})
```

### Cell 4 — Create parameter widget

```r
%r
dbutils.widgets.text("report_date_string", "", "Report Date (YYYY-MM-DD)")
```

### Cell 5 — Resolve report date

```r
%r
report_date_string <- dbutils.widgets.get("report_date_string")

if (!nzchar(trimws(report_date_string))) {
  # Default to today, or call your own date-resolution helper
  report_date_string <- as.character(Sys.Date())
}

# Validate however your project expects
report_date <- as.Date(report_date_string, format = "%Y-%m-%d")
stopifnot(!is.na(report_date))
print(report_date_string)
```

### Cell 6 — **Start MLflow run** *(new)*

```r
%r
# Guard against a leftover run from a previous execution in the same session
if (!is.null(mlflow_active_run())) mlflow_end_run()

mlflow_start_run()

# Params: the "what was I asked to do" of this run
mlflow_log_param("report_date", report_date_string)

# Free-form tags: descriptive metadata
mlflow_set_tag("notebook", "{notebook_name}")
mlflow_set_tag("pipeline", "{pipeline_or_project_name}")
```

### `## DATA EXTRACTION`

### Cell 7 — Read SAS file from Volume, with input tracking

```r
%r
# Build the source path
report_date_compact <- gsub("-", "", report_date_string)
path <- paste0(
  "/Volumes/{catalog}/{schema}/{volume}/{subdir}/",
  "{file_prefix}_", report_date_compact, ".sas7bdat"
)

# ---- Log input file identity BEFORE reading ----
info <- file.info(path)
mlflow_set_tag("input_path",       path)
mlflow_set_tag("input_size_bytes", info$size)
mlflow_set_tag("input_mtime",      as.character(info$mtime))
mlflow_set_tag("input_md5",        digest(file = path, algo = "md5"))
# Skip the md5 line on very large files — it re-reads the whole thing.
# -----------------------------------------------

raw_df <- read_sas(path)
mlflow_log_metric("input_row_count", nrow(raw_df))
```

### `## DATASET PREPARATION`

### Cell 8 — Normalize column names

```r
%r
names(raw_df) <- tolower(names(raw_df))
```

### Cell 9 — Transform (project-specific)

```r
%r
prepared_df <- raw_df |>
  dplyr::mutate(
    snap_dt        = floor_date(report_date, "month"),
    source_file_dt = report_date
    # ... your project's derived fields ...
  ) |>
  dplyr::select(
    snap_dt,
    source_file_dt
    # ... your final column list ...
  ) |>
  as.data.frame()
```

### Cell 10 — Strip SAS attributes (avoids Delta write issues)

```r
%r
# Removes lingering SAS labels/formats that can break the Delta write
attributes(prepared_df) <- attributes(data.frame(prepared_df))

# Log what came out of transformation
mlflow_log_metric("prepared_row_count", nrow(prepared_df))
```

### `## OUTPUT`

### Cell 11 — Hand off to Spark via a temp view

```r
%r
# SparkR is deprecated in DBR 16.0+; sparklyr is the modern choice.
# Kept here for parity with legacy notebooks:
prepared_spark <- SparkR::createDataFrame(prepared_df)
SparkR::createOrReplaceTempView(prepared_spark, "prepared_tmp")

# Sparklyr equivalent (preferred for new work):
# sc <- sparklyr::spark_connect(method = "databricks")
# sparklyr::copy_to(sc, prepared_df, "prepared_tmp", overwrite = TRUE)
```

### Cell 12 — Define destination

```python
%python
CATALOG = "{catalog}"
SCHEMA  = "{schema}"
TABLE   = "{table_name}"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
```

### Cell 13 — Write to Delta

```python
%python
prepared_df = spark.table("prepared_tmp")

# ... your project's write logic (append, merge, replace-partition, etc.) ...
prepared_df.write.mode("append").saveAsTable(TARGET_TABLE)

rows_written = prepared_df.count()
print(f"Wrote {rows_written} rows to {TARGET_TABLE}")
```

### Cell 14 — **Tag Delta version + close MLflow run** *(new)*

```r
%r
# Grab the version we just wrote
hist <- SparkR::sql(paste0("DESCRIBE HISTORY {catalog}.{schema}.{table_name}"))
latest_version <- SparkR::collect(SparkR::limit(hist, 1))$version

mlflow_set_tag("target_table",  "{catalog}.{schema}.{table_name}")
mlflow_set_tag("delta_version", as.character(latest_version))
mlflow_log_metric("output_row_count", nrow(prepared_df))

mlflow_end_run()
```

---

## 4. Template — Python version

Same shape, one language. Uses `pandas.read_sas` on the driver, then pushes to Spark.

### Cell 1 — Load libraries

```python
%python
import hashlib
import os
from datetime import date, datetime

import mlflow
import pandas as pd
from pyspark.sql import functions as F
```

### Cell 2 — Parameter widget

```python
%python
dbutils.widgets.text("report_date_string", "", "Report Date (YYYY-MM-DD)")
```

### Cell 3 — Resolve report date

```python
%python
report_date_string = dbutils.widgets.get("report_date_string").strip()

if not report_date_string:
    report_date_string = date.today().isoformat()

report_date = datetime.strptime(report_date_string, "%Y-%m-%d").date()
print(report_date_string)
```

### Cell 4 — **Start MLflow run** *(new)*

```python
%python
if mlflow.active_run():
    mlflow.end_run()

mlflow.start_run()

mlflow.log_param("report_date", report_date_string)
mlflow.set_tag("notebook", "{notebook_name}")
mlflow.set_tag("pipeline", "{pipeline_or_project_name}")
```

### `## DATA EXTRACTION`

### Cell 5 — Read SAS file from Volume, with input tracking

```python
%python
def file_md5(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Streaming md5 so we don't hold the whole file in memory."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

report_date_compact = report_date_string.replace("-", "")
path = (
    f"/Volumes/{{catalog}}/{{schema}}/{{volume}}/{{subdir}}/"
    f"{{file_prefix}}_{report_date_compact}.sas7bdat"
)

# ---- Log input file identity BEFORE reading ----
stat = os.stat(path)
mlflow.set_tag("input_path",       path)
mlflow.set_tag("input_size_bytes", stat.st_size)
mlflow.set_tag("input_mtime",      datetime.fromtimestamp(stat.st_mtime).isoformat())
mlflow.set_tag("input_md5",        file_md5(path))
# Skip the md5 line on very large files.
# -----------------------------------------------

raw_df = pd.read_sas(path)
mlflow.log_metric("input_row_count", len(raw_df))
```

### `## DATASET PREPARATION`

### Cell 6 — Transform

```python
%python
raw_df.columns = [c.lower() for c in raw_df.columns]

prepared_df = raw_df.assign(
    snap_dt        = pd.Timestamp(report_date).to_period("M").to_timestamp(),
    source_file_dt = pd.Timestamp(report_date),
    # ... your project's derived fields ...
)[[
    "snap_dt",
    "source_file_dt",
    # ... your final column list ...
]]

mlflow.log_metric("prepared_row_count", len(prepared_df))
```

### `## OUTPUT`

### Cell 7 — Write to Delta

```python
%python
CATALOG = "{catalog}"
SCHEMA  = "{schema}"
TABLE   = "{table_name}"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

prepared_spark = spark.createDataFrame(prepared_df)

# ... your project's write logic ...
prepared_spark.write.mode("append").saveAsTable(TARGET_TABLE)

rows_written = prepared_spark.count()
print(f"Wrote {rows_written} rows to {TARGET_TABLE}")
```

### Cell 8 — **Tag Delta version + close MLflow run** *(new)*

```python
%python
latest_version = (
    spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}")
         .select("version")
         .first()[0]
)

mlflow.set_tag("target_table",  TARGET_TABLE)
mlflow.set_tag("delta_version", str(latest_version))
mlflow.log_metric("output_row_count", rows_written)

mlflow.end_run()
```

---

## 5. The audit report

Now every run leaves a durable trail. Two cells produce a "which SAS file → which Delta version" audit table.

### Cell A — Materialize MLflow runs and Delta history as temp views

SQL cannot query MLflow directly or `DESCRIBE HISTORY` as a subquery — this Python cell bridges both into temp views.

```python
%python
import mlflow

# Databricks auto-creates one MLflow experiment per notebook, named for the
# notebook path. Adjust if your team uses a shared experiment.
EXPERIMENT_PATH = "/Workspace/Users/{your-user}/{project-path}/{notebook_name}"
TARGET_TABLE    = "{catalog}.{schema}.{table_name}"

exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
runs_pdf = mlflow.search_runs(experiment_ids=[exp.experiment_id])

# search_runs returns dotted column names ("params.report_date") that break SQL.
runs_pdf.columns = [c.replace(".", "_") for c in runs_pdf.columns]
spark.createDataFrame(runs_pdf).createOrReplaceTempView("mlflow_runs")

spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").createOrReplaceTempView("delta_history")
```

### Cell B — Join and display

```sql
%sql
SELECT
  h.version                                           AS delta_version,
  h.timestamp                                         AS commit_ts,
  h.userName                                          AS committed_by,
  h.operation,
  CAST(h.operationMetrics['numOutputRows'] AS BIGINT) AS rows_written,

  r.run_id                                            AS mlflow_run_id,
  r.params_report_date                                AS report_date,
  r.tags_input_path                                   AS source_file,
  r.tags_input_md5                                    AS source_md5,
  r.tags_input_mtime                                  AS source_mtime,
  CAST(r.metrics_input_row_count  AS BIGINT)          AS source_rows_read,
  CAST(r.metrics_output_row_count AS BIGINT)          AS rows_out,

  r.`tags_mlflow_databricks_jobRunID`                 AS job_run_id,
  r.`tags_mlflow_user`                                AS run_user

FROM delta_history h
LEFT JOIN mlflow_runs r
  ON CAST(r.tags_delta_version AS INT) = h.version

WHERE h.operation IN ('WRITE', 'MERGE', 'APPEND', 'CREATE OR REPLACE TABLE AS SELECT')
ORDER BY h.version DESC
```

### Interpreting the output

| Pattern | What it means |
|---|---|
| Same `source_md5` on two different `delta_version` rows | Same source file loaded twice — duplicate or intentional re-run |
| `source_rows_read` ≠ `rows_written` | Your transformation dropped or added rows — expected if you filter, worth eyeballing otherwise |
| Delta version with no matching MLflow row | Table was written outside this notebook (ad-hoc SQL, manual `RESTORE`, another job) |
| `source_mtime` older than expected | Upstream system didn't refresh the file before you ran |
| Blank `job_run_id` | Interactive execution, not a scheduled job |

### If you skipped the `delta_version` tag

You can still join, but on a timestamp window. Less precise:

```sql
LEFT JOIN mlflow_runs r
  ON h.timestamp BETWEEN r.start_time AND r.end_time
```

Manual `OPTIMIZE`/`VACUUM` operations get mis-attributed with this join. The explicit `delta_version` tag is worth the extra cell.

---

## 6. Ad-hoc lookups

Once MLflow is populated, you can answer questions from R or Python without the SQL view.

### R

```r
library(mlflow)

# All runs for a specific report date
mlflow_search_runs(filter = "params.report_date = '2026-01-12'")[
  , c("run_id", "tags.input_path", "tags.input_md5", "metrics.input_row_count")
]

# The specific run for a job execution
mlflow_search_runs(
  filter = "tags.`mlflow.databricks.jobRunID` = '123456789'"
)
```

### Python

```python
import mlflow

# All runs for a specific report date
mlflow.search_runs(
    filter_string="params.report_date = '2026-01-12'"
)[["run_id", "tags.input_path", "tags.input_md5", "metrics.input_row_count"]]

# The specific run for a job execution
mlflow.search_runs(
    filter_string="tags.`mlflow.databricks.jobRunID` = '123456789'"
)
```

---

## 7. Turning the audit into a standing report

Wrap the SQL from section 5 in a view:

```sql
%sql
CREATE OR REPLACE VIEW {catalog}.{schema}.{table_name}_load_audit AS
SELECT ...  -- the query from section 5
```

Two catches:

1. The view depends on the temp views `mlflow_runs` and `delta_history`, which don't persist across sessions. To make it truly standing, replace the temp views with materialized tables refreshed on a schedule (a small daily job that runs Cell A and writes to `{table_name}_mlflow_snapshot` and `{table_name}_delta_history_snapshot`).
2. Access to MLflow runs is workspace-scoped; users querying the view need permission on both the target table and the underlying MLflow experiment.

Once wrapped, the view is a natural source for a Databricks SQL dashboard or an alert ("notify me when a load happens with an md5 we've already seen").

---

## 8. What to leave off

Not every recommendation from a governance blog is worth the effort. Some things to *not* do unless you have a reason:

- **Don't log the full data as an MLflow artifact.** MLflow can attach files to runs, and it's tempting to attach the input SAS file itself. Volumes already hold the file. Duplicating it in MLflow artifact storage wastes space and creates two answers to "where is the source of truth."
- **Don't hash gigabyte files on every run.** The md5 tag is useful but the read is not free. Skip it, or move it to a nightly reconciliation job, when files get large.
- **Don't rely on MLflow for row-level lineage.** MLflow tracks run-level facts. If you need to trace an individual row back to its source record, that's a job for a lineage column you add to the Delta table itself (e.g. `source_file`, `source_row_num`), not for MLflow tags.

---

## 9. Summary — the minimum viable change

If you already have a working notebook and want to add tracking today, the minimum diff is:

1. `library(mlflow)` / `import mlflow` in your setup cell.
2. Six lines right after your parameter cell: end-any-active-run, start-run, log the report date param.
3. Four lines right before your file read: tag the path, size, mtime, md5.
4. One line right after your file read: log input row count.
5. Four lines right after your Delta write: read the new Delta version, tag it, log output row count, end the run.

Total: ~15 lines added, no existing logic touched. You gain a durable, queryable answer to "what fed this table on any given day" and a clean join key back to the job-run system tables.
