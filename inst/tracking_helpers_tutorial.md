# MLflow Tracking Helpers: Tutorial

*A companion to the input-tracking tutorial. Five small helpers that hide the repetitive parts of the pattern.*

---

## 1. What these solve

The base tracking pattern works, but every notebook that uses it ends up with the same six-line block for the file read, the same four-line block for the Delta version tag, and the same start/end run boilerplate. Multiply by ten notebooks and any typo drift means your audit query starts missing rows because someone tagged `input_md5` in one place and `md5_input` in another.

The helpers do two things:

1. **Enforce a naming convention** so every notebook produces the same tag names, which keeps the audit query in Section 5 of the base tutorial working across all of them.
2. **Cut ~15 lines of boilerplate down to ~5**, without hiding what's happening.

They're deliberately thin. Not a framework — just stubs.

---

## 2. The five helpers at a glance

| Helper | Purpose | Where it goes in the notebook |
|---|---|---|
| `start_tracked_run()` | End any lingering run, start a new one, tag it with notebook/pipeline info | Right after your parameter cell |
| `log_input_file()` | Tag one file's path, size, mtime, and md5 with a chosen prefix | Right before each file read |
| `log_rowcount()` | Log row count with consistent naming | After each read and after each major transform |
| `tag_delta_version()` | Read the Delta version just produced and tag the run with it | Right after your Delta write |
| `end_tracked_run()` | Safely end the current run | Very last cell |

---

## 3. Where to keep the helpers

Both files go in a shared workspace location so every notebook loads the same version:

```
/Workspace/Shared/lib/tracking_helpers.R
/Workspace/Shared/lib/tracking_helpers.py
```

You can pick a different path — under your user folder while prototyping, then move to `Shared` once your team agrees on it.

### R — load at the top of the notebook

```r
%r
source("/Workspace/Shared/lib/tracking_helpers.R")
```

### Python — same idea, via `sys.path`

```python
%python
import sys
LIB_DIR = "/Workspace/Shared/lib"
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

from tracking_helpers import (
    start_tracked_run, log_input_file, log_rowcount,
    tag_delta_version, end_tracked_run,
)
```

This is the same pattern you already use for `functions.py` — it's just pointing at a shared library folder instead of the notebook's own folder.

---

## 4. The R helpers

```r
# start a run, ending any leftover one
start_tracked_run <- function(notebook = NULL, pipeline = NULL, ...) {
  if (!is.null(mlflow_active_run())) mlflow_end_run()
  mlflow_start_run()
  if (!is.null(notebook)) mlflow_set_tag("notebook", notebook)
  if (!is.null(pipeline)) mlflow_set_tag("pipeline", pipeline)
  extras <- list(...)
  for (nm in names(extras)) mlflow_set_tag(nm, as.character(extras[[nm]]))
  invisible(NULL)
}

# tag a file's path + metadata under a namespaced prefix
log_input_file <- function(path, prefix, checksum = TRUE) {
  info <- file.info(path)
  if (is.na(info$size)) stop("File not found: ", path)
  mlflow_set_tag(paste0(prefix, "_path"),       path)
  mlflow_set_tag(paste0(prefix, "_size_bytes"), info$size)
  mlflow_set_tag(paste0(prefix, "_mtime"),      as.character(info$mtime))
  if (checksum) {
    mlflow_set_tag(paste0(prefix, "_md5"), digest::digest(file = path, algo = "md5"))
  }
  invisible(path)
}

# pipe-friendly row-count logger
log_rowcount <- function(df, prefix, stage = "row_count") {
  mlflow_log_metric(paste0(prefix, "_", stage), nrow(df))
  invisible(df)
}

# after write: read + tag the Delta version produced
tag_delta_version <- function(table_fqn) {
  hist <- SparkR::sql(paste0("DESCRIBE HISTORY ", table_fqn))
  v <- SparkR::collect(SparkR::limit(hist, 1L))$version
  mlflow_set_tag("target_table",  table_fqn)
  mlflow_set_tag("delta_version", as.character(v))
  invisible(v)
}

end_tracked_run <- function() {
  if (!is.null(mlflow_active_run())) mlflow_end_run()
  invisible(NULL)
}
```

**Why each is written the way it is:**

- `start_tracked_run(...)` uses `...` for extra tags so the caller can pass anything without editing the helper: `start_tracked_run(notebook = "x", pipeline = "y", team = "credentialing")`.
- `log_input_file()` stops with a clear error if the file isn't there. Better to fail loudly at the tagging step than to fail cryptically inside `read_sas()`.
- `log_rowcount()` returns the data frame `invisible`ly so you can pipe through it: `df |> log_rowcount("hr", "raw")`. It doesn't disturb your assignment on the left.
- `tag_delta_version()` uses `SparkR` because that matches what's already in most existing notebooks. If you migrate to `sparklyr`, swap two lines and the interface stays identical.

---

## 5. The Python helpers

```python
import hashlib, os
from datetime import datetime
from typing import Optional
import mlflow


def _file_md5(path, chunk_size=8 * 1024 * 1024):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def start_tracked_run(notebook=None, pipeline=None, **extra_tags):
    if mlflow.active_run():
        mlflow.end_run()
    mlflow.start_run()
    if notebook: mlflow.set_tag("notebook", notebook)
    if pipeline: mlflow.set_tag("pipeline", pipeline)
    for k, v in extra_tags.items():
        mlflow.set_tag(k, str(v))


def log_input_file(path, prefix, checksum=True):
    stat = os.stat(path)
    mlflow.set_tag(f"{prefix}_path",       path)
    mlflow.set_tag(f"{prefix}_size_bytes", stat.st_size)
    mlflow.set_tag(f"{prefix}_mtime",      datetime.fromtimestamp(stat.st_mtime).isoformat())
    if checksum:
        mlflow.set_tag(f"{prefix}_md5", _file_md5(path))
    return path


def log_rowcount(df, prefix, stage="row_count"):
    n = df.count() if hasattr(df, "rdd") else len(df)
    mlflow.log_metric(f"{prefix}_{stage}", n)
    return n


def tag_delta_version(table_fqn, spark):
    v = spark.sql(f"DESCRIBE HISTORY {table_fqn}").select("version").first()[0]
    mlflow.set_tag("target_table",  table_fqn)
    mlflow.set_tag("delta_version", str(v))
    return v


def end_tracked_run():
    if mlflow.active_run():
        mlflow.end_run()
```

**Notes on the Python side:**

- `_file_md5()` reads in 8 MB chunks so a 4 GB SAS file doesn't blow up memory.
- `log_rowcount()` duck-types the DataFrame: Spark DataFrames have an `.rdd` attribute; pandas DataFrames don't. Same function works for both.
- `tag_delta_version()` takes `spark` as an argument because in a `.py` module you don't automatically have the notebook's global `spark` session. In a notebook, always pass `spark` directly.

---

## 6. Before and after — one input

Here's the input-reading block from the base tutorial, rewritten with the helpers.

### R — before (10 lines of tracking around 2 lines of work)

```r
%r
path <- paste0("/Volumes/.../hr_", date_compact, ".sas7bdat")

info <- file.info(path)
mlflow_set_tag("input_path",       path)
mlflow_set_tag("input_size_bytes", info$size)
mlflow_set_tag("input_mtime",      as.character(info$mtime))
mlflow_set_tag("input_md5",        digest(file = path, algo = "md5"))

raw_df <- read_sas(path)
mlflow_log_metric("input_row_count", nrow(raw_df))
```

### R — after

```r
%r
path <- paste0("/Volumes/.../hr_", date_compact, ".sas7bdat")

log_input_file(path, "hr")
raw_df <- read_sas(path)
log_rowcount(raw_df, "hr", "raw")
```

### Python — before

```python
%python
path = f"/Volumes/.../hr_{date_compact}.sas7bdat"

stat = os.stat(path)
mlflow.set_tag("input_path",       path)
mlflow.set_tag("input_size_bytes", stat.st_size)
mlflow.set_tag("input_mtime",      datetime.fromtimestamp(stat.st_mtime).isoformat())
mlflow.set_tag("input_md5",        file_md5(path))

raw_df = pd.read_sas(path)
mlflow.log_metric("input_row_count", len(raw_df))
```

### Python — after

```python
%python
path = f"/Volumes/.../hr_{date_compact}.sas7bdat"

log_input_file(path, "hr")
raw_df = pd.read_sas(path)
log_rowcount(raw_df, "hr", "raw")
```

Fewer lines, no chance of a typo in a tag name, and consistent naming across every notebook that uses the helpers.

---

## 7. A full notebook skeleton, using the helpers

### R

```r
%r
source("/Workspace/Shared/lib/tracking_helpers.R")
suppressPackageStartupMessages({
  library(dplyr); library(haven); library(lubridate)
  library(mlflow); library(digest)
})
```

```r
%r
dbutils.widgets.text("report_date_string", "", "Report Date")
report_date_string <- dbutils.widgets.get("report_date_string")
if (!nzchar(trimws(report_date_string))) report_date_string <- as.character(Sys.Date())
report_date <- as.Date(report_date_string)
```

```r
%r
start_tracked_run(
  notebook = "{notebook_name}",
  pipeline = "{pipeline_name}"
)
mlflow_log_param("report_date", report_date_string)
```

```r
%r
# ---- extract ----
date_compact <- gsub("-", "", report_date_string)
hr_path <- paste0("/Volumes/{catalog}/{schema}/{volume}/hr/hr_", date_compact, ".sas7bdat")

log_input_file(hr_path, "hr")
hr_df <- read_sas(hr_path)
log_rowcount(hr_df, "hr", "raw")

# ---- prepare ----
prepared_df <- hr_df |>
  # ... your dplyr chain ...
  as.data.frame()

log_rowcount(prepared_df, "hr", "prepared")
```

```r
%r
# ---- hand off to Spark ----
prepared_spark <- SparkR::createDataFrame(prepared_df)
SparkR::createOrReplaceTempView(prepared_spark, "prepared_tmp")
```

```python
%python
TARGET_TABLE = "{catalog}.{schema}.{table_name}"
spark.table("prepared_tmp").write.mode("append").saveAsTable(TARGET_TABLE)
```

```r
%r
# ---- close out ----
tag_delta_version("{catalog}.{schema}.{table_name}")
end_tracked_run()
```

### Python (parallel structure)

```python
%python
import sys
LIB_DIR = "/Workspace/Shared/lib"
if LIB_DIR not in sys.path: sys.path.append(LIB_DIR)

from tracking_helpers import (
    start_tracked_run, log_input_file, log_rowcount,
    tag_delta_version, end_tracked_run,
)

import mlflow
import pandas as pd
from datetime import date, datetime
```

```python
%python
dbutils.widgets.text("report_date_string", "", "Report Date")
report_date_string = dbutils.widgets.get("report_date_string").strip() or date.today().isoformat()
report_date = datetime.strptime(report_date_string, "%Y-%m-%d").date()
```

```python
%python
start_tracked_run(notebook="{notebook_name}", pipeline="{pipeline_name}")
mlflow.log_param("report_date", report_date_string)
```

```python
%python
date_compact = report_date_string.replace("-", "")
hr_path = f"/Volumes/{{catalog}}/{{schema}}/{{volume}}/hr/hr_{date_compact}.sas7bdat"

log_input_file(hr_path, "hr")
hr_df = pd.read_sas(hr_path)
log_rowcount(hr_df, "hr", "raw")

# ... transform ...
prepared_df = hr_df.assign(...)[[...]]
log_rowcount(prepared_df, "hr", "prepared")
```

```python
%python
TARGET_TABLE = "{catalog}.{schema}.{table_name}"
spark.createDataFrame(prepared_df).write.mode("append").saveAsTable(TARGET_TABLE)
```

```python
%python
tag_delta_version(TARGET_TABLE, spark)
end_tracked_run()
```

---

## 8. Two-input example

Just call `log_input_file()` twice, with different prefixes. Everything downstream stays identical.

### R

```r
%r
log_input_file(hr_path,     "hr")
log_input_file(roster_path, "roster")

hr_df     <- read_sas(hr_path);            log_rowcount(hr_df,     "hr",     "raw")
roster_df <- readr::read_csv(roster_path); log_rowcount(roster_df, "roster", "raw")
```

### Python

```python
%python
log_input_file(hr_path,     "hr")
log_input_file(roster_path, "roster")

hr_df     = pd.read_sas(hr_path);       log_rowcount(hr_df,     "hr",     "raw")
roster_df = pd.read_csv(roster_path);   log_rowcount(roster_df, "roster", "raw")
```

Your MLflow run now carries `tags_hr_*` and `tags_roster_*` side by side. The audit query from Section 5 of the base tutorial just gains columns — no structural change.

---

## 9. When to extend, when to leave alone

**Extend when** you find yourself writing the same three lines in three notebooks. That's the threshold. Common candidates you might add:

- A `log_widget_params()` that grabs all `dbutils.widgets.getAll()` values and logs them as MLflow params in one call.
- A `log_transform_step()` that records both a name and a row count, so you get a mini flow diagram in metrics.
- A `write_delta_and_tag()` wrapper that combines the write + `tag_delta_version()` call so you can never forget the tag.

**Don't extend when** the helper would need to know something specific to one notebook (a particular column name, a particular file-naming convention). Those belong in the notebook or in a project-specific `functions.R`, not in the shared tracking library. The shared library should stay small enough that any of your teammates can read it end to end in under a minute.

**Don't extend when** you're tempted to wrap a data-reading library. `haven::read_sas()`, `readr::read_csv()`, `pandas.read_sas()` are already thin. A `read_and_track_sas(path, prefix)` helper saves one line but forces every caller to accept your choice of reader. Keep the tagging and the reading separate — you'll thank yourself when someone wants to swap in `arrow` or a Spark reader.

---

## 10. Summary

The base pattern gives you inputs → outputs traceability. The helpers give you the same thing with less typing and more consistency, at the cost of a small shared file every notebook has to source. For any team maintaining more than one tracked notebook, the trade is worth it.

Once the helpers are in place, adding tracking to a new notebook is roughly:

1. `source()` or `import` the helpers.
2. `start_tracked_run(...)` after your parameter cell.
3. `log_input_file()` + `log_rowcount()` for each read.
4. `tag_delta_version()` after each Delta write.
5. `end_tracked_run()` at the bottom.

The audit query from the base tutorial keeps working, no changes needed.
