"""
tracking_helpers.py

MLflow tracking helpers for Databricks notebooks.

Usage:
    import sys
    LIB_DIR = "/Workspace/Shared/lib"
    if LIB_DIR not in sys.path:
        sys.path.append(LIB_DIR)
    from tracking_helpers import (
        start_tracked_run, log_input_file, log_rowcount,
        tag_delta_version, end_tracked_run,
    )

Depends on: mlflow (bundled with Databricks Runtime).
"""

import hashlib
import os
from datetime import datetime
from typing import Optional

import mlflow


def _file_md5(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Streaming md5 so we don't hold the whole file in memory."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def start_tracked_run(notebook: Optional[str] = None,
                      pipeline: Optional[str] = None,
                      **extra_tags) -> None:
    """
    Start a fresh MLflow run, ending any lingering active run first.
    Optional descriptive tags can be passed as keyword arguments.

    Example:
        start_tracked_run(notebook="daily_load", pipeline="hr_reporting")
    """
    if mlflow.active_run():
        mlflow.end_run()
    mlflow.start_run()

    if notebook:
        mlflow.set_tag("notebook", notebook)
    if pipeline:
        mlflow.set_tag("pipeline", pipeline)

    for k, v in extra_tags.items():
        mlflow.set_tag(k, str(v))


def log_input_file(path: str, prefix: str, checksum: bool = True) -> str:
    """
    Tag an input file's path, size, mtime, and (optionally) md5.
    Tags are namespaced by ``prefix``, so multiple inputs can be
    logged side by side (hr_path, hr_md5, roster_path, roster_md5, ...).

    Set ``checksum=False`` for very large files to skip the md5 read.

    Example:
        log_input_file(hr_path, "hr")
        log_input_file(roster_path, "roster", checksum=False)
    """
    stat = os.stat(path)  # raises FileNotFoundError if missing

    mlflow.set_tag(f"{prefix}_path",       path)
    mlflow.set_tag(f"{prefix}_size_bytes", stat.st_size)
    mlflow.set_tag(f"{prefix}_mtime",      datetime.fromtimestamp(stat.st_mtime).isoformat())

    if checksum:
        mlflow.set_tag(f"{prefix}_md5", _file_md5(path))

    return path


def log_rowcount(df, prefix: str, stage: str = "row_count") -> int:
    """
    Log row count of a DataFrame. Works with either pandas or Spark.
    Returns the count so it can be assigned:

        n = log_rowcount(prepared_df, "prepared")
    """
    # Spark DataFrames have .rdd; pandas doesn't.
    if hasattr(df, "rdd"):
        n = df.count()
    else:
        n = len(df)

    mlflow.log_metric(f"{prefix}_{stage}", n)
    return n


def tag_delta_version(table_fqn: str, spark) -> int:
    """
    After a Delta write, tag the run with the version that was just
    produced. Returns the version number.

    ``spark`` is the SparkSession (pass in the notebook's global ``spark``).

    Example:
        tag_delta_version("main.silver.hr_daily", spark)
    """
    v = (
        spark.sql(f"DESCRIBE HISTORY {table_fqn}")
             .select("version")
             .first()[0]
    )

    mlflow.set_tag("target_table",  table_fqn)
    mlflow.set_tag("delta_version", str(v))
    return v


def end_tracked_run() -> None:
    """Safe end of the current MLflow run. Idempotent."""
    if mlflow.active_run():
        mlflow.end_run()
