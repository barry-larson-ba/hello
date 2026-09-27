# =============================================================
# tracking_helpers.R
# MLflow tracking helpers for Databricks notebooks.
#
# Usage:
#   source("/Workspace/Shared/lib/tracking_helpers.R")
#
# Depends on: mlflow, digest, SparkR (only for tag_delta_version)
# =============================================================

# Start a fresh MLflow run, ending any lingering active run first.
# Optional descriptive tags can be passed as named arguments.
#
# Example:
#   start_tracked_run(notebook = "daily_load", pipeline = "hr_reporting")
start_tracked_run <- function(notebook = NULL, pipeline = NULL, ...) {
  if (!is.null(mlflow_active_run())) mlflow_end_run()
  mlflow_start_run()

  if (!is.null(notebook)) mlflow_set_tag("notebook", notebook)
  if (!is.null(pipeline)) mlflow_set_tag("pipeline", pipeline)

  extras <- list(...)
  for (nm in names(extras)) {
    mlflow_set_tag(nm, as.character(extras[[nm]]))
  }
  invisible(NULL)
}


# Tag an input file's path, size, mtime, and (optionally) md5.
# Tags are namespaced by `prefix`, so you can log multiple inputs
# side by side (hr_path, hr_md5, roster_path, roster_md5, ...).
#
# Set checksum = FALSE for very large files to skip the md5 read.
#
# Example:
#   log_input_file(hr_path, "hr")
#   log_input_file(roster_path, "roster", checksum = FALSE)
log_input_file <- function(path, prefix, checksum = TRUE) {
  info <- file.info(path)
  if (is.na(info$size)) stop("File not found: ", path)

  mlflow_set_tag(paste0(prefix, "_path"),       path)
  mlflow_set_tag(paste0(prefix, "_size_bytes"), info$size)
  mlflow_set_tag(paste0(prefix, "_mtime"),      as.character(info$mtime))

  if (checksum) {
    mlflow_set_tag(
      paste0(prefix, "_md5"),
      digest::digest(file = path, algo = "md5")
    )
  }
  invisible(path)
}


# Log row count of a data frame, namespaced by `prefix` and `stage`.
# Returns the data frame unchanged so it can be used in a pipe:
#
#   hr_df |> log_rowcount("hr", "raw")
log_rowcount <- function(df, prefix, stage = "row_count") {
  n <- nrow(df)
  mlflow_log_metric(paste0(prefix, "_", stage), n)
  invisible(df)
}


# After a Delta write, tag the run with the version that was just
# produced. Returns the version number.
#
# Example:
#   tag_delta_version("main.silver.hr_daily")
tag_delta_version <- function(table_fqn) {
  hist <- SparkR::sql(paste0("DESCRIBE HISTORY ", table_fqn))
  v <- SparkR::collect(SparkR::limit(hist, 1L))$version

  mlflow_set_tag("target_table",  table_fqn)
  mlflow_set_tag("delta_version", as.character(v))
  invisible(v)
}


# Safe end of the current MLflow run. Idempotent.
end_tracked_run <- function() {
  if (!is.null(mlflow_active_run())) mlflow_end_run()
  invisible(NULL)
}
