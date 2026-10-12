# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_lib_delta_helpers
#
# Shared logic for the Bronze -> Silver notebooks (see `../CLAUDE.md`, section 3).
#
# * Fabric: import this file as a notebook named `nb_lib_delta_helpers` and pull it into the ETL
#   notebooks with `%run nb_lib_delta_helpers`.
# * Local / tests: it is plain Python, so `import nb_lib_delta_helpers` works as well.
#
# The helpers contain no connection strings, no hardcoded plants/tables and no I/O side
# effects beyond the Spark session they are given.

# CELL ********************

import json
import re
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------

def validate_identifier(value: str, name: str) -> str:
    """Reject anything that is not a plain identifier.

    Parameters arrive from pipelines / the Control Layer and are interpolated into table
    names and predicates, so they must not be able to carry SQL.
    """
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Parameter {name}={value!r} is not a valid identifier")
    return value


def validate_table_name(value: str, name: str = "p_target_table") -> str:
    """Target table as `table`, `lakehouse.table` or `lakehouse.schema.table`."""
    parts = value.split(".") if isinstance(value, str) else []
    if not 1 <= len(parts) <= 3:
        raise ValueError(f"Parameter {name}={value!r} must be table, lakehouse.table or lakehouse.schema.table")
    for part in parts:
        validate_identifier(part, name)
    return value


def validate_source_path(value: str, name: str = "p_source_path") -> str:
    """The full source path is handed to Spark as is; it only has to be present."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Parameter {name} is required (full source path, globs allowed)")
    return value.strip()


def to_bool(value) -> bool:
    """Pipeline parameters can arrive as strings ("true"/"False"); normalise them."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def sql_literal(value) -> str:
    """Render a Python value as a Spark SQL literal for predicates."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def add_time_partitions(df: DataFrame, time_column: str = "Time") -> DataFrame:
    """Derive `Year`, `Month`, `Date` strictly from the event time column.

    `Date` is a DATE (yyyy-MM-dd). The string-vs-integer-day choice is still an open topic
    (CLAUDE.md section 6.1); changing it only requires changing this function.
    """
    if time_column not in df.columns:
        raise ValueError(f"Time column {time_column!r} not found in source; columns: {df.columns}")
    ts = F.col(time_column).cast("timestamp")
    return (
        df.withColumn(time_column, ts)
          .withColumn("Year", F.year(ts))
          .withColumn("Month", F.month(ts))
          .withColumn("Date", F.to_date(ts))
    )


def telemetry_partition_columns(plant_column: str):
    """Silver telemetry tables are partitioned by plant, then Year / Month / Date (CLAUDE.md 3.1 / 3.2)."""
    return [plant_column, "Year", "Month", "Date"]


def require_columns(df: DataFrame, columns, context: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required column(s) {missing}; available: {df.columns}")


# ---------------------------------------------------------------------------
# Delta operations
# ---------------------------------------------------------------------------

def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    return spark.catalog.tableExists(full_table_name)


def write_partitioned_slice(
    df: DataFrame,
    full_table_name: str,
    partition_columns,
    replace_predicate: str = None,
) -> None:
    """Append `df` to a partitioned Delta table.

    With `replace_predicate`, the write atomically replaces exactly the rows matching the
    predicate (Delta `replaceWhere`) instead of blindly appending. The rest of the table is
    untouched, so it still behaves as an append for every other slice, but a
    re-run of the same slice no longer duplicates rows.
    """
    spark = df.sparkSession
    writer = df.write.format("delta").option("mergeSchema", "true")

    if not table_exists(spark, full_table_name):
        writer.mode("append").partitionBy(*partition_columns).saveAsTable(full_table_name)
        return

    if replace_predicate:
        writer.mode("overwrite").option("replaceWhere", replace_predicate).saveAsTable(full_table_name)
    else:
        writer.mode("append").saveAsTable(full_table_name)


def zorder_optimize(spark: SparkSession, full_table_name: str, where: str, zorder_columns) -> None:
    """Run `OPTIMIZE <table> WHERE <where> ZORDER BY (<cols>)`.

    Delta only accepts partition columns in the OPTIMIZE WHERE clause.
    """
    cols = ", ".join(validate_identifier(c, "zorder_column") for c in zorder_columns)
    sql = f"OPTIMIZE {full_table_name}"
    if where:
        sql += f" WHERE {where}"
    sql += f" ZORDER BY ({cols})"
    print(f"[nb_lib_delta_helpers] {sql}")
    spark.sql(sql)


def merge_scd1(
    spark: SparkSession,
    source_df: DataFrame,
    full_table_name: str,
    key_columns,
) -> dict:
    """SCD Type 1: `MERGE ... WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *`.

    Creates the table from the snapshot on first run. Returns the Delta operation metrics.
    """
    from delta.tables import DeltaTable

    if not table_exists(spark, full_table_name):
        source_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        return {"operation": "CREATE", "numTargetRowsInserted": source_df.count()}

    # Allow new snapshot columns to flow into the target (UPDATE SET * / INSERT *).
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    condition = " AND ".join(f"target.`{c}` <=> source.`{c}`" for c in key_columns)
    (
        DeltaTable.forName(spark, full_table_name).alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = DeltaTable.forName(spark, full_table_name).history(1).collect()[0]
    return {"operation": last["operation"], **(last["operationMetrics"] or {})}


# ---------------------------------------------------------------------------
# Pipeline integration
# ---------------------------------------------------------------------------

def notebook_exit(result: dict) -> None:
    """Return a JSON result to the calling Fabric pipeline; print it when run standalone."""
    payload = json.dumps(result, default=str)
    print(payload)
    try:
        notebookutils.notebook.exit(payload)  # noqa: F821 - provided by the Fabric runtime
    except NameError:
        pass
