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

def add_time_columns(df: DataFrame, time_column: str = "Time") -> DataFrame:
    """Derive `Year`, `Month`, `Date` strictly from the event time column (`Date` is a clustering key).

    `Date` is a DATE (yyyy-MM-dd). The string-vs-integer-day choice is still an open topic
    (CLAUDE.md section 6.1); changing it only requires changing this function.
    """
    (time_column,) = require_columns(df, [time_column], "time column")
    ts = F.col(time_column).cast("timestamp")
    return (
        df.withColumn(time_column, ts)
          .withColumn("Year", F.year(ts))
          .withColumn("Month", F.month(ts))
          .withColumn("Date", F.to_date(ts))
    )


def telemetry_cluster_columns(plant_column: str, dept_column: str, id_column: str):
    """Silver telemetry tables are liquid-clustered by day, plant, department and tag (CLAUDE.md 7.6)."""
    return ["Date", plant_column, dept_column, id_column]


def require_columns(df: DataFrame, columns, context: str) -> list:
    """Check the columns exist, ignoring case like Spark does, and return their actual names.

    Callers use the returned names, so a parameter `TagId` works against a source `TagID`.
    """
    actual = {c.lower(): c for c in df.columns}
    missing = [c for c in columns if c.lower() not in actual]
    if missing:
        raise ValueError(f"{context}: missing required column(s) {missing}; available: {df.columns}")
    return [actual[c.lower()] for c in columns]


# ---------------------------------------------------------------------------
# Delta operations
# ---------------------------------------------------------------------------

def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    return spark.catalog.tableExists(full_table_name)


def ensure_clustered_table(spark: SparkSession, df: DataFrame, full_table_name: str, cluster_columns) -> None:
    """Create the target as a liquid-clustered Delta table from `df`'s schema if it is missing.

    Fails if the table exists with a different layout (e.g. the old partitioned one), since
    appending to it would silently keep the old layout.
    """
    cols = [validate_identifier(c, "cluster_column") for c in cluster_columns]
    if table_exists(spark, full_table_name):
        detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
        current = list(detail["clusteringColumns"] or []) if "clusteringColumns" in detail.asDict() else []
        if [c.lower() for c in current] != [c.lower() for c in cols]:
            raise ValueError(
                f"{full_table_name} exists with clustering {current} and partitioning "
                f"{list(detail['partitionColumns'] or [])}; expected CLUSTER BY {cols}. "
                "Recreate it (a partitioned table cannot be converted in place), or for an "
                "unpartitioned table run ALTER TABLE ... CLUSTER BY and OPTIMIZE FULL."
            )
        return
    column_ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(
        f"CREATE TABLE {full_table_name} ({column_ddl}) USING DELTA "
        f"CLUSTER BY ({', '.join(cols)})"
    )


def write_slice(df: DataFrame, full_table_name: str, replace_predicate: str = None) -> None:
    """Append `df` to an existing Delta table.

    With `replace_predicate`, the write atomically replaces exactly the rows matching the
    predicate (Delta `replaceWhere`) instead of blindly appending. The rest of the table is
    untouched, so it still behaves as an append for every other slice, but a
    re-run of the same slice no longer duplicates rows.
    """
    writer = df.write.format("delta").option("mergeSchema", "true")
    if replace_predicate:
        writer.mode("overwrite").option("replaceWhere", replace_predicate).saveAsTable(full_table_name)
    else:
        writer.mode("append").saveAsTable(full_table_name)


def optimize_table(spark: SparkSession, full_table_name: str) -> None:
    """`OPTIMIZE <table>`: clusters data written since the last run on its clustering columns.

    Fabric Runtime 2.0 clusters incrementally; on Runtime 1.3 (Delta 3.2) the same statement
    rewrites the whole table until a cluster exceeds 100 GB (see CLAUDE.md 7.6).
    """
    print(f"[nb_lib_delta_helpers] OPTIMIZE {full_table_name}")
    spark.sql(f"OPTIMIZE {full_table_name}")


def zorder_optimize(spark: SparkSession, full_table_name: str, zorder_columns) -> None:
    """`OPTIMIZE <table> ZORDER BY (<cols>)` for unpartitioned, non-clustered tables."""
    cols = ", ".join(validate_identifier(c, "zorder_column") for c in zorder_columns)
    print(f"[nb_lib_delta_helpers] OPTIMIZE {full_table_name} ZORDER BY ({cols})")
    spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({cols})")


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
