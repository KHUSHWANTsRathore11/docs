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
from datetime import date, datetime, timezone
from functools import reduce

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


SCD2_OPEN_END = datetime(9999, 12, 31)  # EffectiveEndDate of the current version (timestamp sentinel)


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    full_table_name: str,
    key_columns,
    tracked_columns,
    volatile_columns=(),
    load_ts: datetime = None,
    close_missing: bool = True,
    valid_from: str = "EffectiveStartDate",
    valid_to: str = "EffectiveEndDate",
    is_current: str = "IsCurrent",
    audit_insert: str = "DWInsertDate",
    audit_update: str = "DWUpdateDate",
) -> dict:
    """SCD Type 2 merge of a full snapshot, with volatile columns updated in place.

    * **Tracked** columns: a change creates a new version. The old version is closed
      (`EffectiveEndDate = load_ts`, `IsCurrent = false`) and the new one opened
      (`EffectiveStartDate = load_ts`, `EffectiveEndDate = 9999-12-31`, `IsCurrent = true`).
    * **Volatile** columns: a change overwrites the current version in place, with no new
      version (CLAUDE.md 5.2, alternative 1). Their history is not kept.
    * **Undeclared** non-key columns are tracked (the conservative default).
    * A key present in the target but absent from the snapshot is closed when `close_missing`.
    * Unchanged keys are not touched, so re-running the same snapshot changes nothing.
    * Audit columns (`audit_insert`, `audit_update`) are maintained by the merge and never
      compared: `DWInsertDate` is the run time a version was inserted; `DWUpdateDate` is the run time
      a row was last closed or had a volatile column overwritten (null if never).

    Preconditions: `source_df` has one row per key and no null keys; the session time zone is
    UTC (the timestamps are written as session-local). The target, if it exists, must contain
    every source column plus the three validity columns. Source columns not in the target are
    an error (no schema evolution for SCD2).

    Implemented as one MERGE. Each changed tracked key appears twice in the MERGE source: once
    with its key (to close the current version) and once with a null key (which never matches,
    so it is inserted as the new version). Returns the Delta operation metrics.
    """
    from delta.tables import DeltaTable

    keys = require_columns(source_df, key_columns, "SCD2 source")
    tracked = [validate_identifier(c, "tracked column") for c in tracked_columns]
    volatile = [validate_identifier(c, "volatile column") for c in volatile_columns]
    scd_cols = [valid_from, valid_to, is_current]
    audit_cols = [audit_insert, audit_update]
    if set(scd_cols + audit_cols) & set(source_df.columns):
        raise ValueError(f"Source must not contain the SCD or audit columns {scd_cols + audit_cols}")
    attrs = [c for c in source_df.columns if c not in keys]

    if set(tracked) & set(volatile):
        raise ValueError(f"Columns listed as both tracked and volatile: {sorted(set(tracked) & set(volatile))}")
    undeclared = [c for c in tracked + volatile if c not in attrs]
    if undeclared:
        raise ValueError(f"Tracked/volatile columns not in the source: {undeclared}")
    tracked_all = tracked + [c for c in attrs if c not in tracked and c not in volatile]

    if load_ts is None:
        load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    elif load_ts.tzinfo is not None:
        load_ts = load_ts.astimezone(timezone.utc).replace(tzinfo=None)

    if source_df.groupBy(*keys).count().where("count > 1").limit(1).count():
        raise ValueError(f"SCD2 source has duplicate keys on {keys}; deduplicate before merging")

    if not table_exists(spark, full_table_name):
        initial = (
            source_df.withColumn(valid_from, F.lit(load_ts).cast("timestamp"))
                     .withColumn(valid_to, F.lit(SCD2_OPEN_END).cast("timestamp"))
                     .withColumn(is_current, F.lit(True))
                     .withColumn(audit_insert, F.lit(load_ts).cast("timestamp"))
                     .withColumn(audit_update, F.lit(None).cast("timestamp"))
        )
        n = initial.count()
        initial.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        return {"operation": "CREATE", "numTargetRowsInserted": n}

    target = spark.table(full_table_name)
    missing = [c for c in keys + attrs + scd_cols + audit_cols if c not in target.columns]
    if missing:
        raise ValueError(f"{full_table_name} lacks columns {missing}; SCD2 needs them "
                         "(see CLAUDE.md 3.3 for the migration)")
    new_cols = [c for c in attrs if c not in target.columns]
    if new_cols:
        raise ValueError(f"Source has columns not in {full_table_name}: {new_cols}")

    # Current version per key, and the source joined to it.
    current = (
        target.where(F.col(is_current))
              .select(*keys, *[F.col(a).alias(f"__t_{a}") for a in attrs])
              .withColumn("__t_present", F.lit(True))
    )
    joined = source_df.join(current, keys, "left")
    is_new = F.col("__t_present").isNull()

    def changed(cols):
        if not cols:
            return F.lit(False)
        return reduce(lambda a, b: a | b, [~F.col(c).eqNullSafe(F.col(f"__t_{c}")) for c in cols])

    base = (
        joined
        .withColumn("__tracked_changed", ~is_new & changed(tracked_all))
        .withColumn("__volatile_changed", ~is_new & ~changed(tracked_all) & changed(volatile))
        .drop(*[f"__t_{a}" for a in attrs], "__t_present")
        .select("*", *[F.col(k).alias(f"__k_{k}") for k in keys])
    )
    # Second copy of each tracked change, with null keys: it never matches, so it is inserted.
    versions = base.where(F.col("__tracked_changed"))
    for k in keys:
        versions = versions.withColumn(k, F.lit(None).cast(base.schema[k].dataType))
    staged = base.unionByName(versions)

    stamp = F.lit(load_ts).cast("timestamp")
    close_set = {valid_to: stamp, is_current: F.lit(False), audit_update: stamp}
    insert_values = {k: f"source.`__k_{k}`" for k in keys}
    insert_values.update({a: f"source.`{a}`" for a in attrs})
    insert_values.update({
        valid_from: stamp,
        valid_to: F.lit(SCD2_OPEN_END).cast("timestamp"),
        is_current: F.lit(True),
        audit_insert: stamp,
        audit_update: F.lit(None).cast("timestamp"),
    })
    on = " AND ".join(f"target.`{k}` = source.`{k}`" for k in keys) + f" AND target.`{is_current}` = true"

    merger = (
        DeltaTable.forName(spark, full_table_name).alias("target")
        .merge(staged.alias("source"), on)
        .whenMatchedUpdate(condition="source.`__tracked_changed`", set=close_set)
    )
    if volatile:
        merger = merger.whenMatchedUpdate(
            condition="source.`__volatile_changed`",
            set={**{a: f"source.`{a}`" for a in volatile}, audit_update: stamp},
        )
    merger = merger.whenNotMatchedInsert(values=insert_values)
    if close_missing:
        merger = merger.whenNotMatchedBySourceUpdate(condition=f"target.`{is_current}` = true", set=close_set)
    merger.execute()

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
