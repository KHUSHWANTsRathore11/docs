# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_metadata_full — Daily Metadata Sync, SCD Type 2 (Notebook 6)
#
# Daily sync of a full metadata snapshot (e.g. `TAGS`) into a Type 2 table: a change in a tracked
# column closes the current version and opens a new one; volatile columns are overwritten in
# place. Spec: `CLAUDE.md` section 3.6.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full daily snapshot, e.g. `Files/rlayer_SCADA/PlantA/Boiler/TAGS.parquet` |
# | **Target** | `p_target_table`, e.g. `silver.TAGS_HISTORY` |
# | **Write** | `nb_lib_delta_helpers.scd2_merge`: one Delta MERGE on `(<plant>, <department>, <id>)` |
# | **Columns** | `p_tracked_columns` create a new version on change; `p_volatile_columns` update in place; any other source column is tracked |
# | **Missing keys** | a key absent from the snapshot is closed (`IsCurrent = false`) when `p_close_missing` is true |
#
# Current rows: `IsCurrent = true` and `ValidTo = 9999-12-31`. Query the state at a point in time
# with `ValidFrom <= t AND t < ValidTo`. Rerunning the same snapshot changes nothing.

# PARAMETERS CELL ********************

p_source_path       = "Files/rlayer_SCADA/PlantA/Boiler/TAGS.parquet"
p_target_table      = "silver.TAGS_HISTORY"
p_plant_column      = "PlantName"
p_department_column = "DepartmentName"
p_id_column         = "TagId"
p_tracked_columns   = "PLC_Connection,MESdb_Connection,Unit,EGU,Description,DataType"
p_volatile_columns  = "LastValue,LastLog,Quality"
p_close_missing     = True     # close keys that disappear from the snapshot
p_dry_run           = False

# CELL ********************

# MAGIC %run nb_lib_delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

source_path  = validate_source_path(p_source_path)
target_table = validate_table_name(p_target_table)
plant_column = validate_identifier(p_plant_column, "p_plant_column")
dept_column  = validate_identifier(p_department_column, "p_department_column")
id_column    = validate_identifier(p_id_column, "p_id_column")
close_missing = to_bool(p_close_missing)
dry_run       = to_bool(p_dry_run)

def column_list(text, name):
    names = [c.strip() for c in str(text or "").split(",") if c.strip()]
    return [validate_identifier(c, name) for c in names]

tracked_columns  = column_list(p_tracked_columns, "p_tracked_columns")
volatile_columns = column_list(p_volatile_columns, "p_volatile_columns")

run_started = datetime.now(timezone.utc)
print(f"Source   : {source_path}")
print(f"Target   : {target_table}")
print(f"Tracked  : {tracked_columns}")
print(f"Volatile : {volatile_columns}")
print(f"Dry run  : {dry_run}")

# CELL ********************

# Read & stage the snapshot
raw_df = spark.read.parquet(source_path)
key_columns = require_columns(raw_df, [plant_column, dept_column, id_column], "metadata snapshot")
plant_column, dept_column, id_column = key_columns

rows_read = raw_df.count()

# Rows without a business key can never be matched.
staged = raw_df.filter(F.col(id_column).isNotNull())
null_key_rows = rows_read - staged.count()

# One row per key: the SCD2 merge requires it.
staged = staged.dropDuplicates(key_columns).cache()
rows_staged = staged.count()
duplicate_rows = rows_read - null_key_rows - rows_staged

print(f"Rows read              : {rows_read}")
print(f"Rows dropped (null key): {null_key_rows}")
print(f"Rows dropped (dup key) : {duplicate_rows}")
print(f"Rows staged            : {rows_staged}")

# CELL ********************

# SCD Type 2 merge
merge_metrics = {}
if rows_staged == 0:
    status = "NO_DATA"
elif dry_run:
    staged.printSchema()
    staged.show(10, truncate=False)
    status = "DRY_RUN"
else:
    merge_metrics = scd2_merge(
        spark, staged, target_table, key_columns,
        tracked_columns=tracked_columns,
        volatile_columns=volatile_columns,
        load_ts=run_started,
        close_missing=close_missing,
    )
    print(f"Merge metrics: {merge_metrics}")
    status = "SUCCEEDED"

staged.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_metadata_full",
    "status": status,
    "table": target_table,
    "source_path": source_path,
    "rows_read": rows_read,
    "rows_staged": rows_staged,
    "rows_dropped_null_key": null_key_rows,
    "rows_dropped_duplicate_key": duplicate_rows,
    "merge_metrics": merge_metrics,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
