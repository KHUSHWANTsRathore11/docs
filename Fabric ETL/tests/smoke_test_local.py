"""Local smoke test: runs the three notebooks end to end against local PySpark + Delta.

    pip install pyspark==3.5.3 delta-spark==3.2.1   # needs Java 11/17
    python tests/smoke_test_local.py
"""
import json, os, shutil, sys, tempfile
from datetime import datetime
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
work = tempfile.mkdtemp()
spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", f"{work}/wh")
    .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE DATABASE silver")
bronze = f"{work}/bronze"
helpers = open(f"{ROOT}/utils/nb_lib_delta_helpers.py").read()

def run(nb, _patch="", **params):
    src = open(f"{ROOT}/notebooks/{nb}.py").read()
    # _patch: code run right after the helpers load, to override a helper in one test
    src = src.replace("# MAGIC %run nb_lib_delta_helpers", helpers + "\n" + _patch)
    # Overrides go right after the parameters cell, as Fabric's injected parameters cell does.
    head, tail = src.split("# CELL ********************", 1)
    override = "\n".join(f"{k} = {v!r}" for k, v in params.items())
    src = head + "\n" + override + "\n# CELL ********************" + tail
    g = {"spark": spark, "__name__": "nb"}
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        exec(compile(src, nb, "exec"), g)
    last = [l for l in buf.getvalue().splitlines() if l.startswith("{")][-1]
    return json.loads(last)

def write(path, rows, cols):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    spark.createDataFrame(rows, cols).coalesce(1).write.mode("overwrite").parquet(path + ".dir")
    f = [x for x in os.listdir(path + ".dir") if x.endswith(".parquet")][0]
    shutil.move(f"{path}.dir/{f}", path); shutil.rmtree(path + ".dir")

T = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
cols = ["PlantName", "DepartmentName", "TagId", "Time", "Value"]
# historical: 2 days for PlantA/Boiler (plant/department columns are in the source files)
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/2026/10/01/TAGLOG.parquet", [("PlantA", "Boiler", 1, T("2026-10-01 01:00:00"), 1.0), ("PlantA", "Boiler", 2, T("2026-10-01 05:00:00"), 2.0)], cols)
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/2026/10/02/TAGLOG.parquet", [("PlantA", "Boiler", 1, T("2026-10-02 01:00:00"), 3.0), ("PlantA", "Boiler", None, None, 9.0)], cols)
# metadata snapshot at same level must NOT be picked by historical glob
tag_cols = ["PlantName", "DepartmentName", "TagId", "Description", "Unit", "LastValue"]
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", [("PlantA", "Boiler", 1, "Temp", "C", 10.0), ("PlantA", "Boiler", 2, "Press", "bar", 5.0), ("PlantA", "Boiler", 2, "Press", "bar", 5.0), ("PlantA", "Boiler", None, "x", "y", 0.0)], tag_cols)

hist = dict(p_source_path=f"{bronze}/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet", p_target_table="silver.TAGLOG")
r = run("nb_telemetry_historical", **hist, p_dry_run="true"); print(r); assert r["status"] == "DRY_RUN"
assert not spark.catalog.tableExists("silver.TAGLOG")
r = run("nb_telemetry_historical", **hist); print(r)
assert r["rows_written"] == 3 and r["rows_dropped_null_time"] == 1 and r["plant_departments"] == ["PlantA/Boiler"], r
r = run("nb_telemetry_historical", **hist); assert spark.table("silver.TAGLOG").count() == 3, "historical rerun not idempotent"
layout = spark.sql("DESCRIBE DETAIL silver.TAGLOG").select("partitionColumns", "clusteringColumns").collect()[0]
assert layout[0] == [] and layout[1] == ["Date", "PlantName", "DepartmentName", "TagId"], layout

# incremental: one day per source path
write(f"{bronze}/ilayer_SCADA/PlantA/Boiler/2026/10/03/TAGLOG/part1.parquet", [("PlantA", "Boiler", 1, T("2026-10-03 00:00:00"), 4.0), ("PlantA", "Boiler", 2, T("2026-10-03 23:59:59"), 5.0)], cols)
write(f"{bronze}/ilayer_SCADA/PlantA/Turbine/2026/10/03/TAGLOG/part1.parquet", [("PlantA", "Turbine", 7, T("2026-10-03 12:00:00"), 7.0)], cols)
inc = lambda dept: dict(p_source_path=f"{bronze}/ilayer_SCADA/PlantA/{dept}/2026/10/03/TAGLOG/*.parquet", p_target_table="silver.TAGLOG")
r = run("nb_telemetry_incremental", **inc("Boiler"), p_dry_run=True); assert r["status"] == "DRY_RUN", r
assert spark.table("silver.TAGLOG").count() == 3
r = run("nb_telemetry_incremental", **inc("Boiler")); print(r)
assert r["loaded"] == ["PlantA/Boiler/2026-10-03"] and r["rows_written"] == 2 and r["optimize_error"] is None, r
r = run("nb_telemetry_incremental", **inc("Turbine"))
assert spark.table("silver.TAGLOG").count() == 6, spark.table("silver.TAGLOG").count()
assert spark.table("silver.TAGLOG").where("DepartmentName = 'Turbine'").count() == 1
spark.table("silver.TAGLOG").orderBy("Time").show()

# A failed OPTIMIZE after the append does not fail the run (the data is already committed)
write(f"{bronze}/ilayer_SCADA/PlantA/Cooling/2026/10/03/TAGLOG/part1.parquet", [("PlantA", "Cooling", 3, T("2026-10-03 06:00:00"), 1.5)], cols)
r = run("nb_telemetry_incremental", _patch="def optimize_table(spark, t):\n    raise RuntimeError('simulated concurrent OPTIMIZE')",
        p_source_path=f"{bronze}/ilayer_SCADA/PlantA/Cooling/2026/10/03/TAGLOG/*.parquet", p_target_table="silver.TAGLOG")
assert r["status"] == "SUCCEEDED" and "simulated" in r["optimize_error"] and r["rows_written"] == 1, r
assert spark.table("silver.TAGLOG").count() == 7

# An existing table with the old partitioned layout is rejected, not silently appended to
spark.createDataFrame([("PlantA", 1)], ["PlantName", "Year"]).write.format("delta").partitionBy("PlantName").saveAsTable("silver.OLD")
try:
    run("nb_telemetry_incremental", **{**inc("Boiler"), "p_target_table": "silver.OLD"}); raise SystemExit("old layout accepted")
except ValueError as e: print("rejected:", e)

# reuse in another scenario: different folder layout, column names and target table
write(f"{bronze}/other_source/events/2026-10-03.parquet", [("SiteB", "Packing", "A", T("2026-10-03 08:00:00")), ("SiteB", "Packing", "B", T("2026-10-03 09:00:00"))], ["Site", "Line", "EventCode", "EventTime"])
r = run("nb_telemetry_incremental", p_source_path=f"{bronze}/other_source/events/*.parquet", p_target_table="silver.EVENTS",
        p_plant_column="Site", p_department_column="Line", p_id_column="EventCode", p_time_column="EventTime")
assert r["rows_written"] == 2 and r["loaded"] == ["SiteB/Packing/2026-10-03"], r
assert spark.sql("DESCRIBE DETAIL silver.EVENTS").select("clusteringColumns").collect()[0][0] == ["Date", "Site", "Line", "EventCode"]
# a missing plant/department column fails clearly
try:
    run("nb_telemetry_incremental", p_source_path=f"{bronze}/other_source/events/*.parquet", p_target_table="silver.EVENTS",
        p_id_column="EventCode", p_time_column="EventTime"); raise SystemExit("missing plant column accepted")
except ValueError as e: print("rejected:", e)

# target table validation
from importlib.util import spec_from_file_location, module_from_spec
spec = spec_from_file_location("nb_lib_delta_helpers", f"{ROOT}/utils/nb_lib_delta_helpers.py"); h = module_from_spec(spec); spec.loader.exec_module(h)
for v in ["silver.TAGS", "TAGS", "silver.dbo.TAGS"]:
    h.validate_table_name(v)
for v in ["", "a.b.c.d", "silver.TAGS; DROP TABLE x", "silver..TAGS"]:
    try: h.validate_table_name(v); raise SystemExit(f"accepted {v}")
    except ValueError: pass

# metadata
meta = dict(p_source_path=f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", p_target_table="silver.TAGS")
r = run("nb_metadata_sync", **meta); print(r); assert r["rows_staged"] == 2
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", [("PlantA", "Boiler", 1, "Temp", "K", 11.0), ("PlantA", "Boiler", 3, "Flow", "m3", 1.0)], tag_cols)
r = run("nb_metadata_sync", **meta); print(r)
spark.table("silver.TAGS").orderBy("TagId").show()
rows = {x.TagId: x.Unit for x in spark.table("silver.TAGS").collect()}
tags_layout = spark.sql("DESCRIBE DETAIL silver.TAGS").select("partitionColumns", "clusteringColumns").collect()[0]
assert tags_layout[0] == [] and not tags_layout[1], tags_layout
zo = spark.sql("DESCRIBE HISTORY silver.TAGS").where("operation = 'OPTIMIZE'").select("operationParameters").collect()
assert zo and "PlantName" in zo[0][0]["zOrderBy"] and "DepartmentName" in zo[0][0]["zOrderBy"], zo
assert rows == {1: "K", 2: "bar", 3: "m3"}, rows
for bad in [dict(p_target_table="silver.TAGS; DROP TABLE x"), dict(p_source_path="")]:
    try:
        run("nb_metadata_sync", **{**meta, **bad}); raise SystemExit(f"validation failed for {bad}")
    except ValueError as e: print("rejected:", e)
# spool boundaries: TagID spelled differently from the p_id_column default, TagId 10 reused in PlantB
spark.createDataFrame([("PlantA", "Winding", 10, "LASTSpoolPV"), ("PlantA", "Winding", 11, " lastspoolpv "),
                       ("PlantA", "Winding", 12, "Speed"), ("PlantB", "Winding", 10, "Speed")],
                      ["PlantName", "DepartmentName", "TagID", "Description"]).write.format("delta").saveAsTable("silver.SP_TAGS")
sp_cols = ["PlantName", "DepartmentName", "TagID", "Time", "Value", "Date"]
def sp_rows(rows):
    return spark.createDataFrame([(p, d, t, T(ts), v) for p, d, t, ts, v in rows], sp_cols[:-1]).withColumn("Date", F.to_date("Time"))
from pyspark.sql import functions as F
sp_rows([("PlantA", "Winding", 10, "2026-10-01 01:00:00", 1.0), ("PlantA", "Winding", 10, "2026-10-01 05:00:00", 2.0),
         ("PlantA", "Winding", 10, "2026-10-01 23:30:00", 3.0), ("PlantA", "Winding", 11, "2026-10-01 02:00:00", 7.0),
         ("PlantA", "Winding", 12, "2026-10-01 03:00:00", 50.0), ("PlantA", "Winding", 12, "2026-10-01 04:00:00", 60.0),
         ("PlantB", "Winding", 10, "2026-10-01 03:00:00", 99.0)]).write.format("delta").saveAsTable("silver.SP_TAGLOG")
spool = dict(p_taglog_table="silver.SP_TAGLOG", p_tags_table="silver.SP_TAGS", p_target_table="silver.SPOOL")
r = run("nb_spool_boundaries", **spool); print(r)
assert r["spool_tags"] == 2 and r["spools_written"] == 4, r
# day 2: tag 10's next spool spans midnight
sp_rows([("PlantA", "Winding", 10, "2026-10-02 02:00:00", 4.0), ("PlantA", "Winding", 11, "2026-10-02 06:00:00", 8.0)]
        ).write.format("delta").mode("append").saveAsTable("silver.SP_TAGLOG")
r = run("nb_spool_boundaries", **spool); assert r["spools_written"] == 2, r
r = run("nb_spool_boundaries", **spool); assert r["status"] == "NO_DATA", r
sp = {(x.SpoolTagId, str(x.SpoolEndTime)): (str(x.SpoolStartTime), x.DurationSec, x.LastSpoolPV)
      for x in spark.table("silver.SPOOL").collect()}
assert len(sp) == 6 and all(k[0] in (10, 11) for k in sp), sp
assert sp[(10, "2026-10-02 02:00:00")] == ("2026-10-01 23:30:00", 9000.0, 4.0), sp
assert sp[(10, "2026-10-01 01:00:00")][0] == "None" and sp[(11, "2026-10-02 06:00:00")][0] == "2026-10-01 02:00:00", sp
before = sorted(map(str, spark.table("silver.SPOOL").collect()))
r = run("nb_spool_boundaries", **spool, p_full_rebuild=True); assert r["spools_written"] == 6, r
assert sorted(map(str, spark.table("silver.SPOOL").collect())) == before
# spool characteristics: Speed readings inside spool (01:00, 05:00] of tag 10
n = spark.sql("""SELECT COUNT(*) FROM silver.SPOOL s JOIN silver.SP_TAGLOG t
                 ON t.PlantName = s.PlantName AND t.DepartmentName = s.DepartmentName
                AND t.Time > s.SpoolStartTime AND t.Time <= s.SpoolEndTime
                WHERE s.SpoolTagId = 10 AND s.SpoolEndTime = '2026-10-01 05:00:00' AND t.TagID = 12""").first()[0]
assert n == 2, n

print("ALL OK")
