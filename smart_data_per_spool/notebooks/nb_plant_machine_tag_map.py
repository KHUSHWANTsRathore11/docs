# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_plant_machine_tag_map — Plant Machine Tag Mapping
#
# Maps each SCADA tag to its MES point, connector, unit and resource (machine), plant-scoped.
# Spec: `smart_data_per_spool/plant_machine_tag_mapping.md`.
#
# | Step | |
# |---|---|
# | 1 | Run `nb_scada_mes_plant_map` and read its mapping `{ScadaPlant, Department, MesPlant}` |
# | 2 | MES chain (Silver): `MI_POINT` → `MI_POINT_GROUP` → `MI_DATA_SOURCE` → `MI_CONNECTOR` → `Unit_Characteristic` (`MiConnector` = host) → `Resource_` |
# | 3 | SCADA join: `scada_tags` → plant mapping on (plant, department) → MES chain on `DbTag` = `<OPCpointName>` **and** MES plant |
# | 4 | Analysis printed; final table write is commented out (print mode) |
#
# All joins after the plant mapping are LEFT joins from `scada_tags`, so no tag is dropped;
# `LineageStatus` shows where the chain breaks for each tag.
# Bare table names resolve against the default lakehouse (attach Silver).

# PARAMETERS CELL ********************

# Step 1: plant mapping notebook
p_plant_map_notebook       = "nb_scada_mes_plant_map"
p_plant_map_timeout_sec    = 600
p_plant_map_args_json      = "{}"   # passed through, e.g. '{"p_manual_mapping_json": "[...]"}'

# SCADA tags (Silver)
p_tags_table               = "scada_tags"
p_tags_plant_column        = "PlantName"
p_tags_department_column   = "DepartmentName"
p_tags_id_column           = "TagId"
p_tags_dbtag_column        = "DbTag"

# MES tables (Silver) and their columns
p_point_table              = "MI_POINT"
p_point_id_column          = "Id"
p_point_group_column       = "PointGroupId"
p_point_xml_column         = "Parameters"
p_point_plant_column       = "PlantName"
p_xml_element              = "OPCpointName"   # matched case-insensitively
p_point_group_table        = "MI_POINT_GROUP"
p_point_group_id_column    = "Id"
p_group_data_source_column = "DataSourceId"
p_data_source_table        = "MI_DATA_SOURCE"
p_data_source_id_column    = "Id"
p_data_source_connector_column = "ConnectorId"
p_connector_table          = "MI_CONNECTOR"
p_connector_id_column      = "Id"
p_connector_host_column    = "HostName"
p_unit_char_table          = "Unit_Characteristic"
p_unit_char_unit_column    = "UnitId"
p_unit_char_name_column    = "Characteristic"
p_unit_char_value_column   = "Value_"
p_connector_characteristic = "MiConnector"
p_resource_table           = "Resource_"
p_resource_unit_column     = "UnitId"

# Optional filters on the SCADA side, comma-separated; empty = all
p_scada_plant_filter       = ""
p_department_filter        = ""

# Output (only used by the commented-out write at the end)
p_target_table             = "plant_machine_tag_map"
p_show_rows                = 50

# CELL ********************

import json
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", p_xml_element):
    raise ValueError(f"p_xml_element is not a valid XML element name: {p_xml_element!r}")


def host_key(column):
    """Host names are compared trimmed and case-insensitive (same rule as nb_scada_mes_plant_map)."""
    return F.upper(F.trim(column))


def csv_list(text):
    return [v.strip() for v in (text or "").split(",") if v.strip()]


def extract_xml_element(column, element):
    """Text of the first <element> in an XML string, or NULL.

    Regular expression rather than an XML parser, so a malformed document gives NULL instead of
    failing the query. Case-insensitive element name, optional namespace prefix and attributes,
    CDATA and the five standard entities are handled.
    """
    pattern = (rf"(?is)<(?:[A-Za-z0-9_.-]+:)?{element}(?:\s[^>]*)?>\s*(.*?)\s*"
               rf"</(?:[A-Za-z0-9_.-]+:)?{element}\s*>")
    value = F.regexp_extract(column, pattern, 1)
    value = F.regexp_replace(value, r"(?s)^<!\[CDATA\[(.*)\]\]>$", "$1")
    for entity, char in (("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'), ("&apos;", "'"), ("&amp;", "&")):
        value = F.regexp_replace(value, entity, char)
    value = F.trim(value)
    return F.when(value == "", None).otherwise(value)


def show(title, df, rows=p_show_rows):
    print(f"\n=== {title} ===")
    df.show(rows, truncate=False)

# CELL ********************

# Step 1: SCADA plant → MES plant mapping from nb_scada_mes_plant_map
plant_map_result = json.loads(notebookutils.notebook.run(
    p_plant_map_notebook, int(p_plant_map_timeout_sec), json.loads(p_plant_map_args_json or "{}")))

print(f"Plant mapping status: {plant_map_result['status']}")
for u in plant_map_result.get("unmatched", []):
    print(f"  UNMATCHED {u}")
for a in plant_map_result.get("ambiguous", []):
    print(f"  AMBIGUOUS {a}")

plant_map = spark.createDataFrame(
    [(m["ScadaPlant"], m["Department"], m["MesPlant"], m["Source"]) for m in plant_map_result["mapping"]],
    "ScadaPlant string, Department string, MesPlant string, PlantMapSource string",
)
show("Plant mapping", plant_map, 500)

# CELL ********************

# Step 2: MES chain, one row per MI_POINT (restricted to the mapped MES plants)
mes_plants = [r.MesPlant for r in plant_map.select("MesPlant").distinct().collect()]

points = (
    spark.table(p_point_table)
    .select(
        F.col(p_point_id_column).alias("PointId"),
        F.col(p_point_group_column).alias("PointGroupId"),
        F.trim(F.col(p_point_plant_column)).alias("MesPointPlant"),
        extract_xml_element(F.col(p_point_xml_column), p_xml_element).alias("OpcPointName"),
    )
    .filter(F.col("MesPointPlant").isin(mes_plants))
)
point_groups = spark.table(p_point_group_table).select(
    F.col(p_point_group_id_column).alias("PointGroupId"),
    F.col(p_group_data_source_column).alias("DataSourceId"),
    F.lit(True).alias("_group_found"),
)
data_sources = spark.table(p_data_source_table).select(
    F.col(p_data_source_id_column).alias("DataSourceId"),
    F.col(p_data_source_connector_column).alias("ConnectorId"),
    F.lit(True).alias("_data_source_found"),
)
connectors = spark.table(p_connector_table).select(
    F.col(p_connector_id_column).alias("ConnectorId"),
    F.col(p_connector_host_column).alias("ConnectorHostName"),
    F.lit(True).alias("_connector_found"),
)
# Unit_Characteristic holds one row per unit and characteristic; the MiConnector row's value is the host name
connector_units = (
    spark.table(p_unit_char_table)
    .filter(F.lower(F.trim(F.col(p_unit_char_name_column))) == p_connector_characteristic.strip().lower())
    .select(
        host_key(F.col(p_unit_char_value_column)).alias("_host_key"),
        F.col(p_unit_char_unit_column).alias("UnitId"),
    )
    .dropDuplicates()
)
resource_df = spark.table(p_resource_table)
resources = resource_df.select(
    F.col(p_resource_unit_column).alias("UnitId"),
    *[F.col(c).alias(f"res_{c}") for c in resource_df.columns if c.lower() != p_resource_unit_column.lower()],
    F.lit(True).alias("_resource_found"),
)

mes_chain = (
    points
    .join(point_groups, "PointGroupId", "left")
    .join(data_sources, "DataSourceId", "left")
    .join(connectors, "ConnectorId", "left")
    .withColumn("_host_key", host_key(F.col("ConnectorHostName")))
    .join(connector_units, "_host_key", "left")
    .join(resources, "UnitId", "left")
    .drop("_host_key")
)

# CELL ********************

# MES chain analysis
mes_chain.cache()
show("MES points by plant: XML extraction and chain breaks", mes_chain.groupBy("MesPointPlant").agg(
    F.count(F.lit(1)).alias("Rows"),
    F.countDistinct("PointId").alias("Points"),
    F.sum(F.col("OpcPointName").isNull().cast("int")).alias("NoOpcPointName"),
    F.sum(F.col("_group_found").isNull().cast("int")).alias("NoPointGroup"),
    F.sum(F.col("_data_source_found").isNull().cast("int")).alias("NoDataSource"),
    F.sum(F.col("_connector_found").isNull().cast("int")).alias("NoConnector"),
    F.sum(F.col("UnitId").isNull().cast("int")).alias("NoUnit"),
    F.sum(F.col("_resource_found").isNull().cast("int")).alias("NoResource"),
).orderBy("MesPointPlant"))

show("Duplicate OpcPointName within a MES plant (a DbTag would match several points)",
     mes_chain.filter("OpcPointName IS NOT NULL")
     .groupBy("MesPointPlant", "OpcPointName").agg(F.countDistinct("PointId").alias("Points"))
     .filter("Points > 1").orderBy(F.desc("Points")))

show("Connector hosts linked to more than one unit (fan-out)",
     connector_units.groupBy("_host_key").agg(F.collect_set("UnitId").alias("UnitIds"))
     .filter(F.size("UnitIds") > 1))

# CELL ********************

# Step 3: SCADA tags → plant mapping → MES chain (plant-scoped)
tags = spark.table(p_tags_table).select(
    F.trim(F.col(p_tags_plant_column)).alias("ScadaPlant"),
    F.trim(F.col(p_tags_department_column)).alias("Department"),
    F.col(p_tags_id_column).alias("TagId"),
    F.col(p_tags_dbtag_column).alias("DbTag"),
)
if csv_list(p_scada_plant_filter):
    tags = tags.filter(F.col("ScadaPlant").isin(csv_list(p_scada_plant_filter)))
if csv_list(p_department_filter):
    tags = tags.filter(F.col("Department").isin(csv_list(p_department_filter)))

mapped = (
    tags
    .join(plant_map, ["ScadaPlant", "Department"], "left")
    .withColumn("_dbtag_key", F.trim(F.col("DbTag")))
)
mapping_df = (
    mapped.alias("t")
    .join(
        mes_chain.alias("m"),
        (F.col("t._dbtag_key") == F.col("m.OpcPointName")) & (F.col("t.MesPlant") == F.col("m.MesPointPlant")),
        "left",
    )
    .withColumn(
        "LineageStatus",
        F.when(F.col("t.MesPlant").isNull(), "NO_PLANT_MAPPING")
        .when(F.col("t._dbtag_key").isNull() | (F.col("t._dbtag_key") == ""), "NO_DBTAG")
        .when(F.col("m.PointId").isNull(), "NO_POINT")
        .when(F.col("m._group_found").isNull(), "NO_POINT_GROUP")
        .when(F.col("m._data_source_found").isNull(), "NO_DATA_SOURCE")
        .when(F.col("m._connector_found").isNull(), "NO_CONNECTOR")
        .when(F.col("m.UnitId").isNull(), "NO_UNIT")
        .when(F.col("m._resource_found").isNull(), "NO_RESOURCE")
        .otherwise("MATCHED"),
    )
    .select(
        "t.ScadaPlant", "t.Department", "t.TagId", "t.DbTag", "t.MesPlant", "t.PlantMapSource",
        "m.PointId", "m.PointGroupId", "m.DataSourceId", "m.ConnectorId", "m.ConnectorHostName", "m.UnitId",
        *[f"m.{c}" for c in mes_chain.columns if c.startswith("res_")],
        "LineageStatus",
    )
)

# CELL ********************

# SCADA join analysis
mapping_df.cache()
total_tags = tags.count()
print(f"SCADA tags in scope : {total_tags}")
print(f"Mapping rows        : {mapping_df.count()}")

show("Tags by LineageStatus", mapping_df.groupBy("LineageStatus").count().orderBy(F.desc("count")))

show("LineageStatus by SCADA plant / department", mapping_df.groupBy("ScadaPlant", "Department", "MesPlant")
     .pivot("LineageStatus").count().na.fill(0).orderBy("ScadaPlant", "Department"), 500)

tag_key = ["ScadaPlant", "Department", "TagId"]
show("Tags matched to more than one point or unit (fan-out)",
     mapping_df.withColumn("_rows", F.count(F.lit(1)).over(Window.partitionBy(*tag_key)))
     .filter("_rows > 1").drop("_rows").orderBy(*tag_key))

show("Sample: tags with no point (DbTag not found in the mapped MES plant)",
     mapping_df.filter("LineageStatus = 'NO_POINT'").select("ScadaPlant", "Department", "TagId", "DbTag", "MesPlant"))

show("Sample: final mapping (MATCHED)", mapping_df.filter("LineageStatus = 'MATCHED'"))

# CELL ********************

# Step 4: final mapping table — commented out until the output above has been reviewed
# (mapping_df
#     .write
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(p_target_table))
# print(f"Written: {p_target_table}")

mes_chain.unpersist()
mapping_df.unpersist()
