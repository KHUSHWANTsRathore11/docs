# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_tag_datasource_map — Tag → Point → Group → Data Source (Notebook 5)
#
# Maps every tag to its MI point, point group, data source and connector, and analyses each
# join step. Spec: `CLAUDE.md` section 3.5.
#
# ```
# TAGS.DBTag = <OpcPointName> in MI_POINT.Parameter (XML)  -> MI_POINT.ID, PointGroupID
# MI_POINT.PointGroupID      = MI_POINT_GROUP.ID            -> DataSourceID
# MI_POINT_GROUP.DataSourceID = MI_DATA_SOURCE.ID           -> ConnectorID
# ```
#
# * `DBTag` (`Machine.Characteristic`) is matched in full (trimmed); `Machine` and
#   `Characteristic` are split at the first `.` as extra columns.
# * All joins are LEFT joins from `TAGS`, so no tag is dropped; `LineageStatus` says where the
#   chain breaks: `NO_DBTAG`, `NO_POINT`, `NO_POINT_GROUP`, `NO_DATA_SOURCE` or `MATCHED`.
# * A tag whose DBTag matches several points gets one row per point (reported in the analysis).
# * Output: a view (`p_as_view = True`) or a Delta table. Spark views are visible to Spark, not
#   to the SQL analytics endpoint or Direct Lake; use a table for those.

# PARAMETERS CELL ********************

p_tags_table        = "silver.TAGS"
p_point_table       = "silver.MI_POINT"
p_point_group_table = "silver.MI_POINT_GROUP"
p_data_source_table = "silver.MI_DATA_SOURCE"
p_target            = "silver.TAG_DATASOURCE"
p_as_view           = True     # True: CREATE OR REPLACE VIEW; False: Delta table (CREATE OR REPLACE TABLE ... AS)

p_plant_column        = "PlantName"
p_department_column   = "DepartmentName"
p_id_column           = "TagId"
p_dbtag_column        = "DBTag"
p_point_id_column     = "ID"
p_point_group_column  = "PointGroupID"
p_parameter_column    = "Parameter"
p_xml_element         = "OpcPointName"
p_group_id_column     = "ID"
p_data_source_column  = "DataSourceID"
p_source_id_column    = "ID"
p_connector_column    = "ConnectorID"
p_dry_run             = False   # True: analysis only, nothing written

# CELL ********************

# MAGIC %run nb_lib_delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

tags_table   = validate_table_name(p_tags_table, "p_tags_table")
point_table  = validate_table_name(p_point_table, "p_point_table")
group_table  = validate_table_name(p_point_group_table, "p_point_group_table")
source_table = validate_table_name(p_data_source_table, "p_data_source_table")
target       = validate_table_name(p_target, "p_target")
xml_element  = validate_identifier(p_xml_element, "p_xml_element")
as_view      = to_bool(p_as_view)
dry_run      = to_bool(p_dry_run)

# Resolve every column against its table (case-insensitive) before building any SQL.
t_plant, t_dept, t_id, t_dbtag = require_columns(
    spark.table(tags_table),
    [validate_identifier(c, n) for c, n in [(p_plant_column, "p_plant_column"),
     (p_department_column, "p_department_column"), (p_id_column, "p_id_column"),
     (p_dbtag_column, "p_dbtag_column")]], tags_table)
p_id, p_group, p_param = require_columns(
    spark.table(point_table),
    [validate_identifier(c, n) for c, n in [(p_point_id_column, "p_point_id_column"),
     (p_point_group_column, "p_point_group_column"), (p_parameter_column, "p_parameter_column")]],
    point_table)
g_id, g_source = require_columns(
    spark.table(group_table),
    [validate_identifier(c, n) for c, n in [(p_group_id_column, "p_group_id_column"),
     (p_data_source_column, "p_data_source_column")]], group_table)
s_id, s_connector = require_columns(
    spark.table(source_table),
    [validate_identifier(c, n) for c, n in [(p_source_id_column, "p_source_id_column"),
     (p_connector_column, "p_connector_column")]], source_table)

run_started = datetime.now(timezone.utc)
print(f"Tags        : {tags_table} ({t_plant}, {t_dept}, {t_id}, {t_dbtag})")
print(f"Points      : {point_table} ({p_id}, {p_group}, <{xml_element}> in {p_param})")
print(f"Point groups: {group_table} ({g_id}, {g_source})")
print(f"Data sources: {source_table} ({s_id}, {s_connector})")
print(f"Target      : {target} as {'view' if as_view else 'Delta table'}; dry run: {dry_run}")

# CELL ********************

# The query. Plain Spark SQL (no temp views), so it can back a persisted view.
#
# <OpcPointName> is extracted with a regular expression rather than an XML parser: a parser
# fails the whole query on one malformed document, while the regex returns NULL for it, which
# the analysis below reports. It accepts a namespace prefix and attributes on the element and
# decodes the five standard XML entities. CDATA content is not supported (reported as a
# parse failure).
def xml_value(column):
    pattern = (r"<(?:[A-Za-z0-9_]+:)?" + xml_element + r"(?:\\s[^>]*)?>\\s*([^<]*?)\\s*"
               r"</(?:[A-Za-z0-9_]+:)?" + xml_element + r">")
    raw = f"regexp_extract(`{column}`, '{pattern}', 1)"
    for entity, char in [("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'), ("&apos;", "\\'"), ("&amp;", "&")]:
        raw = f"replace({raw}, '{entity}', '{char}')"
    return f"NULLIF({raw}, '')"

STAGES = {
    "tags": f"""
        SELECT `{t_plant}` AS PlantName, `{t_dept}` AS DepartmentName, `{t_id}` AS TagId,
               `{t_dbtag}` AS DBTag,
               NULLIF(TRIM(`{t_dbtag}`), '') AS DBTagKey,
               substring_index(NULLIF(TRIM(`{t_dbtag}`), ''), '.', 1) AS Machine,
               CASE WHEN instr(TRIM(`{t_dbtag}`), '.') > 0
                    THEN substr(TRIM(`{t_dbtag}`), instr(TRIM(`{t_dbtag}`), '.') + 1) END AS Characteristic
        FROM {tags_table}""",
    "points": f"""
        SELECT `{p_id}` AS PointID, `{p_group}` AS PointGroupID,
               {xml_value(p_param)} AS OpcPointName,
               instr(`{p_param}`, '{xml_element}') > 0 AS HasElement
        FROM {point_table}""",
    "groups": f"SELECT `{g_id}` AS PointGroupID, `{g_source}` AS DataSourceID FROM {group_table}",
    "sources": f"SELECT `{s_id}` AS DataSourceID, `{s_connector}` AS ConnectorID FROM {source_table}",
}

QUERY = f"""
WITH tags    AS ({STAGES['tags']}),
     points  AS ({STAGES['points']}),
     groups  AS ({STAGES['groups']}),
     sources AS ({STAGES['sources']})
SELECT t.PlantName, t.DepartmentName, t.TagId, t.DBTag, t.Machine, t.Characteristic,
       p.PointID, p.PointGroupID, g.DataSourceID, s.ConnectorID,
       CASE WHEN t.DBTagKey IS NULL     THEN 'NO_DBTAG'
            WHEN p.PointID IS NULL      THEN 'NO_POINT'
            WHEN g.PointGroupID IS NULL THEN 'NO_POINT_GROUP'
            WHEN s.DataSourceID IS NULL THEN 'NO_DATA_SOURCE'
            ELSE 'MATCHED' END AS LineageStatus
FROM tags t
LEFT JOIN points  p ON p.OpcPointName = t.DBTagKey
LEFT JOIN groups  g ON g.PointGroupID = p.PointGroupID
LEFT JOIN sources s ON s.DataSourceID = g.DataSourceID
"""
print(QUERY)

# Stage views for the analysis only (session-scoped).
for name, sql in STAGES.items():
    spark.sql(sql).createOrReplaceTempView(f"_stage_{name}")
result = spark.sql(QUERY).cache()
result.createOrReplaceTempView("_stage_result")

def show(title, sql, n=20):
    print(f"\n=== {title}")
    spark.sql(sql).show(n, truncate=False)

def scalar(sql):
    return spark.sql(sql).first()[0]

# MARKDOWN ********************

# ## Step-by-step analysis

# CELL ********************

# Step 1 — TAGS and DBTag quality
show("Step 1: TAGS", """
SELECT COUNT(*)                                             AS tags,
       COUNT(DBTagKey)                                      AS with_dbtag,
       COUNT(*) - COUNT(DBTagKey)                           AS without_dbtag,
       COUNT(DISTINCT DBTagKey)                             AS distinct_dbtags,
       SUM(CASE WHEN DBTagKey IS NOT NULL AND Characteristic IS NULL THEN 1 ELSE 0 END) AS dbtag_without_dot,
       COUNT(DISTINCT Machine)                              AS machines
FROM _stage_tags""")
# The same DBTag in several plants/departments joins to the same MI point for all of them.
show("Step 1: DBTags used by more than one plant/department (first 20)", """
SELECT DBTagKey, COUNT(*) AS tags, collect_set(concat(PlantName, '/', DepartmentName)) AS used_in
FROM _stage_tags WHERE DBTagKey IS NOT NULL
GROUP BY DBTagKey HAVING COUNT(*) > 1 ORDER BY tags DESC""")

# CELL ********************

# Step 2 — MI_POINT: extracting <OpcPointName> from the XML
show("Step 2: MI_POINT", """
SELECT COUNT(*)                                                      AS points,
       SUM(CASE WHEN OpcPointName IS NOT NULL THEN 1 ELSE 0 END)     AS opc_name_extracted,
       SUM(CASE WHEN HasElement AND OpcPointName IS NULL THEN 1 ELSE 0 END) AS element_present_but_not_parsed,
       SUM(CASE WHEN NOT HasElement OR HasElement IS NULL THEN 1 ELSE 0 END) AS no_element,
       COUNT(DISTINCT OpcPointName)                                  AS distinct_opc_names,
       SUM(CASE WHEN PointGroupID IS NULL THEN 1 ELSE 0 END)         AS without_point_group
FROM _stage_points""")
show("Step 2: OpcPointName used by more than one point (first 20)", """
SELECT OpcPointName, COUNT(*) AS points, collect_set(PointID) AS point_ids
FROM _stage_points WHERE OpcPointName IS NOT NULL
GROUP BY OpcPointName HAVING COUNT(*) > 1 ORDER BY points DESC""")

# CELL ********************

# Step 3 — TAGS → MI_POINT (DBTag = OpcPointName)
show("Step 3: tags matched to points", """
SELECT COUNT(DISTINCT CASE WHEN PointID IS NOT NULL THEN concat_ws('|', PlantName, DepartmentName, TagId) END) AS tags_matched,
       COUNT(DISTINCT CASE WHEN LineageStatus = 'NO_POINT' THEN concat_ws('|', PlantName, DepartmentName, TagId) END) AS tags_without_point
FROM _stage_result""")
show("Step 3: DBTags without a point, with a near match (case/whitespace) if any (first 20)", """
SELECT r.DBTag, first(p.OpcPointName, true) AS near_match
FROM _stage_result r
LEFT JOIN _stage_points p ON lower(trim(p.OpcPointName)) = lower(trim(r.DBTag))
WHERE r.LineageStatus = 'NO_POINT'
GROUP BY r.DBTag ORDER BY near_match IS NULL, r.DBTag""")
show("Step 3: tags matching more than one point (fan-out, first 20)", """
SELECT PlantName, DepartmentName, TagId, DBTag, COUNT(*) AS points, collect_set(PointID) AS point_ids
FROM _stage_result WHERE PointID IS NOT NULL
GROUP BY PlantName, DepartmentName, TagId, DBTag HAVING COUNT(*) > 1 ORDER BY points DESC""")

# CELL ********************

# Step 4 — MI_POINT → MI_POINT_GROUP
show("Step 4: point groups", """
SELECT COUNT(*) AS groups, COUNT(DISTINCT PointGroupID) AS distinct_ids,
       SUM(CASE WHEN DataSourceID IS NULL THEN 1 ELSE 0 END) AS without_data_source
FROM _stage_groups""")
show("Step 4: PointGroupIDs referenced by matched points but missing in MI_POINT_GROUP", """
SELECT PointGroupID, COUNT(*) AS rows FROM _stage_result
WHERE LineageStatus = 'NO_POINT_GROUP' GROUP BY PointGroupID ORDER BY rows DESC""")

# CELL ********************

# Step 5 — MI_POINT_GROUP → MI_DATA_SOURCE → ConnectorID
show("Step 5: data sources", """
SELECT COUNT(*) AS data_sources, COUNT(DISTINCT DataSourceID) AS distinct_ids,
       SUM(CASE WHEN ConnectorID IS NULL THEN 1 ELSE 0 END) AS without_connector
FROM _stage_sources""")
show("Step 5: DataSourceIDs referenced but missing in MI_DATA_SOURCE", """
SELECT DataSourceID, COUNT(*) AS rows FROM _stage_result
WHERE LineageStatus = 'NO_DATA_SOURCE' GROUP BY DataSourceID ORDER BY rows DESC""")

# CELL ********************

# Step 6 — Result
status_counts = {r["LineageStatus"]: r["rows"] for r in spark.sql(
    "SELECT LineageStatus, COUNT(*) AS rows FROM _stage_result GROUP BY LineageStatus").collect()}
show("Step 6: rows by LineageStatus", """
SELECT LineageStatus, COUNT(*) AS rows FROM _stage_result GROUP BY LineageStatus ORDER BY rows DESC""")
show("Step 6: matched tags by connector", """
SELECT ConnectorID, COUNT(*) AS tags, COUNT(DISTINCT Machine) AS machines
FROM _stage_result WHERE LineageStatus = 'MATCHED' GROUP BY ConnectorID ORDER BY tags DESC""")
tags_total = scalar("SELECT COUNT(*) FROM _stage_tags")
rows_total = result.count()
fan_out_tags = scalar("""
SELECT COUNT(*) FROM (SELECT 1 FROM _stage_result WHERE PointID IS NOT NULL
                      GROUP BY PlantName, DepartmentName, TagId HAVING COUNT(*) > 1)""")
print(f"\nTags: {tags_total}; result rows: {rows_total}; tags with more than one point: {fan_out_tags}")

# CELL ********************

# Write
if dry_run:
    status = "DRY_RUN"
elif as_view:
    spark.sql(f"CREATE OR REPLACE VIEW {target} AS {QUERY}")
    status = "SUCCEEDED"
else:
    spark.sql(f"CREATE OR REPLACE TABLE {target} USING DELTA AS {QUERY}")
    status = "SUCCEEDED"
print(f"{status}: {target}")
result.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_tag_datasource_map",
    "status": status,
    "target": target,
    "as_view": as_view,
    "tags": tags_total,
    "rows": rows_total,
    "tags_with_multiple_points": fan_out_tags,
    "lineage_status": status_counts,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
