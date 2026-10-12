# Fabric ETL Architecture & Claude Code Guidelines (DRAFT)

> **Status**: DRAFT - Work in progress for review & discussion  
> **Target System**: Microsoft Fabric (Lakehouse Medallion + Fabric SQL Database)

---

## 1. High-Level Workspace Architecture

The workspace consists of three Medallion lakehouses and one relational database:
* **Bronze Lakehouse**: Raw ingestion layer hosting external shortcuts.
* **Silver Lakehouse**: Conformed, unified time-series Delta tables and metadata/dimension tables.
* **Gold Lakehouse**: Aggregated / curated business layer for analytics & Direct Lake semantic models.
* **Fabric SQL Database**: Operational query store / downstream relational serving / Control Layer host.

---

## 2. Ingestion Sources (Bronze Shortcuts)

Source shortcuts are located under `Files/` in the Bronze Lakehouse:

### 2.1 Time-Series Telemetry Sources
1. **`ilayer_SCADA` (Incremental Telemetry)**:
   * **Path Pattern**:  
     `Files/ilayer_SCADA/{Plant}/{Department}/{Year}/{Month}/{Day}/{TableName}/*.parquet`
   * **Characteristics**: Chunked intraday parquet files.
   * **Usage**: Daily incremental ingestion window `[X-1 00:00:00 UTC, X 00:00:00 UTC)`.

2. **`rlayer_SCADA` (Historical Telemetry)**:
   * **Path Pattern**:  
     `Files/rlayer_SCADA/{Plant}/{Department}/{Year}/{Month}/{Day}/{TableName}.parquet`
   * **Characteristics**: Consolidated single parquet files.
   * **Usage**: One-time historical onboarding load for new Plants and Departments.

### 2.2 Metadata & Dimension Sources
* **Path Pattern**:  
  `Files/rlayer_SCADA/{Plant}/{Department}/{TableName}.parquet`
* **Characteristics**: Single consolidated parquet file per table, refreshed continuously on-premise, exported as a **daily full snapshot overwrite**.
* **Key Example**: `TAGS` table containing tag-level definitions for telemetry logged in `TAGLOG`.
* **Notable Fields**: Contains both configuration fields (`TagId`, `Description`, `Unit`, `EGU`, `PLC_Connection`, `MESdb_Connection`) and operational/volatile fields (`LastValue`, `LastLog`, `Quality`).

---

## 3. Bronze to Silver ETL Pipelines (Modular Notebooks)

To maintain strict separation of concerns, high maintainability, and clean pipeline debugging, the Bronze $\rightarrow$ Silver processing is implemented across **3 Modular Notebooks**, plus Silver $\rightarrow$ Silver notebooks for spool boundaries and tag lineage:

```
Fabric ETL/
├── notebooks/
│   ├── nb_telemetry_historical.py   <-- [Notebook 1] One-time onboarding load
│   ├── nb_telemetry_incremental.py  <-- [Notebook 2] Daily scheduled incremental [X-1, X)
│   ├── nb_metadata_sync.py          <-- [Notebook 3] Daily SCD1 metadata/dimension sync
│   ├── nb_spool_boundaries.py       <-- [Notebook 4] Daily spool boundaries from LASTSpoolPV (Silver -> Silver)
│   └── nb_tag_datasource_map.py     <-- [Notebook 5] Tag -> MI point -> group -> data source / connector view
├── utils/
│   └── nb_lib_delta_helpers.py      <-- Shared logic (time columns, clustering, Z-Order, merge)
└── tests/
    └── smoke_test_local.py          <-- Local PySpark + Delta end-to-end test
```

---

### 3.1 Notebook 1: `nb_telemetry_historical` (Historical Onboarding)

* **Purpose**: One-time onboarding load for any new `{Plant}/{Department}`.
* **Source**: `p_source_path`, the full path passed by the caller (globs allowed), e.g. `Files/rlayer_SCADA/{Plant}/{Department}/*/*/*/{TableName}.parquet`
* **Target**: `p_target_table`, e.g. `silver.TAGLOG`
* **Write Mode**: `.mode("append")`
* **Transformation & Layout**:
  * `PlantName` / `DepartmentName` come from the source files (column names in `p_plant_column` / `p_department_column`).
  * Derives `Year = year(col("Time"))`, `Month = month(col("Time"))`, `Date = to_date(col("Time"))`.
  * Writes to a table **liquid-clustered** by `(Date, PlantName, DepartmentName, <p_id_column>)`; no partitioning (see 7.6).
* **Optimization**:
  * Runs `OPTIMIZE <p_target_table>` (incremental clustering on Fabric Runtime 2.0).
* **Parameters**:
  ```python
  p_source_path    = "Files/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet"
  p_target_table   = "silver.TAGLOG"
  p_plant_column      = "PlantName"
  p_department_column = "DepartmentName"
  p_id_column      = "TagId"
  p_time_column    = "Time"
  p_dry_run        = False
  ```

---

### 3.2 Notebook 2: `nb_telemetry_incremental` (Daily Incremental)

* **Purpose**: Scheduled daily run triggered at `X 00:00:00 UTC` to process telemetry from `[X-1 00:00:00 UTC, X 00:00:00 UTC)`.
* **Source**: `p_source_path`, the full path built by the pipeline from the tumbling window start, e.g. `Files/ilayer_SCADA/{Plant}/{Department}/{Year}/{Month}/{Day}/{TableName}/*.parquet`.
* **Source contract**: one path holds exactly one day, `00:00:00` to `23:59:59.999` UTC.
* **Target**: `p_target_table`, e.g. `silver.TAGLOG`
* **Write Mode**: `.mode("append")`
* **Transformation & Layout**:
  * `PlantName` / `DepartmentName` come from the source files (column names in `p_plant_column` / `p_department_column`).
  * Derives `Year`, `Month`, `Date` strictly from column `Time`.
  * Appends to a table **liquid-clustered** by `(Date, PlantName, DepartmentName, <p_id_column>)`; no partitioning (see 7.6).
* **Optimization**:
  * Runs `OPTIMIZE <p_target_table>`, which clusters only the data written since the last run on Fabric Runtime 2.0. A failed `OPTIMIZE` does not fail the run (see 7.4).
* **Parameters**:
  ```python
  p_source_path    = "Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet"
  p_target_table   = "silver.TAGLOG"
  p_plant_column      = "PlantName"
  p_department_column = "DepartmentName"
  p_id_column      = "TagId"
  p_time_column    = "Time"
  p_dry_run        = False
  ```

---

### 3.3 Notebook 3: `nb_metadata_sync` (Daily Metadata Sync - SCD Type 1)

* **Purpose**: Daily sync of metadata / configuration tables (e.g., `TAGS`).
* **Source**: `p_source_path`, the full path of the daily full snapshot, e.g. `Files/rlayer_SCADA/{Plant}/{Department}/{TableName}.parquet`.
* **Target**: `p_target_table`, e.g. `silver.TAGS`
* **Implementation Strategy**: **SCD Type 1 (Delta `MERGE` on Primary Key)**.
  * Natural/Business Key: `(PlantName, DepartmentName, <p_id_column>)`.
  * Merge Pattern:
    ```sql
    MERGE INTO <p_target_table> AS target
    USING staged_daily_metadata AS source
    ON target.PlantName = source.PlantName 
       AND target.DepartmentName = source.DepartmentName 
       AND target.<p_id_column> = source.<p_id_column>
    WHEN MATCHED THEN 
      UPDATE SET *
    WHEN NOT MATCHED THEN 
      INSERT *;
    ```
* **Layout**: no partitioning; after the merge runs `OPTIMIZE <p_target_table> ZORDER BY (PlantName, DepartmentName)`.
* **Parameters**:
  ```python
  p_source_path    = "Files/rlayer_SCADA/PlantA/Boiler/TAGS.parquet"
  p_target_table   = "silver.TAGS"
  p_plant_column      = "PlantName"
  p_department_column = "DepartmentName"
  p_id_column      = "TagId"
  p_dry_run        = False
  ```

---

### 3.4 Notebook 4: `nb_spool_boundaries` (Spool Boundaries)

* **Purpose**: one row per spool, so spool characteristics can be derived from `TAGLOG` without changing it. Runs daily after Notebooks 2 and 3.
* **Why a separate table** instead of a `SpoolId` column on `TAGLOG`: a reading's spool is only known when the spool ends (possibly the next day), so stamping it would turn the append-only telemetry table into a daily `MERGE` over its largest data; boundary corrections would rewrite telemetry; and not every tag belongs to a spool.
* **Spool tags**: one LASTSpoolPV tag per machine/line/position, found in `TAGS` by `Description = p_spool_tag_description` (trimmed, case-insensitive), never by a hardcoded `TagId`. `(PlantName, DepartmentName, TagId)` of that tag identifies the spool series; nothing lower-level is modelled.
* **Boundaries**: every telemetry row of a spool tag is a spool end. A spool covers `SpoolStartTime < Time <= SpoolEndTime`, where the start is the previous end of the same tag; the first event of a tag has a null start.
* **Target**: `p_target_table` (e.g. `silver.SPOOL`):

  | Column | Meaning |
  |---|---|
  | `SpoolKey` | SHA-256 of `PlantName\|DepartmentName\|SpoolTagId\|SpoolEndTime` |
  | `PlantName`, `DepartmentName`, `SpoolTagId` | the spool series (one per machine/line/position) |
  | `SpoolStartTime`, `SpoolEndTime` | `(previous end, this end]` |
  | `LastSpoolPV` | the LASTSpoolPV value at the spool end |
  | `DurationSec`, `Date` | spool length; date of the end |

* **Incremental**: continues from the last `SpoolEndTime` per spool tag already in the target (spools spanning midnight work), `MERGE` on `(PlantName, DepartmentName, SpoolTagId, SpoolEndTime)`, so reruns are idempotent. Telemetry is pruned by `Date` when every spool tag already has spools. Events older than the last known end are not picked up: run with `p_full_rebuild = True` (recompute and overwrite) after a historical (re)load or a correction.
* **Layout**: unpartitioned, `OPTIMIZE ... ZORDER BY (PlantName, DepartmentName, SpoolTagId)`.
* **Spool characteristics**: range join, filtered to the spool's own sensors:
  ```sql
  SELECT s.SpoolKey, t.TagId, AVG(t.Value), MIN(t.Value), MAX(t.Value), COUNT(*)
  FROM   silver.SPOOL s
  JOIN   silver.TAGLOG t
    ON   t.PlantName = s.PlantName AND t.DepartmentName = s.DepartmentName
   AND   t.Time > s.SpoolStartTime AND t.Time <= s.SpoolEndTime
   AND   t.Date BETWEEN to_date(s.SpoolStartTime) AND s.Date   -- lets clustering skip files
  WHERE  t.TagId IN (<the position's tags>)
  GROUP BY s.SpoolKey, t.TagId
  ```
  Materialise this per completed spool into Gold (`fct_spool_tag_stats`) for reporting.
* **Parameters**:
  ```python
  p_taglog_table          = "silver.TAGLOG"
  p_tags_table            = "silver.TAGS"
  p_target_table          = "silver.SPOOL"
  p_spool_tag_description = "LASTSpoolPV"
  p_plant_column          = "PlantName"
  p_department_column     = "DepartmentName"
  p_id_column             = "TagId"
  p_time_column           = "Time"
  p_value_column          = "Value"
  p_description_column    = "Description"
  p_full_rebuild          = False
  p_dry_run               = False
  ```
* **Open**: which telemetry tags belong to each spool position (for the `IN (...)` above) is not modelled yet.

---

### 3.5 Notebook 5: `nb_tag_datasource_map` (Tag → Data Source / Connector)

* **Purpose**: map each tag to its MI point, point group, data source and connector, with a step-by-step analysis of every join.
* **Join chain** (all LEFT joins from `TAGS`, so no tag is dropped):
  ```
  TAGS.DBTag                  = <OpcPointName> in MI_POINT.Parameter (XML) -> MI_POINT.ID, PointGroupID
  MI_POINT.PointGroupID       = MI_POINT_GROUP.ID                          -> DataSourceID
  MI_POINT_GROUP.DataSourceID = MI_DATA_SOURCE.ID                          -> ConnectorID
  ```
* **Output columns**: `PlantName, DepartmentName, TagId, DBTag, Machine, Characteristic, PointID, PointGroupID, DataSourceID, ConnectorID, LineageStatus`.
  * `DBTag` (`Machine.Characteristic`) is matched in full, trimmed and case-sensitive; `Machine` / `Characteristic` are split at the first `.`.
  * `LineageStatus` = `NO_DBTAG`, `NO_POINT`, `NO_POINT_GROUP`, `NO_DATA_SOURCE` or `MATCHED`: where the chain breaks.
  * A DBTag matching several points yields one row per point (fan-out), reported by the analysis.
* **XML**: `<OpcPointName>` is extracted with a regular expression, not an XML parser, so one malformed document yields NULL (reported) instead of failing the query. Handles a namespace prefix, attributes, surrounding whitespace and the five standard entities; CDATA is not supported.
* **Analysis** (printed, steps 1–6): DBTag coverage and DBTags shared across plants/departments; XML extraction failures and duplicate `OpcPointName`s; unmatched DBTags with case/whitespace near-matches; fan-out tags; missing point groups and data sources; final counts by status and by connector.
* **Output**: `p_as_view = True` (default) creates a persisted Spark view, visible to Spark only (not to the SQL analytics endpoint or Direct Lake); `False` writes a Delta table with the same query.
* **Parameters**: table names (`p_tags_table`, `p_point_table`, `p_point_group_table`, `p_data_source_table`, `p_target`), every column name (defaults `DBTag`, `ID`, `PointGroupID`, `Parameter`, `DataSourceID`, `ConnectorID`), `p_xml_element = "OpcPointName"`, `p_as_view`, `p_dry_run` (analysis only).

---

## 4. Parameterized & Decoupled Execution Pattern

* **No Static Config Files**: Configuration is metadata-driven via the upstream Control Layer.
* **Decoupled Notebooks**:
  * Fabric Data Factory / Pipelines extract metadata and pass parameters to Notebooks.
  * Notebooks build **no paths and no table names**: the caller passes the full source path (`p_source_path`) and the full target table (`p_target_table`), so the same notebooks serve any source layout or target.
  * Notebooks contain **no hardcoded SQL connection strings** or database drivers.
  * Notebooks support **standalone / manual execution** with default fallback parameters.

---

## 5. Architectural Evaluation & Future Considerations (Preserved Discussions)

### 5.1 The SCADA Metadata Challenge: Volatile Columns vs. Configuration History
The `TAGS` metadata table combines two opposing data velocities:
1. **Slowly Changing Configurations**: `PLC_Connection`, `MESdb_Connection`, `Unit`, `EGU`, `Description`, `DataType`.
2. **Rapidly Changing Telemetry State**: `LastValue`, `LastLog`, `Quality`.

> **Risk of Full-Row SCD2**: Hashing the whole row results in 100% of tags creating new history rows every single day due to `LastValue` / `LastLog`, causing massive table bloat without adding analytical value.

### 5.2 Evaluated Alternatives for Future Evolution
While starting with **SCD Type 1**, the following architectures have been evaluated for future transition:

* **Alternative 1: Hybrid / Selective SCD2**:
  * **Tracked Columns**: Hash check only configuration fields (`PLC_Connection`, `MESdb_Connection`, `Unit`, `EGU`, `DataType`).
  * **Volatile Columns**: `LastValue`, `LastLog`, `Quality` are excluded from triggering new rows (either updated in-place on `is_current = true` or discarded).
  * **Result**: Point-in-time accuracy for unit/PLC changes without row bloat.

* **Alternative 2: Partitioned Daily Snapshot (`silver.tags_daily_snapshot`)**:
  * Append-only table partitioned by `(PlantName, snapshot_date)`.
  * Zero merge overhead; Parquet dictionary encoding compresses repeating daily snapshots by >90%.
  * Historical comparisons and current state are queried via standard date filtering or SQL window functions (`LAG`).

* **Alternative 3: Handling Retired, Dormant, or Replaced Tags**:
  * **Deletions / Disappeared Tags**: Perform an anti-join (`WHERE target.TagId NOT IN (SELECT TagId FROM source_today)`) to flag deleted tags as `is_active = false` or `lifecycle_status = 'RETIRED'`.
  * **Dormant / Abandoned Tags** (tags replaced on shop floor without being deleted): Cross-reference `TAGLOG` to flag tags with no telemetry in the last 30 days (`last_seen_telemetry < NOW() - 30 days`) as `lifecycle_status = 'DORMANT'`.

---

## 6. Open Topics for Next Iterations
1. **Date Partition Format**: Format of `Date` (`YYYY-MM-DD` string vs. integer day `1-31`).
2. **Gold Layer Modeling**: Downsampling telemetry (e.g., 1-minute averages, 15-minute rollups, daily max/min/avg) and dimensional models.
3. **Control Layer Architecture**: Schema for the Control tables in the Fabric SQL Database (job logs, pipeline parameters, watermark tracking).
4. **Error Handling & Failure Recovery**: Reprocessing missed daily runs and idempotent retry patterns.

---

## 7. Implementation Notes (as built)

The files under `notebooks/` and `utils/` implement sections 3 and 4. Where the build had to
decide something the spec left open, or the spec could not run as written, it is recorded here.

### 7.1 Notebook format & shared code
* Files use the Fabric notebook source format (`# Fabric notebook source`, `# PARAMETERS CELL`, `# CELL`), so they import into a Fabric workspace as notebooks and remain valid Python.
* `utils/nb_lib_delta_helpers.py` is imported as a notebook named `nb_lib_delta_helpers`; each ETL notebook pulls it in with `%run nb_lib_delta_helpers`.
  * `%run` resolves the notebook by name within the same workspace when the cell runs and executes it in the caller's Spark session (no extra session or pool). Names are unique per workspace, renames are not propagated to `%run` cells, and each workspace (dev / test / prod) needs its own copy.
  * Shared helpers use the `nb_lib_` prefix. The helpers are workload-agnostic and shared by every workload in the workspace; workload-specific logic goes in its own `nb_lib_<workload>_helpers` notebook rather than changing existing shared functions.
* Relative `Files/...` source paths resolve against the default lakehouse (attach Bronze); `abfss://...` paths work with any lakehouse attached. The target is addressed exactly as passed in `p_target_table`.
* Each notebook ends with `notebookutils.notebook.exit(<json>)` carrying status (`SUCCEEDED` / `DRY_RUN` / `NO_DATA`), row counts and timings, for the pipeline / Control Layer to log.

### 7.2 Generic parameters
The notebooks take full locations instead of building them, so they can be reused for other sources and targets:

| Parameter | Notebooks | Purpose |
|---|---|---|
| `p_source_path` | all | Full source path handed to `spark.read.parquet` as is; globs allowed (`*`, `{a,b}`) |
| `p_target_table` | all | Target Delta table: `table`, `lakehouse.table` or `lakehouse.schema.table` |
| `p_plant_column`, `p_department_column` | all | Names of the plant / department columns already in the source (default `PlantName` / `DepartmentName`); clustering keys (1, 2), merge and Z-Order keys (3) |
| `p_id_column` | all | Clustering key (1, 2) / business key (3) |
| `p_time_column` | 1, 2 | Event timestamp column (UTC), default `Time` |

Column names are matched case-insensitively, as Spark does (`TagId` finds a source `TagID`), and the notebooks then use the source's own spelling.

`p_target_table` and the column names are validated as identifiers (`^[A-Za-z_][A-Za-z0-9_]*$` per part) before anything runs, since they are interpolated into SQL. Plant / department values read from the data are escaped as SQL literals. `p_source_path` is only checked for being non-empty.

### 7.3 Deviations from the section 3 spec
1. **Liquid clustering instead of partitioning + Z-Order** for the telemetry tables (Notebooks 1 and 2); see 7.6 for the measurements behind it. The first write creates the table with `CLUSTER BY`; a run against an existing table with a different layout (e.g. the earlier partitioned one) fails instead of appending to it.
2. **Idempotent onboarding.** Notebook 1 appends through Delta `replaceWhere`, which atomically replaces only the slice being loaded (the plant/department combinations found in the data + loaded date range), so re-running an onboarding does not duplicate history. It also drops rows with a null `Time`, since they have no `Date`, and counts them in the exit payload. Notebook 2 is a plain append, as specified (see 7.4).
3. **No date or plant parameters.** Notebook 2 takes the plant, department and day from the data; `OPTIMIZE` needs no scope because liquid clustering only processes unclustered data. `Year` and `Month` are still derived for downstream use but are no longer part of the layout.
4. **Metadata staging.** Notebook 3 drops rows with a null business key and de-duplicates on `(PlantName, DepartmentName, <p_id_column>)` before the MERGE, because a MERGE fails when several source rows match one target row. Schema auto-merge is enabled so new snapshot columns flow through `UPDATE SET *` / `INSERT *`. The first run creates the table from the snapshot.
5. **Plant / department from the source.** The source files already carry the plant and department columns, so the notebooks inject nothing; they only take the column names. A source missing either column fails the run.
6. **Schema evolution.** Telemetry writes use `mergeSchema = true`, so a new source column is added to the Silver table rather than failing the load.

### 7.4 Scheduling & reruns (Notebook 2)
* The pipeline runs Notebook 2 from a daily **tumbling window**, formats the window start into the day's folder and passes only `p_source_path`.
* The notebook reads the path, derives `Year` / `Month` / `Date` from `Time`, appends to the clustered table and runs `OPTIMIZE`. A source with no rows ends as `NO_DATA`.
* **Reruns.** The append is atomic: a run that fails before or during the write leaves nothing behind and can simply be re-run. Once the append has committed, the run is `SUCCEEDED` even if the following `OPTIMIZE` fails; the error is reported in `optimize_error` in the exit payload and the next run's `OPTIMIZE` clusters the data. So a failed run never has committed data, and re-running a failed window never duplicates it.
* **Concurrency.** Appends from parallel runs (other plants, departments or days) do not conflict with each other. Two `OPTIMIZE`s on the same table at once can conflict; with the rule above that only shows up as `optimize_error`, not as a failed run.

### 7.5 Decisions still pending (section 6)
* `Date` is written as a `DATE` (`yyyy-MM-dd`); changing it touches only `add_time_columns`.
* A missing source path fails the run (Spark `PATH_NOT_FOUND`); whether an empty day should be `NO_DATA` instead depends on the Control Layer design.

### 7.6 Telemetry table layout: liquid clustering (decision record)
**Decision:** telemetry Silver tables use liquid clustering by `(Date, PlantName, DepartmentName, TagId)` with no partitioning, maintained by `OPTIMIZE <table>`. This **requires Fabric Runtime 2.0**: on Runtime 1.3 (Delta 3.2) `OPTIMIZE` on a clustered table rewrites every file of a Z-Cube until it exceeds 100 GB, i.e. the whole table each run; Runtime 2.0 clusters incrementally. Metadata tables (`TAGS`) stay unpartitioned with `ZORDER BY (PlantName, DepartmentName)`.

**Context:** plants differ (department sets differ, the same department name can exist in several plants), `TagId` is an integer index that restarts per plant/department, and one plant/department/day is about 120–160 MB of parquet.

**Measurement** (local Delta 3.2, synthetic data shaped like the above: 3 plants, 8 plant/departments, TagIds reused, 14 days, 4.2 M rows / 43 MB, 2 MB target file size, one append + `OPTIMIZE` per plant/department/day as Notebook 2 does). Compare ratios, not absolute sizes.

| | Partition `(Plant, Year, Month, Date)` + Z-Order `(Dept, TagId)` | Liquid `(Date, Plant, Dept, TagId)`, OPTIMIZE per append | Liquid, weekly OPTIMIZE |
|---|---|---|---|
| Data rewritten by OPTIMIZE (14 days) | 84 MB (2× table) | 2,406 MB (57× table, Delta 3.2 full rewrite) | 22 MB |
| MB read: one tag, one plant/dept, one day | 0.88 | 2.24 | 0.30 |
| MB read: one tag, one plant/dept, 14 days | 12.3 (14 files) | **2.24 (1 file)** | 4.3 |
| MB read: TagId 42 in every plant/dept, 7 days | 21.6 | **2.2** | 21.9 |
| MB read: one plant, one day | **1.0** | 42.4 (whole table) | 1.05 |
| MB read: everything, one day | **3.1** | 42.4 (whole table) | 3.1 |

**Reading:**
* Liquid clustering reads 5–10× less for tag-history queries, which grow with retention (a partitioned year-long trend reads ≥ 365 files).
* At this small scale the fully clustered table spread each day over all files, so day-scoped queries read everything; with production volumes (GBs per day, many more files) day pruning improves, but this was not measurable locally. Each append already holds one plant/department/day, so unclustered new data prunes well by plant, department and date.
* The Runtime 1.3 rewrite cost is the main drawback, and Runtime 2.0 removes it. At 120–160 MB per plant/department/day, partitioning would not have produced small files, so file size did not decide the choice.

**Query guidance:** always filter `PlantName` + `DepartmentName` + `TagId` together; `TagId` alone matches the same index in every plant/department. A combined tag key (e.g. `PlantName|DepartmentName|TagId`) is recommended for Gold models and joins.

