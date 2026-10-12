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

## 3. Bronze to Silver ETL Pipelines (3-Notebook Architecture)

To maintain strict separation of concerns, high maintainability, and clean pipeline debugging, the Bronze $\rightarrow$ Silver processing is implemented across exactly **3 Modular Notebooks**:

```
Fabric ETL/
├── notebooks/
│   ├── nb_telemetry_historical.py   <-- [Notebook 1] One-time onboarding load
│   ├── nb_telemetry_incremental.py  <-- [Notebook 2] Daily scheduled incremental [X-1, X)
│   └── nb_metadata_sync.py          <-- [Notebook 3] Daily SCD1 metadata/dimension sync
├── utils/
│   └── delta_helpers.py             <-- Shared logic (partition derivation, Z-Order)
└── tests/
    └── smoke_test_local.py          <-- Local PySpark + Delta end-to-end test
```

---

### 3.1 Notebook 1: `nb_telemetry_historical` (Historical Onboarding)

* **Purpose**: One-time onboarding load for any new `{Plant}/{Department}`.
* **Source**: `p_source_path`, the full path passed by the caller (globs allowed), e.g. `Files/rlayer_SCADA/{Plant}/{Department}/*/*/*/{TableName}.parquet`
* **Target**: `p_target_table`, e.g. `silver.TAGLOG`
* **Write Mode**: `.mode("append")`
* **Transformation & Partitioning**:
  * `PlantName` / `DepartmentName` come from the source files (column names in `p_plant_column` / `p_department_column`).
  * Derives `Year = year(col("Time"))`, `Month = month(col("Time"))`, `Date = to_date(col("Time"))`.
  * Appends partitioned by `(PlantName, Year, Month, Date)`.
* **Optimization**:
  * Runs scoped Z-Order:
    ```sql
    OPTIMIZE <p_target_table>
    WHERE PlantName = '<plant>' AND DepartmentName = '<department>'
    ZORDER BY (DepartmentName, <p_id_column>);
    ```
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
* **Transformation & Partitioning**:
  * `PlantName` / `DepartmentName` come from the source files (column names in `p_plant_column` / `p_department_column`).
  * Derives `Year`, `Month`, `Date` strictly from column `Time`.
  * Appends partitioned by `(PlantName, Year, Month, Date)`.
* **Optimization**:
  * Runs partition-scoped Z-Order for the processed date:
    ```sql
    OPTIMIZE <p_target_table>
    WHERE PlantName = '<plant>' 
      AND Year = {target_year} 
      AND Month = {target_month} 
      AND Date = '{target_date}'
    ZORDER BY (DepartmentName, <p_id_column>);
    ```
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
* `utils/delta_helpers.py` is imported as a notebook named `delta_helpers`; each ETL notebook pulls it in with `%run delta_helpers`.
* Relative `Files/...` source paths resolve against the default lakehouse (attach Bronze); `abfss://...` paths work with any lakehouse attached. The target is addressed exactly as passed in `p_target_table`.
* Each notebook ends with `notebookutils.notebook.exit(<json>)` carrying status (`SUCCEEDED` / `DRY_RUN` / `NO_DATA`), row counts and timings, for the pipeline / Control Layer to log.

### 7.2 Generic parameters
The notebooks take full locations instead of building them, so they can be reused for other sources and targets:

| Parameter | Notebooks | Purpose |
|---|---|---|
| `p_source_path` | all | Full source path handed to `spark.read.parquet` as is; globs allowed (`*`, `{a,b}`) |
| `p_target_table` | all | Target Delta table: `table`, `lakehouse.table` or `lakehouse.schema.table` |
| `p_plant_column`, `p_department_column` | all | Names of the plant / department columns already in the source (default `PlantName` / `DepartmentName`); plant is the partition column, both are merge keys |
| `p_id_column` | all | Z-Order column (1, 2) / business key (3) |
| `p_time_column` | 1, 2 | Event timestamp column (UTC), default `Time` |

`p_target_table` and the column names are validated as identifiers (`^[A-Za-z_][A-Za-z0-9_]*$` per part) before anything runs, since they are interpolated into SQL. Plant / department values read from the data are escaped as SQL literals. `p_source_path` is only checked for being non-empty.

### 7.3 Deviations from the section 3 spec
1. **Notebook 1 OPTIMIZE scope.** Delta only accepts partition columns in `OPTIMIZE ... WHERE`, and `DepartmentName` is not a partition column, so the spec's predicate fails. The notebook scopes by the loaded plants and `Date` range; the department column is still clustered by the Z-Order.
2. **Idempotent onboarding.** Notebook 1 appends through Delta `replaceWhere`, which atomically replaces only the slice being loaded (the plant/department combinations found in the data + loaded date range), so re-running an onboarding does not duplicate history. It also drops rows with a null `Time`, since they cannot be partitioned, and counts them in the exit payload. Notebook 2 is a plain append, as specified (see 7.4).
3. **Z-Order scope from the data.** Notebook 2 takes no date or plant parameter: it Z-Orders the `(plant, Year, Month, Date)` partitions present in the loaded rows, which is the one day of the source path.
4. **Metadata staging.** Notebook 3 drops rows with a null business key and de-duplicates on `(PlantName, DepartmentName, <p_id_column>)` before the MERGE, because a MERGE fails when several source rows match one target row. Schema auto-merge is enabled so new snapshot columns flow through `UPDATE SET *` / `INSERT *`. The first run creates the table from the snapshot.
5. **Plant / department from the source.** The source files already carry the plant and department columns, so the notebooks inject nothing; they only take the column names. A source missing either column fails the run.
6. **Schema evolution.** Telemetry writes use `mergeSchema = true`, so a new source column is added to the Silver table rather than failing the load.

### 7.4 Scheduling & reruns (Notebook 2)
* The pipeline runs Notebook 2 from a daily **tumbling window**, formats the window start into the day's folder and passes only `p_source_path`.
* The notebook reads the path, derives `Year` / `Month` / `Date` from `Time`, appends partitioned by `(PlantName, Year, Month, Date)` and Z-Orders that day's partition. A source with no rows ends as `NO_DATA`.
* **Reruns.** The append is atomic: a run that fails before or during the write leaves nothing behind and can simply be re-run. A run that fails *after* the append committed (i.e. during `OPTIMIZE`) has already loaded its data; re-running it appends the day a second time. Check the run's error before re-running, or re-run only the `OPTIMIZE`.
* Keep trigger concurrency at 1 per target table (or per plant). Two writes into the same `(PlantName, Year, Month, Date)` partition at once, such as two departments of one plant on the same day, can fail with a Delta concurrent-write conflict. The failed run can simply be re-run.

### 7.5 Decisions still pending (section 6)
* `Date` is written as a `DATE` (`yyyy-MM-dd`); changing it touches only `add_time_partitions`.
* A missing source path fails the run (Spark `PATH_NOT_FOUND`); whether an empty day should be `NO_DATA` instead depends on the Control Layer design.
