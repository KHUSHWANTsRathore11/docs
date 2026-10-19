# Table Details

Column details for each source table. Columns are listed as provided. Notes are added as they are confirmed.

## SCADA

### Working Source (Silver)

For now, the logic uses the consolidated Silver tables, built by hand from the Bronze shortcuts:

- `scada_tags` (SCADA TAGS)
- `scada_taglog` (SCADA TAGLOG)

The MES tables are also available in Silver. The Bronze to Silver ETL for SCADA is out of scope for now and will be built later.

### Bronze Shortcut Locations (confirmed)

Shortcuts are under `Files/` in the Bronze lakehouse. Each plant and department has its own folder. Nothing is copied.

| Source | Path pattern | Content | Load |
| :--- | :--- | :--- | :--- |
| Incremental telemetry | `Files/ilayer_SCADA/{Plant}/{Department}/{Year}/{Month}/{Day}/{TableName}/*.parquet` | Chunked intraday parquet files | Daily |
| Historical telemetry | `Files/rlayer_SCADA/{Plant}/{Department}/{Year}/{Month}/{Day}/{TableName}.parquet` | One consolidated file per day | One-time onboarding |
| Metadata (TAGS) | `Files/rlayer_SCADA/{Plant}/{Department}/{TableName}.parquet` | One file per table, daily full snapshot | Daily |

### TAGS

| Column | Notes |
| :--- | :--- |
| TagId | |
| PlantName | |
| DepartmentName | |
| Location | |
| Part | |
| Description | |
| DbTag | |
| ... | More columns, to be provided |

### TAGLOG

| Column | Notes |
| :--- | :--- |
| TaglogId | |
| TagId | |
| Value | |
| Time | |
| PlantName | |
| DepartmentName | |

## MES

### MI_POINT

| Column | Notes |
| :--- | :--- |
| Id | |
| PointGroupId | |
| Parameters | XML. The tag name is in the `<OPCpointName>` element. |
| PlantName | MES plant name. |
| ... | More columns, to be provided |

### MI_POINT_GROUP

| Column | Notes |
| :--- | :--- |
| Id | |
| DataSourceId | |
| ... | More columns, to be provided |

### MI_DATA_SOURCE

| Column | Notes |
| :--- | :--- |
| Id | |
| ConnectorId | |
| ... | More columns, to be provided |

### MI_CONNECTOR

| Column | Notes |
| :--- | :--- |
| Id | |
| HostName | |
| *(MES plant)* | Column name to be provided. Used by the plant mapping. |
| ... | More columns, to be provided |

### Unit_Characteristic

Stored in rows, with one column per UnitId.

| Column | Notes |
| :--- | :--- |
| UnitId | |
| Characteristic | |
| Value_ | |
| ... | More columns, to be provided |

### Resource_

| Column | Notes |
| :--- | :--- |
| UnitId | |
| ... | Machine name or ID column, to be provided |

## To Be Provided

- Remaining TAGS columns.
- Remaining MI_POINT columns.
- Columns of MI_POINT_GROUP, MI_DATA_SOURCE, MI_CONNECTOR, Unit_Characteristic and Resource_, beyond the join keys.
- Confirm the spelling of `Value_`.
- Which column in Resource_ holds the machine name or ID.
- Whether TagId is unique across plants. If not, the key must include plant.
- Whether TAGLOG.Time is local plant time or UTC.
- Whether TAGLOG.Value is only numeric or also holds text or booleans.
- Whether TAGLOG is append-only, or readings can be corrected or deleted in SCADA.
