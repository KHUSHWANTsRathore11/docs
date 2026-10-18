# Smart Data per Spool: Framework Design (Microsoft Fabric)

## Status

Draft. Assumptions are marked **[Assumption]** and need confirmation before build. Open questions are at the end.

## Purpose

Build a Microsoft Fabric framework that turns raw plant tag data into **per-spool** datasets. Each consumer should be able to ask "what happened to spool X, and when?" without knowing which plant, server or tag produced the data.

A **spool** is a fabricated section of pipe (or cable/material roll) that is tracked through production and installation. **[Assumption]** Each spool has a unique `spool_id`, belongs to one plant, and is linked to tags for the time window in which it was processed or in use.

## Scope

In scope:
- Ingesting plant/server tag data from ADLS into Fabric (reusing the existing medallion design in `tags`).
- Linking tag events to spools by time window and location.
- Publishing a spool-level gold layer and a spool summary.

Out of scope for this draft:
- Front-end UI.
- Changes to the SCADA or plant systems.
- Spool master data ownership (assumed to come from an existing ERP or fabrication system; see open questions).

## Architecture

Reuses the medallion pattern already described in `tags`, extended with a spool layer.

```
ADLS (per plant/server)
   │  shortcuts, no copy
   ▼
Bronze  (raw tag events, one lane per source)
   │  config-driven notebook, control table per source
   ▼
Silver  (canonical tag events, dim_tag, all plants combined)
   │  spool linking notebook
   ▼
Gold    (spool-level datasets)
   ├── spool_master        one row per spool (from the spool source)
   ├── spool_tag_window    tag events matched to a spool and time window
   └── spool_summary       one row per spool: key metrics, status, duration
```

## Core Entities

| Entity | Key | Notes |
| :--- | :--- | :--- |
| `dim_tag` (silver) | `tag_key` | Existing. `tag_key` = plant + server + tag name. |
| `fact_tag_event` (silver) | `(tag_key, event_ts_utc)` | Existing MERGE key. |
| `dim_spool` (gold) | `spool_id` | **[Assumption]** Source: fabrication/ERP extract. |
| `bridge_spool_tag_window` (gold) | `(spool_id, tag_key, window_start_utc)` | Links tag windows to spools. |
| `spool_summary` (gold) | `spool_id` | Rebuilt or incrementally merged per run. |

## Spool Linking Rules

Tag events are linked to a spool when all of the following hold:

1. The tag belongs to the spool's plant (`plant_id` match).
2. The event time falls inside the spool's active window (`start_ts_utc` to `end_ts_utc`).
3. **[Assumption]** The tag is in the spool's location list (for example, a station or line). If location data is not available, use the time window only and mark the link `match_method = 'time_only'`.

Each link stores `match_method` and `confidence` so consumers can filter weaker matches. A tag event may match more than one spool only if their windows overlap. The rule for that case is an open question.

## Processing Pattern

Follows the existing control-table approach so new plants need no new code:

- A control table lists each source with its shortcut path, timezone, column mapping and last watermark.
- Silver and gold notebooks read the control table and process each source in a loop or with `notebookutils.notebook.runMultiple`.
- Each run MERGEs on its natural key, so reruns are idempotent.
- The watermark advances only after a successful run.

## Data Quality Checks

Run after each gold build and write results to a `dq_result` table:

- Spools with no tag events in their window.
- Tag events with no spool match, grouped by plant (informational).
- Duplicate `spool_id` in `dim_spool`.
- Events with `event_ts_utc` outside the expected range or in the future.
- Null `plant_id`, `tag_key` or `event_ts_utc` in silver.

## Security and Access

- Fabric workspace roles per layer: engineers write to bronze and silver; consumers read gold only.
- **[Assumption]** Gold is the only layer exposed to reporting (Power BI semantic model).
- No credentials in notebooks. Use Fabric connections or workspace identity.

## Folder Layout (proposed for this repo)

```
smart_data_per_spool/
  README.md
  control/            control table seed data (CSV)
  notebooks/          bronze_to_silver, silver_to_gold, link_spool_tags
  sql/                table DDL and views
  dq/                 data quality checks
```

Nothing is created yet beyond this document.

## Open Questions

1. Where does spool master data come from (ERP, fabrication system, spreadsheet)? Which fields are reliable?
2. What defines a spool's active window: installation dates, production steps, or both?
3. Do tags map to spools by station or by location, and is that mapping available?
4. How should a tag event that overlaps two spools be attributed?
5. What "smart" outputs are needed first: traceability, cost per spool, quality/defect history, or anomaly detection?
6. Expected data volume and refresh frequency (batch or near real time)?
7. Who owns the Fabric workspace and the gold layer?

## Next Steps

1. Answer the open questions above, mainly 1 to 3.
2. Confirm the spool entity and its key with the team.
3. Create the control table seed and the `dim_spool` schema.
4. Build the linking notebook for one plant as a pilot, then add plants through the control table.
