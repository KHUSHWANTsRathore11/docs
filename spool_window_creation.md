# Spool Window Creation

Part of the Smart Data per Spool framework. See `smart_data_per_spool_framework.md` for the source tables and the MES join chain.

This section covers building a table of spool windows. Two logics are planned. Details are to be provided.

## Logic 1: Historical Data

Builds spool windows from data that already exists.

To be provided:
- Source of the historical spool information.
- Rules for starting and ending a spool window.
- Which TAGS and TAGLOG data is used for each window.
- Handling of spools with no tag data in their window.

## Logic 2: Future Data

Builds spool windows as new data arrives. Also covers the case where spool information becomes available in SCADA.

To be provided:
- When spool information in SCADA becomes available, and its columns.
- Rules for starting and ending a spool window from live data.
- How new windows are added and how existing windows are updated.
- Refresh frequency.

## Joining Spool Windows to MES Data

To be provided:
- MES tables that hold spool data.
- Join keys between spool windows and MES data.
- Which Machine and Plant values are used when a spool is linked to MES.

## Output Table

To be provided:
- Table name and columns.
- Key of the spool window table.
- Target layer in the framework.
