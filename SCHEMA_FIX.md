# Schema Column Mapping Fix - CRITICAL DISCOVERY

## Problem
The code incorrectly assumed different tables used different column names (`incident` vs `object`). After inspecting the actual schema, **ALL tables use the `object` column for data storage**.

## Actual Schema (Verified)

From running `check_table_schema.py`, **ALL 6 tables have identical structure**:

```
org_id      - partition_key (text)
partition   - partition_key (text)  
created_at  - clustering key (timeuuid)
object      - regular column (text) ← THE DATA COLUMN
```

This applies to:
- ✅ `objects`
- ✅ `reports`
- ✅ `incidents`
- ✅ `incident_summary`
- ✅ `partition_index`
- ✅ `incident_to_partition`

## Solution Applied

Simplified the column mapping logic in `migrator_optimized.py` (around line 322-327):

### Before (WRONG):
```python
if table_name in ["reports", "objects", "incident_summary"]:
    data_col = 'object'
elif table_name in ["incidents", "incident_to_partition", "partition_index"]:
    data_col = 'incident'  # ← THIS WAS WRONG!
else:
    logging.error(f"Unknown table structure for {table_name}")
    return
```

### After (CORRECT):
```python
# IMPORTANT: Based on actual schema inspection, ALL tables use 'object' column!
# Schema shows: org_id, partition, created_at, object (for all 6 tables)
data_col = 'object'
logging.info(f"Using data column '{data_col}' for {table_name}")
```

## Column Mappings by Table (CORRECTED)

| Table Name               | Data Column Name | Previous Assumption | Status |
|-------------------------|------------------|---------------------|--------|
| `objects`               | `object`         | `object` ✓          | Was correct |
| `reports`               | `object`         | `object` ✓          | Was correct |
| `incidents`             | **`object`**     | ~~`incident`~~ ✗    | **FIXED** |
| `incident_summary`      | **`object`**     | ~~`incident`~~ ✗    | **FIXED** |
| `partition_index`       | **`object`**     | ~~`incident`~~ ✗    | **FIXED** |
| `incident_to_partition` | **`object`**     | ~~`incident`~~ ✗    | **FIXED** |

## Key Insights

1. **Don't trust table names**: Table names like `incidents` and `incident_summary` misleadingly suggested they would have `incident` or `summary` columns.

2. **Consistent schema**: All tables in the `precinct` keyspace follow the same schema pattern with an `object` column for data storage.

3. **JSON/Blob storage**: The `object` column likely stores JSON or serialized data regardless of what type of data it represents (incidents, reports, summaries, etc.).

## Verify Schema

Run the schema checker to confirm:

```bash
python check_table_schema.py
```

## Next Steps

1. Run the migration - it should work now:
   ```bash
   python migrator_optimized.py
   ```

2. All tables (`incidents`, `incident_summary`, `partition_index`, `incident_to_partition`) should now migrate successfully! 🎉
