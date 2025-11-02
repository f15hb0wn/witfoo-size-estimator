# Schema Column Mapping Fix

## Problem
The `incident_summary` table was incorrectly assumed to have an `incident` column. After inspecting the actual schema, it was discovered that `incident_summary` uses the `object` column.

## Solution Applied

Updated the column mapping logic in `migrator_optimized.py` (around line 322-329):

### Before:
```python
if table_name == "reports":
    data_col = 'object'
elif table_name in ["incidents", "incident_to_partition", "partition_index", "incident_summary"]:
    data_col = 'incident'
```

### After:
```python
# Based on actual schema inspection - incident_summary uses 'object' column!
if table_name in ["reports", "objects", "incident_summary"]:
    data_col = 'object'
elif table_name in ["incidents", "incident_to_partition", "partition_index"]:
    data_col = 'incident'
```

## Actual Schema Discovery

From running `check_table_schema.py`, the `incident_summary` table contains:
- `count` - regular column
- `created_at` - clustering key
- `index` - regular column
- **`object`** - regular column (the data column!)
- `org_id` - regular column
- `partition` - partition key

## Column Mappings by Table

| Table Name               | Data Column Name | Notes                    |
|-------------------------|------------------|--------------------------|
| `objects`               | `object`         |                          |
| `reports`               | `object`         |                          |
| **`incident_summary`**  | **`object`**     | **Fixed - was incorrect**|
| `incidents`             | `incident`       |                          |
| `incident_to_partition` | `incident`       |                          |
| `partition_index`       | `incident`       |                          |

## Schema Checker Fix

Also fixed the schema checker script to remove the unsupported `ORDER BY position` clause. Now it:
1. Queries columns without ordering
2. Sorts them manually (partition keys → clustering keys → regular columns)
3. Displays them in a readable format

## Verify Schema

Run the schema checker to see all table structures:

```bash
python check_table_schema.py
```

This will show you:
1. All columns in each table with their types
2. Column kinds (partition_key, clustering, regular)
3. Sample rows showing actual data structure

## Next Steps

1. Run the migration again:
   ```bash
   python migrator_optimized.py
   ```

2. The error should be resolved and `incident_summary` should migrate successfully.

## Key Takeaway

**Don't assume table structures based on naming!** The `incident_summary` table name suggests it would have a `summary` or `incident` column, but it actually stores data in an `object` column just like `objects` and `reports` tables.
