# Schema Column Mapping Fix

## Problem
The `incident_summary` table was using the wrong column name. The code was trying to access an `incident` column, but the actual column name is `summary`.

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
if table_name == "reports":
    data_col = 'object'
elif table_name == "objects":
    data_col = 'object'
elif table_name == "incident_summary":
    data_col = 'summary'  # incident_summary uses 'summary' column
elif table_name in ["incidents", "incident_to_partition", "partition_index"]:
    data_col = 'incident'
```

## Column Mappings by Table

| Table Name               | Data Column Name |
|-------------------------|------------------|
| `objects`               | `object`         |
| `reports`               | `object`         |
| `incident_summary`      | `summary`        |
| `incidents`             | `incident`       |
| `incident_to_partition` | `incident`       |
| `partition_index`       | `incident`       |

## Verify Schema (Optional)

If you want to verify the actual table schemas before running the migration, use the provided schema checker:

```bash
python check_table_schema.py
```

This will show you:
1. All columns in each table
2. Column types and kinds (partition key, clustering key, regular)
3. A sample row from incident_summary showing available fields

## Next Steps

1. Run the migration again:
   ```bash
   python migrator_optimized.py
   ```

2. The error should be resolved and `incident_summary` should migrate successfully.

3. If you encounter similar errors for other tables, check the schema output to confirm the correct column names.

## Common Column Name Patterns

- Tables storing raw data objects: `object` column
- Tables storing incidents: `incident` column
- Tables storing summaries: `summary` column
- Always have: `partition`, `created_at` (and optionally `org_id`)
