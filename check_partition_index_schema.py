#!/usr/bin/env python3
"""Check the actual schema of partition_index table."""

import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl

# Load config
with open('migrator_settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

IMPACT_CLUSTER_SEED_NODES = config['IMPACT_CLUSTER_SEED_NODES']
IMPACT_CLUSTER_USERNAME = config['IMPACT_CLUSTER_USERNAME']
IMPACT_CLUSTER_PASSWORD = config['IMPACT_CLUSTER_PASSWORD']

# SSL setup
ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = TLSVersion.TLSv1_2
ssl_context.check_hostname = False
ssl_context.verify_mode = CERT_NONE

# Connect
auth_provider = PlainTextAuthProvider(IMPACT_CLUSTER_USERNAME, IMPACT_CLUSTER_PASSWORD)
cluster = Cluster(
    contact_points=[IMPACT_CLUSTER_SEED_NODES],
    auth_provider=auth_provider,
    ssl_context=ssl_context,
    protocol_version=4
)
session = cluster.connect()

print("="*80)
print("PARTITION_INDEX TABLE SCHEMA")
print("="*80)

# Query schema
query = """
    SELECT column_name, type, kind, position
    FROM system_schema.columns
    WHERE keyspace_name='precinct' AND table_name='partition_index'
"""
rows = session.execute(query)

columns = []
for row in rows:
    columns.append({
        'name': row.column_name,
        'type': row.type,
        'kind': row.kind,
        'position': row.position
    })

# Sort by kind and position
columns.sort(key=lambda x: (0 if x['kind'] == 'partition_key' else 1 if x['kind'] == 'clustering' else 2, x['position'] or 999))

print("\nColumns:")
for col in columns:
    kind_label = {
        'partition_key': 'PARTITION KEY',
        'clustering': 'CLUSTERING',
        'regular': 'REGULAR'
    }.get(col['kind'], col['kind'])
    print(f"  {col['name']:20s} | {col['type']:20s} | {kind_label}")

print("\n" + "="*80)
print("SAMPLE DATA (first 5 rows)")
print("="*80)

# Get sample data
column_names = [col['name'] for col in columns]
sample_query = f"SELECT {', '.join(column_names)} FROM precinct.partition_index LIMIT 5;"
print(f"\nQuery: {sample_query}\n")

try:
    rows = session.execute(sample_query)
    for i, row in enumerate(rows, 1):
        print(f"Row {i}:")
        for col_name in column_names:
            value = getattr(row, col_name, None)
            print(f"  {col_name}: {value}")
        print()
except Exception as e:
    print(f"Error querying sample data: {e}")

# Check for artifact_partition_summary table (from estimator.py)
print("\n" + "="*80)
print("ARTIFACT_PARTITION_SUMMARY TABLE SCHEMA")
print("="*80)

query = """
    SELECT column_name, type, kind, position
    FROM system_schema.columns
    WHERE keyspace_name='artifacts' AND table_name='artifact_partition_summary'
"""
try:
    rows = session.execute(query)
    columns = []
    for row in rows:
        columns.append({
            'name': row.column_name,
            'type': row.type,
            'kind': row.kind,
            'position': row.position
        })
    
    if columns:
        columns.sort(key=lambda x: (0 if x['kind'] == 'partition_key' else 1 if x['kind'] == 'clustering' else 2, x['position'] or 999))
        
        print("\nColumns:")
        for col in columns:
            kind_label = {
                'partition_key': 'PARTITION KEY',
                'clustering': 'CLUSTERING',
                'regular': 'REGULAR'
            }.get(col['kind'], col['kind'])
            print(f"  {col['name']:20s} | {col['type']:20s} | {kind_label}")
        
        print("\n" + "="*80)
        print("SAMPLE DATA (first 5 rows)")
        print("="*80)
        
        column_names = [col['name'] for col in columns]
        sample_query = f"SELECT {', '.join(column_names)} FROM artifacts.artifact_partition_summary LIMIT 5;"
        print(f"\nQuery: {sample_query}\n")
        
        rows = session.execute(sample_query)
        for i, row in enumerate(rows, 1):
            print(f"Row {i}:")
            for col_name in column_names:
                value = getattr(row, col_name, None)
                print(f"  {col_name}: {value}")
            print()
    else:
        print("\nTable 'artifacts.artifact_partition_summary' not found or has no columns.")
except Exception as e:
    print(f"Error querying artifact_partition_summary: {e}")

cluster.shutdown()
