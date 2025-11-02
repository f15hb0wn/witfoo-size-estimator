#!/usr/bin/env python3
"""
Quick script to check the actual schema of Cassandra tables
"""
import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl

# Load configuration
with open('migrator_settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

IMPACT_CLUSTER_SEED_NODES = config['IMPACT_CLUSTER_SEED_NODES']
IMPACT_CLUSTER_USERNAME = config['IMPACT_CLUSTER_USERNAME']
IMPACT_CLUSTER_PASSWORD = config['IMPACT_CLUSTER_PASSWORD']

# Setup SSL context
ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = TLSVersion.TLSv1_2
ssl_context.check_hostname = False
ssl_context.verify_mode = CERT_NONE

# Connection timeout configurations
timeout_config = {
    'connect_timeout': 30,
    'control_connection_timeout': 30
}

# Connect to IMPACT cluster
impact_auth_provider = PlainTextAuthProvider(IMPACT_CLUSTER_USERNAME, IMPACT_CLUSTER_PASSWORD)
impact_cluster = Cluster(
    contact_points=[IMPACT_CLUSTER_SEED_NODES],  # Only use specified node
    auth_provider=impact_auth_provider,
    ssl_context=ssl_context,
    protocol_version=4,
    load_balancing_policy=WhiteListRoundRobinPolicy([IMPACT_CLUSTER_SEED_NODES]),  # Restrict to only this host
    **timeout_config
)

print("Connecting to IMPACT Cassandra server...")
impact = impact_cluster.connect()
print("Connected successfully!\n")

# Tables to check
tables = ['objects', 'reports', 'incidents', 'incident_summary', 'partition_index', 'incident_to_partition']

print("="*80)
print("CASSANDRA TABLE SCHEMAS")
print("="*80)

for table in tables:
    print(f"\nTable: precinct.{table}")
    print("-" * 80)
    
    # Get all columns for this table (without ORDER BY which isn't supported)
    query = """
    SELECT column_name, type, kind 
    FROM system_schema.columns 
    WHERE keyspace_name='precinct' AND table_name=%s
    """
    
    try:
        result = impact.execute(query, [table])
        columns = list(result)
        
        if columns:
            # Sort columns: partition keys first, then clustering keys, then regular
            partition_keys = [c for c in columns if c.kind == 'partition_key']
            clustering_keys = [c for c in columns if c.kind == 'clustering']
            regular_cols = [c for c in columns if c.kind == 'regular']
            sorted_columns = partition_keys + clustering_keys + regular_cols
            
            print(f"  {'Column Name':<25} {'Type':<30} {'Kind':<15}")
            print("  " + "-" * 70)
            for col in sorted_columns:
                print(f"  {col.column_name:<25} {col.type:<30} {col.kind:<15}")
        else:
            print(f"  No columns found or table doesn't exist")
            
    except Exception as e:
        print(f"  Error querying schema: {e}")

print("\n" + "="*80)

# Get a sample row from incident_summary to see actual data
print("\nSample row from incident_summary (first 1 row):")
print("-" * 80)
try:
    query = "SELECT * FROM precinct.incident_summary LIMIT 1"
    result = impact.execute(query)
    row = result.one()
    if row:
        print("Available fields in actual row:")
        for field in dir(row):
            if not field.startswith('_'):
                print(f"  - {field}")
    else:
        print("  No rows found in table")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "="*80)

# Cleanup
impact_cluster.shutdown()
print("\nConnection closed.")
