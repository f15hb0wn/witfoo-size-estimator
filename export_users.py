#!/usr/bin/env python3
"""
Simple script to export user object JSON from Cassandra to disk.
Extracts the 'users' partition from the objects table and saves it as users.json
"""
import json
import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl

# Load configuration
with open('migrator_settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

IMPACT_CLUSTER_SEED_NODES = config['IMPACT_CLUSTER_SEED_NODES']
IMPACT_CLUSTER_USERNAME = config['IMPACT_CLUSTER_USERNAME']
IMPACT_CLUSTER_PASSWORD = config['IMPACT_CLUSTER_PASSWORD']
IMPACT_ORG_ID = config['IMPACT_ORG_ID']

# Setup SSL context
ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = TLSVersion.TLSv1_2
ssl_context.check_hostname = False
ssl_context.verify_mode = CERT_NONE

timeout_config = {
    'connect_timeout': 30,
    'control_connection_timeout': 30
}

# Connect to IMPACT cluster
print("Connecting to IMPACT Cassandra server...")
impact_auth_provider = PlainTextAuthProvider(IMPACT_CLUSTER_USERNAME, IMPACT_CLUSTER_PASSWORD)
impact_cluster = Cluster(
    contact_points=[IMPACT_CLUSTER_SEED_NODES],
    auth_provider=impact_auth_provider,
    ssl_context=ssl_context,
    protocol_version=4,
    load_balancing_policy=WhiteListRoundRobinPolicy([IMPACT_CLUSTER_SEED_NODES]),
    **timeout_config
)
impact = impact_cluster.connect()
impact.default_timeout = 600
impact.default_consistency_level = ConsistencyLevel.ONE
print("Connected successfully!\n")

# Query for users partition
print(f"Querying for 'users' partition with org_id '{IMPACT_ORG_ID}'...")
query = "SELECT partition, created_at, object FROM precinct.objects WHERE partition = ? AND org_id = ?;"
statement = impact.prepare(query)
statement.consistency_level = ConsistencyLevel.ONE

try:
    rows = impact.execute(statement, ['users', IMPACT_ORG_ID])
    rows_list = list(rows)
    
    if not rows_list:
        print("No users data found!")
        print("\nTrying without org_id filter (in case schema doesn't have org_id)...")
        query = "SELECT partition, created_at, object FROM precinct.objects WHERE partition = ?;"
        statement = impact.prepare(query)
        statement.consistency_level = ConsistencyLevel.ONE
        rows = impact.execute(statement, ['users'])
        rows_list = list(rows)
    
    if rows_list:
        print(f"Found {len(rows_list)} row(s) for 'users' partition\n")
        
        # Export all rows (usually just one, but export all if multiple exist)
        users_data = []
        for i, row in enumerate(rows_list):
            print(f"Row {i+1}:")
            print(f"  Partition: {row.partition}")
            print(f"  Created At: {row.created_at}")
            print(f"  Object size: {len(row.object)} characters")
            
            # Try to parse as JSON to validate
            try:
                user_obj = json.loads(row.object)
                users_data.append({
                    'partition': row.partition,
                    'created_at': str(row.created_at),
                    'object': user_obj
                })
                print(f"  JSON parsed successfully - contains {len(user_obj)} top-level keys")
            except json.JSONDecodeError as e:
                print(f"  Warning: Could not parse as JSON: {e}")
                users_data.append({
                    'partition': row.partition,
                    'created_at': str(row.created_at),
                    'object_raw': row.object
                })
        
        # Save to file
        output_file = 'users.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            if len(users_data) == 1:
                # If single row, just save the object content
                json.dump(users_data[0]['object'], f, indent=2, ensure_ascii=False)
            else:
                # If multiple rows, save as array
                json.dump(users_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Successfully exported users data to '{output_file}'")
        print(f"  File size: {len(open(output_file).read())} bytes")
        
    else:
        print("No users data found in the database!")
        
except Exception as e:
    print(f"Error querying database: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Cleanup
    print("\nClosing connection...")
    impact_cluster.shutdown()
    print("Done!")
