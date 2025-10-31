#!/usr/bin/env python3
"""
Quick script to check the schema of objects table in both clusters
"""
import yaml
import ssl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Load settings
with open('migrator_settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)

# SSL setup
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Connect to IMPACT
impact_auth = PlainTextAuthProvider(
    username=settings['impact']['username'],
    password=settings['impact']['password']
)
impact_cluster = Cluster(
    contact_points=[settings['impact']['seed_nodes']],
    auth_provider=impact_auth,
    ssl_context=ssl_context,
    protocol_version=4
)
impact_session = impact_cluster.connect()

# Connect to AIO
aio_auth = PlainTextAuthProvider(
    username=settings['aio']['username'],
    password=settings['aio']['password']
)
aio_cluster = Cluster(
    contact_points=[settings['aio']['ip']],
    auth_provider=aio_auth,
    ssl_context=ssl_context,
    protocol_version=4
)
aio_session = aio_cluster.connect()

print("=" * 80)
print("IMPACT Cluster - objects table schema:")
print("=" * 80)
try:
    result = impact_session.execute("SELECT * FROM system_schema.columns WHERE keyspace_name='precinct' AND table_name='objects'")
    for row in result:
        print(f"  {row.column_name}: {row.type}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 80)
print("AIO Cluster - objects table schema:")
print("=" * 80)
try:
    result = aio_session.execute("SELECT * FROM system_schema.columns WHERE keyspace_name='precinct' AND table_name='objects'")
    for row in result:
        print(f"  {row.column_name}: {row.type}")
except Exception as e:
    print(f"Error: {e}")

# Check for customers keyspace too
print("\n" + "=" * 80)
print("AIO Cluster - checking for customers.objects table:")
print("=" * 80)
try:
    result = aio_session.execute("SELECT * FROM system_schema.columns WHERE keyspace_name='customers' AND table_name='objects'")
    rows = list(result)
    if rows:
        for row in rows:
            print(f"  {row.column_name}: {row.type}")
    else:
        print("  Table does not exist")
except Exception as e:
    print(f"Error: {e}")

impact_cluster.shutdown()
aio_cluster.shutdown()
