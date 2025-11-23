#!/usr/bin/env python3
"""
User Migration Script
Converts IMPACT (old) user format to AIO (new) user format.
Reads from IMPACT Cassandra, transforms users, and writes to AIO Cassandra.

Key differences:
OLD FORMAT (IMPACT):
- id: UUID string (e.g., "172c4c31-e05f-4a6d-b44f-c41808ba3964")
- username: email format (e.g., "dev@impelix.com")
- password: bcrypt hash with $2a$ (e.g., "$2a$10$...")
- function_role: integer (1 = admin)
- data_roles: array of strings (e.g., ["calprivate"])
- Has: display_name, org_id, customer_uuid, timestamps

NEW FORMAT (AIO):
- id: integer (e.g., 4221800001)
- name: display name (e.g., "WitFoo Support")
- password: bcrypt hash with $2y$ (e.g., "$2y$10$...")
- email: email (e.g., "support@witfoo.com")
- function_role: integer (same)
- data_role: integer (0 = high)
- data_roles: array of integers (e.g., [0])
- Has: api_token, function_roles_label, data_roles_label
"""
import json
import hashlib
import re
import yaml
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
import uuid

import uuid


def setup_cassandra_connections(config_file='migrator_settings.yaml'):
    """
    Setup connections to both IMPACT and AIO Cassandra clusters.
    Returns tuple: (impact_session, aio_session, config)
    """
    # Load configuration
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    
    # Setup SSL context
    ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.minimum_version = TLSVersion.TLSv1_2
    ssl_context.check_hostname = False
    ssl_context.verify_mode = CERT_NONE
    
    timeout_config = {
        'connect_timeout': 30,
        'control_connection_timeout': 30
    }
    
    print("Connecting to IMPACT Cassandra server...")
    impact_auth_provider = PlainTextAuthProvider(
        config['IMPACT_CLUSTER_USERNAME'],
        config['IMPACT_CLUSTER_PASSWORD']
    )
    impact_cluster = Cluster(
        contact_points=[config['IMPACT_CLUSTER_SEED_NODES']],
        auth_provider=impact_auth_provider,
        ssl_context=ssl_context,
        protocol_version=4,
        load_balancing_policy=WhiteListRoundRobinPolicy([config['IMPACT_CLUSTER_SEED_NODES']]),
        **timeout_config
    )
    impact = impact_cluster.connect()
    impact.default_timeout = 600
    impact.default_consistency_level = ConsistencyLevel.ONE
    print("✓ Connected to IMPACT")
    
    print("Connecting to AIO Cassandra server...")
    aio_auth_provider = PlainTextAuthProvider(
        config['AIO_USERNAME'],
        config['AIO_PASSWORD']
    )
    aio_cluster = Cluster(
        contact_points=[config['AIO_IP']],
        auth_provider=aio_auth_provider,
        ssl_context=ssl_context,
        protocol_version=4,
        load_balancing_policy=WhiteListRoundRobinPolicy([config['AIO_IP']]),
        **timeout_config
    )
    aio = aio_cluster.connect()
    aio.default_timeout = 600
    aio.default_consistency_level = ConsistencyLevel.ONE
    print("✓ Connected to AIO")
    
    return impact, aio, config, impact_cluster, aio_cluster


def read_users_from_cassandra(session, org_id):
    """
    Read users from IMPACT Cassandra 'objects' table, 'users' partition.
    Returns list of user objects.
    """
    print(f"\nQuerying users from IMPACT Cassandra (org_id: {org_id})...")
    
    # Try with org_id first
    query = "SELECT partition, created_at, object FROM precinct.objects WHERE partition = ? AND org_id = ?;"
    statement = session.prepare(query)
    statement.consistency_level = ConsistencyLevel.ONE
    
    try:
        rows = session.execute(statement, ['users', org_id])
        rows_list = list(rows)
        
        if not rows_list:
            print("  No users found with org_id, trying without org_id filter...")
            query = "SELECT partition, created_at, object FROM precinct.objects WHERE partition = ?;"
            statement = session.prepare(query)
            statement.consistency_level = ConsistencyLevel.ONE
            rows = session.execute(statement, ['users'])
            rows_list = list(rows)
        
        if rows_list:
            print(f"  Found {len(rows_list)} user row(s)")
            
            # Parse the JSON object(s)
            all_users = []
            for row in rows_list:
                try:
                    users_obj = json.loads(row.object)
                    # The object might be a single array of users or contain users
                    if isinstance(users_obj, list):
                        all_users.extend(users_obj)
                    else:
                        all_users.append(users_obj)
                except json.JSONDecodeError as e:
                    print(f"  Warning: Could not parse user object: {e}")
            
            print(f"  Extracted {len(all_users)} user(s) from database")
            return all_users
        else:
            print("  No users found in database")
            return []
            
    except Exception as e:
        print(f"  Error querying users: {e}")
        import traceback
        traceback.print_exc()
        return []


def write_users_to_cassandra(session, org_id, users_list):
    """
    Write transformed users back to AIO Cassandra 'objects' table, 'users' partition.
    """
    print(f"\nWriting {len(users_list)} users to AIO Cassandra...")
    
    # Serialize users list to JSON
    users_json = json.dumps(users_list, ensure_ascii=False)
    
    # Generate a new created_at timestamp
    created_at = uuid.uuid1()
    
    # Check if destination has org_id column
    check_query = "SELECT column_name FROM system_schema.columns WHERE keyspace_name='precinct' AND table_name='objects' AND column_name='org_id'"
    result = session.execute(check_query)
    has_org_id = len(list(result)) > 0
    
    try:
        if has_org_id:
            # Insert with org_id
            insert_query = "INSERT INTO precinct.objects (partition, created_at, org_id, object) VALUES (?, ?, ?, ?);"
            statement = session.prepare(insert_query)
            statement.consistency_level = ConsistencyLevel.ONE
            session.execute(statement, ['users', created_at, org_id, users_json])
        else:
            # Insert without org_id
            insert_query = "INSERT INTO precinct.objects (partition, created_at, object) VALUES (?, ?, ?);"
            statement = session.prepare(insert_query)
            statement.consistency_level = ConsistencyLevel.ONE
            session.execute(statement, ['users', created_at, users_json])
        
        print(f"✓ Successfully wrote users to AIO Cassandra")
        print(f"  Partition: users")
        print(f"  Created at: {created_at}")
        print(f"  Data size: {len(users_json)} bytes")
        return True
        
    except Exception as e:
        print(f"✗ Error writing users to Cassandra: {e}")
        import traceback
        traceback.print_exc()
        return False


def bcrypt_2a_to_2y(password_hash):
    """
    Convert bcrypt $2a$ hash to $2y$ format.
    Both are compatible, just different versions of bcrypt.
    """
    if password_hash and password_hash.startswith('$2a$'):
        return password_hash.replace('$2a$', '$2y$', 1)
    return password_hash


def map_data_role_to_integer(data_roles_list):
    """
    Map data role strings to integers.
    In new system: 0 = High access
    """
    # If data_roles is empty or contains org names, default to 0 (High)
    return 0


def generate_user_id(username, index, org_id_prefix=42218):
    """
    Generate a consistent integer ID for each user based on org ID.
    Format: {org_id}{sequential_number}
    Example: org_id=42218, first user=4221800001, second=4221800002
    """
    # Create ID with org_id prefix followed by padded sequential number
    # Ensures unique IDs: 4221800001, 4221800002, etc.
    sequential_id = index + 1  # Start from 1
    user_id = int(f"{org_id_prefix}{sequential_id:05d}")
    return user_id


def map_function_role_label(function_role):
    """Map function role integer to label."""
    role_map = {
        1: "Admin",
        2: "User",
        3: "Read-Only"
    }
    return role_map.get(function_role, "User")


def map_data_role_label(data_role):
    """Map data role integer to label."""
    role_map = {
        0: "High",
        1: "Medium",
        2: "Low"
    }
    return role_map.get(data_role, "High")


def generate_api_token(username):
    """
    Generate a pseudo API token.
    In production, this should be a proper secure random token.
    """
    hash_obj = hashlib.sha256(username.encode())
    return hash_obj.hexdigest()[:60]


def transform_user(old_user, index, org_id_prefix=42218):
    """Transform old user format to new format."""
    
    # Extract username (email)
    username = old_user.get('username', '')
    
    # Determine the display name
    display_name = old_user.get('display_name', '')
    if not display_name:
        # Fallback: use username without domain
        display_name = username.split('@')[0] if '@' in username else username
    
    # Convert password hash format
    old_password = old_user.get('password', '')
    new_password = bcrypt_2a_to_2y(old_password)
    
    # Map data roles
    old_data_roles = old_user.get('data_roles', [])
    new_data_role = map_data_role_to_integer(old_data_roles)
    new_data_roles = [new_data_role]
    
    # Get function role
    function_role = old_user.get('function_role', 1)
    
    # Generate new user object
    new_user = {
        'id': generate_user_id(username, index, org_id_prefix),
        'name': display_name,
        'password': new_password,
        'email': username,
        'auth_type': old_user.get('auth_type', 'Local'),
        'display_theme': old_user.get('display_theme', 'dark-theme'),
        'api_token': generate_api_token(username),
        'function_role': function_role,
        'data_role': new_data_role,
        'data_roles': new_data_roles,
        'remember_token': None,
        'deleted_at': old_user.get('deleted_at'),
        'function_roles_label': map_function_role_label(function_role),
        'data_roles_label': map_data_role_label(new_data_role)
    }
    
    return new_user


def deduplicate_users(users_list):
    """
    Deduplicate users by username (email).
    Keeps the most recently created/updated user.
    """
    user_map = {}
    
    for user in users_list:
        username = user.get('username', '')
        
        if not username:
            continue
        
        # Determine which timestamp to use
        updated_at = user.get('updated_at', user.get('created_at', ''))
        
        if username not in user_map:
            user_map[username] = (user, updated_at)
        else:
            # Keep the more recent one
            existing_user, existing_time = user_map[username]
            if updated_at > existing_time:
                user_map[username] = (user, updated_at)
                print(f"  Replacing older version of user: {username}")
            else:
                print(f"  Skipping duplicate user: {username} (keeping newer version)")
    
    return [user for user, _ in user_map.values()]


def migrate_users(org_id_prefix=42218, config_file='migrator_settings.yaml', export_json=False, json_output='migrated-users.json'):
    """
    Main migration function.
    Reads users from IMPACT Cassandra, transforms, and writes to AIO Cassandra.
    
    Args:
        org_id_prefix: Organization ID prefix for generating user IDs (default: 42218)
        config_file: Path to configuration YAML file
        export_json: If True, also export migrated users to JSON file
        json_output: Path to save JSON export (if export_json=True)
    """
    print("="*80)
    print("USER MIGRATION SCRIPT - DATABASE TO DATABASE")
    print("="*80)
    print(f"Organization ID Prefix: {org_id_prefix}")
    print(f"Config file: {config_file}")
    
    # Setup database connections
    impact, aio, config, impact_cluster, aio_cluster = setup_cassandra_connections(config_file)
    org_id = config['IMPACT_ORG_ID']
    
    try:
        # Read users from IMPACT
        old_users = read_users_from_cassandra(impact, org_id)
        
        if not old_users:
            print("\n⚠️  No users found to migrate!")
            return []
        
        print(f"\nLoaded {len(old_users)} users from IMPACT Cassandra")
        
        # Deduplicate
        print("\nDeduplicating users by username...")
        unique_users = deduplicate_users(old_users)
        print(f"After deduplication: {len(unique_users)} unique users")
        
        duplicates_removed = len(old_users) - len(unique_users)
        if duplicates_removed > 0:
            print(f"  → Removed {duplicates_removed} duplicate(s)")
        
        # Transform users
        print("\nTransforming users to new format...")
        new_users = []
        
        for index, old_user in enumerate(unique_users):
            username = old_user.get('username', 'unknown')
            try:
                new_user = transform_user(old_user, index, org_id_prefix)
                new_users.append(new_user)
                print(f"  ✓ Transformed: {username} → {new_user['name']} (ID: {new_user['id']})")
            except Exception as e:
                print(f"  ✗ Failed to transform {username}: {e}")
        
        # Write to AIO Cassandra
        success = write_users_to_cassandra(aio, org_id, new_users)
        
        # Optionally export to JSON
        if export_json and new_users:
            print(f"\nExporting migrated users to '{json_output}'...")
            with open(json_output, 'w', encoding='utf-8') as f:
                json.dump(new_users, f, indent=2, ensure_ascii=False)
            print(f"✓ Exported to {json_output}")
        
        # Summary
        print("\n" + "="*80)
        print("MIGRATION SUMMARY")
        print("="*80)
        print(f"Organization ID: {org_id}")
        print(f"Organization ID Prefix: {org_id_prefix}")
        print(f"Original users:     {len(old_users)}")
        print(f"Duplicates removed: {duplicates_removed}")
        print(f"Unique users:       {len(unique_users)}")
        print(f"Successfully migrated: {len(new_users)}")
        print(f"Failed:             {len(unique_users) - len(new_users)}")
        print(f"Written to AIO:     {'Yes' if success else 'No'}")
        
        if new_users:
            print(f"ID Range:           {new_users[0]['id']} - {new_users[-1]['id']}")
        
        print("="*80)
        
        # Show sample comparison
        if new_users:
            print("\nSample user transformation:")
            print("-" * 80)
            sample_old = unique_users[0]
            sample_new = new_users[0]
            print(f"OLD: {sample_old.get('username')} (ID: {sample_old.get('id')})")
            print(f"     Role: {sample_old.get('function_role')}, Data roles: {sample_old.get('data_roles')}")
            print(f"     Password: {sample_old.get('password', '')[:20]}...")
            print()
            print(f"NEW: {sample_new.get('email')} (ID: {sample_new.get('id')})")
            print(f"     Name: {sample_new.get('name')}")
            print(f"     Role: {sample_new.get('function_role')}, Data role: {sample_new.get('data_role')}")
            print(f"     Password: {sample_new.get('password', '')[:20]}...")
            print(f"     API Token: {sample_new.get('api_token', '')[:30]}...")
            print("-" * 80)
        
        return new_users
        
    finally:
        # Cleanup connections
        print("\nClosing database connections...")
        impact_cluster.shutdown()
        aio_cluster.shutdown()
        print("✓ Connections closed")


if __name__ == '__main__':
    import sys
    
    org_id_prefix = int(sys.argv[1]) if len(sys.argv) > 1 else 42218
    config_file = sys.argv[2] if len(sys.argv) > 2 else 'migrator_settings.yaml'
    export_json = '--export' in sys.argv or '-e' in sys.argv
    
    try:
        migrate_users(
            org_id_prefix=org_id_prefix,
            config_file=config_file,
            export_json=export_json,
            json_output='migrated-users.json'
        )
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"\nUsage: python migrate_users.py [org_id_prefix] [config_file] [--export]")
        print(f"Example: python migrate_users.py 42218 migrator_settings.yaml --export")
        print(f"\nDefaults:")
        print(f"  org_id_prefix: 42218 (generates IDs like 4221800001, 4221800002, etc.)")
        print(f"  config_file: migrator_settings.yaml")
        print(f"  --export or -e: Export migrated users to migrated-users.json")
        print(f"\nThis script:")
        print(f"  1. Reads users from IMPACT Cassandra (objects table, users partition)")
        print(f"  2. Transforms users from old format to new format")
        print(f"  3. Writes transformed users to AIO Cassandra")
        print(f"  4. Optionally exports to JSON file with --export flag")
        sys.exit(1)
    except Exception as e:
        print(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
