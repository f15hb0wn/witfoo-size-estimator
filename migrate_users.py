#!/usr/bin/env python3
"""
User Migration Script
Converts IMPACT (old) user format to AIO (new) user format.

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
from datetime import datetime

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


def migrate_users(input_file='old-users.json', output_file='migrated-users.json', org_id_prefix=42218):
    """
    Main migration function.
    Reads old users, deduplicates, transforms, and writes new users.
    
    Args:
        input_file: Path to old users JSON file
        output_file: Path to save migrated users
        org_id_prefix: Organization ID prefix for generating user IDs (default: 42218)
    """
    print("="*80)
    print("USER MIGRATION SCRIPT")
    print("="*80)
    print(f"Organization ID Prefix: {org_id_prefix}")
    
    # Read old users
    print(f"\nReading users from '{input_file}'...")
    with open(input_file, 'r', encoding='utf-8') as f:
        old_users = json.load(f)
    
    print(f"Loaded {len(old_users)} users from old format")
    
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
    
    # Write new users
    print(f"\nWriting {len(new_users)} users to '{output_file}'...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(new_users, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Successfully wrote migrated users to '{output_file}'")
    
    # Summary
    print("\n" + "="*80)
    print("MIGRATION SUMMARY")
    print("="*80)
    print(f"Organization ID Prefix: {org_id_prefix}")
    print(f"Original users:     {len(old_users)}")
    print(f"Duplicates removed: {duplicates_removed}")
    print(f"Unique users:       {len(unique_users)}")
    print(f"Successfully migrated: {len(new_users)}")
    print(f"Failed:             {len(unique_users) - len(new_users)}")
    
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


if __name__ == '__main__':
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'old-users.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'migrated-users.json'
    org_id_prefix = int(sys.argv[3]) if len(sys.argv) > 3 else 42218
    
    try:
        migrate_users(input_file, output_file, org_id_prefix)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"\nUsage: python migrate_users.py [input_file] [output_file] [org_id_prefix]")
        print(f"Example: python migrate_users.py old-users.json migrated-users.json 42218")
        print(f"\nDefault org_id_prefix is 42218 (generates IDs like 4221800001, 4221800002, etc.)")
        sys.exit(1)
    except Exception as e:
        print(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
