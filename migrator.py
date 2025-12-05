import time
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, ConsistencyLevel, Unavailable, WriteTimeout, ReadFailure
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
import yaml
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
import os
import hashlib
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migrator.log'),
        logging.StreamHandler()
    ]
)

# --- Checkpoint Management ---
CHECKPOINT_FILE = "migrator_checkpoint.json"

def load_checkpoint():
    """Load checkpoint data from file."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"completed_tables": [], "completed_partitions": []}

def save_checkpoint(checkpoint_data):
    """Save checkpoint data to file."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)

def mark_table_completed(checkpoint_data, table_name):
    """Mark a table as completed in the checkpoint."""
    if table_name not in checkpoint_data["completed_tables"]:
        checkpoint_data["completed_tables"].append(table_name)
        save_checkpoint(checkpoint_data)
        logging.info(f"✓ Marked {table_name} as completed in checkpoint")

def mark_partition_completed(checkpoint_data, partition_name):
    """Mark a partition as completed in the checkpoint."""
    partition_key = f"objects:{partition_name}"
    if partition_key not in checkpoint_data["completed_partitions"]:
        checkpoint_data["completed_partitions"].append(partition_key)
        save_checkpoint(checkpoint_data)
        logging.info(f"✓ Marked {partition_key} as completed in checkpoint")

def is_table_completed(checkpoint_data, table_name):
    """Check if a table has already been completed."""
    return table_name in checkpoint_data["completed_tables"]

def is_partition_completed(checkpoint_data, partition_name):
    """Check if a partition has already been completed."""
    partition_key = f"objects:{partition_name}"
    return partition_key in checkpoint_data["completed_partitions"]

# Load checkpoint at startup
checkpoint = load_checkpoint()
logging.info(f"Loaded checkpoint: {len(checkpoint['completed_tables'])} tables, {len(checkpoint['completed_partitions'])} partitions completed")


# --- Configuration Loading ---
with open('migrator_settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

IMPACT_CLUSTER_SEED_NODES = config['IMPACT_CLUSTER_SEED_NODES']
IMPACT_CLUSTER_USERNAME = config['IMPACT_CLUSTER_USERNAME']
IMPACT_CLUSTER_PASSWORD = config['IMPACT_CLUSTER_PASSWORD']
IMPACT_ORG_ID = config['IMPACT_ORG_ID']
AIO_IP = config['AIO_IP']
AIO_USERNAME = config['AIO_USERNAME']
AIO_PASSWORD = config['AIO_PASSWORD']

# Performance tuning parameters
MAX_WORKERS = config.get('MAX_WORKERS', 10)  # Increased for smaller tables
BATCH_INSERT_SIZE = config.get('BATCH_INSERT_SIZE', 25)  # Reduced for large objects
FETCH_BATCH_SIZE = config.get('FETCH_BATCH_SIZE', 200)  # Increased chunk size

# Migration behavior
TRUNCATE_BEFORE_COPY = config.get('TRUNCATE_BEFORE_COPY', False)  # Truncate tables before copying

# Table-specific batch sizes (for tables with large blob data)
TABLE_BATCH_SIZES = {
    'reports': 1,   # Reports have very large object blobs - use small batches
    'incidents': 1,  # Incidents can be large
    'incident_summary': 1,
    'objects': 5,  # Objects are usually smaller
}

# --- Cassandra Connections ---
ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = TLSVersion.TLSv1_2
ssl_context.check_hostname = False
ssl_context.verify_mode = CERT_NONE

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
logging.info("Connecting to IMPACT Cassandra server (single node only)...")
impact = impact_cluster.connect()
impact.default_timeout = 1200  # Increased to 20 minutes for large scans
impact.default_consistency_level = ConsistencyLevel.QUORUM
logging.info("Connection to IMPACT Cassandra server successful")

# Connect to AIO cluster
aio_auth_provider = PlainTextAuthProvider(AIO_USERNAME, AIO_PASSWORD)
aio_cluster = Cluster(
    contact_points=[AIO_IP],  # Only use specified node
    auth_provider=aio_auth_provider,
    ssl_context=ssl_context,
    protocol_version=4,
    load_balancing_policy=WhiteListRoundRobinPolicy([AIO_IP]),  # Restrict to only this host
    **timeout_config
)
logging.info("Connecting to AIO Cassandra server (single node only)...")
aio = aio_cluster.connect()
aio.default_timeout = 600
aio.default_consistency_level = ConsistencyLevel.ONE
logging.info("Connection to AIO Cassandra server successful")

# --- Schema Detection ---
def detect_table_has_org_id(session, table_name):
    """
    Detect if a specific table has org_id column.
    Returns True if org_id exists, False otherwise.
    """
    try:
        query = f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='precinct' AND table_name='{table_name}' AND column_name='org_id'"
        result = session.execute(query)
        has_org_id = len(list(result)) > 0
        logging.info(f"Schema detection: {table_name} table {'HAS' if has_org_id else 'DOES NOT HAVE'} org_id column")
        return has_org_id
    except Exception as e:
        logging.warning(f"Could not detect schema for {table_name}: {e}. Assuming old schema with org_id.")
        return True

# Detect schemas for all relevant tables
TABLES_TO_CHECK = ['objects', 'reports', 'incidents', 'incident_summary', 'partition_index', 'incident_to_partition']
SOURCE_SCHEMAS = {}
DEST_SCHEMAS = {}

logging.info("Detecting source cluster (IMPACT) schemas...")
for table in TABLES_TO_CHECK:
    SOURCE_SCHEMAS[table] = detect_table_has_org_id(impact, table)

logging.info("\nDetecting destination cluster (AIO) schemas...")
for table in TABLES_TO_CHECK:
    DEST_SCHEMAS[table] = detect_table_has_org_id(aio, table)

logging.info("\n" + "="*80)
logging.info("Schema Summary:")
logging.info("="*80)
for table in TABLES_TO_CHECK:
    logging.info(f"  {table:25s} | Source: {'WITH org_id' if SOURCE_SCHEMAS[table] else 'NO org_id':15s} | Dest: {'WITH org_id' if DEST_SCHEMAS[table] else 'NO org_id':15s}")
logging.info("="*80)

# Backwards compatibility
SOURCE_HAS_ORG_ID = SOURCE_SCHEMAS.get('objects', True)
DEST_HAS_ORG_ID = DEST_SCHEMAS.get('objects', True)

# --- Helper Functions ---
def truncate_table(session, table_name):
    """
    Truncate a table in the destination cluster.
    WARNING: This permanently deletes all data in the table!
    Skips truncation if the table has already been completed.
    """
    # Check checkpoint before truncating
    if is_table_completed(checkpoint, table_name):
        logging.info(f"  ⏭️  Skipping truncation of {table_name} (already completed)")
        return True
    
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"
    try:
        logging.info(f"  TRUNCATING {full_table_name}...")
        session.execute(f"TRUNCATE {full_table_name};")
        logging.info(f"  ✓ Successfully truncated {full_table_name}")
        return True
    except Exception as e:
        logging.error(f"  ✗ Failed to truncate {full_table_name}: {e}")
        return False


def get_consistency_name(consistency_level):
    """Get the name of a consistency level."""
    consistency_names = {
        ConsistencyLevel.ONE: "ONE",
        ConsistencyLevel.QUORUM: "QUORUM",
        ConsistencyLevel.ALL: "ALL",
        ConsistencyLevel.LOCAL_QUORUM: "LOCAL_QUORUM",
    }
    return consistency_names.get(consistency_level, str(consistency_level))


def execute_with_retry(session, statement, params=None, max_retries=3):
    """Execute a statement with automatic retry on consistency failures."""
    original_consistency = statement.consistency_level
    
    for retry in range(max_retries + 1):
        try:
            if params:
                result = session.execute(statement, params)
            else:
                result = session.execute(statement)
            
            # Check for warnings but suppress LOGGED BATCH warnings
            if hasattr(result, 'warnings') and result.warnings:
                for warning in result.warnings:
                    # Suppress "Executing a LOGGED BATCH" warnings
                    if 'Executing a LOGGED BATCH' in warning:
                        continue  # Skip logging this warning
                    # Log other warnings
                    logging.warning(f"Server warning: {warning}")
            
            return result
            
        except (Unavailable, ReadTimeout, WriteTimeout, ReadFailure) as e:
            if retry < max_retries:
                wait_time = 2 ** retry  # Exponential backoff: 1s, 2s, 4s, 8s
                if isinstance(e, ReadFailure):
                    logging.warning(f"ReadFailure on replica: {e}. Waiting {wait_time}s before retry {retry + 1}/{max_retries}...")
                    time.sleep(wait_time)
                elif statement.consistency_level == ConsistencyLevel.QUORUM:
                    logging.warning(f"Consistency QUORUM failed: {e}. Retrying with ONE...")
                    statement.consistency_level = ConsistencyLevel.ONE
                    time.sleep(wait_time)
                else:
                    logging.warning(f"Query failed: {e}. Waiting {wait_time}s before retry {retry + 1}/{max_retries}...")
                    time.sleep(wait_time)
            else:
                logging.error(f"Final retry failed after {max_retries} attempts: {e}")
                raise
        except NoHostAvailable as e:
            logging.error(f"No hosts available: {e}")
            if retry < max_retries:
                wait_time = 2 ** retry
                logging.warning(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                raise
    
    statement.consistency_level = original_consistency


# --- User Transformation Functions ---
def bcrypt_2a_to_2y(password_hash):
    """Convert bcrypt $2a$ hash to $2y$ format."""
    if password_hash and password_hash.startswith('$2a$'):
        return password_hash.replace('$2a$', '$2y$', 1)
    return password_hash


def map_data_role_to_integer(data_roles_list):
    """Map data role strings to integers. In new system: 0 = High access"""
    return 0


def generate_user_id(username, index, org_id_prefix=42218):
    """
    Generate a consistent integer ID for each user based on org ID.
    Format: {org_id}{sequential_number}
    Example: org_id=42218, first user=4221800001, second=4221800002
    """
    sequential_id = index + 1
    user_id = int(f"{org_id_prefix}{sequential_id:05d}")
    return user_id


def map_function_role_label(function_role):
    """Map function role integer to label."""
    role_map = {1: "Admin", 2: "User", 3: "Read-Only"}
    return role_map.get(function_role, "User")


def map_data_role_label(data_role):
    """Map data role integer to label."""
    role_map = {0: "High", 1: "Medium", 2: "Low"}
    return role_map.get(data_role, "High")


def generate_api_token(username):
    """Generate a pseudo API token from username hash."""
    hash_obj = hashlib.sha256(username.encode())
    return hash_obj.hexdigest()[:60]


def transform_user(old_user, index, org_id_prefix=42218):
    """Transform old IMPACT user format to new AIO format."""
    username = old_user.get('username', '')
    display_name = old_user.get('display_name', '')
    if not display_name:
        display_name = username.split('@')[0] if '@' in username else username
    
    old_password = old_user.get('password', '')
    new_password = bcrypt_2a_to_2y(old_password)
    
    old_data_roles = old_user.get('data_roles', [])
    new_data_role = map_data_role_to_integer(old_data_roles)
    new_data_roles = [new_data_role]
    
    function_role = old_user.get('function_role', 1)
    
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
        
        updated_at = user.get('updated_at', user.get('created_at', ''))
        
        if username not in user_map:
            user_map[username] = (user, updated_at)
        else:
            existing_user, existing_time = user_map[username]
            if updated_at > existing_time:
                user_map[username] = (user, updated_at)
                logging.info(f"    Replacing older version of user: {username}")
            else:
                logging.debug(f"    Skipping duplicate user: {username}")
    
    return [user for user, _ in user_map.values()]


def transform_users_partition(users_json, org_id_prefix=42218):
    """
    Transform users from IMPACT format to AIO format.
    Returns transformed JSON string.
    """
    import json
    
    try:
        old_users = json.loads(users_json)
        
        if not isinstance(old_users, list):
            logging.warning("  Users object is not a list, wrapping it")
            old_users = [old_users]
        
        logging.info(f"  Loaded {len(old_users)} users from partition")
        
        # Deduplicate
        unique_users = deduplicate_users(old_users)
        duplicates = len(old_users) - len(unique_users)
        if duplicates > 0:
            logging.info(f"  Removed {duplicates} duplicate user(s)")
        
        # Transform
        new_users = []
        for index, old_user in enumerate(unique_users):
            try:
                new_user = transform_user(old_user, index, org_id_prefix)
                new_users.append(new_user)
                logging.debug(f"    Transformed: {old_user.get('username', 'unknown')} → ID {new_user['id']}")
            except Exception as e:
                logging.error(f"    Failed to transform user {old_user.get('username', 'unknown')}: {e}")
        
        logging.info(f"  Successfully transformed {len(new_users)} users")
        return json.dumps(new_users, ensure_ascii=False)
        
    except json.JSONDecodeError as e:
        logging.error(f"  Failed to parse users JSON: {e}")
        return users_json  # Return original if parsing fails
    except Exception as e:
        logging.error(f"  Error transforming users: {e}")
        return users_json  # Return original if transformation fails


def get_hourly_partitions(session, org_id, start_date=None, end_date=None):
    """
    Generate list of hourly partitions based on date range.
    Partition format: YYYY-MM-DD-HH (e.g., '2024-12-04-21')
    
    If no date range provided, generates last 365 days of hourly partitions.
    """
    from datetime import datetime, timedelta
    
    if end_date is None:
        end_date = datetime.utcnow()
    if start_date is None:
        # Default: last 365 days
        start_date = end_date - timedelta(days=365)
    
    partitions = []
    current = start_date.replace(minute=0, second=0, microsecond=0)
    
    while current <= end_date:
        partition = current.strftime('%Y-%m-%d-%H')
        partitions.append(partition)
        current += timedelta(hours=1)
    
    logging.info(f"  Generated {len(partitions)} hourly partitions from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    return partitions


def scan_table_by_token_ranges(session, table_name, org_id, consistency_level, num_ranges=10):
    """Scan table in token ranges to avoid overwhelming single query."""
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"
    source_has_org_id = SOURCE_SCHEMAS.get(table_name, True)
    
    logging.info(f"  Using token-range scan with {num_ranges} ranges...")
    
    # Calculate token range boundaries
    # Cassandra token range: -2^63 to 2^63-1
    min_token = -9223372036854775808
    max_token = 9223372036854775807
    range_size = (max_token - min_token) // num_ranges
    
    all_keys = []
    
    for i in range(num_ranges):
        start_token = min_token + (i * range_size)
        end_token = start_token + range_size if i < num_ranges - 1 else max_token
        
        try:
            logging.info(f"    Scanning token range {i+1}/{num_ranges}...")
            
            if source_has_org_id:
                # For tables with org_id, partition key is (org_id, partition)
                # token() must include ALL partition key components
                query = f"SELECT partition, created_at FROM {full_table_name} WHERE token(org_id, partition) >= {start_token} AND token(org_id, partition) < {end_token} AND org_id = ? ALLOW FILTERING;"
                statement = session.prepare(query)
                statement.consistency_level = consistency_level
                statement.fetch_size = 1000
                statement.timeout = 300  # 5 minutes per range
                rows = execute_with_retry(session, statement, [org_id])
            else:
                # For tables without org_id, partition key is just (partition)
                query = f"SELECT partition, created_at FROM {full_table_name} WHERE token(partition) >= {start_token} AND token(partition) < {end_token};"
                statement = session.prepare(query)
                statement.consistency_level = consistency_level
                statement.fetch_size = 1000
                statement.timeout = 300
                rows = execute_with_retry(session, statement)
            
            range_keys = [(row.partition, row.created_at) for row in rows]
            all_keys.extend(range_keys)
            logging.info(f"      Found {len(range_keys)} rows in range {i+1}")
            
        except Exception as e:
            logging.error(f"    Failed to scan token range {i+1}: {e}")
            # Continue with other ranges
    
    logging.info(f"  Token-range scan complete: {len(all_keys)} total rows")
    return all_keys


def copy_objects_partition(impact_session, aio_session, partition_name, org_id, consistency_level):
    """
    Copy specific partition from objects table (users, settings, etc.).
    These are small single-row partitions stored as JSON blobs.
    
    Handles both schema versions:
    - Old: PRIMARY KEY ((org_id, partition), created_at) - with org_id column
    - New: PRIMARY KEY (partition, created_at) - without org_id column
    
    Skips if already completed according to checkpoint.
    """
    # Check checkpoint before copying
    if is_partition_completed(checkpoint, partition_name):
        logging.info(f"  ⏭️  Skipping partition '{partition_name}' (already completed)")
        return 0
    
    keyspace = "precinct"
    table = "objects"
    full_table_name = f"{keyspace}.{table}"
    
    logging.info(f"  Copying partition '{partition_name}' from {table}...")
    
    try:
        # Query from source - adapt based on source schema
        if SOURCE_HAS_ORG_ID:
            query = f"SELECT partition, created_at, object FROM {full_table_name} WHERE partition = ? AND org_id = ?;"
            statement = impact_session.prepare(query)
            statement.consistency_level = consistency_level
            rows = execute_with_retry(impact_session, statement, [partition_name, org_id])
        else:
            query = f"SELECT partition, created_at, object FROM {full_table_name} WHERE partition = ?;"
            statement = impact_session.prepare(query)
            statement.consistency_level = consistency_level
            rows = execute_with_retry(impact_session, statement, [partition_name])
        
        rows_list = list(rows)
        
        if not rows_list:
            logging.warning(f"  No data found for partition '{partition_name}'")
            return 0
        
        logging.info(f"  Found {len(rows_list)} rows for partition '{partition_name}'")
        
        # Special handling for 'users' partition - transform users to new format
        if partition_name == 'users':
            logging.info(f"  Applying user transformations (IMPACT → AIO format)...")
            # Extract org_id prefix from IMPACT_ORG_ID (e.g., "calprivate" → 42218)
            # For now, use hardcoded default. Could be made configurable.
            org_id_prefix = 42218
            
            transformed_rows = []
            for row in rows_list:
                try:
                    transformed_object = transform_users_partition(row.object, org_id_prefix)
                    # Create new row with transformed data
                    class TransformedRow:
                        def __init__(self, partition, created_at, obj):
                            self.partition = partition
                            self.created_at = created_at
                            self.object = obj
                    
                    transformed_rows.append(TransformedRow(
                        row.partition,
                        row.created_at,
                        transformed_object
                    ))
                except Exception as e:
                    logging.error(f"  Failed to transform users row: {e}")
                    # Keep original if transformation fails
                    transformed_rows.append(row)
            
            rows_list = transformed_rows
            logging.info(f"  User transformation complete")
        
        # Delete existing data for this partition in AIO - adapt based on destination schema
        # Skip if we already truncated the table
        if not TRUNCATE_BEFORE_COPY:
            if DEST_HAS_ORG_ID:
                delete_query = f"DELETE FROM {full_table_name} WHERE partition = ? AND org_id = ?;"
                delete_stmt = aio_session.prepare(delete_query)
                delete_stmt.consistency_level = consistency_level
                try:
                    execute_with_retry(aio_session, delete_stmt, [partition_name, org_id])
                except Exception as e:
                    logging.warning(f"  Could not delete existing data (may not exist): {e}")
            else:
                delete_query = f"DELETE FROM {full_table_name} WHERE partition = ?;"
                delete_stmt = aio_session.prepare(delete_query)
                delete_stmt.consistency_level = consistency_level
                try:
                    execute_with_retry(aio_session, delete_stmt, [partition_name])
                except Exception as e:
                    logging.warning(f"  Could not delete existing data (may not exist): {e}")
        
        # Insert data - adapt based on destination schema
        if DEST_HAS_ORG_ID:
            insert_query = f"INSERT INTO {full_table_name} (partition, created_at, org_id, object) VALUES (?, ?, ?, ?);"
            insert_stmt = aio_session.prepare(insert_query)
            insert_stmt.consistency_level = consistency_level
            
            inserted = 0
            for row in rows_list:
                try:
                    execute_with_retry(aio_session, insert_stmt, [
                        row.partition,
                        row.created_at,
                        org_id,
                        row.object
                    ])
                    inserted += 1
                except Exception as e:
                    logging.error(f"  Failed to insert row for partition '{partition_name}': {e}")
        else:
            insert_query = f"INSERT INTO {full_table_name} (partition, created_at, object) VALUES (?, ?, ?);"
            insert_stmt = aio_session.prepare(insert_query)
            insert_stmt.consistency_level = consistency_level
            
            inserted = 0
            for row in rows_list:
                try:
                    execute_with_retry(aio_session, insert_stmt, [
                        row.partition,
                        row.created_at,
                        row.object
                    ])
                    inserted += 1
                except Exception as e:
                    logging.error(f"  Failed to insert row for partition '{partition_name}': {e}")
        
        logging.info(f"  Successfully copied {inserted}/{len(rows_list)} rows for partition '{partition_name}'")
        
        # Mark partition as completed
        mark_partition_completed(checkpoint, partition_name)
        
        return inserted
        
    except Exception as e:
        logging.error(f"  Error copying partition '{partition_name}': {e}")
        return 0


def copy_table_with_org_filter(impact_session, aio_session, table_name, org_id, fetch_size=100, max_retries=3):
    """
    Copy tables that may have org_id filtering: reports, incidents, etc.
    Optimized for parallel processing with batching.
    Handles both old schema (with org_id) and new schema (without org_id).
    Skips if already completed according to checkpoint.
    
    Schema note: All tables use PRIMARY KEY ((org_id, partition), created_at)
    There is no dedicated index table - partition_index is a regular data table.
    """
    # Check checkpoint before copying
    if is_table_completed(checkpoint, table_name):
        logging.info(f"  ⏭️  Skipping table '{table_name}' (already completed)")
        return
    
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"
    
    # Use table-specific batch size if available, otherwise use default
    batch_size = TABLE_BATCH_SIZES.get(table_name, BATCH_INSERT_SIZE)
    logging.info(f"Using batch size of {batch_size} for {table_name}")
    
    # Check if source and destination have org_id
    source_has_org_id = SOURCE_SCHEMAS.get(table_name, True)
    dest_has_org_id = DEST_SCHEMAS.get(table_name, False)
    
    # Determine data column name based on table type
    # IMPORTANT: Based on actual schema inspection, ALL tables use 'object' column!
    # Schema shows: org_id, partition, created_at, object (for all 6 tables)
    data_col = 'object'
    logging.info(f"Using data column '{data_col}' for {table_name}")
    
    # Build column lists based on schemas
    if source_has_org_id:
        source_cols = ['partition', 'created_at', 'org_id', data_col]
    else:
        source_cols = ['partition', 'created_at', data_col]
    
    if dest_has_org_id:
        dest_cols = ['partition', 'created_at', 'org_id', data_col]
        dest_placeholders = '?, ?, ?, ?'
    else:
        dest_cols = ['partition', 'created_at', data_col]
        dest_placeholders = '?, ?, ?'
    
    insert_query_str = f"INSERT INTO {full_table_name} ({', '.join(dest_cols)}) VALUES ({dest_placeholders});"
    
    for attempt in range(max_retries + 1):
        try:
            logging.info(f"\nAttempt {attempt + 1}/{max_retries + 1} to copy {full_table_name}...")
            consistency_level = ConsistencyLevel.ONE  # Use ONE for better performance
            
            matching_keys = []
            
            # Strategy: Full table scan with client-side org_id filtering
            # Scan all rows and discard those not matching our org_id
            # Use fetch_size=1 to minimize memory and network issues
            logging.info(f"Starting full table scan with client-side org_id filtering (fetch_size=1)...")
            
            if source_has_org_id:
                # Scan entire table, fetch org_id along with keys
                scan_query = f"SELECT partition, created_at, org_id FROM {full_table_name};"
                scan_statement = impact_session.prepare(scan_query)
                scan_statement.consistency_level = consistency_level
                scan_statement.fetch_size = 1  # Fetch one row at a time to reduce memory/network load
                scan_statement.timeout = None  # No timeout for full table scan
                
                logging.info(f"Scanning all rows in {full_table_name} (one row at a time)...")
                rows = execute_with_retry(impact_session, scan_statement)
                
                scanned = 0
                matched = 0
                for row in tqdm(rows, desc=f"Scanning {table_name}", unit="row"):
                    scanned += 1
                    if row.org_id == org_id:
                        matching_keys.append((row.partition, row.created_at))
                        matched += 1
                    
                    # Log progress every 10,000 rows
                    if scanned % 10000 == 0:
                        logging.info(f"  Scanned {scanned:,} rows, found {matched:,} matching org_id '{org_id}'")
                
                logging.info(f"Full table scan complete: scanned {scanned:,} rows, found {matched:,} matching rows")
            else:
                # If no org_id in source, scan and take all rows
                scan_query = f"SELECT partition, created_at FROM {full_table_name};"
                scan_statement = impact_session.prepare(scan_query)
                scan_statement.consistency_level = consistency_level
                scan_statement.fetch_size = 1  # Fetch one row at a time
                scan_statement.timeout = None
                
                logging.info(f"Scanning all rows in {full_table_name} (no org_id filtering, one row at a time)...")
                rows = execute_with_retry(impact_session, scan_statement)
                
                for row in tqdm(rows, desc=f"Scanning {table_name}", unit="row"):
                    matching_keys.append((row.partition, row.created_at))
            
            logging.info(f"Found {len(matching_keys):,} rows total for org_id '{org_id}'")
            
            if len(matching_keys) == 0:
                logging.warning(f"No rows found in {full_table_name}")
                return
            
            # 2. Delete existing data in AIO - adapt based on destination schema
            # Skip if we already truncated the table
            if not TRUNCATE_BEFORE_COPY:
                logging.info(f"Deleting existing data from AIO...")
                if dest_has_org_id:
                    delete_query = f"DELETE FROM {full_table_name} WHERE partition = ? AND created_at = ? AND org_id = ?;"
                    delete_stmt = aio_session.prepare(delete_query)
                    delete_stmt.consistency_level = consistency_level
                    
                    # Delete in batches
                    for i in tqdm(range(0, len(matching_keys), batch_size), desc="Deleting old data", unit="batch"):
                        batch_keys = matching_keys[i:i + batch_size]
                        batch = BatchStatement(consistency_level=consistency_level)
                        for partition, created_at in batch_keys:
                            batch.add(delete_stmt, [partition, created_at, org_id])
                        try:
                            execute_with_retry(aio_session, batch)
                        except Exception as e:
                            logging.warning(f"Batch delete failed: {e}")
                else:
                    delete_query = f"DELETE FROM {full_table_name} WHERE partition = ? AND created_at = ?;"
                    delete_stmt = aio_session.prepare(delete_query)
                    delete_stmt.consistency_level = consistency_level
                    
                    # Delete in batches
                    for i in tqdm(range(0, len(matching_keys), batch_size), desc="Deleting old data", unit="batch"):
                        batch_keys = matching_keys[i:i + batch_size]
                        batch = BatchStatement(consistency_level=consistency_level)
                        for partition, created_at in batch_keys:
                            batch.add(delete_stmt, [partition, created_at])
                        try:
                            execute_with_retry(aio_session, batch)
                        except Exception as e:
                            logging.warning(f"Batch delete failed: {e}")
            else:
                logging.info(f"Skipping deletion (table was truncated)")
            
            # 3. Fetch and insert data with parallel processing (individual inserts, no batching)
            logging.info(f"Copying data with {MAX_WORKERS} parallel workers (individual inserts)...")
            
            # Build detail query based on source schema
            if source_has_org_id:
                detail_query = f"SELECT {', '.join(source_cols)} FROM {full_table_name} WHERE partition = ? AND created_at = ? AND org_id = ?;"
            else:
                detail_query = f"SELECT {', '.join(source_cols)} FROM {full_table_name} WHERE partition = ? AND created_at = ?;"
            
            detail_stmt = impact_session.prepare(detail_query)
            detail_stmt.consistency_level = consistency_level
            
            insert_statement = aio_session.prepare(insert_query_str)
            insert_statement.consistency_level = consistency_level
            
            moved = 0
            failed = 0
            
            def fetch_row(partition, created_at):
                try:
                    if source_has_org_id:
                        result = execute_with_retry(impact_session, detail_stmt, [partition, created_at, org_id])
                    else:
                        result = execute_with_retry(impact_session, detail_stmt, [partition, created_at])
                    row = result.one()
                    if row:
                        # Extract data based on source schema
                        if source_has_org_id:
                            return (row.partition, row.created_at, row.org_id, getattr(row, data_col))
                        else:
                            return (row.partition, row.created_at, getattr(row, data_col))
                    return None
                except Exception as e:
                    logging.warning(f"Failed to fetch row: {e}")
                    return None
            
            with tqdm(total=len(matching_keys), desc=f"Copying {table_name}", unit="row") as pbar:
                for chunk_start in range(0, len(matching_keys), FETCH_BATCH_SIZE):
                    chunk_end = min(chunk_start + FETCH_BATCH_SIZE, len(matching_keys))
                    chunk_keys = matching_keys[chunk_start:chunk_end]
                    
                    # Parallel fetch
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        future_to_key = {
                            executor.submit(fetch_row, partition, created_at): (partition, created_at)
                            for partition, created_at in chunk_keys
                        }
                        
                        for future in as_completed(future_to_key):
                            try:
                                row_data = future.result()
                                if row_data:
                                    # Transform data if schemas differ
                                    if source_has_org_id and not dest_has_org_id:
                                        # Remove org_id from tuple (it's at index 2)
                                        insert_data = (row_data[0], row_data[1], row_data[3])
                                    elif not source_has_org_id and dest_has_org_id:
                                        # Add org_id to tuple (insert at index 2)
                                        insert_data = (row_data[0], row_data[1], org_id, row_data[2])
                                    else:
                                        # Schemas match, use as-is
                                        insert_data = row_data
                                    
                                    # Insert row individually (no batching)
                                    try:
                                        execute_with_retry(aio_session, insert_statement, insert_data)
                                        moved += 1
                                    except Exception as e:
                                        logging.error(f"Insert failed: {e}")
                                        failed += 1
                                else:
                                    failed += 1
                            except Exception as e:
                                logging.error(f"Error processing row: {e}")
                                failed += 1
                            finally:
                                pbar.update(1)
            
            logging.info(f"Successfully copied {moved} rows from {full_table_name}")
            if failed > 0:
                logging.warning(f"Failed to copy {failed} rows")
            
            # Mark table as completed
            mark_table_completed(checkpoint, table_name)
            
            return
            
        except (ReadTimeout, Unavailable, WriteTimeout) as e:
            logging.error(f"Cassandra error during copy of {full_table_name}: {e}")
            if attempt < max_retries:
                logging.info(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logging.error(f"Max retries reached for {full_table_name}")
                raise
        except Exception as e:
            logging.error(f"Unexpected error during copy of {full_table_name}: {e}", exc_info=True)
            raise


# --- Main Execution Logic ---
logging.info("="*80)
logging.info("Starting Optimized Migration Process")
logging.info("="*80)

# Truncate tables if requested
if TRUNCATE_BEFORE_COPY:
    logging.info("\n" + "="*80)
    logging.info("TRUNCATING DESTINATION TABLES (as requested in config)")
    logging.info("="*80)
    logging.warning("⚠️  WARNING: This will permanently delete all data in destination tables!")
    
    tables_to_truncate = ['objects', 'reports', 'incidents', 'incident_summary', 'partition_index', 'incident_to_partition']
    for table in tables_to_truncate:
        truncate_table(aio, table)
    
    logging.info("Truncation complete.\n")

migration_stats = {}
consistency_level = ConsistencyLevel.ONE

# PRIORITY 1: Copy users and settings first (from objects table partitions)
# These are critical configuration data stored as single-row partitions
logging.info("\n" + "="*80)
logging.info("PHASE 1: Migrating Users and Settings (Critical Configuration)")
logging.info("="*80)

object_partitions_to_copy = [
    'users',           # User accounts and permissions
    'settings',        # System settings
    'api_tokens',      # API authentication tokens
    'report_index',    # Report metadata index
    'search_jobs',     # Saved search jobs
    'saved_searches',  # Saved search configurations
]

for partition in object_partitions_to_copy:
    try:
        start_time = time.time()
        count = copy_objects_partition(impact, aio, partition, IMPACT_ORG_ID, consistency_level)
        elapsed_time = time.time() - start_time
        migration_stats[f"objects:{partition}"] = {
            'status': 'SUCCESS',
            'count': count,
            'time': elapsed_time
        }
        logging.info(f"Completed objects:{partition} in {elapsed_time:.2f} seconds")
    except Exception as e:
        migration_stats[f"objects:{partition}"] = {
            'status': 'FAILED',
            'error': str(e)
        }
        logging.error(f"Failed to migrate objects:{partition}: {e}")

# PRIORITY 2: Copy reports
logging.info("\n" + "="*80)
logging.info("PHASE 2: Migrating Reports")
logging.info("="*80)

try:
    start_time = time.time()
    copy_table_with_org_filter(impact, aio, "reports", IMPACT_ORG_ID, fetch_size=100)
    elapsed_time = time.time() - start_time
    migration_stats["reports"] = {
        'status': 'SUCCESS',
        'time': elapsed_time
    }
    logging.info(f"Completed reports in {elapsed_time:.2f} seconds")
except Exception as e:
    migration_stats["reports"] = {
        'status': 'FAILED',
        'error': str(e)
    }
    logging.error(f"Failed to migrate reports: {e}")

# PRIORITY 3: Copy incident-related tables
logging.info("\n" + "="*80)
logging.info("PHASE 3: Migrating Incidents")
logging.info("="*80)

incident_tables = [
    "incident_summary",      # Summary data
    "partition_index",       # Index for partition lookups
    "incident_to_partition", # Mapping table
    "incidents",             # Full incident data
]

for table in incident_tables:
    try:
        start_time = time.time()
        copy_table_with_org_filter(impact, aio, table, IMPACT_ORG_ID, fetch_size=100)
        elapsed_time = time.time() - start_time
        migration_stats[table] = {
            'status': 'SUCCESS',
            'time': elapsed_time
        }
        logging.info(f"Completed {table} in {elapsed_time:.2f} seconds")
    except Exception as e:
        migration_stats[table] = {
            'status': 'FAILED',
            'error': str(e)
        }
        logging.error(f"Failed to migrate {table}: {e}")

# --- Final Summary ---
logging.info("\n" + "="*80)
logging.info("MIGRATION SUMMARY")
logging.info("="*80)

total_time = sum(stats.get('time', 0) for stats in migration_stats.values() if stats['status'] == 'SUCCESS')
success_count = sum(1 for stats in migration_stats.values() if stats['status'] == 'SUCCESS')
failed_count = sum(1 for stats in migration_stats.values() if stats['status'] == 'FAILED')

for table, stats in migration_stats.items():
    if stats['status'] == 'SUCCESS':
        time_str = f"{stats['time']:.2f}s"
        count_str = f" ({stats['count']} rows)" if 'count' in stats else ""
        logging.info(f"  ✓ {table}: SUCCESS {time_str}{count_str}")
    else:
        logging.error(f"  ✗ {table}: FAILED - {stats.get('error', 'Unknown error')}")

logging.info("="*80)
logging.info(f"Total: {success_count} succeeded, {failed_count} failed")
logging.info(f"Total migration time: {total_time:.2f} seconds")
logging.info("="*80)

logging.info("\nShutting down connections...")
impact_cluster.shutdown()
aio_cluster.shutdown()
logging.info("Migration complete! Check migrator.log for details.")
