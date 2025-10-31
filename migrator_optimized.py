import time
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, ConsistencyLevel, Unavailable, WriteTimeout
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
import yaml
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migrator.log'),
        logging.StreamHandler()
    ]
)

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
impact.default_timeout = 600
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
    """
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


def execute_with_retry(session, statement, params=None, max_retries=2):
    """Execute a statement with automatic retry on consistency failures."""
    original_consistency = statement.consistency_level
    
    for retry in range(max_retries + 1):
        try:
            if params:
                result = session.execute(statement, params)
            else:
                result = session.execute(statement)
            
            # Check for gc_grace_seconds warning and pause if detected
            if hasattr(result, 'warnings') and result.warnings:
                for warning in result.warnings:
                    if 'gc_grace_seconds' in warning:
                        logging.warning(f"Server warning detected: {warning}")
                        logging.warning("Pausing for 30 seconds to allow batchlog entries to be processed...")
                        time.sleep(30)
                        break
            
            return result
            
        except (Unavailable, ReadTimeout, WriteTimeout) as e:
            if retry < max_retries:
                if statement.consistency_level == ConsistencyLevel.QUORUM:
                    logging.warning(f"Consistency QUORUM failed: {e}. Retrying with ONE...")
                    statement.consistency_level = ConsistencyLevel.ONE
                elif retry == max_retries - 1:
                    logging.error(f"Final retry failed: {e}")
                    raise
            else:
                raise
        except NoHostAvailable as e:
            logging.error(f"No hosts available: {e}")
            if retry < max_retries:
                time.sleep(2 ** retry)
            else:
                raise
    
    statement.consistency_level = original_consistency


def copy_objects_partition(impact_session, aio_session, partition_name, org_id, consistency_level):
    """
    Copy specific partition from objects table (users, settings, etc.).
    These are small single-row partitions stored as JSON blobs.
    
    Handles both schema versions:
    - Old: PRIMARY KEY ((org_id, partition), created_at) - with org_id column
    - New: PRIMARY KEY (partition, created_at) - without org_id column
    """
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
        return inserted
        
    except Exception as e:
        logging.error(f"  Error copying partition '{partition_name}': {e}")
        return 0


def copy_table_with_org_filter(impact_session, aio_session, table_name, org_id, fetch_size=100, max_retries=3):
    """
    Copy tables that may have org_id filtering: reports, incidents, etc.
    Optimized for parallel processing with batching.
    Handles both old schema (with org_id) and new schema (without org_id).
    """
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"
    
    # Use table-specific batch size if available, otherwise use default
    batch_size = TABLE_BATCH_SIZES.get(table_name, BATCH_INSERT_SIZE)
    logging.info(f"Using batch size of {batch_size} for {table_name}")
    
    # Check if source and destination have org_id
    source_has_org_id = SOURCE_SCHEMAS.get(table_name, True)
    dest_has_org_id = DEST_SCHEMAS.get(table_name, False)
    
    # Determine data column name based on table type
    if table_name == "reports":
        data_col = 'object'
    elif table_name in ["incidents", "incident_to_partition", "partition_index", "incident_summary"]:
        data_col = 'incident'
    else:
        logging.error(f"Unknown table structure for {table_name}")
        return
    
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
            
            # 1. Scan for matching rows - adapt query based on source schema
            if source_has_org_id:
                logging.info(f"Scanning {full_table_name} for org_id {org_id}...")
                lightweight_query = f"SELECT partition, created_at FROM {full_table_name} WHERE org_id = ? ALLOW FILTERING;"
                lightweight_statement = impact_session.prepare(lightweight_query)
                lightweight_statement.consistency_level = consistency_level
                lightweight_statement.fetch_size = fetch_size
                lightweight_rows = execute_with_retry(impact_session, lightweight_statement, [org_id])
            else:
                # If source doesn't have org_id, we need to scan all rows
                logging.info(f"Scanning all rows in {full_table_name} (no org_id filtering)...")
                lightweight_query = f"SELECT partition, created_at FROM {full_table_name};"
                lightweight_statement = impact_session.prepare(lightweight_query)
                lightweight_statement.consistency_level = consistency_level
                lightweight_statement.fetch_size = fetch_size
                lightweight_rows = execute_with_retry(impact_session, lightweight_statement)
            
            # Collect all matching keys
            matching_keys = []
            for row in tqdm(lightweight_rows, desc=f"Scanning {table_name}", unit="row"):
                matching_keys.append((row.partition, row.created_at))
            
            logging.info(f"Found {len(matching_keys)} rows")
            
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
            
            # 3. Fetch and insert data with parallel processing
            logging.info(f"Copying data with {MAX_WORKERS} parallel workers...")
            
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
            batch_data = []
            
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
                                    
                                    batch_data.append(insert_data)
                                    
                                    if len(batch_data) >= batch_size:
                                        batch = BatchStatement(consistency_level=consistency_level)
                                        for values in batch_data:
                                            batch.add(insert_statement, values)
                                        try:
                                            execute_with_retry(aio_session, batch)
                                            moved += len(batch_data)
                                        except Exception as e:
                                            error_msg = str(e)
                                            if "Batch too large" in error_msg:
                                                # Insert rows individually instead of batching
                                                logging.warning(f"Batch too large ({len(batch_data)} rows), inserting individually...")
                                                for values in batch_data:
                                                    try:
                                                        execute_with_retry(aio_session, insert_statement, values)
                                                        moved += 1
                                                    except Exception as e2:
                                                        logging.error(f"Individual insert failed: {e2}")
                                                        failed += 1
                                            else:
                                                logging.warning(f"Batch insert failed: {e}")
                                                failed += len(batch_data)
                                        batch_data = []
                                else:
                                    failed += 1
                            except Exception as e:
                                logging.error(f"Error processing row: {e}")
                                failed += 1
                            finally:
                                pbar.update(1)
                
                # Insert remaining batch
                if batch_data:
                    batch = BatchStatement(consistency_level=consistency_level)
                    for values in batch_data:
                        batch.add(insert_statement, values)
                    try:
                        execute_with_retry(aio_session, batch)
                        moved += len(batch_data)
                    except Exception as e:
                        error_msg = str(e)
                        if "Batch too large" in error_msg:
                            # Insert rows individually instead of batching
                            logging.warning(f"Final batch too large ({len(batch_data)} rows), inserting individually...")
                            for values in batch_data:
                                try:
                                    execute_with_retry(aio_session, insert_statement, values)
                                    moved += 1
                                except Exception as e2:
                                    logging.error(f"Individual insert failed: {e2}")
                                    failed += 1
                        else:
                            logging.warning(f"Final batch insert failed: {e}")
                            failed += len(batch_data)
            
            logging.info(f"Successfully copied {moved} rows from {full_table_name}")
            if failed > 0:
                logging.warning(f"Failed to copy {failed} rows")
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
