import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, ConsistencyLevel, Unavailable, WriteTimeout, NoHostAvailable
from cassandra.query import BatchStatement, SimpleStatement
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
import yaml
from cassandra.policies import RetryPolicy
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
# Load configuration from YAML file
with open('migrator_settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Extract configuration values
IMPACT_CLUSTER_SEED_NODES = config['IMPACT_CLUSTER_SEED_NODES']
IMPACT_CLUSTER_USERNAME = config['IMPACT_CLUSTER_USERNAME']
IMPACT_CLUSTER_PASSWORD = config['IMPACT_CLUSTER_PASSWORD']
IMPACT_ORG_ID = config['IMPACT_ORG_ID']
AIO_IP = config['AIO_IP']
AIO_USERNAME = config['AIO_USERNAME']
AIO_PASSWORD = config['AIO_PASSWORD']

# Performance tuning parameters
MAX_WORKERS = config.get('MAX_WORKERS', 5)  # Number of parallel threads
BATCH_INSERT_SIZE = config.get('BATCH_INSERT_SIZE', 50)  # Cassandra batch size
FETCH_BATCH_SIZE = config.get('FETCH_BATCH_SIZE', 100)  # How many rows to process before small delay

# --- Cassandra Connections ---
# Setup SSL context with proper protocol argument (fixing deprecation warning)
ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = TLSVersion.TLSv1_2
ssl_context.check_hostname = False
ssl_context.verify_mode = CERT_NONE

# Connection timeout configurations (in seconds)
timeout_config = {
    'connect_timeout': 30,         # Connection timeout
    'control_connection_timeout': 30  # Control connection timeout
}

# Connect to Cassandra IMPACT cluster
impact_auth_provider = PlainTextAuthProvider(IMPACT_CLUSTER_USERNAME, IMPACT_CLUSTER_PASSWORD)
impact_cluster = Cluster(
    [IMPACT_CLUSTER_SEED_NODES],
    auth_provider=impact_auth_provider,
    ssl_context=ssl_context,
    protocol_version=4,  # Explicitly set protocol version
    **timeout_config  # Apply timeout configuration
)
logging.info("Connecting to IMPACT Cassandra server...")
impact = impact_cluster.connect()
# Set session-level timeout for queries
impact.default_timeout = 600  # 10 minutes timeout for queries
impact.default_consistency_level = ConsistencyLevel.ONE
logging.info("Connection to IMPACT Cassandra server successful")

# Connect to AIO cluster
aio_auth_provider = PlainTextAuthProvider(AIO_USERNAME, AIO_PASSWORD)
aio_cluster = Cluster(
    [AIO_IP],
    auth_provider=aio_auth_provider,
    ssl_context=ssl_context,
    protocol_version=4,  # Explicitly set protocol version
    **timeout_config  # Apply same timeout configuration
)
logging.info("Connecting to AIO Cassandra server...")
aio = aio_cluster.connect()
# Set session-level timeout for queries
aio.default_timeout = 600  # 10 minutes timeout for queries
aio.default_consistency_level = ConsistencyLevel.ONE
logging.info("Connection to AIO Cassandra server successful")

# --- Helper Functions ---
def get_consistency_name(consistency_level):
    """Get the name of a consistency level."""
    consistency_names = {
        ConsistencyLevel.ONE: "ONE",
        ConsistencyLevel.QUORUM: "QUORUM",
        ConsistencyLevel.ALL: "ALL",
        ConsistencyLevel.LOCAL_QUORUM: "LOCAL_QUORUM",
        ConsistencyLevel.EACH_QUORUM: "EACH_QUORUM",
        ConsistencyLevel.LOCAL_ONE: "LOCAL_ONE",
        ConsistencyLevel.ANY: "ANY",
        ConsistencyLevel.TWO: "TWO",
        ConsistencyLevel.THREE: "THREE",
    }
    return consistency_names.get(consistency_level, str(consistency_level))


def execute_with_retry(session, statement, params=None, max_retries=2):
    """Execute a statement with automatic retry on consistency failures."""
    original_consistency = statement.consistency_level
    
    for retry in range(max_retries + 1):
        try:
            if params:
                return session.execute(statement, params)
            else:
                return session.execute(statement)
        except (Unavailable, ReadTimeout, WriteTimeout) as e:
            if retry < max_retries:
                # Try with lower consistency level
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
                time.sleep(2 ** retry)  # Exponential backoff
            else:
                raise
    
    # Reset to original consistency level
    statement.consistency_level = original_consistency


def fetch_row_data(impact_session, full_table_name, cols_to_copy, partition, created_at, org_id, consistency_level):
    """Fetch a single row's data with error handling."""
    try:
        detailed_query = f"SELECT {', '.join(cols_to_copy)} FROM {full_table_name} WHERE partition = ? AND created_at = ? AND org_id = ?;"
        detailed_statement = impact_session.prepare(detailed_query)
        detailed_statement.consistency_level = consistency_level
        
        result = execute_with_retry(impact_session, detailed_statement, [partition, created_at, org_id])
        row = result.one()
        
        if row:
            return tuple(getattr(row, col) for col in cols_to_copy)
        return None
    except Exception as e:
        logging.warning(f"Failed to fetch row (partition={partition}, created_at={created_at}): {e}")
        return None


def insert_batch(aio_session, insert_statement, batch_data):
    """Insert a batch of data with retry logic."""
    if not batch_data:
        return 0
    
    try:
        batch = BatchStatement(consistency_level=insert_statement.consistency_level)
        for values in batch_data:
            batch.add(insert_statement, values)
        
        execute_with_retry(aio_session, batch)
        return len(batch_data)
    except Exception as e:
        logging.warning(f"Batch insert failed: {e}. Falling back to individual inserts...")
        # Fallback: insert one by one
        success_count = 0
        for values in batch_data:
            try:
                execute_with_retry(aio_session, insert_statement, values)
                success_count += 1
            except Exception as insert_err:
                logging.error(f"Failed to insert row: {insert_err}")
        return success_count


# --- Table Copy Function with Optimized Performance ---
def copy_table(impact_session, aio_session, table_name, org_id, fetch_size=100, max_retries=3, retry_delay=5):
    """Copies data for a specific table from IMPACT to AIO with optimized performance."""
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"

    # Define insert queries and columns based on table name
    if table_name in ["objects", "reports"]:
        insert_query_str = f"INSERT INTO {full_table_name} (partition, created_at, object) VALUES (?, ?, ?);"
        cols_to_copy = ('partition', 'created_at', 'object')
    elif table_name in ["incident_to_partition", "partition_index", "incident_summary", "incidents"]:
        insert_query_str = f"INSERT INTO {full_table_name} (partition, created_at, incident) VALUES (?, ?, ?);"
        cols_to_copy = ('partition', 'created_at', 'incident')
    else:
        logging.error(f"Unknown table structure for {table_name}")
        return

    for attempt in range(max_retries + 1):
        try:
            logging.info(f"\nAttempt {attempt + 1}/{max_retries + 1} to copy {full_table_name}...")
            source_row = 0
            consistency_level = ConsistencyLevel.QUORUM
            
            # 1. Scan table for matching rows
            logging.info(f"Querying lightweight fields with consistency {get_consistency_name(consistency_level)}...")
            lightweight_query = f"SELECT org_id, partition, created_at FROM {full_table_name};"
            lightweight_statement = impact_session.prepare(lightweight_query)
            lightweight_statement.consistency_level = consistency_level
            lightweight_statement.fetch_size = fetch_size
            
            try:
                lightweight_rows = execute_with_retry(impact_session, lightweight_statement)
            except Exception as e:
                logging.error(f"Failed to execute lightweight query: {e}")
                raise

            # Collect matching keys
            matching_keys = []
            with tqdm(total=fetch_size, desc="Scanning rows", unit="row") as scan_pbar:
                for row in lightweight_rows:
                    if source_row % 1000 == 0 and source_row > 0:
                        time.sleep(0.5)  # Reduced cooldown time
                    source_row += 1
                    scan_pbar.total = source_row
                    scan_pbar.refresh()

                    if hasattr(row, 'org_id') and row.org_id == org_id:
                        matching_keys.append((row.partition, row.created_at))
                        scan_pbar.set_postfix(found=len(matching_keys))

                    scan_pbar.update(1)

            logging.info(f"Found {len(matching_keys)} rows matching org_id {org_id}")
            
            if len(matching_keys) == 0:
                logging.warning(f"No rows found for org_id {org_id} in {full_table_name}")
                return
            
            # 2. Truncate Destination Table
            logging.info(f"Truncating {full_table_name} in AIO...")
            truncate_query = f"TRUNCATE {full_table_name};"
            truncate_statement = aio_session.prepare(truncate_query)
            truncate_statement.consistency_level = consistency_level
            
            try:
                execute_with_retry(aio_session, truncate_statement)
                logging.info("Truncate complete.")
            except Exception as e:
                logging.error(f"Failed to truncate table: {e}")
                raise

            # 3. Fetch and insert data with parallel processing
            logging.info(f"Fetching and copying data with {MAX_WORKERS} parallel workers...")
            insert_statement = aio_session.prepare(insert_query_str)
            insert_statement.consistency_level = consistency_level
            moved = 0
            failed = 0
            batch_data = []

            with tqdm(total=len(matching_keys), desc="Copying rows", unit="row") as pbar:
                # Process in chunks for better memory management
                for chunk_start in range(0, len(matching_keys), FETCH_BATCH_SIZE):
                    chunk_end = min(chunk_start + FETCH_BATCH_SIZE, len(matching_keys))
                    chunk_keys = matching_keys[chunk_start:chunk_end]
                    
                    # Parallel fetch
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        future_to_key = {
                            executor.submit(
                                fetch_row_data,
                                impact_session,
                                full_table_name,
                                cols_to_copy,
                                partition,
                                created_at,
                                org_id,
                                consistency_level
                            ): (partition, created_at)
                            for partition, created_at in chunk_keys
                        }
                        
                        for future in as_completed(future_to_key):
                            partition, created_at = future_to_key[future]
                            try:
                                row_data = future.result()
                                if row_data:
                                    batch_data.append(row_data)
                                    
                                    # Insert in batches
                                    if len(batch_data) >= BATCH_INSERT_SIZE:
                                        inserted = insert_batch(aio_session, insert_statement, batch_data)
                                        moved += inserted
                                        failed += len(batch_data) - inserted
                                        batch_data = []
                                else:
                                    failed += 1
                            except Exception as e:
                                logging.error(f"Error processing row ({partition}, {created_at}): {e}")
                                failed += 1
                            finally:
                                pbar.update(1)
                    
                    # Small delay between chunks
                    if chunk_end < len(matching_keys):
                        time.sleep(0.05)
                
                # Insert remaining batch
                if batch_data:
                    inserted = insert_batch(aio_session, insert_statement, batch_data)
                    moved += inserted
                    failed += len(batch_data) - inserted

            logging.info(f"Successfully copied {moved} rows from {full_table_name} to AIO.")
            if failed > 0:
                logging.warning(f"Failed to copy {failed} rows.")
            return
            
        except (ReadTimeout, Unavailable, WriteTimeout) as e:
            logging.error(f"Cassandra error during copy of {full_table_name}: {e}")
            if attempt < max_retries:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Max retries reached for {full_table_name}.")
                raise
        except Exception as e:
            logging.error(f"Unexpected error during copy of {full_table_name}: {e}", exc_info=True)
            raise


# --- Main Execution Logic ---
# Define tables to copy and their fetch sizes
tables_to_copy = {
    "objects": 100,
    "reports": 100,
    "incident_to_partition": 100,
    "partition_index": 100,
    "incident_summary": 100,
    "incidents": 100
}

# Call the function for each table
logging.info("Starting migration process...")
migration_stats = {}

for table, fetch_size in tables_to_copy.items():
    try:
        start_time = time.time()
        copy_table(impact, aio, table, IMPACT_ORG_ID, fetch_size=fetch_size)
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

# --- Shutdown ---
logging.info("\n" + "="*60)
logging.info("Migration Summary:")
for table, stats in migration_stats.items():
    if stats['status'] == 'SUCCESS':
        logging.info(f"  {table}: ✓ SUCCESS ({stats['time']:.2f}s)")
    else:
        logging.error(f"  {table}: ✗ FAILED - {stats.get('error', 'Unknown error')}")
logging.info("="*60)

logging.info("\nShutting down connections...")
impact_cluster.shutdown()
aio_cluster.shutdown()
logging.info("Migration process finished. Check migrator.log for details.")