import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, ConsistencyLevel, Unavailable, WriteTimeout # Import ReadTimeout and ConsistencyLevel
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
import yaml
from cassandra.policies import RetryPolicy
from tqdm import tqdm

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
    **timeout_config  # Apply timeout configuration
)
print("Connecting to IMPACT Cassandra server...")
impact = impact_cluster.connect()
# Set session-level timeout for queries
impact.default_timeout = 600  # 10 minutes timeout for queries
impact.default_consistency_level = ConsistencyLevel.ONE
print("Connection to IMPACT Cassandra server successful")

# Connect to AIO cluster
aio_auth_provider = PlainTextAuthProvider(AIO_USERNAME, AIO_PASSWORD)
aio_cluster = Cluster(
    [AIO_IP],
    auth_provider=aio_auth_provider,
    ssl_context=ssl_context,
    **timeout_config  # Apply same timeout configuration
)
print("Connecting to AIO Cassandra server...")
aio = aio_cluster.connect()
# Set session-level timeout for queries
aio.default_timeout = 600  # 10 minutes timeout for queries
aio.default_consistency_level = ConsistencyLevel.ONE
print("Connection to AIO Cassandra server successful")


# --- Helper Function ---
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


# --- Table Copy Function with Retry Logic ---
def copy_table(impact_session, aio_session, table_name, org_id, fetch_size=10, max_retries=3, retry_delay=5):
    """Copies data for a specific table from IMPACT to AIO with retry logic."""
    keyspace = "precinct"
    full_table_name = f"{keyspace}.{table_name}"

    # Define insert queries and columns based on table name
    # Note: Adjust column names if they differ significantly in the actual schema
    if table_name in ["objects", "reports"]:
        insert_query_str = f"INSERT INTO {full_table_name} (partition, created_at, object) VALUES (?, ?, ?);"
        cols_to_copy = ('partition', 'created_at', 'object')
    elif table_name in ["incident_to_partition", "partition_index", "incident_summary", "incidents"]:
        insert_query_str = f"INSERT INTO {full_table_name} (partition, created_at, incident) VALUES (?, ?, ?);"
        # Assuming the third column is 'incident' for these tables based on name
        cols_to_copy = ('partition', 'created_at', 'incident')
    else:
        print(f"Error: Unknown table structure for {table_name}")
        return

    for attempt in range(max_retries + 1):
        try:
            print(f"\nAttempt {attempt + 1}/{max_retries + 1} to copy {full_table_name}...")
            source_row = 0
            consistency_level = ConsistencyLevel.QUORUM
            
            # 1. First query - only get lightweight fields (org_id, partition, created_at)
            print(f"Querying lightweight fields to identify relevant rows with consistency {get_consistency_name(consistency_level)}. This will do a complete table scan and may take a while...")
            lightweight_query = f"SELECT org_id, partition, created_at FROM {full_table_name};"
            lightweight_statement = impact_session.prepare(lightweight_query)
            lightweight_statement.consistency_level = consistency_level
            lightweight_statement.fetch_size = fetch_size
            
            try:
                lightweight_rows = impact_session.execute(lightweight_statement)
            except (Unavailable, ReadTimeout, WriteTimeout) as consistency_error:
                print(f"Consistency QUORUM failed: {consistency_error}")
                print(f"Retrying with consistency ONE...")
                consistency_level = ConsistencyLevel.ONE
                lightweight_statement.consistency_level = consistency_level
                lightweight_rows = impact_session.execute(lightweight_statement)

            # Store the partition and created_at values that match our org_id
            matching_keys = []

            # Initialize progress bar for scanning rows
            with tqdm(total=fetch_size, desc="Scanning rows", unit="row") as scan_pbar:
                for row in lightweight_rows:
                    # For every 1000 source rows, wait for 1 second to cool down the server
                    if source_row % 1000 == 0 and source_row > 0:
                        time.sleep(1)
                    source_row += 1
                    scan_pbar.total = source_row  # Dynamically update total rows scanned
                    scan_pbar.refresh()

                    if hasattr(row, 'org_id') and row.org_id == org_id:
                        matching_keys.append((row.partition, row.created_at))
                        scan_pbar.set_postfix(found=len(matching_keys))

                    scan_pbar.update(1)

            print(f"Found {len(matching_keys)} rows matching org_id {org_id}")
            
            # 2. Truncate Destination Table
            print(f"Truncating {full_table_name} in AIO with consistency {get_consistency_name(consistency_level)}...")
            truncate_query = f"TRUNCATE {full_table_name};"
            truncate_statement = aio_session.prepare(truncate_query)
            truncate_statement.consistency_level = consistency_level
            
            try:
                aio_session.execute(truncate_statement)
                print(f"Truncate complete.")
            except (Unavailable, ReadTimeout, WriteTimeout) as consistency_error:
                print(f"Consistency QUORUM failed for truncate: {consistency_error}")
                print(f"Retrying truncate with consistency ONE...")
                consistency_level = ConsistencyLevel.ONE
                truncate_statement.consistency_level = consistency_level
                aio_session.execute(truncate_statement)
                print(f"Truncate complete.")

            # 3. Fetch and insert only the matching rows with all columns
            print(f"Fetching and copying full data for matching rows with consistency {get_consistency_name(consistency_level)}...")
            insert_statement = aio_session.prepare(insert_query_str)
            insert_statement.consistency_level = consistency_level
            moved = 0

            # Initialize progress bar
            with tqdm(total=len(matching_keys), desc="Copying rows", unit="row") as pbar:
                for partition, created_at in matching_keys:
                    try:
                        # Query the specific row using partition, created_at, and org_id as filters
                        detailed_query = f"SELECT {', '.join(cols_to_copy)} FROM {full_table_name} WHERE partition = ? AND created_at = ? AND org_id = ?;"
                        detailed_statement = impact_session.prepare(detailed_query)
                        detailed_statement.consistency_level = consistency_level
                        
                        try:
                            detailed_row = impact_session.execute(detailed_statement, [partition, created_at, org_id]).one()
                        except (Unavailable, ReadTimeout, WriteTimeout) as consistency_error:
                            # Retry individual row query with consistency ONE
                            print(f"\nConsistency {get_consistency_name(consistency_level)} failed for row query: {consistency_error}")
                            print(f"Retrying with consistency ONE...")
                            detailed_statement.consistency_level = ConsistencyLevel.ONE
                            detailed_row = impact_session.execute(detailed_statement, [partition, created_at, org_id]).one()

                        if detailed_row:
                            # Extract data using the defined column names
                            values = tuple(getattr(detailed_row, col) for col in cols_to_copy)
                            
                            try:
                                aio_session.execute(insert_statement, values)
                            except (Unavailable, ReadTimeout, WriteTimeout) as consistency_error:
                                # Retry insert with consistency ONE
                                print(f"\nConsistency {get_consistency_name(consistency_level)} failed for insert: {consistency_error}")
                                print(f"Retrying insert with consistency ONE...")
                                insert_statement.consistency_level = ConsistencyLevel.ONE
                                aio_session.execute(insert_statement, values)
                            
                            moved += 1

                        # Update progress bar
                        pbar.update(1)

                    except AttributeError as ae:
                        print(f"Warning: Skipping row due to missing attribute: {ae}. Partition: {partition}, created_at: {created_at}")
                        pbar.update(1)
                    except Exception as insert_err:
                        print(f"Warning: Skipping row due to insert error: {insert_err}. Partition: {partition}, created_at: {created_at}")
                        pbar.update(1)

            print(f"Successfully copied {moved} rows from {full_table_name} to AIO.")
            return # Exit function on success
            
        except ReadTimeout as e:
            print(f"Read timeout during copy of {full_table_name}: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Max retries reached for {full_table_name}. Aborting copy for this table.")
                return # Exit function on failure after retries
        except Exception as e:
            # Catch other potential Cassandra errors or general exceptions
            print(f"An unexpected error occurred during copy of {full_table_name}: {e}")
            import traceback
            traceback.print_exc() # Print stack trace for unexpected errors
            print(f"Aborting copy for {full_table_name} due to unexpected error.")
            return # Exit function on unexpected error

# --- Main Execution Logic ---
# Define tables to copy and their fetch sizes
tables_to_copy = {
    "objects": 10,
    "reports": 1,
    "incident_to_partition": 10,
    "partition_index": 10,
    "incident_summary": 10,
    "incidents": 11
}

# Call the function for each table
for table, fetch_size in tables_to_copy.items():
    copy_table(impact, aio, table, IMPACT_ORG_ID, fetch_size=fetch_size)

# --- Shutdown ---
print("\nMigration process finished. Shutting down connections.")
impact_cluster.shutdown()
aio_cluster.shutdown()
print("Connections closed.")