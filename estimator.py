from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, CERT_NONE, TLSVersion
import ssl
from cassandra import ConsistencyLevel
from getpass import getpass

def connect_to_cassandra():
    # Prompt user for connection details
    cassandra_ip = input("Enter the Cassandra server IP address: ")
    username = input("Enter the Cassandra username: ")
    password = getpass("Enter the Cassandra password: ")
    org_id = input("Enter the organization ID: ")
    partition_size = input("Enter the partition size in bytes: ")
    # Validate partition size
    try:
        partition_size = int(partition_size)
        if partition_size <= 0:
            raise ValueError("Partition size must be a positive integer.")
    except ValueError as e:
        print(f"Invalid partition size: {e}")
        return
    # Set up SSL context to ignore CA verification
    ssl_context = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.minimum_version = TLSVersion.TLSv1_2
    ssl_context.check_hostname = False
    ssl_context.verify_mode = CERT_NONE

    # Set up authentication and cluster connection
    auth_provider = PlainTextAuthProvider(username, password)
    cluster = Cluster(
        [cassandra_ip],
        auth_provider=auth_provider,
        ssl_context=ssl_context
    )
    cluster.default_consistency_level = ConsistencyLevel.QUORUM

    try:
        session = cluster.connect()
        print("Connection to Cassandra server successful!")

        query = "SELECT day, org_id FROM artifacts.artifact_partition_summary;"
        days = []
        oldest_day = None
        newest_day = None
        

        # Execute the query with pagination
        statement = session.prepare(query)
        statement.consistency_level = ConsistencyLevel.QUORUM
        statement.fetch_size = 100  # Set fetch size to 100 rows
        rows = session.execute(statement)

        print(f"Scanning rows...")
        # Iterate through paginated results
        for row in rows:
            if row.org_id == org_id:
                # day format: YYYY-MM-DD-HH-MM
                # Extract the day part
                day = row.day.split("-")[0:3]
                # Make day a unix timestamp
                day = int(day[0]) * 10000 + int(day[1]) * 100 + int(day[2])
                # Update oldest and newest days
                if oldest_day is None or day < oldest_day:
                    oldest_day = day
                if newest_day is None or day > newest_day:
                    newest_day = day
                # Append the day to the list
                days.append(row.day)
                if len(days) % 1000 == 0:
                    print(f"Found {len(days)} partitions for org {org_id}...")

        print(f"Partitions for org_id {org_id}: {len(days)}")
        estimated_size = len(days) * partition_size
        # Format the estimated size in bytes
        if estimated_size >= 1024 ** 2:
            estimated_size = f"{estimated_size / (1024 ** 2):.2f} MB"
        elif estimated_size >= 1024:
            estimated_size = f"{estimated_size / 1024:.2f} KB"
        else:
            estimated_size = f"{estimated_size} bytes"
        print(f"Estimated size of partitions: {estimated_size} bytes")
        if oldest_day is not None:
            # Convert oldest_day back to YYYY-MM-DD format
            oldest_day = f"{oldest_day // 10000:04}-{(oldest_day % 10000) // 100:02}-{oldest_day % 100:02}"
            # Convert newest_day back to YYYY-MM-DD format
            newest_day = f"{newest_day // 10000:04}-{(newest_day % 10000) // 100:02}-{newest_day % 100:02}"
            print(f"Newest day: {newest_day}")
            print(f"Oldest day: {oldest_day}")
        return session
    except Exception as e:
        print(f"Failed to connect to Cassandra server: {e}")
    finally:
        cluster.shutdown()

# Example usage
if __name__ == "__main__":
    print("Starting connection to Cassandra with TLS (ignoring CA trust)...")
    connect_to_cassandra()