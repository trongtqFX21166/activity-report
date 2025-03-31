#!/usr/bin/env python3
import argparse
from datetime import datetime
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
import time


def get_cassandra_connection():
    """Create connection to Cassandra cluster"""
    try:
        # Contact points from your configuration files
        contact_points = ["192.168.8.165", "192.168.8.166", "192.168.8.183"]

        # Create cluster connection (adjust auth if needed)
        # auth_provider = PlainTextAuthProvider(username='username', password='password')
        # Use auth provider if your Cassandra requires authentication
        # cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
        cluster = Cluster(contact_points=contact_points)

        # Connect to keyspace
        session = cluster.connect('activity_dev')
        return session, cluster
    except Exception as e:
        print(f"Error connecting to Cassandra: {str(e)}")
        raise


def count_records(session, month, year):
    """Count records matching month and year"""
    try:
        # Use a prepared statement for better performance
        count_query = SimpleStatement(
            "SELECT COUNT(*) FROM activitytransaction WHERE month = %s AND year = %s ALLOW FILTERING",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )

        # Execute query with parameters
        row = session.execute(count_query, [month, year]).one()

        # In Cassandra, count(*) returns a count column
        return row.count if row else 0
    except Exception as e:
        print(f"Error counting records: {str(e)}")
        raise


def get_primary_keys(session, month, year, batch_size=1000):
    """Retrieve primary key values for records to delete"""
    try:
        # Prepare query to get primary keys - only requesting the needed fields
        query = SimpleStatement(
            """
            SELECT id, phone, date, month, year
            FROM activitytransaction
            WHERE month = %s AND year = %s
            ALLOW FILTERING
            """,
            fetch_size=batch_size,  # Control how many records to fetch at once
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )

        # Execute query and return results
        return session.execute(query, [month, year])
    except Exception as e:
        print(f"Error retrieving primary keys: {str(e)}")
        raise


def delete_records(session, primary_keys, dry_run=False, batch_size=20):
    """Delete records using their primary keys"""
    if dry_run:
        print(f"DRY RUN: Would delete records with the retrieved primary keys")
        return 0

    try:
        # Prepare delete statement with only the necessary key fields
        delete_stmt = session.prepare(
            """
            DELETE FROM activitytransaction 
            WHERE id = ? AND phone = ? AND date = ? AND month = ? AND year = ?
            """
        )

        deleted_count = 0
        batch_count = 0
        current_batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

        start_time = time.time()
        print(f"Starting deletion process...")

        # Process records in batches
        for record in primary_keys:
            # Add delete to current batch
            current_batch.add(delete_stmt, [
                record.id, record.phone, record.date, record.month, record.year
            ])
            deleted_count += 1

            # Execute batch when it reaches batch size
            if deleted_count % batch_size == 0:
                session.execute(current_batch)
                batch_count += 1
                current_batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

                # Print progress
                elapsed_time = time.time() - start_time
                rate = deleted_count / elapsed_time if elapsed_time > 0 else 0
                print(f"Deleted {deleted_count} records ({batch_count} batches) - Rate: {rate:.1f} records/second")

        # Execute final batch if there are any remaining statements
        if deleted_count % batch_size != 0:
            session.execute(current_batch)
            batch_count += 1

        total_time = time.time() - start_time
        print(f"Deletion completed. Total time: {total_time:.2f} seconds")
        print(f"Deleted {deleted_count} records in {batch_count} batches")

        return deleted_count

    except Exception as e:
        print(f"Error deleting records: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Delete Activity Transactions from Cassandra by Month and Year')
    parser.add_argument('--month', type=int, required=True, help='Month to delete (1-12)')
    parser.add_argument('--year', type=int, required=True, help='Year to delete')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without actually deleting data')
    parser.add_argument('--batch-size', type=int, default=20, help='Batch size for deletion operations (default: 20)')

    args = parser.parse_args()

    # Validate inputs
    if not 1 <= args.month <= 12:
        print(f"Error: Month must be between 1 and 12, got {args.month}")
        sys.exit(1)

    session = None
    cluster = None

    try:
        print(
            f"{'DRY RUN MODE - NO DATA WILL BE DELETED' if args.dry_run else 'PRODUCTION MODE - DATA WILL BE DELETED'}")
        print(f"Starting Cassandra deletion process for Month={args.month}, Year={args.year}")

        start_time = datetime.now()
        print(f"Start time: {start_time}")

        # Connect to Cassandra
        session, cluster = get_cassandra_connection()

        # Count records to delete
        record_count = count_records(session, args.month, args.year)
        print(f"Found {record_count} records matching month={args.month} and year={args.year}")

        if record_count == 0:
            print("No matching records found. Skipping deletion.")
            return

        # Get primary keys for records to delete
        print(f"Retrieving primary keys for {record_count} records...")
        primary_keys = get_primary_keys(session, args.month, args.year)

        # Delete records
        delete_records(session, primary_keys, args.dry_run, args.batch_size)

        # Verify deletion
        if not args.dry_run:
            remaining_count = count_records(session, args.month, args.year)
            print(
                f"Verification: {record_count - remaining_count} records deleted, {remaining_count} records remaining")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n--- Summary ---")
        print(f"{'DRY RUN - ' if args.dry_run else ''}Deletion process completed")
        print(f"Process duration: {duration:.2f} seconds")

    except Exception as e:
        print(f"Error in deletion process: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up resources
        if session:
            session.shutdown()

        if cluster:
            cluster.shutdown()


if __name__ == "__main__":
    main()