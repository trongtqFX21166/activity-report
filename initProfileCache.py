from redis import Redis
from redis.cluster import RedisCluster, ClusterNode
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import sys
import logging
import argparse
import os


def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('profile-cache-init')


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Initialize Redis Profile Cache')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment to use')
    return parser.parse_args()


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'is_cluster': False,
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-profile-idx',
            'key_prefix': 'activity_profile:'
        }
    else:  # prod
        return {
            'is_cluster': True,
            'nodes': [
                {'host': '192.168.11.227', 'port': 6379},
                {'host': '192.168.11.228', 'port': 6379},
                {'host': '192.168.11.229', 'port': 6379}
            ],
            'password': '',
            'decode_responses': True,
            'index_name': 'activity-profile-idx',
            'key_prefix': 'activity_profile:'
        }


def create_redis_connection(config):
    """Create and test Redis connection (standalone or cluster)"""
    try:
        # Check if we should use cluster mode
        if config.get('is_cluster', False):
            # Convert node dictionaries to ClusterNode objects
            cluster_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in config['nodes']
            ]

            logger.info(
                f"Connecting to Redis Cluster nodes: {', '.join([f'{node.host}:{node.port}' for node in cluster_nodes])}")

            # Create Redis Cluster client
            redis_client = RedisCluster(
                startup_nodes=cluster_nodes,
                password=config['password'],
                decode_responses=config['decode_responses'],
                skip_full_coverage_check=True  # More resilient to node failures
            )
            logger.info("Connected to Redis Cluster successfully")
        else:
            # Standalone Redis connection
            logger.info(f"Connecting to Redis at {config['host']}:{config['port']}")
            redis_client = Redis(
                host=config['host'],
                port=config['port'],
                password=config['password'],
                decode_responses=config['decode_responses']
            )
            logger.info("Connected to Redis successfully")

        # Test connection
        redis_client.ping()
        return redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise


def clean_all_cache_data(redis_client, index_name, prefix, is_cluster=False):
    """Remove all existing data and drop the index"""
    try:
        # First try to drop the index if it exists
        try:
            redis_client.ft(index_name).dropindex(delete_documents=True)
            logger.info(f"Dropped existing index: {index_name} with all associated documents")
        except Exception as e:
            if "Unknown index name" in str(e):
                logger.info(f"Index {index_name} doesn't exist yet, nothing to drop")
            else:
                logger.warning(f"Error while dropping index: {str(e)}")
                # Try again without deleting documents
                try:
                    redis_client.ft(index_name).dropindex(delete_documents=False)
                    logger.info(f"Dropped existing index: {index_name} without deleting documents")
                except:
                    logger.info(f"Index {index_name} couldn't be dropped, continuing...")

        # Find and delete all documents with the prefix - handle differently for cluster
        # if is_cluster:
        #     logger.info(f"Cleaning up keys with prefix '{prefix}' in cluster mode")
        #     # In cluster mode, we'll use a more cautious approach
        #     # We'll process in smaller batches to avoid overwhelming the cluster
        #     total_deleted = 0
        #     batch_size = 100
        #
        #     # Use client-side scan across all nodes in the cluster
        #     for key in redis_client.scan_iter(match=f"{prefix}*", count=batch_size):
        #         try:
        #             redis_client.delete(key)
        #             total_deleted += 1
        #
        #             # Log progress periodically
        #             if total_deleted % batch_size == 0:
        #                 logger.info(f"Deleted {total_deleted} documents with prefix '{prefix}'")
        #         except Exception as delete_error:
        #             logger.warning(f"Error deleting key {key}: {str(delete_error)}")
        #
        #     logger.info(f"Deleted {total_deleted} documents with prefix '{prefix}' in cluster mode")
        # else:
        #     # Standard standalone Redis approach
        #     cursor = 0
        #     deleted_count = 0
        #
        #     while True:
        #         cursor, keys = redis_client.scan(cursor=cursor, match=f"{prefix}*", count=1000)
        #         if keys:
        #             deleted_count += len(keys)
        #             redis_client.delete(*keys)
        #             logger.info(f"Deleted {len(keys)} documents with prefix '{prefix}'")
        #
        #         if cursor == 0:
        #             break
        #
        #     logger.info(f"Deleted {deleted_count} documents with prefix '{prefix}'")
        #
        # # Clear the index set if it exists
        # try:
        #     redis_client.delete(index_name)
        #     logger.info(f"Cleared index set: {index_name}")
        # except Exception as e:
        #     logger.warning(f"Error clearing index set: {str(e)}")

        logger.info(f"Cleanup complete")

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise


def create_profile_index(redis_client, index_name, prefix):
    """Create RediSearch index for Profile Cache"""
    try:
        # Define schema as per requirements
        schema = (
            # Phone field made both searchable and sortable for flexible querying
            TextField("$.Phone", as_name="Phone", sortable=True),

            # Profile level and points
            NumericField("$.Level", as_name="Level", sortable=True),
            NumericField("$.TotalPoints", as_name="TotalPoints", sortable=True),
            NumericField("$.Rank", as_name="Rank", sortable=True),

            # Membership fields
            TextField("$.MembershipCode", as_name="MembershipCode"),
            TextField("$.MembershipName", as_name="MembershipName"),

            # Optional additional fields that might be useful
            NumericField("$.LastUpdated", as_name="LastUpdated", sortable=True)
        )

        # Create the index
        redis_client.ft(index_name).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[prefix],
                index_type=IndexType.JSON
            )
        )

        logger.info(f"""
RediSearch index created successfully:
- Index Name: {index_name}
- Prefix: {prefix}
- Schema:
  - Phone (Text, Sortable, Searchable)
  - Level (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
  - Rank (Numeric, Sortable)
  - MembershipCode (Text)
  - MembershipName (Text)
  - LastUpdated (Numeric, Sortable)
""")

        # Test the index
        info = redis_client.ft(index_name).info()
        logger.info("\nIndex Info:")
        for key, value in info.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")
        raise


def main():
    """Main function to clean and initialize Redis index"""
    # Parse command line arguments
    args = parse_arguments()
    env = args.env
    logger.info(f"Running in {env} environment")

    # Get configuration for the specified environment
    redis_config = get_redis_config(env)
    index_name = redis_config['index_name']
    prefix = redis_config['key_prefix']
    is_cluster = redis_config.get('is_cluster', False)

    logger.info(f"Using configuration:")
    logger.info(f"  Environment: {env}")
    logger.info(f"  Cluster Mode: {'Yes' if is_cluster else 'No'}")
    logger.info(f"  Index Name: {index_name}")
    logger.info(f"  Key Prefix: {prefix}")

    redis_client = None
    try:
        # Connect to Redis
        redis_client = create_redis_connection(redis_config)

        # Clean existing data and index
        logger.info("Starting cleanup process...")
        clean_all_cache_data(redis_client, index_name, prefix, is_cluster)

        # Create the new index
        logger.info("Creating new profile index...")
        create_profile_index(redis_client, index_name, prefix)

        logger.info("Profile cache index initialization completed successfully!")

    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        sys.exit(1)
    finally:
        if redis_client:
            logger.info("Closing Redis connection")
            redis_client.close()


if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Starting Profile Cache Initialization")
    main()
    logger.info("Process completed")