from redis import Redis
from redis.cluster import RedisCluster, ClusterNode
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import sys
import logging
import argparse


def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('membership-cache-init')


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Initialize Redis Membership Cache')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment to use')
    return parser.parse_args()


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'is_cluster': False,  # Not a cluster in dev
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity_membership_yearly:'
        }
    else:  # prod
        return {
            'is_cluster': True,  # Use cluster in production
            'nodes': [
                {'host': '192.168.11.227', 'port': 6379},
                {'host': '192.168.11.228', 'port': 6379},
                {'host': '192.168.11.229', 'port': 6379}
            ],
            'password': '',  # Use appropriate password for production if needed
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity_membership_yearly:'
        }


def create_redis_connection(config):
    """Create and test Redis connection (standalone or cluster)"""
    try:
        # Check if we're using cluster mode
        if config.get('is_cluster', False):
            # Convert node dictionaries to ClusterNode objects
            cluster_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in config['nodes']
            ]

            logger.info(f"Connecting to Redis Cluster...")
            logger.info(f"Cluster nodes: {', '.join([f'{node.host}:{node.port}' for node in cluster_nodes])}")

            # Create Redis Cluster client
            redis_client = RedisCluster(
                startup_nodes=cluster_nodes,
                password=config.get('password', ''),
                decode_responses=config.get('decode_responses', True),
                skip_full_coverage_check=True  # More resilient to node failures
            )
        else:
            # Standalone Redis connection
            logger.info(f"Connecting to Redis at {config['host']}:{config['port']}")
            redis_client = Redis(
                host=config['host'],
                port=config['port'],
                password=config.get('password', ''),
                decode_responses=config.get('decode_responses', True)
            )

        # Test connection
        redis_client.ping()
        logger.info("Connected to Redis successfully")
        return redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise


def clean_all_cache_data(redis_client, index_name, prefix):
    """Remove all existing data and drop the index"""
    try:
        # First try to drop the index if it exists
        try:
            # Convert index_name to string to ensure it's not a dict or complex type
            index_name_str = str(index_name)
            redis_client.ft(index_name_str).dropindex(delete_documents=True)
            logger.info(f"Dropped existing index: {index_name_str} with all associated documents")
        except Exception as e:
            if "Unknown index name" in str(e):
                logger.info(f"Index {index_name} doesn't exist yet, nothing to drop")
            else:
                logger.warning(f"Error while dropping index: {str(e)}")
                # Try again without deleting documents
                try:
                    redis_client.ft(str(index_name)).dropindex(delete_documents=False)
                    logger.info(f"Dropped existing index: {index_name} without deleting documents")
                except Exception as drop_error:
                    logger.info(f"Index {index_name} couldn't be dropped: {str(drop_error)}, continuing...")

        # In cluster mode, we need to scan each node for keys
        # But we can still use the regular scan interface which will be routed appropriately
        cursor = 0
        deleted_count = 0

        # Ensure prefix is a string
        prefix_str = str(prefix)

        while True:
            try:
                cursor, keys = redis_client.scan(cursor=cursor, match=f"{prefix_str}*", count=1000)
                if keys:
                    deleted_count += len(keys)
                    # In a cluster, we need to delete keys one by one to route to correct node
                    for key in keys:
                        try:
                            redis_client.delete(key)
                        except Exception as e:
                            logger.warning(f"Error deleting key {key}: {str(e)}")
                    logger.info(f"Deleted {len(keys)} documents with prefix '{prefix_str}'")
            except Exception as scan_error:
                logger.warning(f"Error during scan operation: {str(scan_error)}")
                # Move to next cursor position to avoid infinite loop
                if cursor == 0:
                    break
                cursor = 0

            if cursor == 0:
                break

        # Clear the index set if it exists
        try:
            redis_client.delete(str(index_name))
            logger.info(f"Cleared index set: {index_name}")
        except Exception as e:
            logger.warning(f"Error clearing index set: {str(e)}")

        logger.info(f"Cleanup complete: Removed {deleted_count} documents with prefix '{prefix_str}'")

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise


def create_ranking_index(redis_client, index_name, prefix):
    """Create RediSearch index for ActivityRankingYearly"""
    try:
        # Convert to string to ensure proper type
        index_name_str = str(index_name)
        prefix_str = str(prefix)

        # Define schema matching the C# model
        schema = (
            # Phone field made both searchable and sortable for flexible querying
            TextField("$.Phone", as_name="Phone", sortable=True),

            # Membership fields
            TagField("$.MembershipCode", as_name="MembershipCode"),
            TextField("$.MembershipName", as_name="MembershipName"),

            # Time period fields
            NumericField("$.Year", as_name="Year"),

            # Ranking fields with sorting enabled
            NumericField("$.Rank", as_name="Rank", sortable=True),
            NumericField("$.TotalPoints", as_name="TotalPoints", sortable=True),
        )

        # Create the index
        redis_client.ft(index_name_str).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[prefix_str],
                index_type=IndexType.JSON
            )
        )

        logger.info(f"""
RediSearch index created successfully:
- Index Name: {index_name_str}
- Prefix: {prefix_str}
- Schema:
  - Phone (Text, Sortable, Searchable)
  - MembershipCode (Tag)
  - MembershipName (Text)
  - Year (Numeric)
  - Rank (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
""")

        # Test the index
        try:
            info = redis_client.ft(index_name_str).info()
            logger.info("\nIndex Info:")
            for key, value in info.items():
                logger.info(f"  {key}: {value}")
        except Exception as info_error:
            logger.warning(f"Could not retrieve index info: {str(info_error)}")

    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")
        raise


def main():
    """Main function to clean and initialize Redis index"""
    # Parse arguments
    args = parse_arguments()
    env = args.env

    logger.info(f"Starting Redis index initialization in {env.upper()} environment")

    # Get Redis configuration for the environment
    redis_config = get_redis_config(env)

    # Extract values for easier access
    index_name = redis_config['index_name']
    prefix = redis_config['key_prefix']

    # Log configuration
    logger.info(f"Redis configuration:")
    for key, value in redis_config.items():
        if key not in ['password', 'nodes']:  # Don't log sensitive info
            logger.info(f"  {key}: {value}")

    if redis_config.get('is_cluster', False):
        logger.info("  Mode: CLUSTER")
        nodes_info = [f"{n['host']}:{n['port']}" for n in redis_config.get('nodes', [])]
        logger.info(f"  Nodes: {', '.join(nodes_info)}")
    else:
        logger.info("  Mode: STANDALONE")

    try:
        # Connect to Redis
        redis_client = create_redis_connection(redis_config)

        if redis_client is None:
            logger.error("Failed to create Redis connection")
            sys.exit(1)

        # Clean existing data and index
        logger.info("Starting cleanup process...")
        try:
            clean_all_cache_data(redis_client, index_name, prefix)
        except Exception as cleanup_error:
            logger.error(f"Cleanup process failed: {str(cleanup_error)}")
            # Continue to index creation even if cleanup fails

        # Create the new index
        logger.info("Creating new index...")
        create_ranking_index(redis_client, index_name, prefix)

        logger.info("Index initialization completed successfully!")

    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        # Print more detailed exception info for debugging
        import traceback
        logger.error(f"Exception details: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if 'redis_client' in locals() and redis_client is not None:
            try:
                redis_client.close()
                logger.info("Redis connection closed")
            except Exception as close_error:
                logger.warning(f"Error closing Redis connection: {str(close_error)}")


if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Starting Membership Transaction Yearly Cache Initialization")
    main()
    logger.info("Process completed")