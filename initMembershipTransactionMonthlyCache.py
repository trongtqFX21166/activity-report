from redis import Redis
from redis.cluster import ClusterNode, RedisCluster
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import sys
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("init-membership-monthly-cache")


def create_ranking_index(redis_client):
    """Create RediSearch index for ActivityRankingMonthly"""
    try:
        # Index configuration
        INDEX_NAME = "activity-ranking-monthly-idx"
        PREFIX = "activity_membership_monthly:"

        # Drop existing index if it exists
        try:
            redis_client.ft(INDEX_NAME).dropindex(delete_documents=True)
            logger.info(f"Dropped existing index: {INDEX_NAME}")
        except Exception as e:
            logger.warning(f"Index drop failed: {str(e)}")
            pass

        # Define schema matching the C# model
        schema = (
            # Phone field made both searchable and sortable for flexible querying
            TextField("$.Phone", as_name="Phone", sortable=True),

            # Membership fields
            TagField("$.MembershipCode", as_name="MembershipCode"),
            TextField("$.MembershipName", as_name="MembershipName"),

            # Time period fields
            NumericField("$.Month", as_name="Month"),
            NumericField("$.Year", as_name="Year"),

            # Ranking fields with sorting enabled
            NumericField("$.Rank", as_name="Rank", sortable=True),
            NumericField("$.TotalPoints", as_name="TotalPoints", sortable=True),
        )

        # Create the index
        redis_client.ft(INDEX_NAME).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[PREFIX],
                index_type=IndexType.JSON
            )
        )

        logger.info(f"""
RediSearch index created successfully:
- Index Name: {INDEX_NAME}
- Prefix: {PREFIX}
- Schema:
  - Phone (Text, Sortable, Searchable)
  - MembershipCode (Tag)
  - MembershipName (Text)
  - Month (Numeric)
  - Year (Numeric)
  - Rank (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
  - LastUpdatedAt (Numeric, Sortable)
  - LastUpdatedBy (Tag)
""")

        # Test the index
        info = redis_client.ft(INDEX_NAME).info()
        logger.info("\nIndex Info:")
        for key, value in info.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Initialize Redis Membership Cache')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment to use')
    return parser.parse_args()


def main():
    """Main function to create Redis index"""
    args = parse_args()
    env = args.env
    logger.info(f"Running in {env} environment")

    redis_client = None

    try:
        # Redis Cluster configuration based on environment
        if env == 'prod':
            # Production cluster nodes
            logger.info("Connecting to Redis Cluster in production environment...")
            # Create ClusterNode objects directly instead of using dictionaries
            cluster_nodes = [
                ClusterNode(host="192.168.11.227", port=6379),
                ClusterNode(host="192.168.11.228", port=6379),
                ClusterNode(host="192.168.11.229", port=6379)
            ]
            password = ''
        else:
            # Development cluster/standalone settings
            logger.info("Connecting to Redis in development environment...")
            cluster_nodes = [ClusterNode(host="192.168.8.226", port=6379)]
            password = '0ef1sJm19w3OKHiH'

        # Create the Redis Cluster client
        redis_client = RedisCluster(
            startup_nodes=cluster_nodes,
            password=password,
            decode_responses=True,
            skip_full_coverage_check=True
        )

        # Test the connection
        redis_client.ping()
        logger.info(f"Connected to Redis{'Cluster' if env == 'prod' else ''} successfully")

        # Create the index
        create_ranking_index(redis_client)
        logger.info("Index creation completed successfully!")

    except Exception as e:
        logger.error(f"Error connecting to Redis: {str(e)}")
        sys.exit(1)
    finally:
        # Close Redis connection if it exists
        if redis_client is not None:
            try:
                redis_client.close()
                logger.info("Redis connection closed")
            except:
                pass


if __name__ == "__main__":
    main()
