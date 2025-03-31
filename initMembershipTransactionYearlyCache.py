from redis import Redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import sys
import logging


def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('membership-cache-init')


def create_redis_connection(config):
    """Create and test Redis connection"""
    try:
        logger.info(f"Connecting to Redis at {config['host']}:{config['port']}")
        redis_client = Redis(**config, decode_responses=True)
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

        # Find and delete all documents with the prefix
        cursor = 0
        deleted_count = 0

        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=f"{prefix}*", count=1000)
            if keys:
                deleted_count += len(keys)
                redis_client.delete(*keys)
                logger.info(f"Deleted {len(keys)} documents with prefix '{prefix}'")

            if cursor == 0:
                break

        # Clear the index set if it exists
        redis_client.delete(index_name)
        logger.info(f"Cleared index set: {index_name}")

        logger.info(f"Cleanup complete: Removed {deleted_count} documents with prefix '{prefix}'")

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise


def create_ranking_index(redis_client, index_name, prefix):
    """Create RediSearch index for ActivityRankingYearly"""
    try:
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
  - MembershipCode (Tag)
  - MembershipName (Text)
  - Year (Numeric)
  - Rank (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
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
    # Configuration
    REDIS_CONFIG = {
        'host': '192.168.8.226',
        'port': 6379,
        'password': '0ef1sJm19w3OKHiH'
    }

    INDEX_NAME = "activity-ranking-yearly-idx"
    PREFIX = "activity-dev:membership:yearly:"

    try:
        # Connect to Redis
        redis_client = create_redis_connection(REDIS_CONFIG)

        # Clean existing data and index
        logger.info("Starting cleanup process...")
        clean_all_cache_data(redis_client, INDEX_NAME, PREFIX)

        # Create the new index
        logger.info("Creating new index...")
        create_ranking_index(redis_client, INDEX_NAME, PREFIX)

        logger.info("Index initialization completed successfully!")

    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Starting Membership Transaction Yearly Cache Initialization")
    main()
    logger.info("Process completed")