from redis import Redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import sys


def create_ranking_index(redis_client):
    """Create RediSearch index for ActivityRankingMonthly"""
    try:
        # Index configuration
        INDEX_NAME = "activity-ranking-monthly-idx"
        PREFIX = "activity-dev:membership:monthly:"

        # Drop existing index if it exists
        try:
            redis_client.ft(INDEX_NAME).dropindex(delete_documents=True)
            print(f"Dropped existing index: {INDEX_NAME}")
        except:
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

        print(f"""
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
        print("\nIndex Info:")
        for key, value in info.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"Failed to create index: {str(e)}")
        raise


def main():
    """Main function to create Redis index"""
    REDIS_CONFIG = {
        'host': '192.168.8.226',
        'port': 6379,
        'password': '0ef1sJm19w3OKHiH'
    }

    try:
        # Connect to Redis
        redis_client = Redis(**REDIS_CONFIG, decode_responses=True)
        redis_client.ping()
        print("Connected to Redis successfully")

        # Create the index
        create_ranking_index(redis_client)
        print("Index creation completed successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()