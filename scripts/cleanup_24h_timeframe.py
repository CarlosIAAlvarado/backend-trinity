"""
Script to delete all candlesticks with timeframe '24h' from the database
We only want to keep '1d' which is calculated using our Rolling Period method
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

async def cleanup_24h_timeframe():
    """Delete all candlesticks with timeframe '24h'"""

    print("=" * 80)
    print("CLEANUP: Removing '24h' timeframe from database")
    print("=" * 80)

    try:
        # Get database
        db_name = os.getenv('DB_NAME', 'trinity_market')
        uri = os.getenv('MONGODB_URI')

        print(f"\nConnecting to PRIMARY database: {db_name}")

        # Create client
        client = AsyncIOMotorClient(uri)
        db = client[db_name]
        collection = db['trinity_candlesticks']

        # Count how many '24h' candlesticks exist
        count_24h = await collection.count_documents({'timeframe': '24h'})
        print(f"\nFound {count_24h} candlesticks with timeframe '24h'")

        if count_24h == 0:
            print("\nNo '24h' candlesticks to delete. Database is clean!")
        else:
            # Delete all '24h' candlesticks
            print(f"\nDeleting {count_24h} '24h' candlesticks...")
            result = await collection.delete_many({'timeframe': '24h'})
            print(f"✓ Deleted {result.deleted_count} candlesticks with timeframe '24h'")

        # Show remaining timeframes
        print("\n" + "-" * 80)
        print("REMAINING TIMEFRAMES IN DATABASE:")
        print("-" * 80)

        timeframes = await collection.distinct('timeframe')
        for tf in sorted(timeframes):
            count = await collection.count_documents({'timeframe': tf})
            print(f"  {tf}: {count} candlesticks")

        print("\n" + "=" * 80)
        print("CLEANUP COMPLETED SUCCESSFULLY!")
        print("=" * 80)

        client.close()

    except Exception as e:
        print(f"\nError during cleanup: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(cleanup_24h_timeframe())
