"""
Script to check database size and collection statistics
Helps identify what's using space in the secondary database
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

# Load environment variables from backend/.env
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

async def check_database_size():
    """Check size of all collections in the secondary database"""

    print("=" * 80)
    print("DATABASE SIZE ANALYSIS - Secondary Database")
    print("=" * 80)

    try:
        # Get database
        db_name = os.getenv('SECONDARY_DB_NAME', 'Dev')
        uri = os.getenv('SECONDARY_MONGODB_URI')

        print(f"\nConnecting to: {uri}")
        print(f"Database: {db_name}\n")

        # Create client directly
        client = AsyncIOMotorClient(uri)
        db = client[db_name]

        # Get database stats
        db_stats = await db.command("dbStats")

        print("DATABASE OVERVIEW")
        print("-" * 80)
        print(f"Database Size:     {db_stats.get('dataSize', 0) / (1024 * 1024):.2f} MB")
        print(f"Storage Size:      {db_stats.get('storageSize', 0) / (1024 * 1024):.2f} MB")
        print(f"Index Size:        {db_stats.get('indexSize', 0) / (1024 * 1024):.2f} MB")
        print(f"Total Size:        {(db_stats.get('dataSize', 0) + db_stats.get('indexSize', 0)) / (1024 * 1024):.2f} MB")
        print(f"Collections:       {db_stats.get('collections', 0)}")
        print(f"Indexes:           {db_stats.get('indexes', 0)}")
        print(f"Objects:           {db_stats.get('objects', 0)}")

        # Get list of collections
        collection_names = await db.list_collection_names()

        print("\nCOLLECTIONS BREAKDOWN")
        print("-" * 80)
        print(f"{'Collection Name':<50} {'Documents':>12} {'Size (MB)':>12}")
        print("-" * 80)

        total_size = 0
        collection_details = []

        for collection_name in sorted(collection_names):
            try:
                collection = db[collection_name]

                # Get collection stats
                stats = await db.command("collStats", collection_name)

                doc_count = stats.get('count', 0)
                size_mb = stats.get('size', 0) / (1024 * 1024)
                total_size += size_mb

                collection_details.append({
                    'name': collection_name,
                    'count': doc_count,
                    'size': size_mb
                })

                print(f"{collection_name:<50} {doc_count:>12,} {size_mb:>11.2f} MB")

            except Exception as e:
                print(f"{collection_name:<50} {'ERROR':>12} {str(e)}")

        print("-" * 80)
        print(f"{'TOTAL':<50} {'':<12} {total_size:>11.2f} MB")

        # Show largest collections
        print("\nTOP 5 LARGEST COLLECTIONS")
        print("-" * 80)
        collection_details.sort(key=lambda x: x['size'], reverse=True)

        for i, col in enumerate(collection_details[:5], 1):
            percentage = (col['size'] / total_size * 100) if total_size > 0 else 0
            print(f"{i}. {col['name']:<45} {col['size']:>8.2f} MB ({percentage:>5.1f}%)")

        # Recommendations
        print("\nRECOMMENDATIONS")
        print("-" * 80)

        large_collections = [c for c in collection_details if c['size'] > 10]
        if large_collections:
            print("\nLarge collections detected (> 10 MB):")
            for col in large_collections:
                print(f"   - {col['name']}: {col['size']:.2f} MB ({col['count']:,} documents)")
                print(f"     Consider cleaning old data or archiving")

        # Check for old market analysis
        if 'trinity_performance_marketAnalysis' in collection_names:
            ma_collection = db['trinity_performance_marketAnalysis']
            ma_count = await ma_collection.count_documents({})

            if ma_count > 1:
                print(f"\nMarket Analysis has {ma_count} documents (should be 1)")
                print(f"     Run cleanup to remove old documents with structure:")
                print(f"     - Delete documents with 'timeframe' field (old structure)")
                print(f"     - Keep only document with 'candlesByTimeframe' field (new structure)")

        print("\n" + "=" * 80)

        # Close connection
        client.close()

    except Exception as e:
        print(f"\nError checking database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_database_size())
