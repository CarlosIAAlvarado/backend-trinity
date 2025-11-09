"""
Test script to manually save data to secondary database
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

async def test_save():
    """Test saving data to secondary database"""

    print("=" * 80)
    print("TESTING SECONDARY DATABASE WRITE")
    print("=" * 80)

    try:
        # Get credentials
        uri = os.getenv('SECONDARY_MONGODB_URI')
        db_name = os.getenv('SECONDARY_DB_NAME', 'Dev')

        print(f"\nConnecting to: {uri}")
        print(f"Database: {db_name}")
        print(f"Collection: trinity_performance_marketAnalysis\n")

        # Create client
        client = AsyncIOMotorClient(uri)
        db = client[db_name]
        collection = db['trinity_performance_marketAnalysis']

        # Create test document
        test_data = {
            "direction": "FLAT",
            "directionNumber": 0.5,
            "directionNumberReal": 0.5594,
            "candlesByTimeframe": {
                "15m": {"best": [], "worst": []},
                "30m": {"best": [], "worst": []},
                "1H": {"best": [], "worst": []},
                "4H": {"best": [], "worst": []},
                "12H": {"best": [], "worst": []},
                "1D": {"best": [], "worst": []}
            },
            "timestamp": datetime.now(),
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }

        print("Attempting to save test document...")
        print(f"Document size: ~{len(str(test_data))} bytes\n")

        # Try to insert
        result = await collection.insert_one(test_data)

        print(f"SUCCESS! Document inserted with _id: {result.inserted_id}")

        # Verify
        count = await collection.count_documents({})
        print(f"Total documents in collection: {count}")

        # Read back
        saved_doc = await collection.find_one({"_id": result.inserted_id})
        if saved_doc:
            print(f"\nVerified - Document saved successfully:")
            print(f"  Direction: {saved_doc.get('direction')}")
            print(f"  Direction Number: {saved_doc.get('directionNumber')}")
            print(f"  Direction Real: {saved_doc.get('directionNumberReal')}")

        print("\n" + "=" * 80)
        print("TEST PASSED - Secondary database is working!")
        print("=" * 80)

        client.close()

    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()

        print("\n" + "=" * 80)
        print("TEST FAILED - Cannot write to secondary database")
        print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_save())
