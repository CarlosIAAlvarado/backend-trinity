"""
Test script to verify UPSERT functionality for Market Analysis
Ensures only 2 records exist (one per timeframe) after multiple updates
"""
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import db_config
from services.market_analysis_service import market_analysis_service

async def test_upsert():
    """Test that UPSERT maintains only 2 records in database"""
    print("=" * 70)
    print("TESTING UPSERT FUNCTIONALITY")
    print("=" * 70)

    try:
        # Connect to database
        print("\n[1] Connecting to MongoDB...")
        await db_config.connect()
        print("    Connected successfully!")

        # Count records before test
        collection = db_config.get_collection('marketAnalysis')
        count_before = await collection.count_documents({})
        print(f"\n[2] Records BEFORE test: {count_before}")

        # Run market analysis 3 times (should still only have 2 records after)
        print("\n[3] Running market analysis 3 times...")

        for i in range(1, 4):
            print(f"\n    Iteration {i}:")
            result = await market_analysis_service.analyze_and_save()
            print(f"      - Status: {result['status']}")
            print(f"      - Message: {result['message']}")

            # Check count after each iteration
            count_current = await collection.count_documents({})
            print(f"      - Current record count: {count_current}")

        # Count records after test
        count_after = await collection.count_documents({})
        print(f"\n[4] Records AFTER test: {count_after}")

        # Verify only 2 records exist
        print("\n[5] Verification:")
        if count_after == 2:
            print("    [SUCCESS] Exactly 2 records exist (1 per timeframe)")
        else:
            print(f"    [FAILURE] Expected 2 records, found {count_after}")

        # Show the 2 records
        print("\n[6] Current records in database:")
        cursor = collection.find({})
        records = await cursor.to_list(length=None)

        for record in records:
            print(f"\n    Timeframe: {record.get('timeframe')}")
            print(f"      - Status: {record.get('market_status')}")
            print(f"      - createdAt: {record.get('createdAt')}")
            print(f"      - updatedAt: {record.get('updatedAt')}")
            print(f"      - timestamp: {record.get('timestamp')}")

        print("\n" + "=" * 70)
        if count_after == 2:
            print("[SUCCESS] UPSERT WORKING CORRECTLY")
        else:
            print("[FAILURE] UPSERT NOT WORKING AS EXPECTED")
        print("=" * 70)

        # Disconnect
        await db_config.disconnect()

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()

        try:
            await db_config.disconnect()
        except:
            pass

if __name__ == "__main__":
    asyncio.run(test_upsert())
