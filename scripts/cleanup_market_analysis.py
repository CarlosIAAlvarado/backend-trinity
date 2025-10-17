"""
Script temporal para limpiar registros duplicados o antiguos de Market Analysis
Ejecutar una vez para resolver el problema de datos obsoletos
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import db_config
from repositories.market_analysis_repository import MarketAnalysisRepository

async def cleanup_market_analysis():
    """
    Limpia la colección de market analysis dejando solo los registros más recientes
    para cada timeframe (12h y 24h)
    """
    print("=" * 70)
    print("MARKET ANALYSIS CLEANUP SCRIPT")
    print("=" * 70)

    try:
        # Connect to database first
        print("\nConnecting to MongoDB...")
        await db_config.connect()
        print("Connected successfully!")

        # Initialize repository
        repo = MarketAnalysisRepository()
        collection = db_config.get_collection('marketAnalysis')

        # Count total records before cleanup
        total_before = await collection.count_documents({})
        print(f"\n[STATS] Total records BEFORE cleanup: {total_before}")

        # Get all records grouped by timeframe
        print("\n[ANALYZING] Records by timeframe...")

        for timeframe in ['12h', '24h']:
            print(f"\n  Timeframe: {timeframe}")

            # Count records for this timeframe
            count = await collection.count_documents({'timeframe': timeframe})
            print(f"    - Total records: {count}")

            if count > 1:
                # Get all records for this timeframe, sorted by createdAt
                cursor = collection.find(
                    {'timeframe': timeframe},
                    sort=[('createdAt', -1)]
                )
                records = await cursor.to_list(length=None)

                if records:
                    # Keep the most recent one
                    latest = records[0]
                    latest_created = latest.get('createdAt', 'Unknown')
                    latest_timestamp = latest.get('timestamp', 'Unknown')

                    print(f"    - Latest record:")
                    print(f"      * createdAt: {latest_created}")
                    print(f"      * timestamp: {latest_timestamp}")
                    print(f"      * status: {latest.get('market_status')}")

                    # Delete all older records
                    ids_to_delete = [r['_id'] for r in records[1:]]

                    if ids_to_delete:
                        result = await collection.delete_many({
                            '_id': {'$in': ids_to_delete}
                        })
                        print(f"    - Deleted {result.deleted_count} old records")
            else:
                print(f"    - Only 1 record found, no cleanup needed")

        # Also delete any records older than 7 days
        print("\n[CLEANUP] Deleting records older than 7 days...")
        cutoff_date = datetime.now() - timedelta(days=7)
        old_records_result = await collection.delete_many({
            'createdAt': {'$lt': cutoff_date}
        })
        print(f"    - Deleted {old_records_result.deleted_count} old records (>7 days)")

        # Count total records after cleanup
        total_after = await collection.count_documents({})
        print(f"\n[STATS] Total records AFTER cleanup: {total_after}")
        print(f"[SUCCESS] Removed {total_before - total_after} records in total")

        # Show final state
        print("\n[FINAL STATE] Current records:")
        for timeframe in ['12h', '24h']:
            analysis = await collection.find_one(
                {'timeframe': timeframe},
                sort=[('createdAt', -1)]
            )
            if analysis:
                print(f"  {timeframe}:")
                print(f"    - Status: {analysis.get('market_status')}")
                print(f"    - createdAt: {analysis.get('createdAt')}")
                print(f"    - timestamp: {analysis.get('timestamp')}")
            else:
                print(f"  {timeframe}: No records found")

        print("\n" + "=" * 70)
        print("[SUCCESS] CLEANUP COMPLETED SUCCESSFULLY")
        print("=" * 70)

        # Disconnect from database
        await db_config.disconnect()

    except Exception as e:
        print(f"\n[ERROR] Error during cleanup: {e}")
        import traceback
        traceback.print_exc()

        # Try to disconnect even on error
        try:
            await db_config.disconnect()
        except:
            pass

if __name__ == "__main__":
    asyncio.run(cleanup_market_analysis())
