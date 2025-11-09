"""
Script to check ALL databases in the cluster
Identifies which databases are using space
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

async def check_all_databases():
    """Check all databases in the cluster"""

    print("=" * 80)
    print("CLUSTER-WIDE DATABASE ANALYSIS")
    print("=" * 80)

    try:
        # Get credentials
        uri = os.getenv('SECONDARY_MONGODB_URI')

        print(f"\nConnecting to cluster: {uri}\n")

        # Create client
        client = AsyncIOMotorClient(uri)

        # List all databases
        db_list = await client.list_database_names()

        print(f"Total databases in cluster: {len(db_list)}\n")

        print("DATABASE DETAILS")
        print("-" * 80)
        print(f"{'Database Name':<30} {'Collections':>12} {'Size (MB)':>15}")
        print("-" * 80)

        total_size = 0
        db_details = []

        for db_name in sorted(db_list):
            try:
                db = client[db_name]
                stats = await db.command("dbStats")

                size_mb = (stats.get('dataSize', 0) + stats.get('indexSize', 0)) / (1024 * 1024)
                collections = stats.get('collections', 0)

                total_size += size_mb

                db_details.append({
                    'name': db_name,
                    'size': size_mb,
                    'collections': collections
                })

                print(f"{db_name:<30} {collections:>12} {size_mb:>14.2f} MB")

            except Exception as e:
                print(f"{db_name:<30} {'ERROR':>12} {str(e)}")

        print("-" * 80)
        print(f"{'TOTAL CLUSTER SIZE':<30} {'':<12} {total_size:>14.2f} MB")
        print(f"{'CLUSTER LIMIT (Free Tier)':<30} {'':<12} {'512.00 MB':>15}")
        print(f"{'AVAILABLE SPACE':<30} {'':<12} {(512 - total_size):>14.2f} MB")
        print("-" * 80)

        # Show largest databases
        print("\nLARGEST DATABASES")
        print("-" * 80)
        db_details.sort(key=lambda x: x['size'], reverse=True)

        for i, db in enumerate(db_details[:10], 1):
            percentage = (db['size'] / total_size * 100) if total_size > 0 else 0
            print(f"{i}. {db['name']:<28} {db['size']:>8.2f} MB ({percentage:>5.1f}%)")

        # Recommendations
        print("\nRECOMMENDATIONS")
        print("-" * 80)

        if total_size > 512:
            print("\nCRITICAL: Cluster is over quota!")
            print(f"  Current: {total_size:.2f} MB")
            print(f"  Limit:   512.00 MB")
            print(f"  Over by: {total_size - 512:.2f} MB\n")

            large_dbs = [db for db in db_details if db['size'] > 50]
            if large_dbs:
                print("Large databases to consider cleaning:")
                for db in large_dbs:
                    print(f"  - {db['name']}: {db['size']:.2f} MB")

        print("\nTo free space, you can:")
        print("  1. Delete unused databases")
        print("  2. Delete old collections in large databases")
        print("  3. Upgrade to a paid tier (M2, M5, etc.)")
        print("  4. Create a new free cluster for Dev database only")

        print("\n" + "=" * 80)

        client.close()

    except Exception as e:
        print(f"\nError checking cluster: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_all_databases())
