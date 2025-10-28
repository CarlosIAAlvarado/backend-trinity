"""
Test script to verify dual database synchronization for market analysis
This script tests that market analysis data is saved to both databases:
1. Primary DB (trinity_market) - marketAnalysis collection
2. Secondary DB (Dev) - trinity_performance_marketAnalysis collection
"""

import asyncio
import logging
from config.database import db_config, secondary_db_config
from services.market_analysis_service import market_analysis_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_dual_database_sync():
    """
    Test that market analysis is saved to both databases
    """
    try:
        logger.info("="*70)
        logger.info("DUAL DATABASE SYNCHRONIZATION TEST")
        logger.info("="*70)

        # Connect to both databases
        logger.info("\n1. Connecting to PRIMARY database...")
        await db_config.connect()
        logger.info("   ✓ PRIMARY database connected (trinity_market)")

        logger.info("\n2. Connecting to SECONDARY database...")
        await secondary_db_config.connect()
        logger.info("   ✓ SECONDARY database connected (sample_mflix)")

        # Run market analysis and save to both databases
        logger.info("\n3. Running market analysis for both timeframes (12h, 24h)...")
        result = await market_analysis_service.analyze_and_save()

        logger.info("\n4. Analysis Results:")
        logger.info(f"   Status: {result['status']}")
        logger.info(f"   Message: {result['message']}")

        if result.get('data'):
            for item in result['data']:
                logger.info(f"   - [{item['timeframe']}] Market Status: {item['status']}")

        # Verify data in PRIMARY database
        logger.info("\n5. Verifying data in PRIMARY database (trinity_market)...")
        primary_12h = await market_analysis_service.market_repository.get_latest_analysis('12h')
        primary_24h = await market_analysis_service.market_repository.get_latest_analysis('24h')

        if primary_12h:
            logger.info(f"   ✓ [12h] Found in PRIMARY: {primary_12h['market_status']}")
        else:
            logger.error("   ✗ [12h] NOT found in PRIMARY database!")

        if primary_24h:
            logger.info(f"   ✓ [24h] Found in PRIMARY: {primary_24h['market_status']}")
        else:
            logger.error("   ✗ [24h] NOT found in PRIMARY database!")

        # Verify data in SECONDARY database
        logger.info("\n6. Verifying data in SECONDARY database (Dev)...")
        secondary_12h = await market_analysis_service.secondary_market_repository.get_latest_analysis('12h')
        secondary_24h = await market_analysis_service.secondary_market_repository.get_latest_analysis('24h')

        if secondary_12h:
            logger.info(f"   ✓ [12h] Found in SECONDARY: {secondary_12h['market_status']}")
            logger.info(f"       Database: Dev")
            logger.info(f"       Collection: trinity_performance_marketAnalysis")
        else:
            logger.error("   ✗ [12h] NOT found in SECONDARY database!")

        if secondary_24h:
            logger.info(f"   ✓ [24h] Found in SECONDARY: {secondary_24h['market_status']}")
            logger.info(f"       Database: Dev")
            logger.info(f"       Collection: trinity_performance_marketAnalysis")
        else:
            logger.error("   ✗ [24h] NOT found in SECONDARY database!")

        # Count records in both databases
        logger.info("\n7. Record counts:")
        primary_count = await market_analysis_service.market_repository.count_records()
        secondary_count = await market_analysis_service.secondary_market_repository.count_records()

        logger.info(f"   PRIMARY database: {primary_count} records")
        logger.info(f"   SECONDARY database: {secondary_count} records")

        # Final summary
        logger.info("\n" + "="*70)
        logger.info("TEST SUMMARY")
        logger.info("="*70)

        all_tests_passed = (
            primary_12h is not None and
            primary_24h is not None and
            secondary_12h is not None and
            secondary_24h is not None and
            primary_count == secondary_count
        )

        if all_tests_passed:
            logger.info("✓ ALL TESTS PASSED!")
            logger.info("✓ Market analysis is synchronized in both databases")
            logger.info("\nDatabases:")
            logger.info(f"  - PRIMARY: trinity_market.marketAnalysis")
            logger.info(f"  - SECONDARY: Dev.trinity_performance_marketAnalysis")
        else:
            logger.error("✗ SOME TESTS FAILED!")
            logger.error("✗ Please check the logs above for details")

        logger.info("="*70)

        # Disconnect from databases
        await db_config.disconnect()
        await secondary_db_config.disconnect()
        logger.info("\nDisconnected from both databases")

        return all_tests_passed

    except Exception as e:
        logger.error(f"\nERROR during test: {e}", exc_info=True)

        # Cleanup
        try:
            await db_config.disconnect()
            await secondary_db_config.disconnect()
        except:
            pass

        return False

if __name__ == "__main__":
    success = asyncio.run(test_dual_database_sync())
    exit(0 if success else 1)
