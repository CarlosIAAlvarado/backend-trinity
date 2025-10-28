import logging
from typing import Dict, Any, Optional
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryMarketAnalysisRepository:
    """
    Repository for Market Analysis operations in SECONDARY database (sample_mflix)
    Handles CRUD operations for trinity_performance_marketAnalysis collection
    This is a replica/backup of the main market analysis data
    """

    def __init__(self):
        self.collection_name = 'trinity_performance_marketAnalysis'
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def insert_analysis_with_retry(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update market analysis record (UPSERT) with retry logic

        This method will retry up to max_retries times if the operation fails.
        If all retries fail, it logs the error but doesn't raise an exception
        to avoid blocking the main database operation.

        Args:
            analysis_data: Dictionary containing market analysis data

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._insert_analysis(analysis_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved after {attempt + 1} attempts")
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[SECONDARY DB] Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"[SECONDARY DB] All {self.max_retries} attempts failed. "
                        f"Data NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _insert_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to insert or update market analysis record (UPSERT)

        Uses upsert to ensure only ONE document per timeframe exists.
        If document exists for the timeframe, it updates it.
        If not, it creates a new one.

        Args:
            analysis_data: Dictionary containing market analysis data

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            timeframe = analysis_data.get('timeframe')
            if not timeframe:
                raise ValueError("timeframe is required in analysis_data")

            # Set timestamps
            current_time = datetime.now()
            analysis_data['updatedAt'] = current_time

            # Check if document exists to determine if this is create or update
            existing = await collection.find_one({'timeframe': timeframe})

            if not existing:
                # First time: set createdAt
                analysis_data['createdAt'] = current_time
                action = "created"
            else:
                # Update: preserve original createdAt
                analysis_data['createdAt'] = existing.get('createdAt', current_time)
                action = "updated"

            # Upsert: update if exists, insert if not
            result = await collection.update_one(
                {'timeframe': timeframe},  # Filter by timeframe
                {'$set': analysis_data},   # Update/set all fields
                upsert=True                # Create if doesn't exist
            )

            logger.info(
                f"[SECONDARY DB] Market analysis {action} [{timeframe}]: "
                f"{analysis_data['market_status']} -> trinity_performance_marketAnalysis"
            )

            return {
                'modified_count': result.modified_count,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None,
                'status': 'success',
                'action': action
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error upserting market analysis: {e}")
            raise

    async def get_latest_analysis(self, timeframe: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent market analysis from secondary database

        Args:
            timeframe: Filter by specific timeframe ('12h' or '24h'), or None for any

        Returns:
            Dictionary with analysis data or None if not found
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            # Build query filter
            query = {}
            if timeframe:
                query['timeframe'] = timeframe

            # Sort by createdAt (insertion time)
            analysis = await collection.find_one(
                query,
                sort=[('createdAt', -1)]
            )

            return analysis

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error getting latest market analysis: {e}")
            return None

    async def count_records(self) -> int:
        """
        Count total market analysis records in secondary database

        Returns:
            Number of records
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting market analysis records: {e}")
            return 0
