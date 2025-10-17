import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from config.database import db_config

logger = logging.getLogger(__name__)

class MarketAnalysisRepository:
    """
    Repository for Market Analysis operations
    Handles CRUD operations for marketAnalysis collection
    """

    def __init__(self):
        self.collection_name = 'marketAnalysis'

    @property
    def collection(self):
        """Get the MongoDB collection"""
        return db_config.get_collection(self.collection_name)

    async def insert_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update market analysis record (UPSERT)

        Uses upsert to ensure only ONE document per timeframe exists.
        If document exists for the timeframe, it updates it.
        If not, it creates a new one.

        This prevents database accumulation and ensures fresh data.
        """
        try:
            collection = db_config.get_collection(self.collection_name)

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

            logger.info(f"Market analysis {action} [{timeframe}]: {analysis_data['market_status']}")

            return {
                'modified_count': result.modified_count,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None,
                'status': 'success',
                'action': action
            }

        except Exception as e:
            logger.error(f"Error upserting market analysis: {e}")
            raise

    async def get_latest_analysis(self, timeframe: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent market analysis

        Args:
            timeframe: Filter by specific timeframe ('12h' or '24h'), or None for any
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Build query filter
            query = {}
            if timeframe:
                query['timeframe'] = timeframe

            # FIXED: Sort by createdAt (insertion time) instead of timestamp (analysis time)
            # This ensures we always get the most recently inserted record
            analysis = await collection.find_one(
                query,
                sort=[('createdAt', -1)]
            )

            return analysis

        except Exception as e:
            logger.error(f"Error getting latest market analysis: {e}")
            return None

    async def get_all_analyses(self) -> List[Dict[str, Any]]:
        """
        Get all market analysis records (should only be 2: one for 12h, one for 24h)

        Returns:
            List of all market analysis documents
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            cursor = collection.find({})
            analyses = await cursor.to_list(length=None)

            return analyses

        except Exception as e:
            logger.error(f"Error getting all market analyses: {e}")
            return []

    async def count_records(self) -> int:
        """
        Count total market analysis records
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count

        except Exception as e:
            logger.error(f"Error counting market analysis records: {e}")
            return 0
