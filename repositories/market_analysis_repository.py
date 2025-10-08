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
        Insert a new market analysis record
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            analysis_data['createdAt'] = datetime.now()
            analysis_data['updatedAt'] = datetime.now()

            result = await collection.insert_one(analysis_data)

            logger.info(f"Inserted market analysis: {analysis_data['market_status']}")

            return {
                'inserted_id': str(result.inserted_id),
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Error inserting market analysis: {e}")
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

            analysis = await collection.find_one(
                query,
                sort=[('timestamp', -1)]
            )

            return analysis

        except Exception as e:
            logger.error(f"Error getting latest market analysis: {e}")
            return None

    async def get_history(self, limit: int = 100, timeframe: str = None) -> List[Dict[str, Any]]:
        """
        Get historical market analysis records

        Args:
            limit: Maximum number of records to retrieve
            timeframe: Filter by specific timeframe ('12h' or '24h'), or None for all
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Build query filter
            query = {}
            if timeframe:
                query['timeframe'] = timeframe

            cursor = collection.find(
                query,
                sort=[('timestamp', -1)],
                limit=limit
            )

            history = await cursor.to_list(length=limit)

            return history

        except Exception as e:
            logger.error(f"Error getting market analysis history: {e}")
            return []

    async def get_history_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get market analysis records within a date range
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            cursor = collection.find(
                {
                    'timestamp': {
                        '$gte': start_date,
                        '$lte': end_date
                    }
                },
                sort=[('timestamp', -1)]
            )

            history = await cursor.to_list(length=None)

            return history

        except Exception as e:
            logger.error(f"Error getting market analysis by date range: {e}")
            return []

    async def delete_old_records(self, days_old: int = 30) -> int:
        """
        Delete market analysis records older than specified days
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            cutoff_date = datetime.now() - timedelta(days=days_old)

            result = await collection.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })

            deleted_count = result.deleted_count
            logger.info(f"Deleted {deleted_count} old market analysis records (older than {days_old} days)")

            return deleted_count

        except Exception as e:
            logger.error(f"Error deleting old market analysis records: {e}")
            return 0

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
