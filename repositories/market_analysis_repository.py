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

        NEW VERSION: Uses a single document for ALL timeframes.
        The document contains nested timeframe data in candlesByTimeframe.

        Always replaces the entire document to ensure fresh data.
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Set timestamps
            current_time = datetime.now()
            analysis_data['updatedAt'] = current_time

            # Check if any document exists
            existing = await collection.find_one({})

            if not existing:
                # First time: set createdAt
                analysis_data['createdAt'] = current_time
                action = "created"
            else:
                # Update: preserve original createdAt
                analysis_data['createdAt'] = existing.get('createdAt', current_time)
                action = "updated"

            # Delete all existing documents and insert the new one
            # This ensures we only have ONE document with ALL timeframes
            await collection.delete_many({})
            result = await collection.insert_one(analysis_data)

            direction = analysis_data.get('direction', 'UNKNOWN')
            logger.info(f"Market analysis {action}: {direction}")

            return {
                'inserted_id': str(result.inserted_id),
                'status': 'success',
                'action': action
            }

        except Exception as e:
            logger.error(f"Error upserting market analysis: {e}")
            raise

    async def get_latest_analysis(self, timeframe: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent market analysis

        NEW VERSION: Returns the single document with ALL timeframes.
        The timeframe parameter is kept for backward compatibility but ignored.

        Returns:
            The single market analysis document with nested timeframe data
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Get the single document (there should only be one)
            analysis = await collection.find_one({})

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
