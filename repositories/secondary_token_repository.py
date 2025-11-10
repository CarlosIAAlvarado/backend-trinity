import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryTokenRepository:
    """
    Repository for Token operations in SECONDARY database
    Handles CRUD operations for trinity_performance_tokens collection
    This is a replica/backup of the main token data
    """

    def __init__(self):
        self.collection_name = 'trinity_performance_tokens'
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def bulk_upsert_tokens_with_retry(self, tokens_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk insert or update tokens with retry logic

        Args:
            tokens_data: List of token dictionaries

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._bulk_upsert_tokens(tokens_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved tokens after {attempt + 1} attempts")
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
                        f"Tokens NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _bulk_upsert_tokens(self, tokens_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Internal method to bulk upsert tokens

        Args:
            tokens_data: List of token dictionaries

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            if not tokens_data:
                return {
                    'status': 'success',
                    'action': 'no_data',
                    'upserted': 0,
                    'modified': 0
                }

            # Prepare bulk operations
            from pymongo import UpdateOne
            operations = []
            current_time = datetime.now()

            for token in tokens_data:
                # Add/update timestamps
                token['lastUpdated'] = current_time
                if 'createdAt' not in token:
                    token['createdAt'] = current_time

                operations.append(
                    UpdateOne(
                        {'cmcId': token['cmcId']},
                        {'$set': token},
                        upsert=True
                    )
                )

            # Execute bulk write
            result = await collection.bulk_write(operations, ordered=False)

            logger.info(
                f"[SECONDARY DB] Tokens synced: "
                f"upserted={result.upserted_count}, modified={result.modified_count} "
                f"-> trinity_performance_tokens"
            )

            return {
                'status': 'success',
                'action': 'upserted',
                'upserted': result.upserted_count,
                'modified': result.modified_count,
                'total': len(tokens_data)
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error bulk upserting tokens: {e}")
            raise

    async def delete_all_tokens_with_retry(self) -> Dict[str, Any]:
        """
        Delete all tokens with retry logic

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._delete_all_tokens()
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully deleted tokens after {attempt + 1} attempts")
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
                        f"Tokens NOT deleted from secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _delete_all_tokens(self) -> Dict[str, Any]:
        """
        Internal method to delete all tokens

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            result = await collection.delete_many({})

            logger.info(
                f"[SECONDARY DB] Deleted {result.deleted_count} tokens "
                f"-> trinity_performance_tokens"
            )

            return {
                'status': 'success',
                'action': 'deleted',
                'deleted_count': result.deleted_count
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error deleting tokens: {e}")
            raise

    async def count_tokens(self) -> int:
        """
        Count total tokens in secondary database

        Returns:
            Number of tokens
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting tokens: {e}")
            return 0
