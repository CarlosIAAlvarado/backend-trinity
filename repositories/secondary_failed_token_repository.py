import logging
from typing import Dict, Any, List
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryFailedTokenRepository:
    """
    Repository for Failed Token operations in SECONDARY database
    Handles CRUD operations for trinity_Tokens_Performance_NotInOKX collection
    This is a replica/backup of tokens not available in OKX
    """

    def __init__(self):
        self.collection_name = 'trinity_Tokens_Performance_NotInOKX'
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def bulk_upsert_failed_tokens_with_retry(self, failed_tokens_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk insert or update failed tokens with retry logic

        Args:
            failed_tokens_data: List of failed token dictionaries

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._bulk_upsert_failed_tokens(failed_tokens_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved failed tokens after {attempt + 1} attempts")
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
                        f"Failed tokens NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _bulk_upsert_failed_tokens(self, failed_tokens_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Internal method to bulk upsert failed tokens

        Args:
            failed_tokens_data: List of failed token dictionaries

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            if not failed_tokens_data:
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

            for token in failed_tokens_data:
                # Add/update timestamps
                token['updatedAt'] = current_time
                if 'createdAt' not in token:
                    token['createdAt'] = current_time

                operations.append(
                    UpdateOne(
                        {'symbol': token['symbol']},
                        {'$set': token},
                        upsert=True
                    )
                )

            # Execute bulk write
            result = await collection.bulk_write(operations, ordered=False)

            logger.info(
                f"[SECONDARY DB] Failed tokens synced: "
                f"upserted={result.upserted_count}, modified={result.modified_count} "
                f"-> trinity_Tokens_Performance_NotInOKX"
            )

            return {
                'status': 'success',
                'action': 'upserted',
                'upserted': result.upserted_count,
                'modified': result.modified_count,
                'total': len(failed_tokens_data)
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error bulk upserting failed tokens: {e}")
            raise

    async def delete_all_with_retry(self) -> Dict[str, Any]:
        """
        Delete all failed tokens with retry logic

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._delete_all()
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully deleted failed tokens after {attempt + 1} attempts")
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
                        f"Failed tokens NOT deleted from secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _delete_all(self) -> Dict[str, Any]:
        """
        Internal method to delete all failed tokens

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            result = await collection.delete_many({})

            logger.info(
                f"[SECONDARY DB] Deleted {result.deleted_count} failed tokens "
                f"-> trinity_Tokens_Performance_NotInOKX"
            )

            return {
                'status': 'success',
                'action': 'deleted',
                'deleted_count': result.deleted_count
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error deleting failed tokens: {e}")
            raise

    async def count_failed_tokens(self) -> int:
        """
        Count total failed tokens in secondary database

        Returns:
            Number of failed tokens
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting failed tokens: {e}")
            return 0

    async def delete_by_symbols_with_retry(self, symbols: list) -> Dict[str, Any]:
        """
        Delete specific failed tokens by symbols with retry logic

        Args:
            symbols: List of token symbols to delete

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._delete_by_symbols(symbols)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully deleted failed tokens after {attempt + 1} attempts")
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
                        f"Failed tokens NOT deleted from secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _delete_by_symbols(self, symbols: list) -> Dict[str, Any]:
        """
        Internal method to delete specific failed tokens by symbols

        Args:
            symbols: List of token symbols to delete

        Returns:
            Dictionary with operation result
        """
        try:
            if not symbols:
                return {
                    'status': 'success',
                    'action': 'no_data',
                    'deleted_count': 0
                }

            collection = secondary_db_config.get_collection(self.collection_name)
            result = await collection.delete_many({
                'symbol': {'$in': [s.upper() for s in symbols]}
            })

            logger.info(
                f"[SECONDARY DB] Deleted {result.deleted_count} failed tokens by symbols "
                f"-> trinity_Tokens_Performance_NotInOKX"
            )

            return {
                'status': 'success',
                'action': 'deleted',
                'deleted_count': result.deleted_count
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error deleting failed tokens by symbols: {e}")
            raise
