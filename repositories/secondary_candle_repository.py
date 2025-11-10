import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryCandleRepository:
    """
    Repository for Candle operations in SECONDARY database
    Handles CRUD operations for trinity_performance_candles collection
    This is a replica/backup of the main candle data
    """

    def __init__(self):
        self.collection_name = 'trinity_performance_candles'
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def bulk_upsert_candles_with_retry(self, candles_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk insert or update candles with retry logic

        Args:
            candles_data: List of candle dictionaries

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._bulk_upsert_candles(candles_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved candles after {attempt + 1} attempts")
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
                        f"Candles NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _bulk_upsert_candles(self, candles_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Internal method to bulk upsert candles

        Args:
            candles_data: List of candle dictionaries

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            if not candles_data:
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

            for candle in candles_data:
                # Add/update timestamps
                candle['updatedAt'] = current_time
                if 'createdAt' not in candle:
                    candle['createdAt'] = current_time

                operations.append(
                    UpdateOne(
                        {
                            'symbol': candle['symbol'],
                            'timeframe': candle['timeframe']
                        },
                        {'$set': candle},
                        upsert=True
                    )
                )

            # Execute bulk write
            result = await collection.bulk_write(operations, ordered=False)

            logger.info(
                f"[SECONDARY DB] Candles synced: "
                f"upserted={result.upserted_count}, modified={result.modified_count} "
                f"-> trinity_performance_candles"
            )

            return {
                'status': 'success',
                'action': 'upserted',
                'upserted': result.upserted_count,
                'modified': result.modified_count,
                'total': len(candles_data)
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error bulk upserting candles: {e}")
            raise

    async def delete_candles_by_symbol_with_retry(self, symbol: str) -> Dict[str, Any]:
        """
        Delete candles by symbol with retry logic

        Args:
            symbol: Token symbol

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._delete_candles_by_symbol(symbol)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully deleted candles after {attempt + 1} attempts")
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
                        f"Candles NOT deleted from secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _delete_candles_by_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Internal method to delete candles by symbol

        Args:
            symbol: Token symbol

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            result = await collection.delete_many({'symbol': symbol.upper()})

            logger.info(
                f"[SECONDARY DB] Deleted {result.deleted_count} candles for {symbol} "
                f"-> trinity_performance_candles"
            )

            return {
                'status': 'success',
                'action': 'deleted',
                'deleted_count': result.deleted_count,
                'symbol': symbol
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error deleting candles: {e}")
            raise

    async def count_candles(self, timeframe: str = None) -> int:
        """
        Count total candles in secondary database

        Args:
            timeframe: Optional timeframe filter

        Returns:
            Number of candles
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            query = {}
            if timeframe:
                query['timeframe'] = timeframe

            count = await collection.count_documents(query)
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting candles: {e}")
            return 0
