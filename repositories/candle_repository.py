import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from config.database import db_config

logger = logging.getLogger(__name__)

class CandleRepository:
    """
    Repository for Candle operations
    Handles CRUD operations for trinityCandles collection
    """

    def __init__(self):
        self.collection_name = 'trinityCandles'

    @property
    def collection(self):
        """Get the MongoDB collection"""
        return db_config.get_collection(self.collection_name)

    async def upsert_candle(self, candle_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update a candle
        Uses unique combination of symbol + timeframe (always latest candle)
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            filter_query = {
                'symbol': candle_data['symbol'],
                'timeframe': candle_data['timeframe']
            }

            update_data = {
                '$set': {
                    **candle_data,
                    'updatedAt': datetime.now()
                },
                '$setOnInsert': {
                    'createdAt': datetime.now()
                }
            }

            result = await collection.update_one(
                filter_query,
                update_data,
                upsert=True
            )

            return {
                'matched': result.matched_count,
                'modified': result.modified_count,
                'upserted': result.upserted_id is not None
            }

        except Exception as e:
            logger.error(f"Error upserting candle: {e}")
            raise

    async def upsert_one(self, candle_data: Dict[str, Any]) -> Dict[str, Any]:
        """Alias for upsert_candle for consistency with upsert_many"""
        return await self.upsert_candle(candle_data)

    async def upsert_many(self, candles: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert or update multiple candles
        """
        try:
            inserted = 0
            modified = 0

            for candle in candles:
                result = await self.upsert_candle(candle)
                if result['upserted']:
                    inserted += 1
                elif result['modified'] > 0:
                    modified += 1

            logger.info(f"Candles upsert: {inserted} inserted, {modified} modified")
            return {'inserted': inserted, 'modified': modified}

        except Exception as e:
            logger.error(f"Error upserting multiple candles: {e}")
            raise

    async def find_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get all candles"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find().sort('timestamp', -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error finding all candles: {e}")
            raise

    async def find_by_timeframe(self, timeframe: str) -> List[Dict[str, Any]]:
        """Get all candles for a specific timeframe"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find({'timeframe': timeframe})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error finding candles by timeframe {timeframe}: {e}")
            raise

    async def find_all_ordered_by_performance(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get all candles ordered by 24h performance (descending)
        Groups all timeframes by symbol, ordered by their 24h performance
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Step 1: Get all 24h candles ordered by performance DESC
            candles_24h = await collection.find(
                {'timeframe': '24h'}
            ).sort('performance', -1).to_list(length=limit)

            # Step 2: For each symbol in order, get all its timeframes
            result = []
            symbols_processed = set()

            for candle_24h in candles_24h:
                symbol = candle_24h['symbol']

                if symbol in symbols_processed:
                    continue

                symbols_processed.add(symbol)

                # Get all timeframes for this symbol
                symbol_candles = await collection.find(
                    {'symbol': symbol}
                ).sort([('timeframe', 1)]).to_list(length=10)

                # Sort timeframes in specific order: 15m, 30m, 1h, 12h, 24h
                timeframe_order = {'15m': 1, '30m': 2, '1h': 3, '12h': 4, '24h': 5}
                symbol_candles.sort(key=lambda x: timeframe_order.get(x['timeframe'], 999))

                result.extend(symbol_candles)

                if len(result) >= limit:
                    break

            logger.info(f"Found {len(result)} candles ordered by 24h performance")
            return result[:limit]

        except Exception as e:
            logger.error(f"Error finding candles ordered by performance: {e}")
            raise

    async def find_by_symbol(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get candles for a specific symbol"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find({'symbol': symbol.upper()}).sort('timestamp', -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error finding candles by symbol: {e}")
            raise

    async def find_by_symbol_and_timeframe(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get candles for a specific symbol and timeframe"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find({
                'symbol': symbol.upper(),
                'timeframe': timeframe
            }).sort('timestamp', -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error finding candles by symbol and timeframe: {e}")
            raise

    async def delete_old_candles(self, days_old: int = 7) -> int:
        """Delete candles older than specified days"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cutoff_date = datetime.now() - timedelta(days=days_old)

            result = await collection.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })

            logger.info(f"Deleted {result.deleted_count} old candles")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting old candles: {e}")
            raise

    async def get_latest_candle(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Get the most recent candle for a symbol and timeframe"""
        try:
            collection = db_config.get_collection(self.collection_name)
            candle = await collection.find_one(
                {'symbol': symbol.upper(), 'timeframe': timeframe},
                sort=[('timestamp', -1)]
            )
            return candle
        except Exception as e:
            logger.error(f"Error getting latest candle: {e}")
            raise

    async def count_candles(self) -> int:
        """Get total count of candles"""
        try:
            collection = db_config.get_collection(self.collection_name)
            return await collection.count_documents({})
        except Exception as e:
            logger.error(f"Error counting candles: {e}")
            raise

    async def delete_all(self) -> int:
        """
        Delete ALL candles from the collection
        Used before full refresh to ensure data consistency
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            result = await collection.delete_many({})
            logger.info(f"Deleted ALL candles: {result.deleted_count} documents removed")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting all candles: {e}")
            raise

    async def insert_many(self, candles: List[Dict[str, Any]]) -> int:
        """
        Insert multiple candles at once (used after delete_all)
        Returns count of inserted documents
        """
        try:
            if not candles:
                return 0

            collection = db_config.get_collection(self.collection_name)

            # Add timestamps
            now = datetime.now()
            for candle in candles:
                candle['createdAt'] = now
                candle['updatedAt'] = now

            result = await collection.insert_many(candles)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} new candles")
            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting multiple candles: {e}")
            raise

    async def update_price_snapshot(
        self,
        symbol: str,
        timeframe: str,
        update_data: Dict[str, Any]
    ) -> bool:
        """
        Actualiza solo el precio actual (close) y performance de una vela existente
        Usado para snapshots de precios en tiempo real cada 1 minuto
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            filter_query = {
                'symbol': symbol,
                'timeframe': timeframe
            }

            update_query = {
                '$set': {
                    **update_data,
                    'updatedAt': datetime.now()
                }
            }

            result = await collection.update_one(filter_query, update_query)
            return result.modified_count > 0

        except Exception as e:
            logger.error(f"Error updating price snapshot for {symbol} {timeframe}: {e}")
            return False
