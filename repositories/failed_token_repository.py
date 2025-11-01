import logging
from typing import List, Dict, Any
from datetime import datetime
from config.database import db_config

logger = logging.getLogger(__name__)

class FailedTokenRepository:
    """
    Repository for Failed Token operations
    Handles CRUD operations for trinityTokensNotInOKX collection
    """

    def __init__(self):
        self.collection_name = 'trinityTokensNotInOKX'

    async def delete_all(self) -> int:
        """
        Delete ALL failed tokens from the collection
        Used before full refresh to ensure fresh historical data
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            result = await collection.delete_many({})
            logger.info(f"Deleted ALL failed tokens: {result.deleted_count} documents removed")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting all failed tokens: {e}")
            raise

    async def delete_by_symbols(self, symbols: List[str]) -> int:
        """
        Delete specific tokens by their symbols
        Used to remove tokens that are now available in OKX

        Args:
            symbols: List of token symbols to delete

        Returns:
            Number of deleted documents
        """
        try:
            if not symbols:
                return 0

            collection = db_config.get_collection(self.collection_name)
            result = await collection.delete_many({
                'symbol': {'$in': [s.upper() for s in symbols]}
            })
            logger.info(f"Deleted {result.deleted_count} tokens that are now available in OKX: {symbols}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting tokens by symbols: {e}")
            raise

    async def upsert_many(self, failed_tokens: List[Dict[str, Any]]) -> int:
        """
        Insert or update multiple failed tokens
        Uses symbol as unique key to avoid duplicates

        Args:
            failed_tokens: List of failed token documents

        Returns:
            Number of upserted documents
        """
        try:
            if not failed_tokens:
                logger.info("No failed tokens to upsert")
                print("DEBUG: No failed tokens to upsert")
                return 0

            print(f"\n=== DEBUG: Starting upsert of {len(failed_tokens)} failed tokens ===")
            collection = db_config.get_collection(self.collection_name)

            # Add/update timestamps
            now = datetime.now()
            upserted_count = 0

            for token in failed_tokens:
                token['updatedAt'] = now

                print(f"Upserting token: {token['symbol']}")

                # Upsert: update if exists, insert if not
                result = await collection.update_one(
                    {'symbol': token['symbol']},
                    {
                        '$set': token,
                        '$setOnInsert': {'createdAt': now}
                    },
                    upsert=True
                )

                if result.upserted_id or result.modified_count > 0:
                    upserted_count += 1
                    print(f"Success for {token['symbol']}: upserted_id={result.upserted_id}, modified={result.modified_count}")

            logger.info(f"Upserted {upserted_count} failed tokens into {self.collection_name}")
            print(f"=== DEBUG: Completed upsert - {upserted_count} tokens saved to {self.collection_name} ===\n")
            return upserted_count

        except Exception as e:
            logger.error(f"Error upserting failed tokens: {e}")
            print(f"ERROR upserting failed tokens: {e}")
            raise

    async def insert_many(self, failed_tokens: List[Dict[str, Any]]) -> int:
        """
        Insert multiple failed tokens at once
        Returns count of inserted documents
        """
        try:
            if not failed_tokens:
                logger.info("No failed tokens to insert")
                return 0

            collection = db_config.get_collection(self.collection_name)

            # Add timestamps
            now = datetime.now()
            for token in failed_tokens:
                token['timestamp'] = now
                token['createdAt'] = now

            result = await collection.insert_many(failed_tokens)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} failed tokens into {self.collection_name}")
            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting multiple failed tokens: {e}")
            raise

    async def find_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get all failed tokens
        Returns list sorted by symbol alphabetically
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find().sort('symbol', 1).limit(limit)
            failed_tokens = await cursor.to_list(length=limit)
            logger.info(f"Found {len(failed_tokens)} failed tokens")
            return failed_tokens
        except Exception as e:
            logger.error(f"Error finding all failed tokens: {e}")
            raise

    async def count_failed_tokens(self) -> int:
        """Get total count of failed tokens"""
        try:
            collection = db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count
        except Exception as e:
            logger.error(f"Error counting failed tokens: {e}")
            raise

    async def find_by_symbol(self, symbol: str) -> Dict[str, Any]:
        """Get failed token by symbol"""
        try:
            collection = db_config.get_collection(self.collection_name)
            failed_token = await collection.find_one({'symbol': symbol.upper()})
            return failed_token
        except Exception as e:
            logger.error(f"Error finding failed token by symbol: {e}")
            raise

    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about failed tokens
        Returns aggregated data for analysis
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            total_failed = await collection.count_documents({})

            # Group by reason
            pipeline = [
                {
                    '$group': {
                        '_id': '$reason',
                        'count': {'$sum': 1}
                    }
                }
            ]

            reasons_cursor = collection.aggregate(pipeline)
            reasons = await reasons_cursor.to_list(length=100)

            return {
                'total_failed': total_failed,
                'failure_reasons': reasons
            }
        except Exception as e:
            logger.error(f"Error getting failed token statistics: {e}")
            raise
