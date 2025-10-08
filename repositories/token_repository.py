# ==========================
# Token Repository
# ==========================
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
from pymongo import UpdateOne
from models.token_model import TokenModel
from config.database import db_config

logger = logging.getLogger(__name__)

class TokenRepository:
    """
    Repository pattern for token data access
    Follows Interface Segregation Principle
    """

    def __init__(self):
        self.collection_name = "trinity_market_cap_tokens"

    async def find_by_market_cap(
        self,
        min_market_cap: float,
        is_on_okx: Optional[bool] = None,
        limit: int = 100,
        condition: str = 'greater'
    ) -> List[Dict[str, Any]]:
        """Find tokens by market cap with condition (greater, less, equal)"""
        try:
            collection = db_config.get_collection(self.collection_name)

            # Build market cap filter based on condition
            if condition == 'greater':
                market_cap_filter = {"$gte": min_market_cap}
            elif condition == 'less':
                market_cap_filter = {"$lte": min_market_cap}
            elif condition == 'equal':
                market_cap_filter = {"$eq": min_market_cap}
            else:
                market_cap_filter = {"$gte": min_market_cap}

            query = {"marketCap": market_cap_filter}
            if is_on_okx is not None:
                query["isOnOKX"] = is_on_okx

            cursor = collection.find(query).sort("marketCap", -1).limit(limit)
            tokens = await cursor.to_list(length=limit)

            logger.info(f"Found {len(tokens)} tokens with market cap {condition} ${min_market_cap:,}")

            return tokens

        except Exception as e:
            logger.error(f"Error finding tokens by market cap: {e}")
            raise

    async def find_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Find token by symbol"""
        try:
            collection = db_config.get_collection(self.collection_name)
            token = await collection.find_one({"symbol": symbol.upper()})
            return token

        except Exception as e:
            logger.error(f"Error finding token by symbol: {e}")
            raise

    async def find_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Find all tokens"""
        try:
            collection = db_config.get_collection(self.collection_name)
            cursor = collection.find().sort("marketCap", -1).limit(limit)
            tokens = await cursor.to_list(length=limit)
            logger.info(f"Found {len(tokens)} total tokens")
            return tokens

        except Exception as e:
            logger.error(f"Error finding all tokens: {e}")
            raise

    async def upsert_many(self, tokens: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert or update multiple tokens"""
        try:
            collection = db_config.get_collection(self.collection_name)
            operations = []

            for token in tokens:
                if not all(k in token for k in ["symbol", "name", "cmcId", "marketCap"]):
                    logger.warning(f"Skipping incomplete token: {token}")
                    continue

                operation = UpdateOne(
                    {
                        "$or": [
                            {"cmcId": token["cmcId"]},
                            {"symbol": token["symbol"].upper()}
                        ]
                    },
                    {
                        "$set": {
                            "symbol": token["symbol"].upper(),
                            "name": token["name"],
                            "cmcId": token["cmcId"],
                            "marketCap": token["marketCap"],
                            "price": token.get("price"),
                            "cmcRank": token.get("cmcRank"),
                            "exchanges": token.get("exchanges", []),
                            "isOnOKX": token.get("isOnOKX", False),
                            "exchangeCount": len(token.get("exchanges", [])),
                            "lastUpdated": datetime.now()
                        }
                    },
                    upsert=True
                )
                operations.append(operation)

            if not operations:
                return {"inserted": 0, "modified": 0, "matched": 0}

            result = await collection.bulk_write(operations)

            return {
                "inserted": result.upserted_count,
                "modified": result.modified_count,
                "matched": result.matched_count
            }

        except Exception as e:
            logger.error(f"Error upserting tokens: {e}")
            raise

    async def delete_old_tokens(self, days: int = 30) -> int:
        """Delete tokens not updated in specified days"""
        try:
            collection = db_config.get_collection(self.collection_name)

            cutoff_date = datetime.now() - timedelta(days=days)
            result = await collection.delete_many({"lastUpdated": {"$lt": cutoff_date}})

            return result.deleted_count

        except Exception as e:
            logger.error(f"Error deleting old tokens: {e}")
            raise