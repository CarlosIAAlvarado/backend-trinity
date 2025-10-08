# ==========================
# Token Service
# ==========================
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from repositories.token_repository import TokenRepository
from services.coinmarketcap_service import CoinMarketCapService
from models.token_model import TokenModel, TokenResponse
from services.websocket_service import websocket_service

logger = logging.getLogger(__name__)

class TokenService:
    """
    Business logic service for token operations
    Follows Dependency Inversion Principle
    """

    def __init__(self):
        self.repository = TokenRepository()
        self.cmc_service = CoinMarketCapService()
        self.cache_time = 5 * 60  # 5 minutes in seconds
        self.batch_size = 5
        self.batch_delay = 2  # seconds

    async def get_high_market_cap_tokens(
        self,
        min_market_cap: float = 800000000,
        limit: int = 150,
        currency: str = 'USD',
        check_exchanges: bool = True,
        refresh: bool = False,
        condition: str = 'greater'
    ) -> TokenResponse:
        """Get tokens with high market cap"""
        try:
            # Check database cache first
            if not refresh:
                db_tokens = await self.repository.find_by_market_cap(
                    min_market_cap,
                    is_on_okx=True if check_exchanges else None,
                    limit=limit,
                    condition=condition
                )

                if db_tokens:
                    tokens = [TokenModel(**token) for token in db_tokens]
                    return TokenResponse(
                        status="Success",
                        message=f"Data from database (Market Cap {condition} ${min_market_cap:,})",
                        source="database",
                        count=len(tokens),
                        data=tokens
                    )

            # Fetch from API
            logger.info("Fetching data from CoinMarketCap API...")
            listings = await self.cmc_service.get_latest_listings(limit, currency)

            if not listings:
                return TokenResponse(
                    status="Error",
                    message="No data available from CoinMarketCap API",
                    source="api",
                    count=0,
                    data=[],
                    api_error="No data returned from API"
                )

            # Check if first item is an error
            if listings and isinstance(listings[0], dict) and 'error' in listings[0]:
                error_info = listings[0]

                # Update global API error status
                await websocket_service.update_api_error(error_info['error'])

                # Try to get cached data from database
                db_tokens = await self.repository.find_by_market_cap(
                    min_market_cap,
                    is_on_okx=True if check_exchanges else None,
                    limit=limit,
                    condition=condition
                )

                tokens = [TokenModel(**token) for token in db_tokens] if db_tokens else []
                return TokenResponse(
                    status="Warning" if db_tokens else "Error",
                    message=f"Using cached data - API Error: {error_info['error']}" if db_tokens else error_info['error'],
                    source="database" if db_tokens else "api",
                    count=len(tokens),
                    data=tokens,
                    api_error=error_info['error']
                )

            # Clear API error if we got here (API is working)
            await websocket_service.update_api_error(None)

            # Process and filter tokens
            tokens = []
            for coin in listings:
                quote = coin.get('quote', {}).get(currency, {})
                market_cap = quote.get('market_cap', 0)

                if market_cap < min_market_cap:
                    continue

                token_data = {
                    "symbol": coin['symbol'],
                    "name": coin['name'],
                    "cmcId": coin['id'],
                    "marketCap": market_cap,
                    "price": quote.get('price'),
                    "cmcRank": coin.get('cmc_rank'),
                    "exchanges": [],
                    "isOnOKX": False,
                    "exchangeCount": 0
                }

                tokens.append(token_data)

            # Sort by market cap
            tokens.sort(key=lambda x: x['marketCap'], reverse=True)

            # Check exchanges if requested
            if check_exchanges:
                tokens = await self._verify_exchanges(tokens)

            # Save to database if refreshing
            if refresh and tokens:
                result = await self.repository.upsert_many(tokens)
                logger.info(f"Database upsert: {result['inserted']} inserted, {result['modified']} modified")

                # Cleanup tokens that don't meet criteria anymore
                deleted_count = await self._cleanup_old_tokens(min_market_cap, limit)
                if deleted_count > 0:
                    logger.info(f"Cleanup: Removed {deleted_count} tokens that don't meet criteria")

            # Convert to models
            token_models = [TokenModel(**token) for token in tokens]

            return TokenResponse(
                status="Success",
                message=f"Tokens with market cap > {min_market_cap}",
                source="api" if refresh else "database",
                count=len(token_models),
                data=token_models
            )

        except Exception as e:
            logger.error(f"Error getting high market cap tokens: {e}")
            raise

    async def _verify_exchanges(
        self,
        tokens: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Verify exchanges for tokens"""
        logger.info(f"Verifying exchanges for {len(tokens)} tokens...")

        results = []
        for i in range(0, len(tokens), self.batch_size):
            batch = tokens[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1}...")

            # Process batch concurrently
            import asyncio
            tasks = []
            for token in batch:
                task = self._check_token_exchanges(token)
                tasks.append(task)

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error processing token: {result}")
                else:
                    results.append(result)

            # Delay between batches
            if i + self.batch_size < len(tokens):
                await asyncio.sleep(self.batch_delay)

        logger.info(f"Exchange verification completed: {len(results)} tokens processed")
        return results

    async def _check_token_exchanges(
        self,
        token: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check exchanges for a single token"""
        try:
            exchanges = await self.cmc_service.get_token_exchanges(token['symbol'])

            # Check if token is on OKX
            is_on_okx = any(
                'okx' in exchange.lower() or 'okex' in exchange.lower()
                for exchange in exchanges
            )

            token['exchanges'] = exchanges
            token['exchangeCount'] = len(exchanges)
            token['isOnOKX'] = is_on_okx

            logger.info(f"{token['symbol']}: {len(exchanges)} exchanges")

            return token

        except Exception as e:
            logger.warning(f"Error checking exchanges for {token['symbol']}: {e}")
            token['exchanges'] = []
            token['exchangeCount'] = 0
            token['isOnOKX'] = False
            return token

    async def get_token_by_symbol(self, symbol: str) -> Optional[TokenModel]:
        """Get token information by symbol"""
        try:
            # Check database first
            db_token = await self.repository.find_by_symbol(symbol)
            if db_token:
                return TokenModel(**db_token)

            # Fetch from API
            quote_data = await self.cmc_service.get_token_quote(symbol)
            if not quote_data:
                return None

            token_data = {
                "symbol": quote_data['symbol'],
                "name": quote_data['name'],
                "cmcId": quote_data['id'],
                "marketCap": quote_data['quote']['USD']['market_cap'],
                "price": quote_data['quote']['USD']['price'],
                "cmcRank": quote_data.get('cmc_rank'),
                "exchanges": [],
                "isOnOKX": False,
                "exchangeCount": 0
            }

            return TokenModel(**token_data)

        except Exception as e:
            logger.error(f"Error getting token by symbol: {e}")
            return None

    async def _cleanup_old_tokens(self, min_market_cap: float, max_rank: int) -> int:
        """
        Cleanup tokens that don't meet criteria
        Removes tokens with rank > max_rank OR market cap < min_market_cap
        """
        try:
            from config.database import db_config
            collection = db_config.get_collection('trinity_market_cap_tokens')

            # Delete tokens that don't meet BOTH criteria
            result = await collection.delete_many({
                '$or': [
                    {'cmcRank': {'$gt': max_rank}},
                    {'marketCap': {'$lt': min_market_cap}}
                ]
            })

            return result.deleted_count

        except Exception as e:
            logger.error(f"Error cleaning up old tokens: {e}")
            return 0