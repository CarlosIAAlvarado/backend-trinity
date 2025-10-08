# ==========================
# CoinMarketCap Service
# ==========================
import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

class CoinMarketCapService:
    """
    Service for interacting with CoinMarketCap API
    Follows Open/Closed Principle - open for extension, closed for modification
    """

    def __init__(self):
        self.api_key = os.getenv('CM_API_KEY', 'tu_api_key_aqui')
        self.base_url = 'https://pro-api.coinmarketcap.com/v1'
        self.headers = {'X-CMC_PRO_API_KEY': self.api_key}
        self.timeout = 15
        self.max_retries = 3
        self.base_delay = 1  # seconds

    async def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        retry_count: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Make API request with retry logic"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/{endpoint}",
                    params=params,
                    headers=self.headers
                )

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 401:
                    error_msg = "CoinMarketCap API key is invalid or expired"
                    logger.error(f"API error 401: {error_msg}")
                    return {"error": error_msg, "status_code": 401}

                elif response.status_code == 429 and retry_count < self.max_retries:
                    delay = self.base_delay * (2 ** retry_count)
                    logger.info(f"Rate limit hit, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    return await self._make_request(endpoint, params, retry_count + 1)

                elif response.status_code == 404:
                    logger.warning(f"Resource not found: {endpoint}")
                    return None

                else:
                    error_msg = f"CoinMarketCap API error: {response.status_code}"
                    logger.error(error_msg)
                    return {"error": error_msg, "status_code": response.status_code}

        except asyncio.TimeoutError:
            error_msg = "CoinMarketCap API request timeout"
            logger.error(error_msg)
            return {"error": error_msg, "status_code": 408}
        except Exception as e:
            error_msg = f"CoinMarketCap API request failed: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg, "status_code": 500}

    async def get_latest_listings(
        self,
        limit: int = 150,
        currency: str = 'USD'
    ) -> List[Dict[str, Any]]:
        """Get latest cryptocurrency listings"""
        params = {
            'start': 1,
            'limit': limit,
            'convert': currency
        }

        data = await self._make_request('cryptocurrency/listings/latest', params)

        if not data:
            return []

        # Check if there's an error
        if 'error' in data:
            return [{"error": data['error'], "status_code": data.get('status_code')}]

        if 'data' not in data:
            return []

        return data['data']

    async def get_token_exchanges(
        self,
        symbol: str,
        limit: int = 150
    ) -> List[str]:
        """Get exchanges for a specific token"""
        params = {
            'symbol': symbol,
            'limit': limit
        }

        data = await self._make_request('cryptocurrency/market-pairs/latest', params)

        if not data or 'data' not in data:
            return []

        token_data = data['data']
        if isinstance(token_data, list):
            token_data = next((t for t in token_data if t['symbol'] == symbol), None)

        if not token_data or 'market_pairs' not in token_data:
            return []

        # Extract unique exchange names
        exchanges = set()
        for pair in token_data['market_pairs']:
            if pair.get('exchange', {}).get('name'):
                exchanges.add(pair['exchange']['name'])

        return list(exchanges)

    async def get_token_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote for a specific token"""
        params = {'symbol': symbol.upper()}

        data = await self._make_request('cryptocurrency/quotes/latest', params)

        if not data or 'data' not in data:
            return None

        return data['data'].get(symbol.upper())