import httpx
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class OKXService:
    """
    Service for interacting with OKX API
    Fetches candlestick (OHLC) data for cryptocurrency pairs
    """

    def __init__(self):
        self.base_url = 'https://www.okx.com'
        self.api_key = '2d0e0c6c-a67c-4666-a8ad-99ab58780a76'
        self.secret_key = '96E8FA666DAE6E5CB782E7DC5900DF0A'
        self.timeout = 30
        self.max_retries = 5  # Increased from 3 to 5 for better recovery
        self.base_delay = 1

        # Concurrent request limits
        self.max_concurrent_requests = 10  # Reduced from 20 to 10 to minimize rate limiting
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        # Shared HTTP client with connection pooling
        self._client = None

        # Timeframe mapping: our format -> OKX format
        self.timeframe_map = {
            '15m': '15m',
            '30m': '30m',
            '1h': '1H',
            '12h': '12H',
            '24h': '1D'
        }

    def _build_instrument_id(self, symbol: str) -> str:
        """
        Build OKX instrument ID from symbol
        Example: BTC -> BTC-USDT
        """
        return f"{symbol.upper()}-USDT"

    def _get_okx_timeframe(self, timeframe: str) -> str:
        """
        Convert our timeframe format to OKX format
        Example: 1h -> 1H
        """
        return self.timeframe_map.get(timeframe, timeframe)

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client with connection pooling"""
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20
            )
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=limits
            )
        return self._client

    async def close_client(self):
        """Close the HTTP client"""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        retry_count: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Make API request with retry logic using shared client"""
        try:
            client = await self._get_client()
            response = await client.get(
                f"{self.base_url}{endpoint}",
                params=params
            )

            if response.status_code == 200:
                data = response.json()

                # OKX returns code "0" for success
                if data.get('code') == '0':
                    return data
                else:
                    logger.error(f"OKX API error for {params.get('instId')}: {data.get('msg', 'Unknown error')}")
                    return None

            elif response.status_code == 429 and retry_count < self.max_retries:
                delay = self.base_delay * (2 ** retry_count)
                logger.info(f"Rate limit hit for {params.get('instId')}, retrying in {delay}s...")
                await asyncio.sleep(delay)
                return await self._make_request(endpoint, params, retry_count + 1)

            else:
                logger.error(f"OKX API HTTP {response.status_code} for {params.get('instId')}")
                return None

        except asyncio.TimeoutError:
            logger.error(f"OKX request timeout for {params.get('instId')}")
            return None
        except httpx.ConnectError as e:
            logger.error(f"OKX connection error for {params.get('instId')}: {str(e)[:100]}")
            return None
        except Exception as e:
            logger.error(f"OKX request failed for {params.get('instId')}: {type(e).__name__} - {str(e)[:100]}")
            return None

    async def get_candlestick(
        self,
        symbol: str,
        timeframe: str = '15m',
        name: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the latest candlestick for a symbol and timeframe
        Performance calculado como CoinMarketCap: ultimos X minutos COMPLETOS

        Args:
            symbol: Token symbol (e.g., "BTC", "ETH")
            timeframe: Timeframe (15m, 30m, 1h, 12h, 24h)

        Returns:
            Dict with candle data or None if error
        """
        async with self.semaphore:
            try:
                instrument_id = self._build_instrument_id(symbol)
                okx_timeframe = self._get_okx_timeframe(timeframe)

                candles_needed = {
                    '15m': 2,
                    '30m': 3,
                    '1H': 2,
                    '12H': 2,
                    '1D': 2
                }
                limit = candles_needed.get(okx_timeframe, 2)

                params = {
                    'instId': instrument_id,
                    'bar': okx_timeframe,
                    'limit': limit
                }

                data = await self._make_request('/api/v5/market/candles', params)

                if not data or 'data' not in data or not data['data']:
                    return None

                candles = data['data']
                current_candle = candles[0]

                timestamp_ms = int(current_candle[0])
                open_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

                timeframe_minutes = {
                    '15m': 15, '30m': 30, '1H': 60, '12H': 720, '1D': 1440
                }
                minutes = timeframe_minutes.get(okx_timeframe, 15)
                close_timestamp = open_timestamp + timedelta(minutes=minutes)

                current_close = float(current_candle[4])
                current_high = float(current_candle[2])
                current_low = float(current_candle[3])

                price_from_period_ago = None
                if timeframe == '15m' and len(candles) >= 2:
                    price_from_period_ago = float(candles[1][4])
                elif timeframe == '30m' and len(candles) >= 3:
                    price_from_period_ago = float(candles[2][4])
                elif timeframe == '1h' and len(candles) >= 2:
                    price_from_period_ago = float(candles[1][4])
                elif timeframe == '12h' and len(candles) >= 2:
                    price_from_period_ago = float(candles[1][4])
                elif timeframe == '24h' and len(candles) >= 2:
                    price_from_period_ago = float(candles[1][4])
                else:
                    price_from_period_ago = float(current_candle[1])

                performance = ((current_close - price_from_period_ago) / price_from_period_ago) * 100 if price_from_period_ago != 0 else 0.0

                # IMPORTANTE: 'open' debe ser el precio de apertura REAL de la vela actual,
                # NO el price_from_period_ago que es solo para calcular el performance
                current_open = float(current_candle[1])

                candle = {
                    'symbol': symbol.upper(),
                    'name': name if name else symbol.upper(),
                    'timeframe': timeframe,
                    'open': current_open,
                    'high': current_high,
                    'low': current_low,
                    'close': current_close,
                    'performance': round(performance, 2),
                    'openTimestamp': open_timestamp,
                    'closeTimestamp': close_timestamp
                }

                return candle

            except Exception as e:
                logger.error(f"Error getting candlestick for {symbol} [{timeframe}]: {e}")
                return None

    async def get_multiple_candlesticks(
        self,
        symbol: str,
        timeframes: list,
        name: str = None
    ) -> list:
        """
        Get candlesticks for multiple timeframes of a symbol
        Uses concurrent processing for better performance

        Args:
            symbol: Token symbol
            timeframes: List of timeframes ['15m', '30m', '1h', '12h', '24h']
            name: Token name (e.g., 'Bitcoin')

        Returns:
            List of candle dicts
        """
        # Create tasks for all timeframes concurrently
        tasks = [
            self.get_candlestick(symbol, timeframe, name)
            for timeframe in timeframes
        ]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None values and exceptions
        candles = [
            result for result in results
            if result is not None and not isinstance(result, Exception)
        ]

        return candles

    async def get_candlesticks_batch(
        self,
        tokens: List[Dict[str, Any]],
        timeframes: list
    ) -> List[Dict[str, Any]]:
        """
        Get candlesticks for multiple tokens in parallel
        MASSIVE PERFORMANCE IMPROVEMENT for bulk updates

        Args:
            tokens: List of token dicts with 'symbol' and 'name'
            timeframes: List of timeframes

        Returns:
            List of all candles from all tokens
        """
        logger.info(f"Starting batch processing for {len(tokens)} tokens with {len(timeframes)} timeframes each")
        logger.info(f"Total requests: {len(tokens) * len(timeframes)} (processing {self.max_concurrent_requests} at a time)")

        # Create tasks for ALL tokens and timeframes
        tasks = []
        for token in tokens:
            symbol = token.get('symbol')
            name = token.get('name', symbol)

            if not symbol:
                continue

            # Create task for each timeframe
            for timeframe in timeframes:
                tasks.append(self.get_candlestick(symbol, timeframe, name))

        # Execute ALL tasks concurrently (semaphore limits actual parallelism)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None values and exceptions
        candles = [
            result for result in results
            if result is not None and not isinstance(result, Exception)
        ]

        logger.info(f"Batch processing completed: {len(candles)} candles retrieved successfully")

        return candles
