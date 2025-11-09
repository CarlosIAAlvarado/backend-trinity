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
            '4h': '4H',    # Nuevo timeframe
            '12h': '12H',
            '1d': '1D',    # Cambiado de 24h a 1d
            '24h': '1D'    # Mantener compatibilidad con codigo antiguo
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
        Performance calculado como CoinMarketCap: ROLLING PERIOD
        Compara PRECIO ACTUAL EN TIEMPO REAL vs precio hace EXACTAMENTE el periodo especificado

        Metodología Rolling Period (igual a CoinMarketCap):
        - Precio actual: Obtenido desde /api/v5/market/ticker (último precio negociado)
        - 15m: precio actual vs precio hace exactamente 15 minutos (vela [1] close)
        - 30m: precio actual vs precio hace exactamente 30 minutos (vela [1] close)
        - 1h: precio actual vs precio hace exactamente 1 hora (vela [1] close)
        - 12h: precio actual vs precio hace exactamente 12 horas (vela [12] close de 1h)
        - 1d: precio actual vs precio hace exactamente 1 día (vela [24] close de 1h)

        Args:
            symbol: Token symbol (e.g., "BTC", "ETH")
            timeframe: Timeframe (15m, 30m, 1h, 12h, 1d)

        Returns:
            Dict with candle data or None if error
        """
        async with self.semaphore:
            try:
                instrument_id = self._build_instrument_id(symbol)
                okx_timeframe = self._get_okx_timeframe(timeframe)

                # PASO 1: Obtener datos del ticker API (precio actual + datos 1d)
                ticker_params = {'instId': instrument_id}
                ticker_data = await self._make_request('/api/v5/market/ticker', ticker_params)

                ticker_price = None
                ticker_open24h = None
                ticker_high24h = None
                ticker_low24h = None

                if not ticker_data or 'data' not in ticker_data or not ticker_data['data']:
                    logger.warning(f"[TICKER] No ticker data for {symbol}, falling back to candlestick data")
                else:
                    ticker_info = ticker_data['data'][0]
                    ticker_price = float(ticker_info['last'])

                    # OKX Ticker incluye open24h, high24h, low24h para performance de 1d
                    if 'open24h' in ticker_info and ticker_info['open24h']:
                        ticker_open24h = float(ticker_info['open24h'])
                    if 'high24h' in ticker_info and ticker_info['high24h']:
                        ticker_high24h = float(ticker_info['high24h'])
                    if 'low24h' in ticker_info and ticker_info['low24h']:
                        ticker_low24h = float(ticker_info['low24h'])

                    if timeframe == '1d' and ticker_open24h:
                        logger.info(
                            f"[TICKER] {symbol} 1d: last=${ticker_price:.2f}, "
                            f"open24h=${ticker_open24h:.2f}, high24h=${ticker_high24h:.2f}, low24h=${ticker_low24h:.2f}"
                        )

                # PASO 2: Obtener velas históricas para calcular performance
                # ROLLING PERIOD: Necesitamos velas para retroceder exactamente el periodo
                # Para períodos largos (12h, 1d) usamos velas de 1 hora para mayor precisión
                if timeframe in ['12h', '1d']:
                    # Para períodos largos, usar velas de 1 hora
                    okx_timeframe = '1H'
                    if timeframe == '12h':
                        limit = 13  # 13 velas de 1h = 12 horas atrás
                    else:  # 1d
                        limit = 26  # 26 velas para tener margen y calcular interpolación
                else:
                    # Para períodos cortos, usar el timeframe nativo
                    candles_needed = {
                        '15m': 2,   # Vela [1] = hace 15 minutos
                        '30m': 2,   # Vela [1] = hace 30 minutos
                        '1H': 2     # Vela [1] = hace 1 hora
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

                # Log para debug: verificar cuántas velas obtuvimos
                if timeframe == '1d':
                    logger.info(f"[CANDLES] {symbol} {timeframe}: Obtenidas {len(candles)} velas de {okx_timeframe}")

                timestamp_ms = int(current_candle[0])
                open_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

                # Calcular close_timestamp basado en el timeframe SOLICITADO
                timeframe_minutes = {
                    '15m': 15, '30m': 30, '1h': 60, '12h': 720, '1d': 1440
                }
                minutes = timeframe_minutes.get(timeframe, 15)
                close_timestamp = open_timestamp + timedelta(minutes=minutes)

                # PRECIO ACTUAL: Usar ticker price (tiempo real) si está disponible
                current_close = ticker_price if ticker_price else float(current_candle[4])

                # Para 1d: Si tenemos datos del ticker, usar high24h y low24h de OKX
                # Esto garantiza 100% coincidencia con la metodología de OKX
                if timeframe == '1d' and ticker_high24h and ticker_low24h:
                    current_high = ticker_high24h
                    current_low = ticker_low24h
                elif timeframe == '12h':
                    # Para 12h, calcular de las velas
                    all_highs = [float(c[2]) for c in candles]
                    all_lows = [float(c[3]) for c in candles]
                    current_high = max(all_highs)
                    current_low = min(all_lows)
                else:
                    # Para períodos cortos, usar high/low de la vela actual
                    current_high = float(current_candle[2])
                    current_low = float(current_candle[3])

                # ROLLING PERIOD CALCULATION (CoinMarketCap methodology)
                # Obtenemos el precio de hace exactamente el periodo especificado
                price_from_period_ago = None

                if timeframe == '1d' and ticker_open24h:
                    # ÓPTIMO: Para 1d, usar open24h del ticker de OKX
                    # Esto es exactamente lo que OKX usa para calcular su % de 1d
                    # Garantiza 100% coincidencia con OKX y CoinMarketCap
                    price_from_period_ago = ticker_open24h

                elif timeframe == '15m' and len(candles) >= 2:
                    # Vela [1] close = precio hace exactamente 15 minutos
                    price_from_period_ago = float(candles[1][4])

                elif timeframe == '30m' and len(candles) >= 2:
                    # Vela [1] close = precio hace exactamente 30 minutos
                    price_from_period_ago = float(candles[1][4])

                elif timeframe == '1h' and len(candles) >= 2:
                    # Vela [1] close = precio hace exactamente 1 hora
                    price_from_period_ago = float(candles[1][4])

                elif timeframe == '12h' and len(candles) >= 13:
                    # Usamos velas de 1h: vela [12] close = precio hace exactamente 12 horas
                    price_from_period_ago = float(candles[12][4])

                elif timeframe == '1d' and len(candles) >= 25:
                    # Fallback para 1d si no tenemos ticker_open24h
                    price_from_period_ago = float(candles[24][4])
                    logger.warning(f"[1d] {symbol}: Using candle fallback, ticker_open24h not available")

                else:
                    # Fallback general: usar open de la vela actual
                    price_from_period_ago = float(current_candle[1])
                    logger.warning(f"Not enough candles for {symbol} {timeframe}, using fallback")

                # IMPORTANTE: Para timeframes largos, el "open" debe ser el precio del inicio del período
                # Para 1d: open = precio hace 1 día (para mostrar el rango completo del período)
                if timeframe in ['12h', '1d'] and price_from_period_ago:
                    current_open = price_from_period_ago  # Open = inicio del período
                else:
                    current_open = float(current_candle[1])  # Open = open de la vela actual

                # Cálculo de performance: (precio_actual - precio_hace_periodo) / precio_hace_periodo * 100
                performance = ((current_close - price_from_period_ago) / price_from_period_ago) * 100 if price_from_period_ago != 0 else 0.0

                # Log para debug
                if timeframe in ['12h', '1d']:
                    logger.info(
                        f"[ROLLING PERIOD] {symbol} {timeframe}: "
                        f"CLOSE=${current_close:.2f} ({'ticker' if ticker_price else 'candle'}), "
                        f"OPEN(precio hace {timeframe})=${price_from_period_ago:.2f}, "
                        f"HIGH=${current_high:.2f}, LOW=${current_low:.2f}, "
                        f"PERFORMANCE={performance:.2f}%"
                    )

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
            timeframes: List of timeframes ['15m', '30m', '1h', '12h', '1d']
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
