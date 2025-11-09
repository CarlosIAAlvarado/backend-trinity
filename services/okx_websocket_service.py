import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import websockets
from repositories.candle_repository import CandleRepository
from repositories.token_repository import TokenRepository
from services.websocket_service import websocket_service

logger = logging.getLogger(__name__)

class OKXWebSocketService:
    """
    Service for connecting to OKX WebSocket and receiving real-time price updates
    Supports both candle updates and ticker updates for live price feeds
    """

    def __init__(self):
        self.ws_url = 'wss://ws.okx.com:8443/ws/v5/public'
        self.ws = None
        self.is_running = False
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10

        self.candle_repository = None
        self.token_repository = None
        self.websocket_service = None

        self.tokens = []
        self.timeframes = ['15m', '30m', '1h', '12h', '1d']

        self.timeframe_map = {
            '15m': 'candle15m',
            '30m': 'candle30m',
            '1h': 'candle1H',
            '12h': 'candle12H',
            '1d': 'candle1D'
        }

        self.subscriptions_count = 0
        self.messages_received = 0
        self.candle_updates_processed = 0
        self.ticker_updates_processed = 0
        self.errors_count = 0
        self.last_message_time = None
        self.start_time = None

        self.ticker_batch_interval = 3
        self.ticker_buffer = {}
        self.batch_emit_task = None

        self.snapshot_interval = 60
        self.snapshot_buffer = {}
        self.snapshot_task = None
        self.snapshots_saved = 0

    def inject_dependencies(
        self,
        candle_repository: CandleRepository,
        token_repository: TokenRepository,
        websocket_service
    ):
        """Inject dependencies (used in main.py)"""
        self.candle_repository = candle_repository
        self.token_repository = token_repository
        self.websocket_service = websocket_service
        logger.info("OKX WebSocket dependencies injected")

    async def start(self):
        """Start the WebSocket connection"""
        if self.is_running:
            logger.warning("OKX WebSocket is already running")
            return

        logger.info("=" * 70)
        logger.info("STARTING OKX WEBSOCKET SERVICE (REAL-TIME)")
        logger.info("=" * 70)
        logger.info(f"WebSocket URL: {self.ws_url}")
        logger.info(f"Timeframes: {self.timeframes}")
        logger.info(f"Batch Update Interval: {self.ticker_batch_interval} seconds")
        logger.info(f"Snapshot Save Interval: {self.snapshot_interval} seconds")
        logger.info("=" * 70)

        self.is_running = True
        self.start_time = datetime.now()

        await self._load_tokens()
        asyncio.create_task(self._connect_loop())
        asyncio.create_task(self._batch_emit_loop())
        asyncio.create_task(self._snapshot_save_loop())

    async def stop(self):
        """Stop the WebSocket connection"""
        logger.info("Stopping OKX WebSocket service...")
        self.is_running = False

        if self.ws:
            await self.ws.close()

        logger.info("OKX WebSocket service stopped")

    async def _load_tokens(self):
        """Load tokens from database"""
        try:
            self.tokens = await self.token_repository.find_all()
            logger.info(f"Loaded {len(self.tokens)} tokens from database")
        except Exception as e:
            logger.error(f"Error loading tokens: {e}")
            self.tokens = []

    async def _batch_emit_loop(self):
        """
        Loop que emite batch de tickers cada X segundos
        SINCRONIZACIÓN: Todos los tickers se envían juntos
        """
        while self.is_running:
            try:
                await asyncio.sleep(self.ticker_batch_interval)

                if not self.ticker_buffer:
                    continue

                tickers_to_emit = list(self.ticker_buffer.values())

                if tickers_to_emit:
                    await self.websocket_service.emit_realtime_ticker_batch(tickers_to_emit)
                    logger.info(f"[BATCH EMIT] Sent {len(tickers_to_emit)} tickers simultaneously")

                    for ticker in tickers_to_emit:
                        self.snapshot_buffer[ticker['symbol']] = ticker

            except Exception as e:
                logger.error(f"Error in batch emit loop: {e}")

    async def _snapshot_save_loop(self):
        """
        DESHABILITADO - Ya no usamos snapshots de tickers
        Ahora cada timeframe se actualiza consultando OKX directamente
        """
        return
        # CODIGO VIEJO DESHABILITADO
        while self.is_running:
            try:
                await asyncio.sleep(self.snapshot_interval)

                if not self.snapshot_buffer:
                    continue

                snapshots_to_save = list(self.snapshot_buffer.values())
                self.snapshot_buffer.clear()

                if snapshots_to_save:
                    saved_count = await self._save_snapshots_to_db(snapshots_to_save)
                    self.snapshots_saved += saved_count
                    logger.info(f"[SNAPSHOT SAVE] Saved {saved_count} price snapshots to database (Total: {self.snapshots_saved})")

            except Exception as e:
                logger.error(f"Error in snapshot save loop: {e}")

    async def _connect_loop(self):
        """Main connection loop with automatic reconnection"""
        while self.is_running:
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"Connection error: {e}")
                self.is_connected = False

                if self.reconnect_attempts < self.max_reconnect_attempts:
                    self.reconnect_attempts += 1
                    delay = min(5 * self.reconnect_attempts, 30)
                    logger.info(f"Reconnecting in {delay}s (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})...")
                    await asyncio.sleep(delay)
                else:
                    logger.error("Max reconnection attempts reached. Stopping service.")
                    self.is_running = False
                    break

    async def _connect(self):
        """Connect to OKX WebSocket"""
        try:
            logger.info(f"Connecting to OKX WebSocket: {self.ws_url}")

            async with websockets.connect(self.ws_url) as websocket:
                self.ws = websocket
                self.is_connected = True
                self.reconnect_attempts = 0

                logger.info("Connected to OKX WebSocket")

                await self._subscribe_to_channels()

                logger.info("Listening for real-time updates...")
                await self._listen_messages()

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.is_connected = False
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            self.is_connected = False
            raise

    async def _subscribe_to_channels(self):
        """Subscribe to candle and ticker channels"""
        try:
            if not self.tokens:
                logger.warning("No tokens to subscribe")
                return

            candle_channels = []
            ticker_channels = []

            for token in self.tokens:
                symbol = token.get('symbol')
                if not symbol:
                    continue

                inst_id = f"{symbol.upper()}-USDT"

                for timeframe in self.timeframes:
                    okx_channel = self.timeframe_map.get(timeframe)
                    if okx_channel:
                        candle_channels.append({
                            "channel": okx_channel,
                            "instId": inst_id
                        })

                ticker_channels.append({
                    "channel": "tickers",
                    "instId": inst_id
                })

            all_channels = candle_channels + ticker_channels
            total_channels = len(all_channels)

            logger.info(f"Subscribing to {len(self.tokens)} tokens:")
            logger.info(f"  - {len(candle_channels)} candle channels")
            logger.info(f"  - {len(ticker_channels)} ticker channels")
            logger.info(f"  - Total: {total_channels} channels")

            batch_size = 100
            for i in range(0, len(all_channels), batch_size):
                batch = all_channels[i:i + batch_size]

                subscribe_message = {
                    "op": "subscribe",
                    "args": batch
                }

                await self.ws.send(json.dumps(subscribe_message))
                logger.info(f"Sent subscription batch {i // batch_size + 1} ({len(batch)} channels)")

                await asyncio.sleep(0.5)

            self.subscriptions_count = total_channels
            logger.info(f"Subscribed to {self.subscriptions_count} channels")

        except Exception as e:
            logger.error(f"Error subscribing to channels: {e}")
            raise

    async def _listen_messages(self):
        """Listen for incoming WebSocket messages"""
        try:
            async for message in self.ws:
                self.messages_received += 1
                self.last_message_time = datetime.now()

                try:
                    data = json.loads(message)

                    if 'event' in data:
                        await self._handle_event_message(data)
                    elif 'arg' in data and 'data' in data:
                        await self._handle_data_message(data)

                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON message: {message[:100]}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.errors_count += 1

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            raise
        except Exception as e:
            logger.error(f"Error listening to messages: {e}")
            raise

    async def _handle_event_message(self, data: Dict[str, Any]):
        """Handle event messages (subscribe, error, etc.)"""
        event = data.get('event')

        if event == 'subscribe':
            logger.info(f"Subscription confirmed for channel: {data.get('arg', {}).get('channel')}")
        elif event == 'error':
            error_msg = data.get('msg', 'Unknown error')
            logger.error(f"OKX WebSocket error: {error_msg}")
            self.errors_count += 1

    async def _handle_data_message(self, data: Dict[str, Any]):
        """Handle data messages (candles, tickers)"""
        try:
            arg = data.get('arg', {})
            channel = arg.get('channel', '')
            inst_id = arg.get('instId', '')

            if not inst_id:
                return

            symbol = inst_id.split('-')[0]

            if channel.startswith('candle'):
                await self._process_candle_message(symbol, channel, data)
            elif channel == 'tickers':
                await self._process_ticker_message(symbol, data)

        except Exception as e:
            logger.error(f"Error handling data message: {e}")

    async def _process_candle_message(self, symbol: str, channel: str, data: Dict[str, Any]):
        """Process candle update messages"""
        try:
            candle_array = data['data'][0]

            timeframe = self._get_timeframe_from_channel(channel)
            if not timeframe:
                return

            timestamp_ms = int(candle_array[0])
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

            open_price = float(candle_array[1])
            high_price = float(candle_array[2])
            low_price = float(candle_array[3])
            close_price = float(candle_array[4])
            confirmed = candle_array[8] == '1'

            performance = ((close_price - open_price) / open_price) * 100 if open_price != 0 else 0.0

            token_info = next((t for t in self.tokens if t.get('symbol') == symbol), None)
            token_name = token_info.get('name', symbol) if token_info else symbol

            candle_obj = {
                'symbol': symbol.upper(),
                'name': token_name,
                'timeframe': timeframe,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'performance': round(performance, 2),
                'timestamp': timestamp,
                'confirmed': confirmed
            }

            await self.candle_repository.upsert_one(candle_obj)

            if confirmed:
                await self.websocket_service.emit_realtime_candle_update(candle_obj)

            self.candle_updates_processed += 1

            if self.candle_updates_processed % 100 == 0:
                logger.info(f"Processed {self.candle_updates_processed} candle updates (Total messages: {self.messages_received})")

        except Exception as e:
            logger.error(f"Error processing candle message: {e}")

    async def _process_ticker_message(self, symbol: str, data: Dict[str, Any]):
        """
        Process ticker update messages (real-time price)
        BUFFERED: Acumula tickers en buffer, se emiten todos juntos cada 3s
        """
        try:
            ticker_data = data['data'][0]

            current_price = float(ticker_data['last'])
            high_24h = float(ticker_data['high24h'])
            low_24h = float(ticker_data['low24h'])
            open_24h = float(ticker_data['open24h'])
            volume_24h = float(ticker_data.get('volCcy24h', 0))

            performance_24h = ((current_price - open_24h) / open_24h) * 100 if open_24h != 0 else 0.0

            token_info = next((t for t in self.tokens if t.get('symbol') == symbol), None)
            token_name = token_info.get('name', symbol) if token_info else symbol

            ticker_obj = {
                'symbol': symbol.upper(),
                'name': token_name,
                'current_price': current_price,
                'high_24h': high_24h,
                'low_24h': low_24h,
                'open_24h': open_24h,
                'volume_24h': volume_24h,
                'performance_24h': round(performance_24h, 2),
                'timestamp': datetime.now()
            }

            self.ticker_buffer[symbol] = ticker_obj

            self.ticker_updates_processed += 1

        except Exception as e:
            logger.error(f"Error processing ticker message: {e}")

    async def _save_snapshots_to_db(self, snapshots: List[Dict[str, Any]]) -> int:
        """
        Guarda snapshots de precios en MongoDB cada 60 segundos
        Actualiza close, high, low y calcula performance CORRECTAMENTE
        usando el 'open' de cada vela individual (no open_24h)
        """
        try:
            saved_count = 0

            # PASO 1: Obtener todas las velas actuales de BD (1 query por timeframe)
            # Esto nos da el 'open' correcto de cada vela
            candles_in_db = {}

            for timeframe in self.timeframes:
                candles = await self.candle_repository.find_by_timeframe(timeframe)
                for candle in candles:
                    key = f"{candle['symbol']}-{timeframe}"
                    candles_in_db[key] = {
                        'open': candle.get('open', 0),
                        'high': candle.get('high', 0),
                        'low': candle.get('low', float('inf'))
                    }

            logger.info(f"[SNAPSHOT] Loaded {len(candles_in_db)} candles from DB for price update")

            # PASO 2: Actualizar cada snapshot con performance CORRECTO
            for snapshot in snapshots:
                symbol = snapshot['symbol']
                current_price = snapshot['current_price']

                for timeframe in self.timeframes:
                    key = f"{symbol}-{timeframe}"

                    # Obtener open de BD para este timeframe específico
                    candle_data = candles_in_db.get(key)
                    if not candle_data:
                        continue  # Skip si la vela no existe en BD

                    open_price = candle_data['open']
                    if open_price == 0:
                        continue  # Skip si open es 0 (evitar división por cero)

                    # Calcular performance CORRECTO: usa el open de ESTA vela específica
                    performance = ((current_price - open_price) / open_price) * 100

                    update_data = {
                        'close': current_price,
                        'performance': round(performance, 2)
                    }

                    # Actualizar high si el precio actual es mayor
                    current_high = candle_data['high']
                    if current_price > current_high:
                        update_data['high'] = current_price

                    # Actualizar low si el precio actual es menor
                    current_low = candle_data['low']
                    if current_price < current_low:
                        update_data['low'] = current_price

                    result = await self.candle_repository.update_price_snapshot(
                        symbol=symbol,
                        timeframe=timeframe,
                        update_data=update_data
                    )

                    if result:
                        saved_count += 1

            logger.info(f"[SNAPSHOT] Updated {saved_count} candles with correct performance calculation")
            return saved_count

        except Exception as e:
            logger.error(f"Error saving snapshots to database: {e}")
            return 0

    def _get_timeframe_from_channel(self, channel: str) -> Optional[str]:
        """Convert OKX channel name to our timeframe format"""
        for timeframe, okx_channel in self.timeframe_map.items():
            if okx_channel == channel:
                return timeframe
        return None

    def get_status(self) -> Dict[str, Any]:
        """Get current service status"""
        uptime_seconds = 0
        if self.start_time:
            uptime_seconds = int((datetime.now() - self.start_time).total_seconds())

        return {
            'is_running': self.is_running,
            'is_connected': self.is_connected,
            'subscriptions_count': self.subscriptions_count,
            'messages_received': self.messages_received,
            'candle_updates_processed': self.candle_updates_processed,
            'ticker_updates_processed': self.ticker_updates_processed,
            'snapshots_saved': self.snapshots_saved,
            'errors_count': self.errors_count,
            'last_message_time': self.last_message_time.isoformat() if self.last_message_time else None,
            'uptime_seconds': uptime_seconds
        }

okx_websocket_service = OKXWebSocketService()
