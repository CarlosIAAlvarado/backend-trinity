# ==========================
# WebSocket Service
# ==========================
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import socketio
from config.database import db_config

logger = logging.getLogger(__name__)

class WebSocketService:
    """
    Service for WebSocket communication and global configuration
    Manages real-time updates and multi-user synchronization
    """

    def __init__(self):
        self.sio = socketio.AsyncServer(
            async_mode='asgi',
            cors_allowed_origins='*',
            logger=False,
            engineio_logger=False
        )
        self.config_collection_name = 'global_config'
        self._setup_events()

    def _setup_events(self):
        """Setup WebSocket event handlers"""

        @self.sio.event
        async def connect(sid, environ):
            """Client connection handler"""
            logger.info(f'[WEBSOCKET] Client connected: {sid}')
            config = await self.get_global_config()
            await self.sio.emit('config_update', config, room=sid)

        @self.sio.event
        async def disconnect(sid):
            """Client disconnection handler"""
            logger.info(f'[WEBSOCKET] Client disconnected: {sid}')

        @self.sio.event
        async def request_config(sid):
            """Client requests current configuration"""
            config = await self.get_global_config()
            await self.sio.emit('config_update', config, room=sid)

    async def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration from database"""
        try:
            collection = db_config.get_collection(self.config_collection_name)
            config = await collection.find_one({'type': 'app_config'})

            if config is not None:
                return {
                    'market_cap_filter': config.get('market_cap_filter', 800000000),
                    'filter_condition': config.get('filter_condition', 'greater'),
                    'update_interval_hours': config.get('update_interval_hours', 24),
                    'api_error': config.get('api_error', None)
                }
            else:
                # Create default configuration
                default_config = {
                    'type': 'app_config',
                    'market_cap_filter': 800000000,
                    'filter_condition': 'greater',
                    'update_interval_hours': 24,
                    'last_updated': datetime.now()
                }
                await collection.insert_one(default_config)
                return {
                    'market_cap_filter': 800000000,
                    'filter_condition': 'greater',
                    'update_interval_hours': 24,
                    'api_error': None
                }
        except Exception as e:
            logger.error(f"Error getting global config: {e}")
            return {
                'market_cap_filter': 800000000,
                'filter_condition': 'greater',
                'update_interval_hours': 24,
                'api_error': None
            }

    async def update_global_config(self, field: str, value: Any) -> Dict[str, Any]:
        """Update global configuration in database"""
        try:
            collection = db_config.get_collection(self.config_collection_name)

            await collection.update_one(
                {'type': 'app_config'},
                {
                    '$set': {
                        field: value,
                        'last_updated': datetime.now()
                    }
                },
                upsert=True
            )

            # Get updated config
            updated_config = await self.get_global_config()

            # Broadcast to all connected clients
            await self.sio.emit('config_update', updated_config)

            logger.info(f"[CONFIG UPDATE] {field} changed to: {value}")
            logger.info(f"[WEBSOCKET] Broadcasted update to all clients")

            return updated_config

        except Exception as e:
            logger.error(f"Error updating global config: {e}")
            raise

    async def update_market_cap_filter(self, new_market_cap: int, condition: str = 'greater') -> Dict[str, Any]:
        """Update market cap filter configuration with condition"""
        try:
            collection = db_config.get_collection(self.config_collection_name)

            await collection.update_one(
                {'type': 'app_config'},
                {
                    '$set': {
                        'market_cap_filter': new_market_cap,
                        'filter_condition': condition,
                        'last_updated': datetime.now()
                    }
                },
                upsert=True
            )

            # Get updated config
            updated_config = await self.get_global_config()

            # Broadcast to all connected clients
            await self.sio.emit('config_update', updated_config)

            logger.info(f"[CONFIG UPDATE] Market cap filter changed to: ${new_market_cap:,} ({condition})")
            logger.info(f"[WEBSOCKET] Broadcasted update to all clients")

            return updated_config

        except Exception as e:
            logger.error(f"Error updating market cap filter: {e}")
            raise

    async def update_interval(self, new_interval: int) -> Dict[str, Any]:
        """Update update interval configuration"""
        return await self.update_global_config('update_interval_hours', new_interval)

    async def update_api_error(self, error_message: Optional[str]) -> Dict[str, Any]:
        """Update API error status in global configuration"""
        try:
            collection = db_config.get_collection(self.config_collection_name)

            await collection.update_one(
                {'type': 'app_config'},
                {
                    '$set': {
                        'api_error': error_message,
                        'last_updated': datetime.now()
                    }
                },
                upsert=True
            )

            # Get updated config
            updated_config = await self.get_global_config()

            # Broadcast to all connected clients
            await self.sio.emit('config_update', updated_config)

            if error_message:
                logger.warning(f"[API ERROR] {error_message}")
            else:
                logger.info("[API STATUS] API working normally")

            return updated_config

        except Exception as e:
            logger.error(f"Error updating API error status: {e}")
            raise

    async def emit_candlesticks_updated(self, data: Dict[str, Any]):
        """
        Emit event when candlesticks are updated
        Notifies all connected clients to refresh their data
        """
        try:
            await self.sio.emit('candlesticks_updated', data)
            logger.info(f"[WEBSOCKET] Broadcasted candlesticks_updated event: {data.get('updated_count')} candles")
        except Exception as e:
            logger.error(f"Error emitting candlesticks_updated event: {e}")

    async def emit_realtime_candle_update(self, candle_data: Dict[str, Any]):
        """
        Emit event when a single candlestick is updated in real-time.
        Used by OKX WebSocket service to broadcast individual candle updates.

        Args:
            candle_data: Dictionary with candle information
                {
                    'symbol': str,
                    'name': str,
                    'timeframe': str,
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'performance': float,
                    'timestamp': datetime,
                    'confirmed': bool
                }
        """
        try:
            # Convertir datetime a string para JSON serialization
            candle_payload = {
                **candle_data,
                'timestamp': candle_data['timestamp'].isoformat() if isinstance(candle_data['timestamp'], datetime) else candle_data['timestamp']
            }

            await self.sio.emit('realtime_candle_update', candle_payload)

            # Log solo cada 50 updates para no saturar logs
            if not hasattr(self, '_update_counter'):
                self._update_counter = 0

            self._update_counter += 1
            if self._update_counter % 50 == 0:
                logger.info(f"[WEBSOCKET] Broadcasted {self._update_counter} realtime candle updates")

        except Exception as e:
            logger.error(f"Error emitting realtime_candle_update event: {e}")

    async def emit_realtime_ticker(self, ticker_data: Dict[str, Any]):
        """
        Emit event when a ticker price is updated in real-time.
        DEPRECATED: Use emit_realtime_ticker_batch() instead for synchronized updates
        """
        try:
            ticker_payload = {
                **ticker_data,
                'timestamp': ticker_data['timestamp'].isoformat() if isinstance(ticker_data['timestamp'], datetime) else ticker_data['timestamp']
            }

            await self.sio.emit('realtime_ticker_update', ticker_payload)

            if not hasattr(self, '_ticker_counter'):
                self._ticker_counter = 0

            self._ticker_counter += 1
            if self._ticker_counter % 100 == 0:
                logger.info(f"[WEBSOCKET] Broadcasted {self._ticker_counter} realtime ticker updates")

        except Exception as e:
            logger.error(f"Error emitting realtime_ticker_update event: {e}")

    async def emit_realtime_ticker_batch(self, tickers: List[Dict[str, Any]]):
        """
        Emit batch of ticker updates simultaneously.
        ALL tickers are sent in one message for synchronized table update.

        Args:
            tickers: List of ticker dictionaries
        """
        try:
            batch_payload = []
            for ticker in tickers:
                batch_payload.append({
                    **ticker,
                    'timestamp': ticker['timestamp'].isoformat() if isinstance(ticker['timestamp'], datetime) else ticker['timestamp']
                })

            await self.sio.emit('realtime_ticker_batch', batch_payload)

            logger.info(f"[WEBSOCKET BATCH] Broadcasted {len(batch_payload)} tickers simultaneously")

        except Exception as e:
            logger.error(f"Error emitting realtime_ticker_batch event: {e}")

    async def emit_market_analysis_updated(self, analysis_data: Dict[str, Any]):
        """
        Emit event when market analysis is updated.
        Notifies all connected clients to refresh market analysis display.

        Args:
            analysis_data: Dictionary with market analysis information
        """
        try:
            # Convertir timestamps a string
            payload = {
                **analysis_data,
                'timestamp': analysis_data['timestamp'].isoformat() if isinstance(analysis_data['timestamp'], datetime) else analysis_data['timestamp']
            }

            await self.sio.emit('market_analysis_updated', payload)

            logger.info(f"[WEBSOCKET] Broadcasted market_analysis_updated: {analysis_data.get('market_status')} [{analysis_data.get('timeframe')}]")

        except Exception as e:
            logger.error(f"Error emitting market_analysis_updated event: {e}")

    async def emit_new_notification(self, notification_data: Dict[str, Any]):
        """
        Emit event when a new notification is created.
        Notifies all connected clients to display the notification.

        Args:
            notification_data: Dictionary with notification information
                {
                    'type': str,
                    'title': str,
                    'message': str,
                    'symbol': str (optional),
                    'data': dict (optional),
                    'timestamp': datetime
                }
        """
        try:
            # Convert timestamps to string for JSON serialization
            payload = {
                **notification_data,
                'timestamp': notification_data['timestamp'].isoformat() if isinstance(notification_data['timestamp'], datetime) else notification_data['timestamp']
            }

            await self.sio.emit('new_notification', payload)

            logger.info(f"[WEBSOCKET] Broadcasted new_notification: {notification_data.get('type')} - {notification_data.get('title')}")

        except Exception as e:
            logger.error(f"Error emitting new_notification event: {e}")

    def get_asgi_app(self):
        """Get the ASGI application for integration with FastAPI"""
        return self.sio

# Singleton instance
websocket_service = WebSocketService()
