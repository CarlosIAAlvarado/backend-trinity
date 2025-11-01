import logging
import asyncio
from typing import List, Dict, Any
from datetime import datetime
from config.database import db_config
from repositories.candle_repository import CandleRepository
from repositories.token_repository import TokenRepository
from services.okx_service import OKXService
from services.websocket_service import websocket_service
from services.failed_token_service import FailedTokenService
from models.candle_model import CandleModel, CandleResponse

logger = logging.getLogger(__name__)

class CandlestickService:
    """
    Business logic service for candlestick operations
    Orchestrates OKX API calls and database operations
    """

    def __init__(self):
        self.candle_repository = CandleRepository()
        self.token_repository = TokenRepository()
        self.okx_service = OKXService()
        self.failed_token_service = FailedTokenService()
        self.timeframes = ['15m', '30m', '1h', '12h', '24h']

    async def update_all_candlesticks(self) -> Dict[str, Any]:
        """
        Update candlesticks for all Trinity tokens
        INCREMENTAL UPDATE STRATEGY:
        1. Fetch fresh data from OKX for all tokens
        2. UPSERT candlesticks (update if exists, insert if new)
        3. Update failed tokens intelligently (add new, remove now-available)
        This avoids duplicates and preserves data integrity
        """
        try:
            logger.info("=" * 70)
            logger.info("STARTING INCREMENTAL CANDLESTICK UPDATE")
            logger.info("=" * 70)

            # STEP 1: Skip deletion - we'll use UPSERT instead
            logger.info("STEP 1: Using UPSERT strategy - no deletion needed")
            deleted_count = 0

            # STEP 2: Get all Trinity tokens
            logger.info("STEP 2: Fetching tokens from database...")
            tokens = await self.token_repository.find_all()

            if not tokens:
                logger.warning("No tokens found in Trinity collection")
                return {
                    'status': 'error',
                    'message': 'No tokens found',
                    'updated_count': 0,
                    'deleted_count': deleted_count
                }

            logger.info(f"Found {len(tokens)} tokens to process")

            # STEP 3: Fetch fresh data from OKX using CONCURRENT BATCH PROCESSING
            logger.info("STEP 3: Fetching fresh candlestick data from OKX API (concurrent mode)...")
            logger.info(f"Processing {len(tokens)} tokens x {len(self.timeframes)} timeframes = {len(tokens) * len(self.timeframes)} total requests")

            # Use new batch method for massive performance improvement
            all_candles = await self.okx_service.get_candlesticks_batch(
                tokens,
                self.timeframes
            )

            # STEP 3B: Identify successful and failed tokens
            logger.info("STEP 3B: Analyzing successful and failed tokens...")
            successful_symbols = set(candle['symbol'] for candle in all_candles)
            failed_tokens_data = []

            print(f"\n=== DEBUG: Analyzing tokens ===")
            print(f"Total tokens to check: {len(tokens)}")
            print(f"Candlesticks retrieved: {len(all_candles)}")
            print(f"Unique successful symbols: {len(successful_symbols)}")

            for token in tokens:
                symbol = token.get('symbol')
                if symbol and symbol not in successful_symbols:
                    # This token failed - no candlesticks retrieved
                    print(f"FAILED TOKEN: {symbol} ({token.get('name')}) - adding to failed tokens list")
                    failed_tokens_data.append({
                        'symbol': symbol,
                        'name': token.get('name', symbol),
                        'market_cap': token.get('market_cap'),
                        'rank': token.get('rank'),
                        'attempted_pair': f"{symbol}-USDT",
                        'reason': "No data retrieved from OKX (pair may not exist)",
                        'timeframes_failed': self.timeframes,
                        'total_attempts': len(self.timeframes)
                    })

            logger.info(f"Successful tokens: {len(successful_symbols)}")
            logger.info(f"Failed tokens: {len(failed_tokens_data)}")
            print(f"Failed tokens count: {len(failed_tokens_data)}")
            if failed_tokens_data:
                print(f"Failed token symbols: {[t['symbol'] for t in failed_tokens_data]}")

            success_count = len(all_candles)
            expected_count = len(tokens) * len(self.timeframes)
            error_count = expected_count - success_count

            # STEP 4A: UPSERT all candles (update existing, insert new)
            logger.info("STEP 4A: Upserting candlesticks into database...")
            if all_candles:
                upserted_count = await self.candle_repository.upsert_many(all_candles)
                logger.info(f"Successfully upserted {upserted_count} candlesticks")
            else:
                upserted_count = 0
                logger.warning("No candlesticks to upsert")

            # STEP 4B: Update failed tokens intelligently
            logger.info("STEP 4B: Updating failed tokens table intelligently...")
            failed_result = await self.failed_token_service.update_failed_tokens(
                successful_symbols=list(successful_symbols),
                failed_tokens_data=failed_tokens_data
            )
            logger.info(
                f"Failed tokens updated: {failed_result['removed_count']} removed (now in OKX), "
                f"{failed_result['upserted_count']} added/updated (not in OKX)"
            )

            logger.info("=" * 70)
            logger.info(
                f"INCREMENTAL UPDATE COMPLETED: "
                f"{upserted_count} candles upserted, "
                f"{len(successful_symbols)} tokens successful, "
                f"{len(failed_tokens_data)} tokens failed"
            )
            logger.info("=" * 70)

            result = {
                'status': 'success',
                'message': f'Incremental update completed: {upserted_count} candlesticks, {len(failed_tokens_data)} failed tokens',
                'updated_count': upserted_count,
                'deleted_count': deleted_count,
                'error_count': error_count,
                'total_tokens': len(tokens),
                'successful_tokens': len(successful_symbols),
                'failed_tokens': len(failed_tokens_data)
            }

            # Emit WebSocket event to notify all connected clients
            await websocket_service.emit_candlesticks_updated({
                'updated_count': upserted_count,
                'deleted_count': deleted_count,
                'timestamp': datetime.now().isoformat()
            })

            return result

        except Exception as e:
            logger.error(f"CRITICAL ERROR during candlestick update: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'updated_count': 0
            }

    async def get_all_candlesticks(
        self,
        symbol: str = None,
        timeframe: str = None,
        limit: int = 1000
    ) -> CandleResponse:
        """
        Get candlesticks from database with optional filters

        Args:
            symbol: Filter by symbol (optional)
            timeframe: Filter by timeframe (optional)
            limit: Maximum number of results
        """
        try:
            if symbol and timeframe:
                # Get specific symbol and timeframe
                candles = await self.candle_repository.find_by_symbol_and_timeframe(
                    symbol,
                    timeframe,
                    limit
                )
                message = f"Candles for {symbol} [{timeframe}]"

            elif symbol:
                # Get all timeframes for a symbol
                candles = await self.candle_repository.find_by_symbol(symbol, limit)
                message = f"Candles for {symbol}"

            else:
                # Get all candles ordered by 24h performance
                candles = await self.candle_repository.find_all_ordered_by_performance(limit)
                message = "All candlesticks ordered by 24h performance"

            # Convert to Pydantic models
            candle_models = [CandleModel(**candle) for candle in candles]

            return CandleResponse(
                status="success",
                message=message,
                count=len(candle_models),
                data=candle_models
            )

        except Exception as e:
            logger.error(f"Error getting candlesticks: {e}")
            return CandleResponse(
                status="error",
                message=str(e),
                count=0,
                data=[]
            )

    async def get_candlestick_stats(self) -> Dict[str, Any]:
        """Get statistics about candlesticks"""
        try:
            total_candles = await self.candle_repository.count_candles()

            return {
                'status': 'success',
                'total_candles': total_candles,
                'timeframes': self.timeframes,
                'last_check': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting candlestick stats: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def update_specific_timeframe(self, timeframe: str) -> Dict[str, Any]:
        """
        Update candlesticks for a SPECIFIC timeframe only
        This is called by the scheduler at the appropriate interval
        Example: update_specific_timeframe('15m') updates ALL tokens but only the 15m timeframe
        """
        try:
            logger.info("=" * 70)
            logger.info(f"TIMEFRAME UPDATE: {timeframe}")
            logger.info("=" * 70)

            if timeframe not in self.timeframes:
                return {
                    'status': 'error',
                    'message': f'Invalid timeframe: {timeframe}',
                    'updated_count': 0
                }

            tokens = await self.token_repository.find_all()

            if not tokens:
                logger.warning("No tokens found in database")
                return {
                    'status': 'error',
                    'message': 'No tokens found',
                    'updated_count': 0
                }

            logger.info(f"Updating {len(tokens)} tokens for timeframe {timeframe}...")

            all_candles = await self.okx_service.get_candlesticks_batch(
                tokens,
                [timeframe]
            )

            if all_candles:
                updated_count = await self.candle_repository.upsert_many(all_candles)
                logger.info(f"Successfully updated {updated_count} candles for timeframe {timeframe}")

                # Emit update notification to frontend
                await websocket_service.emit_candlesticks_updated({
                    'updated_count': updated_count['inserted'] + updated_count['modified'],
                    'deleted_count': 0,
                    'timeframe': timeframe,
                    'timestamp': datetime.now().isoformat()
                })

                logger.info("=" * 70)
                logger.info(f"TIMEFRAME UPDATE COMPLETED: {timeframe} ({updated_count} candles)")
                logger.info("=" * 70)

                return {
                    'status': 'success',
                    'message': f'Updated {updated_count} candles for timeframe {timeframe}',
                    'updated_count': updated_count,
                    'timeframe': timeframe,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.warning(f"No candles retrieved for timeframe {timeframe}")
                return {
                    'status': 'warning',
                    'message': f'No candles retrieved for timeframe {timeframe}',
                    'updated_count': 0
                }

        except Exception as e:
            logger.error(f"Error updating timeframe {timeframe}: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'updated_count': 0
            }


    async def _refresh_single_candle(self, symbol: str, name: str, timeframe: str) -> bool:
        """
        Actualiza una sola vela consultando OKX
        Si la vela cambio (nuevo openTimestamp), actualiza TODO incluyendo open y timestamps
        Si es la misma vela, solo actualiza close, high, low, performance
        """
        try:
            # Obtener vela actual de OKX
            candle_okx = await self.okx_service.get_candlestick(
                symbol=symbol,
                timeframe=timeframe,
                name=name
            )

            if not candle_okx:
                return False

            # Obtener vela actual de BD para comparar timestamps
            collection = db_config.get_collection('trinityCandles')
            candle_bd = await collection.find_one({
                'symbol': symbol,
                'timeframe': timeframe
            })

            if not candle_bd:
                # No existe en BD, hacer upsert completo
                await self.candle_repository.upsert_candle(candle_okx)
                return True

            # Comparar openTimestamp para detectar si es nueva vela
            okx_open_ts = candle_okx['openTimestamp']
            bd_open_ts = candle_bd.get('openTimestamp')

            if okx_open_ts != bd_open_ts:
                # NUEVA VELA: La vela cerró y OKX tiene una nueva
                # Actualizar TODO incluyendo open, openTimestamp, closeTimestamp
                await self.candle_repository.upsert_candle(candle_okx)
                return True
            else:
                # MISMA VELA: Solo actualizar precios intermedios
                update_data = {
                    'close': candle_okx['close'],
                    'high': candle_okx['high'],
                    'low': candle_okx['low'],
                    'performance': candle_okx['performance']
                }

                result = await self.candle_repository.update_price_snapshot(
                    symbol=symbol,
                    timeframe=timeframe,
                    update_data=update_data
                )

                return result

        except Exception as e:
            logger.error(f"Error refreshing {symbol} {timeframe}: {e}")
            return False

    async def refresh_tier1_candles(self) -> Dict[str, Any]:
        """
        TIER 1: TOP 10 tokens - Updated every 5 seconds
        BTC, ETH, XRP, BNB, SOL, USDT, USDC, DOGE, ADA, TRX
        50 candles (10 tokens x 5 timeframes) - Completes in ~3 seconds
        """
        tier1_symbols = ['BTC', 'ETH', 'XRP', 'BNB', 'SOL', 'USDT', 'USDC', 'DOGE', 'ADA', 'TRX']
        return await self._refresh_tokens_by_symbols(tier1_symbols, tier='TIER1')

    async def refresh_tier2_candles(self) -> Dict[str, Any]:
        """
        TIER 2: Important tokens (Market Cap > $5B) - Updated every 30 seconds
        Approximately 20 tokens - 100 candles - Completes in ~8 seconds
        """
        try:
            tokens = await self.token_repository.find_by_market_cap(
                min_market_cap=5_000_000_000,
                limit=50
            )

            tier1_symbols = ['BTC', 'ETH', 'XRP', 'BNB', 'SOL', 'USDT', 'USDC', 'DOGE', 'ADA', 'TRX']
            tier2_symbols = [t['symbol'] for t in tokens if t['symbol'] not in tier1_symbols]

            return await self._refresh_tokens_by_symbols(tier2_symbols, tier='TIER2')

        except Exception as e:
            logger.error(f"[TIER2] Error: {e}")
            return {'status': 'error', 'tier': 'TIER2', 'message': str(e), 'updated_count': 0}

    async def refresh_tier3_candles(self) -> Dict[str, Any]:
        """
        TIER 3: Rest of tokens - Updated every 60 seconds
        Approximately 61 tokens - 305 candles - Completes in ~20 seconds
        """
        try:
            all_tokens = await self.token_repository.find_all()

            tier1_symbols = ['BTC', 'ETH', 'XRP', 'BNB', 'SOL', 'USDT', 'USDC', 'DOGE', 'ADA', 'TRX']

            tokens_tier2 = await self.token_repository.find_by_market_cap(
                min_market_cap=5_000_000_000,
                limit=50
            )
            tier2_symbols = [t['symbol'] for t in tokens_tier2 if t['symbol'] not in tier1_symbols]

            excluded = tier1_symbols + tier2_symbols
            tier3_symbols = [t['symbol'] for t in all_tokens if t['symbol'] not in excluded]

            return await self._refresh_tokens_by_symbols(tier3_symbols, tier='TIER3')

        except Exception as e:
            logger.error(f"[TIER3] Error: {e}")
            return {'status': 'error', 'tier': 'TIER3', 'message': str(e), 'updated_count': 0}

    async def _refresh_tokens_by_symbols(self, symbols: List[str], tier: str) -> Dict[str, Any]:
        """
        Helper method to refresh specific tokens by their symbols
        Used by tier-based update methods
        """
        try:
            logger.info(f"[{tier}] Updating {len(symbols)} tokens...")

            updated_count = 0
            update_tasks = []

            for timeframe in self.timeframes:
                for symbol in symbols:
                    token = await self.token_repository.find_by_symbol(symbol)
                    if not token:
                        continue

                    name = token.get('name', symbol)
                    task = self._refresh_single_candle(symbol, name, timeframe)
                    update_tasks.append(task)

            results = await asyncio.gather(*update_tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, bool) and result:
                    updated_count += 1

            logger.info(f"[{tier}] Updated {updated_count}/{len(update_tasks)} candles")

            return {
                'status': 'success',
                'tier': tier,
                'tokens': len(symbols),
                'updated_count': updated_count,
                'message': f'{tier}: Updated {updated_count} candles for {len(symbols)} tokens'
            }

        except Exception as e:
            logger.error(f"[{tier}] Error: {e}")
            return {
                'status': 'error',
                'tier': tier,
                'message': str(e),
                'updated_count': 0
            }

    async def update_single_token(self, symbol: str) -> Dict[str, Any]:
        """
        Update candlesticks for a single token
        Useful for manual refresh
        """
        try:
            logger.info(f"Updating candlesticks for {symbol}...")

            candles = await self.okx_service.get_multiple_candlesticks(
                symbol,
                self.timeframes
            )

            if not candles:
                return {
                    'status': 'error',
                    'message': f'No candles retrieved for {symbol}',
                    'updated_count': 0
                }

            result = await self.candle_repository.upsert_many(candles)

            return {
                'status': 'success',
                'message': f'Updated {len(candles)} candles for {symbol}',
                'updated_count': len(candles),
                'data': candles
            }

        except Exception as e:
            logger.error(f"Error updating single token {symbol}: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'updated_count': 0
            }
