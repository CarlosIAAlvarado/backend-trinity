# ==========================
# Scheduler Service
# ==========================
import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from typing import Optional
import asyncio
from services.event_bus import event_bus

logger = logging.getLogger(__name__)

class SchedulerService:
    """
    Service for managing scheduled tasks
    Follows Single Responsibility Principle
    """

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.last_update: Optional[datetime] = None
        self.update_interval_hours = 24
        self.is_updating = False
        self.token_service = None  # Will be injected
        self.candlestick_service = None  # Will be injected
        self.market_analysis_service = None  # Will be injected
        self.is_updating_candles = False
        self.is_analyzing_market = False

    def inject_token_service(self, token_service):
        """Inject token service dependency"""
        self.token_service = token_service

    def inject_candlestick_service(self, candlestick_service):
        """Inject candlestick service dependency"""
        self.candlestick_service = candlestick_service

    def inject_market_analysis_service(self, market_analysis_service):
        """Inject market analysis service dependency"""
        self.market_analysis_service = market_analysis_service

    async def update_tokens_task(self, min_market_cap: int = 800000000, condition: str = 'greater'):
        """Task to update tokens from CoinMarketCap"""
        if self.is_updating:
            logger.info("Update already in progress, skipping...")
            return

        try:
            self.is_updating = True
            logger.info(f"Starting scheduled token update at {datetime.now()}")

            if not self.token_service:
                logger.error("Token service not injected")
                return

            # Force refresh from API with dynamic filter
            logger.info(f"Using market cap filter: ${min_market_cap:,} ({condition})")
            result = await self.token_service.get_high_market_cap_tokens(
                min_market_cap=min_market_cap,
                limit=150,  # Top 150 tokens
                refresh=True,
                check_exchanges=False,  # Skip exchange check for faster updates
                condition=condition
            )

            self.last_update = datetime.now()

            logger.info(f"Successfully updated {result.count} tokens")
            logger.info(f"Next update scheduled in {self.update_interval_hours} hours")

        except Exception as e:
            logger.error(f"Error during scheduled update: {e}")
        finally:
            self.is_updating = False

    async def update_candlesticks_task(self):
        """Task to update candlesticks from OKX every 24 hours"""
        if self.is_updating_candles:
            logger.info("Candlestick update already in progress, skipping...")
            return

        try:
            self.is_updating_candles = True
            logger.info(f"Starting scheduled candlestick update at {datetime.now()}")

            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_all_candlesticks()

            logger.info(f"Candlestick update completed: {result['message']}")

        except Exception as e:
            logger.error(f"Error during scheduled candlestick update: {e}")
        finally:
            self.is_updating_candles = False

    async def update_15m_timeframe_task(self):
        """Update 15-minute candlesticks"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_specific_timeframe('15m')
            logger.info(f"15m update: {result['message']}")

        except Exception as e:
            logger.error(f"Error updating 15m timeframe: {e}")

    async def update_30m_timeframe_task(self):
        """Update 30-minute candlesticks"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_specific_timeframe('30m')
            logger.info(f"30m update: {result['message']}")

        except Exception as e:
            logger.error(f"Error updating 30m timeframe: {e}")

    async def update_1h_timeframe_task(self):
        """Update 1-hour candlesticks"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_specific_timeframe('1h')
            logger.info(f"1h update: {result['message']}")

        except Exception as e:
            logger.error(f"Error updating 1h timeframe: {e}")

    async def update_12h_timeframe_task(self):
        """Update 12-hour candlesticks"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_specific_timeframe('12h')
            logger.info(f"12h update: {result['message']}")

        except Exception as e:
            logger.error(f"Error updating 12h timeframe: {e}")

    async def update_24h_timeframe_task(self):
        """Update 24-hour candlesticks"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.update_specific_timeframe('24h')
            logger.info(f"24h update: {result['message']}")

        except Exception as e:
            logger.error(f"Error updating 24h timeframe: {e}")

    async def analyze_market_task(self):
        """Analyze market sentiment and save to database (every 1 minute)"""
        if self.is_analyzing_market:
            logger.info("Market analysis already in progress, skipping...")
            return

        try:
            self.is_analyzing_market = True

            if not self.market_analysis_service:
                logger.error("Market analysis service not injected")
                return

            result = await self.market_analysis_service.analyze_and_save()
            logger.info(f"Market analysis completed: {result['message']}")

        except Exception as e:
            logger.error(f"Error during market analysis: {e}")
        finally:
            self.is_analyzing_market = False


    async def refresh_tier1_task(self):
        """Refresh TIER 1 tokens (TOP 10) every 5 seconds"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.refresh_tier1_candles()
            updated_count = result.get('updated_count', 0)
            logger.info(f"[TIER1] Updated {updated_count} candles")

            # NUEVO: Emitir evento si hubo actualizaciones
            if updated_count > 0:
                await event_bus.emit_debounced('tier1_updated', {
                    'tier': 1,
                    'updated_count': updated_count,
                    'timestamp': datetime.now().isoformat()
                }, delay=5)

        except Exception as e:
            logger.error(f"[TIER1] Error refreshing candles: {e}")

    async def refresh_tier2_task(self):
        """Refresh TIER 2 tokens (Market Cap > $5B) every 30 seconds"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.refresh_tier2_candles()
            updated_count = result.get('updated_count', 0)
            logger.info(f"[TIER2] Updated {updated_count} candles")

            # NUEVO: Emitir evento si hubo actualizaciones
            if updated_count > 0:
                await event_bus.emit_debounced('tier2_updated', {
                    'tier': 2,
                    'updated_count': updated_count,
                    'timestamp': datetime.now().isoformat()
                }, delay=5)

        except Exception as e:
            logger.error(f"[TIER2] Error refreshing candles: {e}")

    async def refresh_tier3_task(self):
        """Refresh TIER 3 tokens (Rest of tokens) every 60 seconds"""
        try:
            if not self.candlestick_service:
                logger.error("Candlestick service not injected")
                return

            result = await self.candlestick_service.refresh_tier3_candles()
            logger.info(f"[TIER3] Updated {result.get('updated_count', 0)} candles")

        except Exception as e:
            logger.error(f"[TIER3] Error refreshing candles: {e}")

    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            # Schedule daily update at midnight
            self.scheduler.add_job(
                self.update_tokens_task,
                IntervalTrigger(hours=self.update_interval_hours),
                id='update_tokens',
                name='Update cryptocurrency tokens',
                replace_existing=True,
                max_instances=1
            )

            # Also run immediately on startup
            self.scheduler.add_job(
                self.update_tokens_task,
                id='initial_update',
                name='Initial token update',
                replace_existing=True,
                max_instances=1
            )

            # Schedule FULL candlestick update every 24 hours (all timeframes)
            self.scheduler.add_job(
                self.update_candlesticks_task,
                IntervalTrigger(hours=24),
                id='update_candlesticks_full',
                name='Full candlestick update (all timeframes)',
                replace_existing=True,
                max_instances=1
            )

            # Initial candlestick update on startup
            self.scheduler.add_job(
                self.update_candlesticks_task,
                id='initial_candlestick_update',
                name='Initial candlestick update',
                replace_existing=True,
                max_instances=1
            )

            # TIMEFRAME-SPECIFIC SCHEDULERS (run at their own intervals)

            # 15-minute updates
            self.scheduler.add_job(
                self.update_15m_timeframe_task,
                IntervalTrigger(minutes=15),
                id='update_15m',
                name='Update 15m candlesticks',
                replace_existing=True,
                max_instances=1
            )

            # 30-minute updates
            self.scheduler.add_job(
                self.update_30m_timeframe_task,
                IntervalTrigger(minutes=30),
                id='update_30m',
                name='Update 30m candlesticks',
                replace_existing=True,
                max_instances=1
            )

            # 1-hour updates
            self.scheduler.add_job(
                self.update_1h_timeframe_task,
                IntervalTrigger(hours=1),
                id='update_1h',
                name='Update 1h candlesticks',
                replace_existing=True,
                max_instances=1
            )

            # 12-hour updates
            self.scheduler.add_job(
                self.update_12h_timeframe_task,
                IntervalTrigger(hours=12),
                id='update_12h',
                name='Update 12h candlesticks',
                replace_existing=True,
                max_instances=1
            )

            # 24-hour updates
            self.scheduler.add_job(
                self.update_24h_timeframe_task,
                IntervalTrigger(hours=24),
                id='update_24h',
                name='Update 24h candlesticks',
                replace_existing=True,
                max_instances=1
            )

            # Market analysis every 1 minute (changed from 5 for better sync with TIER updates)
            self.scheduler.add_job(
                self.analyze_market_task,
                IntervalTrigger(minutes=1),
                id='analyze_market',
                name='Analyze market sentiment',
                replace_existing=True,
                max_instances=1
            )

            # Initial market analysis on startup
            self.scheduler.add_job(
                self.analyze_market_task,
                id='initial_market_analysis',
                name='Initial market analysis',
                replace_existing=True,
                max_instances=1
            )

            # TIER 1: TOP 10 tokens - Every 5 seconds
            # BTC, ETH, XRP, BNB, SOL, USDT, USDC, DOGE, ADA, TRX
            # 50 candles (10 tokens x 5 timeframes) - Completes in ~3 seconds
            self.scheduler.add_job(
                self.refresh_tier1_task,
                IntervalTrigger(seconds=5),
                id='refresh_tier1',
                name='Refresh TIER 1 (TOP 10) every 5s',
                replace_existing=True,
                max_instances=1
            )

            # TIER 2: Important tokens (Market Cap > $5B) - Every 30 seconds
            # ~20 tokens - 100 candles - Completes in ~8 seconds
            self.scheduler.add_job(
                self.refresh_tier2_task,
                IntervalTrigger(seconds=30),
                id='refresh_tier2',
                name='Refresh TIER 2 (Market Cap > $5B) every 30s',
                replace_existing=True,
                max_instances=1
            )

            # TIER 3: Rest of tokens - Every 60 seconds
            # ~61 tokens - 305 candles - Completes in ~20 seconds
            self.scheduler.add_job(
                self.refresh_tier3_task,
                IntervalTrigger(seconds=60),
                id='refresh_tier3',
                name='Refresh TIER 3 (Rest) every 60s',
                replace_existing=True,
                max_instances=1
            )

            self.scheduler.start()
            logger.info(f"Scheduler started - Token updates every {self.update_interval_hours} hours")
            logger.info("Scheduler started - Full candlestick updates every 24 hours")
            logger.info("Scheduler started - Timeframe updates: 15m, 30m, 1h, 12h, 24h")
            logger.info("Scheduler started - Market analysis every 1 minute (UPSERT mode - always 2 records)")
            logger.info("Scheduler started - TIER 1 (TOP 10) updates every 5 seconds")
            logger.info("Scheduler started - TIER 2 (Market Cap > $5B) updates every 30 seconds")
            logger.info("Scheduler started - TIER 3 (Rest) updates every 60 seconds")

    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    def get_next_update_time(self) -> Optional[datetime]:
        """Get the time of next scheduled update"""
        job = self.scheduler.get_job('update_tokens')
        if job and job.next_run_time:
            return job.next_run_time
        return None

    def get_status(self) -> dict:
        """Get scheduler status"""
        next_update = self.get_next_update_time()
        now = datetime.now(timezone.utc)

        return {
            "scheduler_running": self.scheduler.running,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "next_update": next_update.isoformat() if next_update else None,
            "update_interval_hours": self.update_interval_hours,
            "is_updating": self.is_updating,
            "time_until_next_update": str(next_update - now) if next_update else None
        }

    async def force_update(self, min_market_cap: int = 800000000, condition: str = 'greater') -> dict:
        """Force an immediate update with custom market cap filter and condition"""
        logger.info(f"Forcing manual token update with min_market_cap: ${min_market_cap:,} ({condition})")
        await self.update_tokens_task(min_market_cap, condition)
        return self.get_status()

# Singleton instance
scheduler_service = SchedulerService()