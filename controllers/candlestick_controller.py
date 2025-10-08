from fastapi import APIRouter, Query
from typing import Optional
import logging
from services.candlestick_service import CandlestickService
from services.scheduler_service import scheduler_service
from models.candle_model import CandleResponse

logger = logging.getLogger(__name__)

class CandlestickController:
    """
    REST API Controller for candlestick operations
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/candlesticks", tags=["candlesticks"])
        self.service = CandlestickService()
        # Inject candlestick service into scheduler
        scheduler_service.inject_candlestick_service(self.service)
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "",
            self.get_candlesticks,
            methods=["GET"],
            response_model=CandleResponse,
            response_model_by_alias=True
        )
        self.router.add_api_route(
            "/update",
            self.update_all,
            methods=["POST"]
        )
        self.router.add_api_route(
            "/update/{symbol}",
            self.update_single,
            methods=["POST"]
        )
        self.router.add_api_route(
            "/stats",
            self.get_stats,
            methods=["GET"]
        )

    async def get_candlesticks(
        self,
        symbol: Optional[str] = Query(None, description="Filter by symbol"),
        timeframe: Optional[str] = Query(None, description="Filter by timeframe (15m, 30m, 1h)"),
        limit: int = Query(default=1000, description="Maximum number of results")
    ) -> CandleResponse:
        """
        Get candlesticks with optional filters

        Examples:
            GET /api/candlesticks - Get all candles
            GET /api/candlesticks?symbol=BTC - Get all BTC candles
            GET /api/candlesticks?symbol=BTC&timeframe=15m - Get specific candle
        """
        logger.info(f"GET candlesticks - symbol: {symbol}, timeframe: {timeframe}")
        return await self.service.get_all_candlesticks(symbol, timeframe, limit)

    async def update_all(self):
        """
        Force update candlesticks for all Trinity tokens
        This manually triggers what the scheduler does every 24h
        """
        logger.info("Manual update of all candlesticks requested")
        result = await self.service.update_all_candlesticks()
        return result

    async def update_single(self, symbol: str):
        """
        Update candlesticks for a single token

        Args:
            symbol: Token symbol (e.g., BTC, ETH)
        """
        logger.info(f"Manual update requested for {symbol}")
        result = await self.service.update_single_token(symbol)
        return result

    async def get_stats(self):
        """Get candlestick statistics"""
        logger.info("Candlestick stats requested")
        return await self.service.get_candlestick_stats()

# Create controller instance
candlestick_controller = CandlestickController()
