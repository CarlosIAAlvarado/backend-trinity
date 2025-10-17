from fastapi import APIRouter, Query
from typing import Optional
import logging
from services.market_analysis_service import market_analysis_service
from models.market_analysis_model import MarketAnalysisResponse, MarketHistoryResponse

logger = logging.getLogger(__name__)

class MarketAnalysisController:
    """
    REST API Controller for market analysis operations
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/market-analysis", tags=["market-analysis"])
        self.service = market_analysis_service
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "/latest",
            self.get_latest_analysis,
            methods=["GET"],
            response_model=MarketAnalysisResponse,
            summary="Get latest market analysis"
        )
        self.router.add_api_route(
            "/analyze",
            self.analyze_now,
            methods=["POST"],
            response_model=MarketAnalysisResponse,
            summary="Analyze market now and return results"
        )

    async def get_latest_analysis(
        self,
        timeframe: str = Query(default='24h', description="Timeframe: 12h or 24h")
    ) -> MarketAnalysisResponse:
        """
        Get the most recent market analysis from database for specific timeframe
        If no data exists, generates fresh analysis
        """
        try:
            logger.info(f"GET /api/market-analysis/latest?timeframe={timeframe} - Fetching latest analysis")
            response = await self.service.get_latest_analysis(timeframe)
            return response

        except Exception as e:
            logger.error(f"Error in get_latest_analysis endpoint: {e}")
            return MarketAnalysisResponse(
                status="error",
                message=str(e),
                data=None
            )

    async def analyze_now(
        self,
        timeframe: str = Query(default='24h', description="Timeframe: 12h or 24h")
    ) -> MarketAnalysisResponse:
        """
        Analyze market immediately and return fresh results for specific timeframe
        Uses UPSERT to update the existing record (no accumulation)
        """
        try:
            logger.info(f"POST /api/market-analysis/analyze?timeframe={timeframe} - Generating fresh analysis")
            analysis = await self.service.analyze_market(timeframe)

            # Save to database using UPSERT (updates existing record)
            analysis_dict = analysis.model_dump()
            await self.service.market_repository.insert_analysis(analysis_dict)
            logger.info(f"Fresh analysis upserted to database for {timeframe}")

            return MarketAnalysisResponse(
                status="success",
                message=f"Market analysis completed for {timeframe}",
                data=analysis
            )

        except Exception as e:
            logger.error(f"Error in analyze_now endpoint: {e}")
            return MarketAnalysisResponse(
                status="error",
                message=str(e),
                data=None
            )

market_analysis_controller = MarketAnalysisController()
