from fastapi import APIRouter
from fastapi.responses import JSONResponse
import logging
from services.market_analysis_service import market_analysis_service

logger = logging.getLogger(__name__)

class MarketAnalysisController:
    """
    REST API Controller for market analysis operations
    NEW VERSION: Returns analysis for ALL timeframes in a single document
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/market-analysis", tags=["market-analysis"])
        self.service = market_analysis_service
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "/analyze",
            self.analyze_now,
            methods=["POST"],
            summary="Analyze all timeframes and return nested structure"
        )

    async def analyze_now(self):
        """
        Analyze ALL timeframes and return nested structure
        Returns a single document with all timeframe analyses

        Response structure:
        {
            "success": true,
            "data": {
                "direction": "SHORT" | "LONG",
                "directionNumber": 0 | 1,
                "directionNumberReal": 0.0-1.0,
                "candlesByTimeframe": {
                    "15m": {"best": [...], "worst": [...]},
                    "30m": {"best": [...], "worst": [...]},
                    "1H": {"best": [...], "worst": [...]},
                    "4H": {"best": [...], "worst": [...]},
                    "12H": {"best": [...], "worst": [...]},
                    "1D": {"best": [...], "worst": [...]}
                }
            }
        }
        """
        try:
            logger.info("POST /api/market-analysis/analyze - Generating analysis for ALL timeframes")

            # Generate analysis for all timeframes
            analysis = await self.service.analyze_all_timeframes()

            # Save to both databases
            analysis_dict = await self.service.save_analysis(analysis)
            logger.info("Analysis saved to both databases")

            # Return with new format using JSONResponse
            return JSONResponse(
                content={
                    "success": True,
                    "data": analysis_dict
                },
                status_code=200
            )

        except Exception as e:
            logger.error(f"Error in analyze_now endpoint: {e}")
            return JSONResponse(
                content={
                    "success": False,
                    "data": {"error": str(e), "status": 500}
                },
                status_code=500
            )

market_analysis_controller = MarketAnalysisController()
