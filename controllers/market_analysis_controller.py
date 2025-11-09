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
            "/all",
            self.get_all_timeframes,
            methods=["GET"],
            summary="Get latest analysis for all timeframes"
        )
        self.router.add_api_route(
            "/analyze",
            self.analyze_now,
            methods=["POST"],
            summary="Analyze all timeframes and return nested structure"
        )

    async def get_all_timeframes(self):
        """
        GET endpoint to retrieve the latest analysis for ALL timeframes
        Returns the most recent analysis document from the database

        Response structure:
        {
            "success": true,
            "data": {
                "direction": "SHORT" | "FLAT" | "LONG",
                "directionNumber": 0 | 0.5 | 1,
                "directionNumberReal": 0.0-1.0,
                "candlesByTimeframe": {
                    "15m": {"best": [...], "worst": [...]},
                    "30m": {"best": [...], "worst": [...]},
                    "1H": {"best": [...], "worst": [...]},
                    "4H": {"best": [...], "worst": [...]},
                    "12H": {"best": [...], "worst": [...]},
                    "1D": {"best": [...], "worst": [...]}
                },
                "timestamp": "...",
                "updatedAt": "..."
            }
        }
        """
        try:
            logger.info("GET /api/market-analysis/all - Retrieving latest analysis")

            # Get latest analysis from database
            analysis = await self.service.get_latest_analysis()

            if not analysis:
                logger.warning("No analysis found in database")
                return JSONResponse(
                    content={
                        "success": False,
                        "message": "No analysis found",
                        "data": None
                    },
                    status_code=404
                )

            return JSONResponse(
                content={
                    "success": True,
                    "data": analysis
                },
                status_code=200
            )

        except Exception as e:
            logger.error(f"Error in get_all_timeframes endpoint: {e}")
            return JSONResponse(
                content={
                    "success": False,
                    "message": str(e),
                    "data": None
                },
                status_code=500
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
