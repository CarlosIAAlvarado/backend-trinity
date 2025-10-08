# ==========================
# Config Controller
# ==========================
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging
from services.websocket_service import websocket_service

logger = logging.getLogger(__name__)

class MarketCapUpdate(BaseModel):
    market_cap_filter: int
    filter_condition: str = 'greater'

class IntervalUpdate(BaseModel):
    update_interval_hours: int

class ConfigController:
    """
    REST API Controller for configuration operations
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/config", tags=["configuration"])
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "",
            self.get_config,
            methods=["GET"]
        )
        self.router.add_api_route(
            "/market-cap",
            self.update_market_cap,
            methods=["POST"]
        )
        self.router.add_api_route(
            "/interval",
            self.update_interval,
            methods=["POST"]
        )

    async def get_config(self):
        """
        Get current global configuration

        Returns:
            Current configuration settings
        """
        try:
            config = await websocket_service.get_global_config()
            return {
                "status": "Success",
                "config": config
            }
        except Exception as e:
            logger.error(f"Error getting config: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def update_market_cap(self, data: MarketCapUpdate):
        """
        Update market cap filter configuration

        Args:
            data: Market cap filter value and condition

        Returns:
            Updated configuration
        """
        try:
            new_market_cap = data.market_cap_filter
            condition = data.filter_condition

            logger.info(f"[CONFIG UPDATE] Market Cap changed to: ${new_market_cap:,} ({condition})")

            # Update configuration and broadcast to all clients
            updated_config = await websocket_service.update_market_cap_filter(new_market_cap, condition)

            return {
                "status": "Success",
                "message": f"Market cap filter updated to ${new_market_cap:,} ({condition})",
                "config": updated_config
            }

        except Exception as e:
            logger.error(f"[CONFIG UPDATE] Error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def update_interval(self, data: IntervalUpdate):
        """
        Update update interval configuration

        Args:
            data: Update interval in hours

        Returns:
            Updated configuration
        """
        try:
            new_interval = data.update_interval_hours

            logger.info(f"[CONFIG UPDATE] Update interval changed to: {new_interval} hours")

            # Update configuration and broadcast to all clients
            updated_config = await websocket_service.update_interval(new_interval)

            return {
                "status": "Success",
                "message": f"Update interval changed to {new_interval} hours",
                "config": updated_config
            }

        except Exception as e:
            logger.error(f"[CONFIG UPDATE] Error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
