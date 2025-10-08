# ==========================
# Token Controller
# ==========================
from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional
from pydantic import BaseModel
import logging
from services.token_service import TokenService
from services.scheduler_service import scheduler_service
from models.token_model import TokenResponse

logger = logging.getLogger(__name__)

class ForceUpdateRequest(BaseModel):
    min_market_cap: int = 800000000
    filter_condition: str = 'greater'

class TokenController:
    """
    REST API Controller for token operations
    Follows Single Responsibility Principle
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/tokens", tags=["tokens"])
        self.service = TokenService()
        # Inject token service into scheduler
        scheduler_service.inject_token_service(self.service)
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "/high-market-cap",
            self.get_high_market_cap_tokens,
            methods=["GET"],
            response_model=TokenResponse,
            response_model_by_alias=True
        )
        self.router.add_api_route(
            "/refresh",
            self.refresh_data,
            methods=["POST"],
            response_model=TokenResponse,
            response_model_by_alias=True
        )
        self.router.add_api_route(
            "/{symbol}",
            self.get_token_by_symbol,
            methods=["GET"]
        )
        self.router.add_api_route(
            "/scheduler/status",
            self.get_scheduler_status,
            methods=["GET"]
        )
        self.router.add_api_route(
            "/scheduler/force-update",
            self.force_update,
            methods=["POST"]
        )

    async def get_high_market_cap_tokens(
        self,
        min_market_cap: float = Query(default=800000000, description="Minimum market cap"),
        limit: int = Query(default=100, description="Number of tokens to return"),
        currency: str = Query(default="USD", description="Currency for conversion"),
        check_exchanges: bool = Query(default=True, description="Check exchanges"),
        refresh: bool = Query(default=False, description="Force refresh from API"),
        condition: str = Query(default="greater", description="Filter condition (greater, less, equal)")
    ) -> TokenResponse:
        """
        Get tokens with high market capitalization

        Args:
            min_market_cap: Minimum market cap filter
            limit: Maximum number of results
            currency: Currency for market cap calculation
            check_exchanges: Whether to verify exchanges
            refresh: Force data refresh from API
            condition: Filter condition (greater, less, equal)

        Returns:
            TokenResponse with list of tokens
        """
        try:
            result = await self.service.get_high_market_cap_tokens(
                min_market_cap=min_market_cap,
                limit=limit,
                currency=currency,
                check_exchanges=check_exchanges,
                refresh=refresh,
                condition=condition
            )
            return result

        except Exception as e:
            logger.error(f"Error in get_high_market_cap_tokens: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def refresh_data(self) -> TokenResponse:
        """
        Force refresh token data from API

        Returns:
            TokenResponse with updated tokens
        """
        try:
            result = await self.service.get_high_market_cap_tokens(
                refresh=True,
                limit=100
            )
            return result

        except Exception as e:
            logger.error(f"Error in refresh_data: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_token_by_symbol(self, symbol: str):
        """
        Get token information by symbol

        Args:
            symbol: Token symbol (e.g., BTC, ETH)

        Returns:
            Token information
        """
        try:
            token = await self.service.get_token_by_symbol(symbol)

            if not token:
                raise HTTPException(status_code=404, detail="Token not found")

            return {
                "status": "Success",
                "data": token.dict()
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in get_token_by_symbol: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_scheduler_status(self):
        """
        Get scheduler status and update information

        Returns:
            Scheduler status including last update and next scheduled update
        """
        try:
            status = scheduler_service.get_status()
            return {
                "status": "Success",
                "data": status
            }
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def force_update(self, request: ForceUpdateRequest = Body(default=ForceUpdateRequest())):
        """
        Force an immediate update of token data

        Args:
            request: Request body with min_market_cap

        Returns:
            Update status
        """
        try:
            min_market_cap = request.min_market_cap
            condition = request.filter_condition

            logger.info(f"Force update requested with min_market_cap: ${min_market_cap:,} ({condition})")

            status = await scheduler_service.force_update(min_market_cap, condition)
            return {
                "status": "Success",
                "message": f"Update initiated with market cap filter: ${min_market_cap:,} ({condition})",
                "data": status
            }
        except Exception as e:
            logger.error(f"Error forcing update: {e}")
            raise HTTPException(status_code=500, detail=str(e))