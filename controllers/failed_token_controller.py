from fastapi import APIRouter, Query
from typing import Optional
import logging
from services.failed_token_service import FailedTokenService
from models.failed_token_model import FailedTokenResponse

logger = logging.getLogger(__name__)

class FailedTokenController:
    """
    REST API Controller for failed token operations
    Handles tokens that are not available in OKX Exchange
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/failed-tokens", tags=["failed-tokens"])
        self.service = FailedTokenService()
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "",
            self.get_all_failed_tokens,
            methods=["GET"],
            response_model=FailedTokenResponse,
            response_model_by_alias=True
        )
        self.router.add_api_route(
            "/stats",
            self.get_stats,
            methods=["GET"]
        )
        self.router.add_api_route(
            "/search",
            self.search_by_symbol,
            methods=["GET"]
        )
        self.router.add_api_route(
            "/clear",
            self.clear_history,
            methods=["DELETE"]
        )

    async def get_all_failed_tokens(
        self,
        limit: int = Query(default=1000, description="Maximum number of results")
    ) -> FailedTokenResponse:
        """
        Get all tokens that failed to retrieve data from OKX

        Examples:
            GET /api/failed-tokens - Get all failed tokens
            GET /api/failed-tokens?limit=50 - Get first 50 failed tokens
        """
        logger.info(f"GET failed tokens - limit: {limit}")
        return await self.service.get_all_failed_tokens(limit)

    async def get_stats(
        self,
        total_tokens: int = Query(..., description="Total tokens attempted"),
        successful_candlesticks: int = Query(..., description="Successful candlesticks inserted")
    ):
        """
        Get statistics about failed tokens

        Args:
            total_tokens: Total number of tokens processed
            successful_candlesticks: Number of candlesticks successfully inserted

        Examples:
            GET /api/failed-tokens/stats?total_tokens=100&successful_candlesticks=450
        """
        logger.info(f"GET failed tokens stats - total: {total_tokens}, successful: {successful_candlesticks}")
        return await self.service.get_failed_token_stats(total_tokens, successful_candlesticks)

    async def search_by_symbol(
        self,
        symbol: str = Query(..., description="Token symbol to search")
    ):
        """
        Search for a specific failed token by symbol

        Args:
            symbol: Token symbol (e.g., HYPE, PENGU)

        Examples:
            GET /api/failed-tokens/search?symbol=HYPE
        """
        logger.info(f"GET failed token by symbol: {symbol}")
        return await self.service.get_failed_token_by_symbol(symbol)

    async def clear_history(self):
        """
        Clear all failed tokens from both PRIMARY and SECONDARY databases
        This is useful before a fresh update

        Examples:
            DELETE /api/failed-tokens/clear
        """
        logger.info("DELETE all failed tokens requested")
        try:
            deleted_count = await self.service.clear_history()
            return {
                'status': 'success',
                'message': f'Cleared {deleted_count} failed tokens from both databases',
                'deleted_count': deleted_count
            }
        except Exception as e:
            logger.error(f"Error clearing failed tokens: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'deleted_count': 0
            }

# Create controller instance
failed_token_controller = FailedTokenController()
