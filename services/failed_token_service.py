import logging
from typing import List, Dict, Any
from datetime import datetime
from repositories.failed_token_repository import FailedTokenRepository
from models.failed_token_model import FailedTokenModel, FailedTokenResponse, FailedTokenStats

logger = logging.getLogger(__name__)

class FailedTokenService:
    """
    Business logic service for failed token operations
    Manages tokens that are not available in OKX Exchange
    """

    def __init__(self):
        self.failed_token_repository = FailedTokenRepository()

    async def clear_history(self) -> int:
        """
        Clear all failed tokens from database
        Used before fresh update to ensure clean historical data
        """
        try:
            deleted_count = await self.failed_token_repository.delete_all()
            logger.info(f"Cleared failed tokens history: {deleted_count} records removed")
            return deleted_count
        except Exception as e:
            logger.error(f"Error clearing failed tokens history: {e}")
            raise

    async def record_failed_tokens(
        self,
        failed_tokens_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Record tokens that failed to retrieve data from OKX

        Args:
            failed_tokens_data: List of dicts with token information and failure details

        Returns:
            Dict with count of inserted failed tokens
        """
        try:
            if not failed_tokens_data:
                logger.info("No failed tokens to record")
                return {
                    'status': 'success',
                    'message': 'No failed tokens',
                    'count': 0
                }

            # Insert all failed tokens
            inserted_count = await self.failed_token_repository.insert_many(failed_tokens_data)

            logger.info(f"Successfully recorded {inserted_count} failed tokens")

            return {
                'status': 'success',
                'message': f'Recorded {inserted_count} failed tokens',
                'count': inserted_count
            }

        except Exception as e:
            logger.error(f"Error recording failed tokens: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'count': 0
            }

    async def get_all_failed_tokens(self, limit: int = 1000) -> FailedTokenResponse:
        """
        Get all tokens that failed in the last update

        Returns:
            FailedTokenResponse with list of failed tokens
        """
        try:
            failed_tokens = await self.failed_token_repository.find_all(limit)

            # Convert to Pydantic models
            failed_token_models = [FailedTokenModel(**token) for token in failed_tokens]

            return FailedTokenResponse(
                status="success",
                message=f"Found {len(failed_token_models)} failed tokens",
                count=len(failed_token_models),
                data=failed_token_models
            )

        except Exception as e:
            logger.error(f"Error getting failed tokens: {e}")
            return FailedTokenResponse(
                status="error",
                message=str(e),
                count=0,
                data=[]
            )

    async def get_failed_token_stats(
        self,
        total_tokens_attempted: int,
        successful_candlesticks: int
    ) -> Dict[str, Any]:
        """
        Get statistics about failed tokens and success rate

        Args:
            total_tokens_attempted: Total number of tokens processed
            successful_candlesticks: Number of candlesticks successfully inserted

        Returns:
            Dictionary with comprehensive statistics
        """
        try:
            # Get count of failed tokens
            failed_count = await self.failed_token_repository.count_failed_tokens()

            # Calculate successful tokens (assuming 5 timeframes per token)
            successful_tokens = successful_candlesticks // 5 if successful_candlesticks > 0 else 0

            # Calculate success rate
            success_rate = (successful_tokens / total_tokens_attempted * 100) if total_tokens_attempted > 0 else 0

            # Get failure reasons breakdown
            detailed_stats = await self.failed_token_repository.get_statistics()

            stats = FailedTokenStats(
                total_tokens_attempted=total_tokens_attempted,
                successful_tokens=successful_tokens,
                failed_tokens=failed_count,
                success_rate=round(success_rate, 2),
                total_candlesticks=successful_candlesticks,
                last_update=datetime.now()
            )

            return {
                'status': 'success',
                'stats': stats.model_dump(),
                'failure_breakdown': detailed_stats.get('failure_reasons', [])
            }

        except Exception as e:
            logger.error(f"Error getting failed token stats: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def get_failed_token_by_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Check if a specific token failed in the last update

        Args:
            symbol: Token symbol to check

        Returns:
            Failed token data or None
        """
        try:
            failed_token = await self.failed_token_repository.find_by_symbol(symbol)

            if failed_token:
                return {
                    'status': 'success',
                    'found': True,
                    'data': FailedTokenModel(**failed_token).model_dump()
                }
            else:
                return {
                    'status': 'success',
                    'found': False,
                    'message': f'Token {symbol} was successful or not processed'
                }

        except Exception as e:
            logger.error(f"Error getting failed token by symbol: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
