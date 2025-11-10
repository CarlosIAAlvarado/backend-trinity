import logging
from typing import List, Dict, Any
from datetime import datetime
from repositories.failed_token_repository import FailedTokenRepository
from repositories.secondary_failed_token_repository import SecondaryFailedTokenRepository
from models.failed_token_model import FailedTokenModel, FailedTokenResponse, FailedTokenStats

logger = logging.getLogger(__name__)

class FailedTokenService:
    """
    Business logic service for failed token operations
    Manages tokens that are not available in OKX Exchange
    """

    def __init__(self):
        self.failed_token_repository = FailedTokenRepository()
        self.secondary_failed_token_repository = SecondaryFailedTokenRepository()

    async def clear_history(self) -> int:
        """
        Clear all failed tokens from database
        Used before fresh update to ensure clean historical data
        """
        try:
            # Clear from PRIMARY database
            deleted_count = await self.failed_token_repository.delete_all()
            logger.info(f"[PRIMARY DB] Cleared failed tokens history: {deleted_count} records removed")

            # Clear from SECONDARY database (with retry logic)
            try:
                secondary_result = await self.secondary_failed_token_repository.delete_all_with_retry()
                if secondary_result['status'] == 'success':
                    logger.info(f"[SECONDARY DB] Cleared {secondary_result['deleted_count']} failed tokens from secondary DB")
            except Exception as e:
                logger.error(f"[SECONDARY DB] Failed to clear failed tokens from secondary DB: {e}")

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

            # Insert all failed tokens to PRIMARY database
            inserted_count = await self.failed_token_repository.insert_many(failed_tokens_data)
            logger.info(f"[PRIMARY DB] Successfully recorded {inserted_count} failed tokens")

            # Sync to SECONDARY database (with retry logic)
            try:
                secondary_result = await self.secondary_failed_token_repository.bulk_upsert_failed_tokens_with_retry(failed_tokens_data)
                if secondary_result['status'] == 'success':
                    logger.info(f"[SECONDARY DB] Failed tokens synced successfully")
            except Exception as e:
                logger.error(f"[SECONDARY DB] Failed to sync failed tokens: {e}")

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

    async def update_failed_tokens(
        self,
        successful_symbols: List[str],
        failed_tokens_data: List[Dict[str, Any]],
        all_tokens: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update the failed tokens table intelligently:
        1. Check which failed tokens are now successful
        2. Create notifications for tokens that became available
        3. Remove tokens that are now successful in OKX
        4. Add/update tokens that failed

        Args:
            successful_symbols: List of symbols that successfully retrieved data from OKX
            failed_tokens_data: List of tokens that failed to retrieve data
            all_tokens: All tokens being processed (for notification data)

        Returns:
            Dict with statistics about the update
        """
        try:
            # Step 1: Get previously failed tokens that are now successful
            newly_available_tokens = []
            if successful_symbols:
                for symbol in successful_symbols:
                    # Check if this token was previously in failed tokens table
                    failed_token = await self.failed_token_repository.find_by_symbol(symbol)
                    if failed_token:
                        # This token was failed before but is now available!
                        token_info = {
                            'symbol': symbol,
                            'name': failed_token.get('name', symbol),
                            'market_cap': failed_token.get('market_cap')
                        }
                        newly_available_tokens.append(token_info)
                        logger.info(f"🎉 Token {symbol} is now available in OKX!")

            # Step 2: Create notifications for newly available tokens
            notifications_created = 0
            if newly_available_tokens:
                from services.notification_service import notification_service
                notifications_created = await notification_service.create_bulk_token_available_notifications(
                    newly_available_tokens
                )
                logger.info(f"Created {notifications_created} notifications for newly available tokens")

            # Step 3: Remove tokens that are now available in OKX
            deleted_count = 0
            if successful_symbols:
                # Delete from PRIMARY database
                deleted_count = await self.failed_token_repository.delete_by_symbols(successful_symbols)
                logger.info(f"[PRIMARY DB] Removed {deleted_count} tokens that are now available in OKX")

                # Delete from SECONDARY database (with retry logic)
                try:
                    secondary_result = await self.secondary_failed_token_repository.delete_by_symbols_with_retry(successful_symbols)
                    if secondary_result['status'] == 'success':
                        logger.info(f"[SECONDARY DB] Removed {secondary_result['deleted_count']} tokens from secondary DB")
                except Exception as e:
                    logger.error(f"[SECONDARY DB] Failed to remove tokens from secondary DB: {e}")

            # Step 4: Add or update failed tokens
            upserted_count = 0
            if failed_tokens_data:
                # Upsert to PRIMARY database
                upserted_count = await self.failed_token_repository.upsert_many(failed_tokens_data)
                logger.info(f"[PRIMARY DB] Upserted {upserted_count} failed tokens")

                # Sync to SECONDARY database (with retry logic)
                try:
                    secondary_result = await self.secondary_failed_token_repository.bulk_upsert_failed_tokens_with_retry(failed_tokens_data)
                    if secondary_result['status'] == 'success':
                        logger.info(f"[SECONDARY DB] Failed tokens synced successfully")
                except Exception as e:
                    logger.error(f"[SECONDARY DB] Failed to sync failed tokens: {e}")

            return {
                'status': 'success',
                'message': f'Updated failed tokens: {deleted_count} removed, {upserted_count} upserted, {notifications_created} notifications created',
                'removed_count': deleted_count,
                'upserted_count': upserted_count,
                'notifications_created': notifications_created,
                'newly_available_tokens': newly_available_tokens
            }

        except Exception as e:
            logger.error(f"Error updating failed tokens: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'removed_count': 0,
                'upserted_count': 0,
                'notifications_created': 0,
                'newly_available_tokens': []
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
