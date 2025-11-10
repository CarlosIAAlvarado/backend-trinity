import logging
from typing import List, Dict, Any
from datetime import datetime
from repositories.notification_repository import NotificationRepository
from repositories.secondary_notification_repository import SecondaryNotificationRepository
from models.notification_model import NotificationModel, NotificationResponse

logger = logging.getLogger(__name__)

class NotificationService:
    """
    Business logic service for notification operations
    Manages system notifications for users
    Writes to PRIMARY and SECONDARY databases
    """

    def __init__(self):
        self.notification_repository = NotificationRepository()
        self.secondary_notification_repository = SecondaryNotificationRepository()

    async def create_token_available_notification(
        self,
        symbol: str,
        name: str,
        market_cap: float = None
    ) -> Dict[str, Any]:
        """
        Create a notification when a token becomes available in OKX

        Args:
            symbol: Token symbol
            name: Token name
            market_cap: Token market cap (optional)

        Returns:
            Created notification
        """
        try:
            notification_data = {
                'type': 'token_available',
                'title': f'🎉 {symbol} Now Available!',
                'message': f'{name} ({symbol}) is now available in OKX Exchange and can be traded',
                'symbol': symbol,
                'data': {
                    'name': name,
                    'market_cap': market_cap
                },
                'read': False,
                'timestamp': datetime.now()
            }

            # Save to PRIMARY database
            notification = await self.notification_repository.insert_one(notification_data)
            logger.info(f"[PRIMARY DB] Created token_available notification for {symbol}")

            # Save to SECONDARY database (with retry logic)
            try:
                secondary_result = await self.secondary_notification_repository.insert_notification_with_retry(notification_data)
                if secondary_result['status'] == 'success':
                    logger.info(f"[SECONDARY DB] Notification synced successfully")
            except Exception as e:
                logger.error(f"[SECONDARY DB] Failed to sync notification: {e}")
                # Don't raise, continue with primary DB operation

            # Emit WebSocket event for real-time notification
            from services.websocket_service import websocket_service
            await websocket_service.emit_new_notification(notification_data)

            return notification

        except Exception as e:
            logger.error(f"Error creating token_available notification: {e}")
            raise

    async def create_token_unavailable_notification(
        self,
        symbol: str,
        name: str
    ) -> Dict[str, Any]:
        """
        Create a notification when a token becomes unavailable in OKX

        Args:
            symbol: Token symbol
            name: Token name

        Returns:
            Created notification
        """
        try:
            notification_data = {
                'type': 'token_unavailable',
                'title': f'⚠️ {symbol} No Longer Available',
                'message': f'{name} ({symbol}) is no longer available in OKX Exchange',
                'symbol': symbol,
                'data': {
                    'name': name
                },
                'read': False,
                'timestamp': datetime.now()
            }

            notification = await self.notification_repository.insert_one(notification_data)
            logger.info(f"Created token_unavailable notification for {symbol}")

            return notification

        except Exception as e:
            logger.error(f"Error creating token_unavailable notification: {e}")
            raise

    async def create_bulk_token_available_notifications(
        self,
        tokens: List[Dict[str, Any]]
    ) -> int:
        """
        Create multiple token_available notifications at once

        Args:
            tokens: List of tokens that became available
                    Each dict should have: symbol, name, market_cap

        Returns:
            Number of notifications created
        """
        try:
            notifications = []

            for token in tokens:
                notification_data = {
                    'type': 'token_available',
                    'title': f'🎉 {token["symbol"]} Now Available!',
                    'message': f'{token["name"]} ({token["symbol"]}) is now available in OKX Exchange',
                    'symbol': token['symbol'],
                    'data': {
                        'name': token['name'],
                        'market_cap': token.get('market_cap')
                    },
                    'read': False,
                    'timestamp': datetime.now()
                }
                notifications.append(notification_data)

            if notifications:
                count = await self.notification_repository.insert_many(notifications)
                logger.info(f"Created {count} token_available notifications")

                # Emit WebSocket events for each notification
                from services.websocket_service import websocket_service
                for notification_data in notifications:
                    await websocket_service.emit_new_notification(notification_data)

                return count

            return 0

        except Exception as e:
            logger.error(f"Error creating bulk token_available notifications: {e}")
            raise

    async def get_all_notifications(
        self,
        unread_only: bool = False,
        limit: int = 100,
        skip: int = 0
    ) -> NotificationResponse:
        """
        Get all notifications with optional filters

        Args:
            unread_only: If True, only return unread notifications
            limit: Maximum number of notifications
            skip: Number to skip (for pagination)

        Returns:
            NotificationResponse with list of notifications
        """
        try:
            notifications = await self.notification_repository.find_all(
                unread_only=unread_only,
                limit=limit,
                skip=skip
            )

            unread_count = await self.notification_repository.count_unread()

            # Convert to Pydantic models
            notification_models = [NotificationModel(**notif) for notif in notifications]

            return NotificationResponse(
                status="success",
                message=f"Found {len(notification_models)} notifications",
                count=len(notification_models),
                unread_count=unread_count,
                data=notification_models
            )

        except Exception as e:
            logger.error(f"Error getting notifications: {e}")
            return NotificationResponse(
                status="error",
                message=str(e),
                count=0,
                unread_count=0,
                data=[]
            )

    async def mark_as_read(self, notification_id: str) -> Dict[str, Any]:
        """
        Mark a notification as read

        Args:
            notification_id: ID of notification

        Returns:
            Success status
        """
        try:
            # Mark in PRIMARY database
            success = await self.notification_repository.mark_as_read(notification_id)

            # Mark in SECONDARY database (with retry logic)
            if success:
                try:
                    await self.secondary_notification_repository.mark_as_read_with_retry(notification_id)
                    logger.info(f"[SECONDARY DB] Notification marked as read")
                except Exception as e:
                    logger.error(f"[SECONDARY DB] Failed to mark notification as read: {e}")

            return {
                'status': 'success' if success else 'error',
                'message': 'Notification marked as read' if success else 'Notification not found'
            }

        except Exception as e:
            logger.error(f"Error marking notification as read: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def mark_all_as_read(self) -> Dict[str, Any]:
        """
        Mark all notifications as read

        Returns:
            Number of notifications marked as read
        """
        try:
            count = await self.notification_repository.mark_all_as_read()

            return {
                'status': 'success',
                'message': f'Marked {count} notifications as read',
                'count': count
            }

        except Exception as e:
            logger.error(f"Error marking all notifications as read: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'count': 0
            }

    async def get_unread_count(self) -> int:
        """
        Get count of unread notifications

        Returns:
            Number of unread notifications
        """
        try:
            count = await self.notification_repository.count_unread()
            return count

        except Exception as e:
            logger.error(f"Error getting unread count: {e}")
            return 0

    async def cleanup_old_notifications(self, days: int = 30) -> int:
        """
        Delete notifications older than specified days

        Args:
            days: Delete notifications older than this many days

        Returns:
            Number of deleted notifications
        """
        try:
            count = await self.notification_repository.delete_old_notifications(days)
            logger.info(f"Cleaned up {count} old notifications")
            return count

        except Exception as e:
            logger.error(f"Error cleaning up old notifications: {e}")
            return 0

# Singleton instance
notification_service = NotificationService()
