import logging
from typing import Dict, Any, Optional
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryNotificationRepository:
    """
    Repository for Notification operations in SECONDARY database
    Handles CRUD operations for trinity_performance_notifications collection
    This is a replica/backup of the main notification data
    """

    def __init__(self):
        self.collection_name = 'trinity_performance_notifications'
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def insert_notification_with_retry(self, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert notification with retry logic

        Args:
            notification_data: Dictionary containing notification data

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._insert_notification(notification_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved notification after {attempt + 1} attempts")
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[SECONDARY DB] Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"[SECONDARY DB] All {self.max_retries} attempts failed. "
                        f"Notification NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _insert_notification(self, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to insert notification

        Args:
            notification_data: Dictionary containing notification data

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            # Set timestamps
            current_time = datetime.now()
            notification_data['createdAt'] = current_time
            notification_data['updatedAt'] = current_time

            # Ensure timestamp field exists
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = current_time

            # Insert notification
            result = await collection.insert_one(notification_data)

            notification_type = notification_data.get('type', 'unknown')
            symbol = notification_data.get('symbol', 'N/A')

            logger.info(
                f"[SECONDARY DB] Notification created: "
                f"type={notification_type}, symbol={symbol} "
                f"-> trinity_performance_notifications"
            )

            return {
                'inserted_id': str(result.inserted_id),
                'status': 'success',
                'action': 'created'
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error inserting notification: {e}")
            raise

    async def mark_as_read_with_retry(self, notification_id: str) -> Dict[str, Any]:
        """
        Mark notification as read with retry logic

        Args:
            notification_id: ID of the notification

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._mark_as_read(notification_id)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully marked as read after {attempt + 1} attempts")
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[SECONDARY DB] Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"[SECONDARY DB] All {self.max_retries} attempts failed. "
                        f"Notification NOT marked as read in secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _mark_as_read(self, notification_id: str) -> Dict[str, Any]:
        """
        Internal method to mark notification as read

        Args:
            notification_id: ID of the notification

        Returns:
            Dictionary with operation result
        """
        try:
            from bson import ObjectId
            collection = secondary_db_config.get_collection(self.collection_name)

            result = await collection.update_one(
                {'_id': ObjectId(notification_id)},
                {'$set': {'read': True, 'updatedAt': datetime.now()}}
            )

            logger.info(
                f"[SECONDARY DB] Notification marked as read: {notification_id} "
                f"-> trinity_performance_notifications"
            )

            return {
                'status': 'success',
                'action': 'updated',
                'modified_count': result.modified_count
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error marking notification as read: {e}")
            raise

    async def delete_old_notifications_with_retry(self, days: int) -> Dict[str, Any]:
        """
        Delete old notifications with retry logic

        Args:
            days: Delete notifications older than this many days

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._delete_old_notifications(days)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully deleted old notifications after {attempt + 1} attempts")
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[SECONDARY DB] Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"[SECONDARY DB] All {self.max_retries} attempts failed. "
                        f"Old notifications NOT deleted from secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _delete_old_notifications(self, days: int) -> Dict[str, Any]:
        """
        Internal method to delete old notifications

        Args:
            days: Delete notifications older than this many days

        Returns:
            Dictionary with operation result
        """
        try:
            from datetime import timedelta
            collection = secondary_db_config.get_collection(self.collection_name)

            cutoff_date = datetime.now() - timedelta(days=days)

            result = await collection.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })

            logger.info(
                f"[SECONDARY DB] Deleted {result.deleted_count} notifications older than {days} days "
                f"-> trinity_performance_notifications"
            )

            return {
                'status': 'success',
                'action': 'deleted',
                'deleted_count': result.deleted_count,
                'days': days
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error deleting old notifications: {e}")
            raise

    async def count_notifications(self, unread_only: bool = False) -> int:
        """
        Count notifications in secondary database

        Args:
            unread_only: Count only unread notifications

        Returns:
            Number of notifications
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            query = {'read': False} if unread_only else {}
            count = await collection.count_documents(query)
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting notifications: {e}")
            return 0
