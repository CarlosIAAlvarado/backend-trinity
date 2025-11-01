import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from config.database import db_config

logger = logging.getLogger(__name__)

class NotificationRepository:
    """
    Repository for Notification operations
    Handles CRUD operations for trinityNotifications collection
    """

    def __init__(self):
        self.collection_name = 'trinityNotifications'

    @property
    def collection(self):
        """Get the MongoDB collection"""
        return db_config.get_collection(self.collection_name)

    async def insert_one(self, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single notification

        Args:
            notification_data: Dictionary with notification information

        Returns:
            Inserted notification with ID
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            # Add timestamp if not present
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = datetime.now()

            if 'read' not in notification_data:
                notification_data['read'] = False

            result = await collection.insert_one(notification_data)
            notification_data['_id'] = str(result.inserted_id)

            logger.info(f"Inserted notification: {notification_data.get('type')} - {notification_data.get('title')}")
            return notification_data

        except Exception as e:
            logger.error(f"Error inserting notification: {e}")
            raise

    async def insert_many(self, notifications: List[Dict[str, Any]]) -> int:
        """
        Insert multiple notifications at once

        Args:
            notifications: List of notification documents

        Returns:
            Number of inserted documents
        """
        try:
            if not notifications:
                return 0

            collection = db_config.get_collection(self.collection_name)

            # Add timestamps
            now = datetime.now()
            for notification in notifications:
                if 'timestamp' not in notification:
                    notification['timestamp'] = now
                if 'read' not in notification:
                    notification['read'] = False

            result = await collection.insert_many(notifications)
            inserted_count = len(result.inserted_ids)

            logger.info(f"Inserted {inserted_count} notifications")
            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting multiple notifications: {e}")
            raise

    async def find_all(
        self,
        unread_only: bool = False,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get all notifications with optional filters

        Args:
            unread_only: If True, only return unread notifications
            limit: Maximum number of notifications to return
            skip: Number of notifications to skip (for pagination)

        Returns:
            List of notifications sorted by timestamp (newest first)
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            query = {}
            if unread_only:
                query['read'] = False

            cursor = collection.find(query).sort('timestamp', -1).skip(skip).limit(limit)
            notifications = await cursor.to_list(length=limit)

            logger.info(f"Found {len(notifications)} notifications")
            return notifications

        except Exception as e:
            logger.error(f"Error finding notifications: {e}")
            raise

    async def find_by_type(
        self,
        notification_type: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get notifications by type

        Args:
            notification_type: Type of notification to filter
            limit: Maximum number of notifications

        Returns:
            List of notifications of specified type
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            cursor = collection.find({'type': notification_type}).sort('timestamp', -1).limit(limit)
            notifications = await cursor.to_list(length=limit)

            return notifications

        except Exception as e:
            logger.error(f"Error finding notifications by type: {e}")
            raise

    async def mark_as_read(self, notification_id: str) -> bool:
        """
        Mark a notification as read

        Args:
            notification_id: ID of notification to mark as read

        Returns:
            True if successful, False otherwise
        """
        try:
            from bson import ObjectId
            collection = db_config.get_collection(self.collection_name)

            result = await collection.update_one(
                {'_id': ObjectId(notification_id)},
                {'$set': {'read': True}}
            )

            return result.modified_count > 0

        except Exception as e:
            logger.error(f"Error marking notification as read: {e}")
            raise

    async def mark_all_as_read(self) -> int:
        """
        Mark all notifications as read

        Returns:
            Number of notifications marked as read
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            result = await collection.update_many(
                {'read': False},
                {'$set': {'read': True}}
            )

            logger.info(f"Marked {result.modified_count} notifications as read")
            return result.modified_count

        except Exception as e:
            logger.error(f"Error marking all notifications as read: {e}")
            raise

    async def count_unread(self) -> int:
        """
        Count unread notifications

        Returns:
            Number of unread notifications
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            count = await collection.count_documents({'read': False})
            return count

        except Exception as e:
            logger.error(f"Error counting unread notifications: {e}")
            raise

    async def delete_old_notifications(self, days: int = 30) -> int:
        """
        Delete notifications older than specified days

        Args:
            days: Delete notifications older than this many days

        Returns:
            Number of deleted notifications
        """
        try:
            collection = db_config.get_collection(self.collection_name)

            cutoff_date = datetime.now() - timedelta(days=days)
            result = await collection.delete_many({
                'timestamp': {'$lt': cutoff_date}
            })

            logger.info(f"Deleted {result.deleted_count} notifications older than {days} days")
            return result.deleted_count

        except Exception as e:
            logger.error(f"Error deleting old notifications: {e}")
            raise

    async def delete_all(self) -> int:
        """
        Delete all notifications (use with caution)

        Returns:
            Number of deleted notifications
        """
        try:
            collection = db_config.get_collection(self.collection_name)
            result = await collection.delete_many({})

            logger.info(f"Deleted ALL {result.deleted_count} notifications")
            return result.deleted_count

        except Exception as e:
            logger.error(f"Error deleting all notifications: {e}")
            raise
