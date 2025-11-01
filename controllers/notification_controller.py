from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import logging
from services.notification_service import notification_service
from models.notification_model import NotificationResponse

logger = logging.getLogger(__name__)

class NotificationController:
    """
    REST API Controller for notification operations
    Manages system notifications for token availability and other events
    """

    def __init__(self):
        self.router = APIRouter(prefix="/api/notifications", tags=["notifications"])
        self.service = notification_service
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""
        self.router.add_api_route(
            "",
            self.get_all_notifications,
            methods=["GET"],
            response_model=NotificationResponse,
            summary="Get all notifications with optional filters"
        )
        self.router.add_api_route(
            "/unread-count",
            self.get_unread_count,
            methods=["GET"],
            summary="Get count of unread notifications"
        )
        self.router.add_api_route(
            "/{notification_id}/read",
            self.mark_notification_as_read,
            methods=["PUT"],
            summary="Mark a specific notification as read"
        )
        self.router.add_api_route(
            "/mark-all-read",
            self.mark_all_as_read,
            methods=["PUT"],
            summary="Mark all notifications as read"
        )
        self.router.add_api_route(
            "/cleanup",
            self.cleanup_old_notifications,
            methods=["DELETE"],
            summary="Delete notifications older than specified days"
        )

    async def get_all_notifications(
        self,
        unread_only: bool = Query(default=False, description="Filter only unread notifications"),
        limit: int = Query(default=100, description="Maximum number of notifications to return"),
        skip: int = Query(default=0, description="Number of notifications to skip for pagination")
    ) -> NotificationResponse:
        """
        Get all notifications with optional filters

        Returns notifications sorted by timestamp (newest first)
        """
        try:
            logger.info(f"GET /api/notifications - unread_only={unread_only}, limit={limit}, skip={skip}")
            response = await self.service.get_all_notifications(
                unread_only=unread_only,
                limit=limit,
                skip=skip
            )
            return response

        except Exception as e:
            logger.error(f"Error in get_all_notifications endpoint: {e}")
            return NotificationResponse(
                status="error",
                message=str(e),
                count=0,
                unread_count=0,
                data=[]
            )

    async def get_unread_count(self):
        """
        Get count of unread notifications

        Returns:
            Dictionary with unread notification count
        """
        try:
            logger.info("GET /api/notifications/unread-count")
            unread_count = await self.service.get_unread_count()

            return {
                "status": "success",
                "unread_count": unread_count
            }

        except Exception as e:
            logger.error(f"Error in get_unread_count endpoint: {e}")
            return {
                "status": "error",
                "message": str(e),
                "unread_count": 0
            }

    async def mark_notification_as_read(self, notification_id: str):
        """
        Mark a specific notification as read

        Args:
            notification_id: ID of the notification to mark as read

        Returns:
            Success/failure status
        """
        try:
            logger.info(f"PUT /api/notifications/{notification_id}/read")
            result = await self.service.mark_as_read(notification_id)

            if result:
                return {
                    "status": "success",
                    "message": f"Notification {notification_id} marked as read"
                }
            else:
                raise HTTPException(status_code=404, detail="Notification not found")

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in mark_notification_as_read endpoint: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    async def mark_all_as_read(self):
        """
        Mark all notifications as read

        Returns:
            Number of notifications marked as read
        """
        try:
            logger.info("PUT /api/notifications/mark-all-read")
            modified_count = await self.service.mark_all_as_read()

            return {
                "status": "success",
                "message": f"Marked {modified_count} notifications as read",
                "modified_count": modified_count
            }

        except Exception as e:
            logger.error(f"Error in mark_all_as_read endpoint: {e}")
            return {
                "status": "error",
                "message": str(e),
                "modified_count": 0
            }

    async def cleanup_old_notifications(
        self,
        days: int = Query(default=30, description="Delete notifications older than this many days")
    ):
        """
        Delete notifications older than specified days

        Args:
            days: Delete notifications older than this many days (default: 30)

        Returns:
            Number of deleted notifications
        """
        try:
            logger.info(f"DELETE /api/notifications/cleanup?days={days}")
            deleted_count = await self.service.delete_old_notifications(days)

            return {
                "status": "success",
                "message": f"Deleted {deleted_count} notifications older than {days} days",
                "deleted_count": deleted_count
            }

        except Exception as e:
            logger.error(f"Error in cleanup_old_notifications endpoint: {e}")
            return {
                "status": "error",
                "message": str(e),
                "deleted_count": 0
            }

# Create singleton instance
notification_controller = NotificationController()
