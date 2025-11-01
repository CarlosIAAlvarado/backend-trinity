from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class NotificationModel(BaseModel):
    """
    Model for system notifications
    Used to notify users about important events like tokens becoming available
    """
    type: str = Field(..., description="Notification type: token_available, token_unavailable, system_update")
    title: str = Field(..., description="Notification title")
    message: str = Field(..., description="Notification message")
    symbol: Optional[str] = Field(None, description="Token symbol (if applicable)")
    data: Optional[dict] = Field(default_factory=dict, description="Additional data")
    read: bool = Field(default=False, description="Whether notification has been read")
    timestamp: datetime = Field(default_factory=datetime.now, description="When notification was created")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "token_available",
                "title": "Token Now Available",
                "message": "BTC is now available in OKX Exchange",
                "symbol": "BTC",
                "data": {"market_cap": 1300000000000},
                "read": False,
                "timestamp": "2025-10-31T12:00:00Z"
            }
        }

class NotificationResponse(BaseModel):
    """Response model for notifications"""
    status: str
    message: str
    count: int
    unread_count: int = 0
    data: List[NotificationModel]
