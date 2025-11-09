from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

class CandleModel(BaseModel):
    """
    Candle model representing OHLC (Open, High, Low, Close) data
    for a specific token and timeframe
    """
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="Token symbol (uppercase)")
    name: str = Field(default="", description="Token name (e.g., Bitcoin, Ethereum)")
    timeframe: str = Field(..., description="Timeframe: 15m, 30m, 1h, 12h, 1d")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    performance: float = Field(..., description="Performance percentage: ((close - open) / open) * 100")
    openTimestamp: datetime = Field(..., description="Timestamp when candle opened")
    closeTimestamp: datetime = Field(..., description="Timestamp when candle closes")
    timestamp: Optional[datetime] = Field(None, description="Deprecated - use openTimestamp instead")

class CandleResponse(BaseModel):
    """Response model for candle endpoints"""
    model_config = ConfigDict(populate_by_name=True, by_alias=True)

    status: str
    message: str
    count: int
    data: List[CandleModel]
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)
