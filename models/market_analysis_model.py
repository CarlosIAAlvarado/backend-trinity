from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

class TopPerformer(BaseModel):
    """Model for top/worst performing tokens"""
    symbol: str
    name: str
    avg_performance: float

class MarketAnalysisModel(BaseModel):
    """
    Market Analysis model representing overall market sentiment
    Based on analysis of a specific timeframe (12h or 24h)
    """
    model_config = ConfigDict(populate_by_name=True)

    market_status: str = Field(..., description="ALCISTA, BAJISTA, or NEUTRAL")
    timeframe: str = Field(..., description="Timeframe analyzed: 12h or 24h")
    total_tokens: int = Field(..., description="Total number of tokens analyzed")
    bullish_tokens: int = Field(..., description="Number of tokens with positive performance")
    bearish_tokens: int = Field(..., description="Number of tokens with negative performance")
    neutral_tokens: int = Field(..., description="Number of tokens with zero performance")
    bullish_percentage: float = Field(..., description="Percentage of bullish tokens")
    bearish_percentage: float = Field(..., description="Percentage of bearish tokens")
    neutral_percentage: float = Field(..., description="Percentage of neutral tokens")
    timestamp: datetime = Field(default_factory=datetime.now, description="Analysis timestamp")
    top_performers: Optional[List[TopPerformer]] = Field(default=[], description="Top 10 performing tokens")
    worst_performers: Optional[List[TopPerformer]] = Field(default=[], description="Worst 10 performing tokens")

class MarketAnalysisResponse(BaseModel):
    """Response model for market analysis endpoints"""
    model_config = ConfigDict(populate_by_name=True)

    status: str
    message: str
    data: Optional[MarketAnalysisModel] = None
    timestamp: datetime = Field(default_factory=datetime.now)

class MarketHistoryResponse(BaseModel):
    """Response model for market analysis history"""
    model_config = ConfigDict(populate_by_name=True)

    status: str
    message: str
    count: int
    data: List[MarketAnalysisModel]
    timestamp: datetime = Field(default_factory=datetime.now)
