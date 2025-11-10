from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

class TopPerformer(BaseModel):
    """Model for top/worst performing tokens (DEPRECATED - kept for backward compatibility)"""
    symbol: str
    name: str
    avg_performance: float

class TimeframeAnalysis(BaseModel):
    """Analysis data for a specific timeframe"""
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    # Accept lists of ANY dictionary (complete candle objects)
    # This allows storing full candle data: _id, symbol, timeframe, open, high, low, close, volume, timestamp, performance, etc.
    best: List[Dict[str, Any]] = Field(default=[], description="Best performing tokens with full candle data")
    worst: List[Dict[str, Any]] = Field(default=[], description="Worst performing tokens with full candle data")

class CandlesByTimeframe(BaseModel):
    """Container for all timeframe analyses"""
    model_config = ConfigDict(populate_by_name=True)

    timeframe_15m: Optional[TimeframeAnalysis] = Field(default=None, alias="15m")
    timeframe_30m: Optional[TimeframeAnalysis] = Field(default=None, alias="30m")
    timeframe_1h: Optional[TimeframeAnalysis] = Field(default=None, alias="1H")
    timeframe_4h: Optional[TimeframeAnalysis] = Field(default=None, alias="4H")
    timeframe_12h: Optional[TimeframeAnalysis] = Field(default=None, alias="12H")
    timeframe_1d: Optional[TimeframeAnalysis] = Field(default=None, alias="1D")

class MarketAnalysisModel(BaseModel):
    """
    NEW Market Analysis model with nested timeframe structure
    Contains analysis for multiple timeframes in a single document
    """
    model_config = ConfigDict(populate_by_name=True)

    direction: str = Field(..., description="SHORT, FLAT, or LONG")
    directionNumber: float = Field(..., description="0 for SHORT, 0.5 for FLAT, 1 for LONG")
    directionNumberReal: float = Field(..., description="Real direction number with decimals")
    candlesByTimeframe: CandlesByTimeframe = Field(..., description="Analysis grouped by timeframes")
    timestamp: datetime = Field(default_factory=datetime.now, description="Analysis timestamp")

# ===== OLD MODELS (keep for backward compatibility) =====

class MarketAnalysisModelOld(BaseModel):
    """
    OLD Market Analysis model (DEPRECATED)
    Kept for backward compatibility
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

    success: bool
    data: Optional[Dict[str, Any]] = None

class MarketAnalysisResponseOld(BaseModel):
    """OLD Response model (DEPRECATED)"""
    model_config = ConfigDict(populate_by_name=True)

    status: str
    message: str
    data: Optional[MarketAnalysisModelOld] = None
    timestamp: datetime = Field(default_factory=datetime.now)

class MarketHistoryResponse(BaseModel):
    """Response model for market analysis history"""
    model_config = ConfigDict(populate_by_name=True)

    status: str
    message: str
    count: int
    data: List[MarketAnalysisModel]
    timestamp: datetime = Field(default_factory=datetime.now)
