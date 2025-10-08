from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

class FailedTokenModel(BaseModel):
    """
    Model representing a token that failed to retrieve data from OKX
    Used to track which tokens are not available on OKX Exchange
    """
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="Token symbol (e.g., HYPE, USDT)")
    name: str = Field(..., description="Token full name (e.g., Hyperliquid, Tether)")
    market_cap: Optional[float] = Field(None, description="Market capitalization in USD")
    rank: Optional[int] = Field(None, description="CoinMarketCap rank")
    attempted_pair: str = Field(..., description="Trading pair attempted (e.g., HYPE-USDT)")
    reason: str = Field(..., description="Failure reason from OKX API")
    timeframes_failed: List[str] = Field(default_factory=list, description="List of timeframes that failed")
    total_attempts: int = Field(default=0, description="Number of timeframes attempted")
    timestamp: datetime = Field(default_factory=datetime.now, description="When the failure was recorded")

class FailedTokenResponse(BaseModel):
    """Response model for failed token endpoints"""
    model_config = ConfigDict(populate_by_name=True)

    status: str
    message: str
    count: int
    data: List[FailedTokenModel]
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

class FailedTokenStats(BaseModel):
    """Statistics about failed tokens"""
    model_config = ConfigDict(populate_by_name=True)

    total_tokens_attempted: int = Field(..., description="Total tokens processed")
    successful_tokens: int = Field(..., description="Tokens with data in OKX")
    failed_tokens: int = Field(..., description="Tokens not available in OKX")
    success_rate: float = Field(..., description="Success rate percentage")
    total_candlesticks: int = Field(..., description="Total candlesticks inserted")
    last_update: datetime = Field(default_factory=datetime.now, description="Last update timestamp")
