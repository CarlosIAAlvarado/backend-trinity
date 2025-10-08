# ==========================
# Token Model
# ==========================
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

class TokenModel(BaseModel):
    """
    Token model following Single Responsibility Principle
    Represents a cryptocurrency token
    """
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="Token symbol")
    name: str = Field(..., description="Token name")
    cmc_id: int = Field(..., alias="cmcId", description="CoinMarketCap ID")
    market_cap: float = Field(..., alias="marketCap", description="Market capitalization")
    price: Optional[float] = Field(None, description="Current price")
    cmc_rank: Optional[int] = Field(None, alias="cmcRank", description="CoinMarketCap rank")
    exchanges: List[str] = Field(default_factory=list, description="List of exchanges")
    is_on_okx: bool = Field(False, alias="isOnOKX", description="Token listed on OKX")
    exchange_count: int = Field(0, alias="exchangeCount", description="Number of exchanges")
    last_updated: datetime = Field(default_factory=datetime.now, alias="lastUpdated")

class TokenResponse(BaseModel):
    """Response model for token endpoints"""
    model_config = ConfigDict(populate_by_name=True, by_alias=True)

    status: str
    message: str
    source: str
    count: int
    data: List[TokenModel]
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)
    api_error: Optional[str] = Field(None, alias="apiError", description="API error message if any")