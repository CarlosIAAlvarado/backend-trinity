import logging
from typing import Dict, Any, List
from datetime import datetime
from collections import defaultdict
from repositories.market_analysis_repository import MarketAnalysisRepository
from repositories.candle_repository import CandleRepository
from models.market_analysis_model import MarketAnalysisModel, MarketAnalysisResponse, MarketHistoryResponse, TopPerformer

logger = logging.getLogger(__name__)

class MarketAnalysisService:
    """
    Service for market analysis operations
    Analyzes market sentiment based on token performance
    """

    def __init__(self):
        self.market_repository = MarketAnalysisRepository()
        self.candle_repository = CandleRepository()
        self.available_timeframes = ['12h', '24h']

    async def analyze_market(self, timeframe: str = '24h') -> MarketAnalysisModel:
        """
        Analyze market sentiment based on a specific timeframe (12h or 24h)
        Returns market status: ALCISTA, BAJISTA, or NEUTRAL

        Args:
            timeframe: '12h' or '24h' (default: '24h')
        """
        try:
            # Validate timeframe
            if timeframe not in self.available_timeframes:
                logger.warning(f"Invalid timeframe '{timeframe}', using default '24h'")
                timeframe = '24h'

            logger.info("=" * 70)
            logger.info(f"STARTING MARKET ANALYSIS - TIMEFRAME: {timeframe}")
            logger.info("=" * 70)

            collection = self.candle_repository.collection

            # Get candles for the specific timeframe only
            candles = await collection.find({
                'timeframe': timeframe
            }).to_list(length=None)

            logger.info(f"Retrieved {len(candles)} candles for {timeframe} timeframe")

            token_data = []
            bullish_count = 0
            bearish_count = 0
            neutral_count = 0

            for candle in candles:
                symbol = candle.get('symbol')
                performance = candle.get('performance', 0)
                name = candle.get('name', symbol)

                token_data.append({
                    'symbol': symbol,
                    'name': name,
                    'performance': round(performance, 2)
                })

                if performance > 0:
                    bullish_count += 1
                elif performance < 0:
                    bearish_count += 1
                else:
                    neutral_count += 1

            total_tokens = len(token_data)

            if total_tokens == 0:
                logger.warning(f"No tokens found for {timeframe} analysis")
                return self._create_empty_analysis(timeframe)

            bullish_percentage = (bullish_count / total_tokens) * 100
            bearish_percentage = (bearish_count / total_tokens) * 100
            neutral_percentage = (neutral_count / total_tokens) * 100

            if bullish_percentage >= 60:
                market_status = "ALCISTA"
            elif bearish_percentage >= 60:
                market_status = "BAJISTA"
            else:
                market_status = "NEUTRAL"

            # Sort by performance
            token_data.sort(key=lambda x: x['performance'], reverse=True)

            # Adjust performers based on market status
            if market_status == "ALCISTA":
                # Show top 10 best performers
                top_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[:10]
                ]
                worst_performers = []
            elif market_status == "BAJISTA":
                # Show top 10 worst performers
                top_performers = []
                worst_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[-10:]
                ]
            else:  # NEUTRAL
                # Show top 5 best and top 5 worst
                top_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[:5]
                ]
                worst_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[-5:]
                ]

            analysis = MarketAnalysisModel(
                market_status=market_status,
                timeframe=timeframe,
                total_tokens=total_tokens,
                bullish_tokens=bullish_count,
                bearish_tokens=bearish_count,
                neutral_tokens=neutral_count,
                bullish_percentage=round(bullish_percentage, 2),
                bearish_percentage=round(bearish_percentage, 2),
                neutral_percentage=round(neutral_percentage, 2),
                timestamp=datetime.now(),
                top_performers=top_performers,
                worst_performers=worst_performers
            )

            logger.info("=" * 70)
            logger.info(f"MARKET ANALYSIS COMPLETED [{timeframe}]: {market_status}")
            logger.info(f"Total Tokens: {total_tokens}")
            logger.info(f"Bullish: {bullish_count} ({bullish_percentage:.2f}%)")
            logger.info(f"Bearish: {bearish_count} ({bearish_percentage:.2f}%)")
            logger.info(f"Neutral: {neutral_count} ({neutral_percentage:.2f}%)")
            logger.info("=" * 70)

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing market for {timeframe}: {e}")
            raise

    async def analyze_and_save(self) -> Dict[str, Any]:
        """
        Analyze market for BOTH timeframes (12h and 24h) and save results to database
        Called by scheduler every 5 minutes
        """
        try:
            results = []

            # Analyze and save for each timeframe
            for timeframe in self.available_timeframes:
                analysis = await self.analyze_market(timeframe)
                analysis_dict = analysis.model_dump()
                await self.market_repository.insert_analysis(analysis_dict)

                logger.info(f"Market analysis saved [{timeframe}]: {analysis.market_status}")
                results.append({
                    'timeframe': timeframe,
                    'status': analysis.market_status
                })

            return {
                'status': 'success',
                'message': f'Market analysis completed for {len(results)} timeframes',
                'data': results
            }

        except Exception as e:
            logger.error(f"Error in analyze_and_save: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def get_latest_analysis(self, timeframe: str = '24h') -> MarketAnalysisResponse:
        """
        Get the most recent market analysis from database for specific timeframe

        Args:
            timeframe: '12h' or '24h' (default: '24h')
        """
        try:
            # Validate timeframe
            if timeframe not in self.available_timeframes:
                logger.warning(f"Invalid timeframe '{timeframe}', using default '24h'")
                timeframe = '24h'

            analysis_data = await self.market_repository.get_latest_analysis(timeframe)

            if not analysis_data:
                # Generate fresh analysis if no data exists
                analysis = await self.analyze_market(timeframe)
                return MarketAnalysisResponse(
                    status="success",
                    message=f"No historical data for {timeframe}, generated fresh analysis",
                    data=analysis
                )

            analysis_data.pop('_id', None)
            analysis_data.pop('createdAt', None)
            analysis_data.pop('updatedAt', None)

            analysis = MarketAnalysisModel(**analysis_data)

            return MarketAnalysisResponse(
                status="success",
                message=f"Latest market analysis retrieved for {timeframe}",
                data=analysis
            )

        except Exception as e:
            logger.error(f"Error getting latest analysis for {timeframe}: {e}")
            return MarketAnalysisResponse(
                status="error",
                message=str(e),
                data=None
            )

    async def get_history(self, limit: int = 100, timeframe: str = None) -> MarketHistoryResponse:
        """
        Get historical market analysis records

        Args:
            limit: Maximum number of records to retrieve
            timeframe: Filter by specific timeframe ('12h' or '24h'), or None for all
        """
        try:
            history_data = await self.market_repository.get_history(limit, timeframe)

            analyses = []
            for data in history_data:
                data.pop('_id', None)
                data.pop('createdAt', None)
                data.pop('updatedAt', None)
                analyses.append(MarketAnalysisModel(**data))

            timeframe_msg = f" for {timeframe}" if timeframe else ""
            return MarketHistoryResponse(
                status="success",
                message=f"Retrieved {len(analyses)} historical records{timeframe_msg}",
                count=len(analyses),
                data=analyses
            )

        except Exception as e:
            logger.error(f"Error getting market history: {e}")
            return MarketHistoryResponse(
                status="error",
                message=str(e),
                count=0,
                data=[]
            )

    def _create_empty_analysis(self, timeframe: str = '24h') -> MarketAnalysisModel:
        """
        Create an empty analysis when no data is available

        Args:
            timeframe: The timeframe for this empty analysis
        """
        return MarketAnalysisModel(
            market_status="NEUTRAL",
            timeframe=timeframe,
            total_tokens=0,
            bullish_tokens=0,
            bearish_tokens=0,
            neutral_tokens=0,
            bullish_percentage=0.0,
            bearish_percentage=0.0,
            neutral_percentage=0.0,
            timestamp=datetime.now(),
            top_performers=[],
            worst_performers=[]
        )

market_analysis_service = MarketAnalysisService()
