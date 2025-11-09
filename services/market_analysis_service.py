import logging
import json
from typing import Dict, Any, List
from datetime import datetime
from repositories.market_analysis_repository import MarketAnalysisRepository
from repositories.secondary_market_analysis_repository import SecondaryMarketAnalysisRepository
from repositories.candle_repository import CandleRepository
from models.market_analysis_model import (
    MarketAnalysisModel,
    CandlesByTimeframe,
    TimeframeAnalysis,
    TopPerformer
)

logger = logging.getLogger(__name__)

class MarketAnalysisService:
    """
    Market Analysis Service
    Generates nested structure with all timeframes in a single document
    """

    def __init__(self):
        self.market_repository = MarketAnalysisRepository()
        self.secondary_market_repository = SecondaryMarketAnalysisRepository()
        self.candle_repository = CandleRepository()
        self.available_timeframes = ['15m', '30m', '1h', '4h', '12h', '1d']

    async def analyze_all_timeframes(self) -> MarketAnalysisModel:
        """
        Analyze all timeframes and generate nested structure
        Returns a single document with all timeframe analyses
        """
        try:
            logger.info("=" * 70)
            logger.info("[MARKET ANALYSIS NEW] Starting analysis for ALL timeframes")
            logger.info("=" * 70)

            # Dictionary to hold analysis for each timeframe
            timeframe_analyses = {}
            total_bullish = 0
            total_bearish = 0
            total_count = 0

            # Analyze each timeframe
            for timeframe in self.available_timeframes:
                logger.info(f"[MARKET ANALYSIS] Analyzing {timeframe}...")

                # Get candles for this timeframe
                candles = await self.candle_repository.find_by_timeframe(timeframe)

                if not candles or len(candles) == 0:
                    logger.warning(f"No candles found for {timeframe}, skipping...")
                    timeframe_analyses[timeframe] = TimeframeAnalysis(best=[], worst=[])
                    continue

                # Collect token data
                token_data = []
                bullish = 0
                bearish = 0

                for candle in candles:
                    performance = candle.get('performance', 0)
                    token_data.append({
                        'symbol': candle['symbol'],
                        'name': candle.get('name', candle['symbol']),
                        'performance': round(performance, 2)
                    })

                    if performance > 0:
                        bullish += 1
                    elif performance < 0:
                        bearish += 1

                # Sort by performance
                token_data.sort(key=lambda x: x['performance'], reverse=True)

                # Get best and worst performers
                best_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[:10]  # Top 10
                ]

                worst_performers = [
                    TopPerformer(
                        symbol=t['symbol'],
                        name=t['name'],
                        avg_performance=t['performance']
                    )
                    for t in token_data[-10:]  # Worst 10
                ]

                # Create TimeframeAnalysis
                timeframe_analyses[timeframe] = TimeframeAnalysis(
                    best=best_performers,
                    worst=worst_performers
                )

                # Accumulate for overall direction
                total_bullish += bullish
                total_bearish += bearish
                total_count += len(candles)

                logger.info(f"[{timeframe}] Bullish: {bullish}, Bearish: {bearish}, Total: {len(candles)}")

            # Calculate overall market direction
            # Thresholds:
            # - LONG (ALCISTA): >= 60% bullish
            # - SHORT (BAJISTA): <= 40% bullish
            # - FLAT (LATERAL): between 40% and 60%

            if total_count > 0:
                bullish_percentage = (total_bullish / total_count) * 100
                bearish_percentage = (total_bearish / total_count) * 100

                # Determine market direction based on thresholds
                if bullish_percentage >= 60:
                    direction = "LONG"
                    direction_number = 1
                elif bullish_percentage <= 40:
                    direction = "SHORT"
                    direction_number = 0
                else:
                    direction = "FLAT"
                    direction_number = 0.5  # Flat is between 0 and 1

                direction_number_real = bullish_percentage / 100
            else:
                direction = "FLAT"
                direction_number = 0.5
                direction_number_real = 0.5
                bullish_percentage = 0

            logger.info(f"[OVERALL] Direction: {direction} ({bullish_percentage:.2f}% bullish, Threshold: 60%+ = LONG, 40%- = SHORT)")

            # Create CandlesByTimeframe object
            candles_by_timeframe = CandlesByTimeframe(
                timeframe_15m=timeframe_analyses.get('15m'),
                timeframe_30m=timeframe_analyses.get('30m'),
                timeframe_1h=timeframe_analyses.get('1h'),
                timeframe_4h=timeframe_analyses.get('4h'),
                timeframe_12h=timeframe_analyses.get('12h'),
                timeframe_1d=timeframe_analyses.get('1d')
            )

            # Create final analysis model
            analysis = MarketAnalysisModel(
                direction=direction,
                directionNumber=direction_number,
                directionNumberReal=round(direction_number_real, 4),
                candlesByTimeframe=candles_by_timeframe,
                timestamp=datetime.now()
            )

            logger.info("=" * 70)
            logger.info(f"[MARKET ANALYSIS NEW] Analysis completed: {direction}")
            logger.info("=" * 70)

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing all timeframes: {e}")
            raise

    def _serialize_for_json(self, obj: Any) -> Any:
        """
        Recursively serialize datetime and ObjectId objects for JSON
        """
        from bson import ObjectId

        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            # Remove _id field if present (MongoDB internal field)
            return {
                key: self._serialize_for_json(value)
                for key, value in obj.items()
                if key != '_id'
            }
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        else:
            return obj

    async def save_analysis(self, analysis: MarketAnalysisModel) -> Dict[str, Any]:
        """
        Save analysis to BOTH databases (primary and secondary)
        Returns the analysis dict WITHOUT MongoDB's _id field, fully JSON serializable
        """
        try:
            # Convert to dict using model_dump with by_alias=True to use aliases
            analysis_dict = analysis.model_dump(by_alias=True, mode='json')

            # Save to primary database (MongoDB accepts datetime objects)
            result = await self.market_repository.insert_analysis(analysis_dict)
            logger.info("[PRIMARY DB] Market analysis saved")

            # Save to secondary database with retry
            try:
                await self.secondary_market_repository.insert_analysis_with_retry(analysis_dict)
                logger.info("[SECONDARY DB] Market analysis synced")
            except Exception as secondary_error:
                logger.error(f"[SECONDARY DB] Failed to sync: {secondary_error}")
                # Don't raise error, primary is already saved

            # Serialize datetime objects to strings for JSON response
            json_safe_dict = self._serialize_for_json(analysis_dict)

            return json_safe_dict

        except Exception as e:
            logger.error(f"Error saving market analysis: {e}")
            raise

# Singleton instance
market_analysis_service = MarketAnalysisService()
