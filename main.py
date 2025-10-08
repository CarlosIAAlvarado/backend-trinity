# ==========================
# Main Application - Unified Server
# ==========================
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os
import socketio
from controllers.token_controller import TokenController
from controllers.config_controller import ConfigController
from controllers.candlestick_controller import candlestick_controller
from controllers.market_analysis_controller import market_analysis_controller
from config.database import db_config
from services.scheduler_service import scheduler_service
from services.websocket_service import websocket_service
from services.okx_websocket_service import okx_websocket_service
from services.market_analysis_service import market_analysis_service
from repositories.candle_repository import CandleRepository
from repositories.token_repository import TokenRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager
    """
    # Startup
    logger.info("="*70)
    logger.info(" TRINITY MARKET CAP - UNIFIED SERVER")
    logger.info("="*70)
    logger.info("Starting application...")

    # Connect to database
    await db_config.connect()
    logger.info("MongoDB: Connected successfully")

    # Inject dependencies into OKX WebSocket service
    candle_repo = CandleRepository()
    token_repo = TokenRepository()
    okx_websocket_service.inject_dependencies(
        candle_repository=candle_repo,
        token_repository=token_repo,
        websocket_service=websocket_service
    )

    # Start OKX WebSocket service for real-time price updates
    await okx_websocket_service.start()
    logger.info("OKX WebSocket: Started - Real-time price updates active")

    # Inject market analysis service into scheduler
    scheduler_service.inject_market_analysis_service(market_analysis_service)

    # Start scheduler for automatic updates
    scheduler_service.start()
    logger.info("Scheduler: Started - Timeframe updates running")
    logger.info("Scheduler: Market analysis running every 5 minutes")

    # Get initial configuration
    config = await websocket_service.get_global_config()
    logger.info(f"Global Market Cap Filter: ${config['market_cap_filter']:,}")
    logger.info(f"Global Update Interval: {config['update_interval_hours']} hours")

    logger.info("="*70)
    logger.info(" Server running on http://localhost:8000")
    logger.info(" WebSocket available at ws://localhost:8000/socket.io/")
    logger.info(" API Documentation at http://localhost:8000/docs")
    logger.info(" REAL-TIME: Live price updates + automatic timeframe updates")
    logger.info("="*70)

    yield

    # Shutdown
    logger.info("Shutting down application...")
    await okx_websocket_service.stop()
    scheduler_service.stop()
    await db_config.disconnect()
    logger.info("Application shut down successfully")

# Create FastAPI application
app = FastAPI(
    title="Trinity Market Cap API",
    description="Unified microservice for cryptocurrency market data with real-time WebSocket support",
    version="2.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount SocketIO application
sio_asgi_app = socketio.ASGIApp(
    websocket_service.sio,
    other_asgi_app=app,
    socketio_path='/socket.io'
)

# Register controllers
token_controller = TokenController()
config_controller = ConfigController()

app.include_router(token_controller.router)
app.include_router(config_controller.router)
app.include_router(candlestick_controller.router)
app.include_router(market_analysis_controller.router)

# Health check endpoint
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "Trinity Market Cap Unified API",
        "version": "2.0.0",
        "features": ["REST API", "WebSocket", "Scheduler", "MongoDB"]
    }

# Root endpoint
@app.get("/")
async def root():
    """
    Root endpoint
    """
    return {
        "message": "Trinity Market Cap Unified API",
        "version": "2.0.0",
        "documentation": "/docs",
        "health": "/health",
        "websocket": "ws://localhost:8000/socket.io/",
        "endpoints": {
            "tokens": "/api/tokens",
            "config": "/api/config",
            "candlesticks": "/api/candlesticks",
            "market_analysis": "/api/market-analysis"
        }
    }

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")

    # Run unified server with SocketIO integration
    uvicorn.run(
        sio_asgi_app,
        host=host,
        port=port,
        log_level="info"
    )
