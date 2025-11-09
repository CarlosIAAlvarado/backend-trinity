"""
Script de verificacion de timeframes
Verifica que los nuevos timeframes (4h y 1d) esten configurados correctamente
"""
import asyncio
import sys
import os

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import db_config
from services.candlestick_service import CandlestickService
from repositories.candle_repository import CandleRepository

async def verify_timeframes():
    """Verificar configuracion de timeframes"""

    print("=" * 70)
    print("VERIFICACION DE TIMEFRAMES")
    print("=" * 70)
    print()

    # 1. Verificar configuracion del servicio
    candlestick_service = CandlestickService()
    print("1. Timeframes configurados en CandlestickService:")
    print(f"   {candlestick_service.timeframes}")
    print()

    # 2. Conectar a la base de datos
    print("2. Conectando a MongoDB...")
    await db_config.connect()
    print("   Conectado exitosamente")
    print()

    # 3. Verificar candlesticks existentes por timeframe
    print("3. Candlesticks existentes en la base de datos:")
    candle_repo = CandleRepository()

    for timeframe in candlestick_service.timeframes:
        candles = await candle_repo.find_by_timeframe(timeframe)
        count = len(candles) if candles else 0
        status = "OK" if count > 0 else "VACIO"
        print(f"   [{status}] {timeframe:5s} -> {count} candlesticks")

    print()

    # 4. Mostrar resumen
    print("=" * 70)
    print("RESUMEN:")
    print("-" * 70)

    expected = ['15m', '30m', '1h', '4h', '12h', '1d']
    current = candlestick_service.timeframes

    missing = set(expected) - set(current)
    extra = set(current) - set(expected)

    if missing:
        print(f"TIMEFRAMES FALTANTES: {missing}")

    if extra:
        print(f"TIMEFRAMES EXTRA: {extra}")

    if not missing and not extra:
        print("Todos los timeframes esperados estan configurados correctamente")

    print("=" * 70)
    print()

    # 5. Verificar OKX Service mapping
    from services.okx_service import OKXService
    okx_service = OKXService()

    print("4. Mapeo de timeframes en OKXService:")
    for tf, okx_tf in okx_service.timeframe_map.items():
        print(f"   {tf:5s} -> {okx_tf}")
    print()

    # Desconectar
    await db_config.disconnect()

if __name__ == "__main__":
    asyncio.run(verify_timeframes())
