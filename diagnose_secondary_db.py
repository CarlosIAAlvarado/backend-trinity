"""
Script de diagnóstico para verificar conexión a base de datos secundaria
Ejecutar en producción para identificar problemas
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def diagnose():
    """Diagnosticar configuración de base de datos secundaria"""

    print("="*70)
    print("DIAGNÓSTICO DE BASE DE DATOS SECUNDARIA")
    print("="*70)

    # 1. Verificar variables de entorno
    print("\n1. VERIFICANDO VARIABLES DE ENTORNO...")

    primary_uri = os.getenv('MONGODB_URI')
    primary_db = os.getenv('DB_NAME')
    secondary_uri = os.getenv('SECONDARY_MONGODB_URI')
    secondary_db = os.getenv('SECONDARY_DB_NAME')

    print(f"   PRIMARY_URI: {'✓ Configurado' if primary_uri else '✗ NO ENCONTRADO'}")
    if primary_uri:
        # Ocultar password
        safe_uri = primary_uri.split('@')[1] if '@' in primary_uri else primary_uri
        print(f"   PRIMARY_URI (seguro): ...@{safe_uri}")

    print(f"   PRIMARY_DB: {primary_db if primary_db else '✗ NO ENCONTRADO'}")
    print(f"   SECONDARY_URI: {'✓ Configurado' if secondary_uri else '✗ NO ENCONTRADO'}")

    if secondary_uri:
        # Ocultar password
        safe_uri = secondary_uri.split('@')[1] if '@' in secondary_uri else secondary_uri
        print(f"   SECONDARY_URI (seguro): ...@{safe_uri}")

    print(f"   SECONDARY_DB: {secondary_db if secondary_db else '✗ NO ENCONTRADO'}")

    if not secondary_uri or not secondary_db:
        print("\n⚠️  PROBLEMA ENCONTRADO:")
        print("   Las variables SECONDARY_MONGODB_URI y/o SECONDARY_DB_NAME no están configuradas")
        print("\n   SOLUCIÓN:")
        print("   Agrega estas variables a tu archivo .env o configuración del servidor:")
        print("   SECONDARY_MONGODB_URI=mongodb+srv://urieldev:urieldev@cluster0.yru42a6.mongodb.net/")
        print("   SECONDARY_DB_NAME=Dev")
        return False

    # 2. Intentar conexión a PRIMARY
    print("\n2. PROBANDO CONEXIÓN A PRIMARY DATABASE...")
    try:
        from config.database import db_config
        await db_config.connect()
        print(f"   ✓ Conectado a PRIMARY: {primary_db}")
        await db_config.disconnect()
    except Exception as e:
        print(f"   ✗ Error conectando a PRIMARY: {e}")
        return False

    # 3. Intentar conexión a SECONDARY
    print("\n3. PROBANDO CONEXIÓN A SECONDARY DATABASE...")
    try:
        from config.database import secondary_db_config
        await secondary_db_config.connect()
        print(f"   ✓ Conectado a SECONDARY: {secondary_db}")

        # 4. Verificar si la colección existe
        print("\n4. VERIFICANDO COLECCIÓN EN SECONDARY...")
        collection = secondary_db_config.get_collection('trinity_performance_marketAnalysis')
        count = await collection.count_documents({})
        print(f"   ✓ Colección existe con {count} documentos")

        await secondary_db_config.disconnect()

    except Exception as e:
        print(f"   ✗ Error conectando a SECONDARY: {e}")
        print("\n   POSIBLES CAUSAS:")
        print("   1. IP del servidor no está en whitelist de MongoDB Atlas")
        print("   2. Credenciales incorrectas en SECONDARY_MONGODB_URI")
        print("   3. Firewall bloqueando la conexión")
        print("\n   SOLUCIONES:")
        print("   1. Ve a MongoDB Atlas → Security → Network Access")
        print("   2. Agrega la IP de tu servidor de producción")
        print("   3. O agrega 0.0.0.0/0 para permitir todas las IPs (solo para testing)")
        return False

    # 5. Probar escritura
    print("\n5. PROBANDO ESCRITURA EN SECONDARY...")
    try:
        from repositories.secondary_market_analysis_repository import SecondaryMarketAnalysisRepository
        from datetime import datetime

        repo = SecondaryMarketAnalysisRepository()
        await secondary_db_config.connect()

        test_data = {
            'timeframe': '12h',
            'market_status': 'TEST',
            'total_tokens': 0,
            'bullish_tokens': 0,
            'bearish_tokens': 0,
            'neutral_tokens': 0,
            'bullish_percentage': 0.0,
            'bearish_percentage': 0.0,
            'neutral_percentage': 0.0,
            'top_performers': [],
            'worst_performers': [],
            'timestamp': datetime.now()
        }

        result = await repo.insert_analysis_with_retry(test_data)

        if result.get('status') == 'success':
            print(f"   ✓ Escritura exitosa en SECONDARY")
        else:
            print(f"   ✗ Escritura falló: {result.get('message')}")
            return False

        await secondary_db_config.disconnect()

    except Exception as e:
        print(f"   ✗ Error escribiendo en SECONDARY: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("✓ TODOS LOS TESTS PASARON")
    print("✓ La base de datos secundaria está configurada correctamente")
    print("="*70)
    return True

if __name__ == "__main__":
    success = asyncio.run(diagnose())
    exit(0 if success else 1)
