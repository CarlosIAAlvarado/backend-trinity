# Market Analysis - UPSERT Implementation

## Resumen

El sistema de Market Analysis ahora usa **UPSERT** (Update or Insert) en lugar de INSERT simple. Esto garantiza que **siempre existan exactamente 2 documentos** en MongoDB (uno para 12h y otro para 24h), eliminando completamente la acumulación de datos históricos.

---

## ¿Por Qué UPSERT?

### Problema Anterior (INSERT)
- Cada análisis creaba un **nuevo documento** en MongoDB
- Se acumulaban **cientos de registros duplicados** (767 en el caso real)
- Causaba confusión al recuperar datos (¿cuál es el más reciente?)
- Requería limpieza periódica automática
- Desperdiciaba espacio en base de datos

### Solución Actual (UPSERT)
- Cada análisis **actualiza** el documento existente o lo crea si no existe
- Mantiene **exactamente 2 documentos** (12h y 24h)
- Información siempre actualizada
- No requiere limpieza automática
- Base de datos limpia y eficiente

---

## Implementación Técnica

### Cambio en el Repositorio

**Archivo**: `backend/repositories/market_analysis_repository.py`

**Método modificado**: `insert_analysis()` → Ahora hace UPSERT

```python
async def insert_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert or update market analysis record (UPSERT)

    Uses upsert to ensure only ONE document per timeframe exists.
    If document exists for the timeframe, it updates it.
    If not, it creates a new one.

    This prevents database accumulation and ensures fresh data.
    """
    collection = db_config.get_collection(self.collection_name)

    timeframe = analysis_data.get('timeframe')
    current_time = datetime.now()
    analysis_data['updatedAt'] = current_time

    # Check if document exists
    existing = await collection.find_one({'timeframe': timeframe})

    if not existing:
        # First time: set createdAt
        analysis_data['createdAt'] = current_time
        action = "created"
    else:
        # Update: preserve original createdAt
        analysis_data['createdAt'] = existing.get('createdAt', current_time)
        action = "updated"

    # Upsert: update if exists, insert if not
    result = await collection.update_one(
        {'timeframe': timeframe},  # Filter by timeframe
        {'$set': analysis_data},   # Update/set all fields
        upsert=True                # Create if doesn't exist
    )

    logger.info(f"Market analysis {action} [{timeframe}]: {analysis_data['market_status']}")

    return {
        'modified_count': result.modified_count,
        'upserted_id': str(result.upserted_id) if result.upserted_id else None,
        'status': 'success',
        'action': action
    }
```

**Características clave**:
1. **Filtro**: `{'timeframe': timeframe}` - Busca documento por timeframe
2. **$set**: Actualiza todos los campos del documento
3. **upsert=True**: Si no existe, lo crea automáticamente
4. **createdAt preservation**: Mantiene la fecha de creación original
5. **updatedAt**: Siempre actualiza a la hora actual

---

## Comportamiento del Sistema

### Primera Ejecución (Colección Vacía)
```
Estado inicial: 0 documentos

Análisis 12h → CREA documento 12h → 1 documento
Análisis 24h → CREA documento 24h → 2 documentos

Estado final: 2 documentos (✓)
```

### Ejecuciones Posteriores (Colección con Datos)
```
Estado inicial: 2 documentos

Análisis 12h → ACTUALIZA documento 12h → 2 documentos
Análisis 24h → ACTUALIZA documento 24h → 2 documentos

Estado final: 2 documentos (✓)
```

### Múltiples Ejecuciones en Corto Tiempo
```
Estado inicial: 2 documentos

Análisis completo (12h + 24h) → 2 actualizaciones → 2 documentos
Análisis completo (12h + 24h) → 2 actualizaciones → 2 documentos
Análisis completo (12h + 24h) → 2 actualizaciones → 2 documentos

Estado final: 2 documentos (✓)
```

**Conclusión**: Sin importar cuántas veces se ejecute, **siempre habrá exactamente 2 documentos**.

---

## Prueba de Verificación

### Script de Prueba

**Archivo**: `backend/scripts/test_upsert.py`

**Qué hace**:
1. Conecta a MongoDB
2. Cuenta documentos iniciales
3. Ejecuta `analyze_and_save()` **3 veces**
4. Verifica que sigan existiendo solo **2 documentos**

**Resultado de la prueba**:
```
[2] Records BEFORE test: 2

[3] Running market analysis 3 times...
    Iteration 1: Current record count: 2
    Iteration 2: Current record count: 2
    Iteration 3: Current record count: 2

[4] Records AFTER test: 2

[5] Verification:
    [SUCCESS] Exactly 2 records exist (1 per timeframe)
```

✅ **PRUEBA EXITOSA**: Confirmado que UPSERT mantiene exactamente 2 documentos.

---

## Cambios en Otros Componentes

### 1. Scheduler Service

**Eliminado**:
- `cleanup_old_market_analysis_task()` - Ya no es necesaria
- Tarea programada de limpieza diaria - Removida

**Modificado**:
- Log de inicio ahora dice: `"Market analysis every 1 minute (UPSERT mode - always 2 records)"`

### 2. Market Analysis Service

**Eliminado**:
- `get_history()` - Ya no hay historial para consultar

**Sin cambios**:
- `analyze_market()` - Lógica de análisis igual
- `analyze_and_save()` - Usa el mismo método `insert_analysis()` que ahora hace UPSERT
- `get_latest_analysis()` - Funciona igual (siempre devuelve el documento único)

### 3. Market Analysis Controller

**Eliminado**:
- Endpoint `/history` - Ya no existe historial
- Método `get_history()` - Removido

**Endpoints restantes**:
- `GET /api/market-analysis/latest?timeframe=24h` - Obtiene análisis actual
- `POST /api/market-analysis/analyze?timeframe=24h` - Genera análisis inmediato

### 4. Repository Simplificado

**Eliminado**:
- `get_history()` - No hay historial
- `get_history_by_date_range()` - No hay historial
- `delete_old_records()` - No hay nada que limpiar

**Agregado**:
- `get_all_analyses()` - Devuelve los 2 documentos (útil para debugging)

**Sin cambios**:
- `get_latest_analysis()` - Devuelve documento único por timeframe
- `count_records()` - Cuenta documentos (siempre debería ser 2)

---

## Estructura de Documentos en MongoDB

### Documento de Market Analysis

```json
{
  "_id": ObjectId("..."),
  "timeframe": "24h",
  "market_status": "BAJISTA",
  "total_tokens": 91,
  "bullish_tokens": 30,
  "bearish_tokens": 55,
  "neutral_tokens": 6,
  "bullish_percentage": 32.97,
  "bearish_percentage": 60.44,
  "neutral_percentage": 6.59,
  "timestamp": ISODate("2025-10-17T14:37:43.359Z"),
  "createdAt": ISODate("2025-10-17T14:37:32.506Z"),  // Nunca cambia
  "updatedAt": ISODate("2025-10-17T14:37:43.359Z"),  // Se actualiza cada vez
  "top_performers": [...],
  "worst_performers": [...]
}
```

**Campos importantes**:
- `timeframe`: Identificador único (12h o 24h)
- `createdAt`: Cuándo se creó el documento por primera vez (inmutable)
- `updatedAt`: Cuándo se actualizó por última vez (cambia en cada análisis)
- `timestamp`: Cuándo se realizó el análisis actual

---

## Monitoreo

### Verificar Cantidad de Documentos

```python
from config.database import db_config
from repositories.market_analysis_repository import MarketAnalysisRepository

# Conectar a base de datos
await db_config.connect()

# Contar documentos
repo = MarketAnalysisRepository()
count = await repo.count_records()

print(f"Total de documentos: {count}")

# ESPERADO: 2
# ALERTA SI: != 2
```

### Logs a Monitorear

**Primera ejecución** (documentos no existen):
```
[MARKET ANALYSIS] [24h] Status: BAJISTA
Market analysis created [24h]: BAJISTA
```

**Ejecuciones posteriores** (documentos ya existen):
```
[MARKET ANALYSIS] [24h] Status: BAJISTA
Market analysis updated [24h]: BAJISTA
```

**Nota**: La palabra clave cambia de **"created"** a **"updated"** después de la primera vez.

---

## Ventajas del Sistema UPSERT

### 1. Base de Datos Limpia
- Solo 2 documentos en todo momento
- No requiere limpieza periódica
- Fácil de respaldar y mantener

### 2. Performance Mejorado
- Queries más rápidos (menos documentos que filtrar)
- Menos espacio en disco
- MongoDB optimiza mejor con colecciones pequeñas

### 3. Código Más Simple
- No necesita lógica de limpieza
- No hay confusión sobre "cuál es el último"
- Menos endpoints en el API

### 4. Datos Siempre Actualizados
- `get_latest_analysis()` siempre devuelve el único documento
- No hay riesgo de obtener datos viejos
- `updatedAt` muestra claramente cuándo fue la última actualización

### 5. Predecible
- Siempre 2 documentos, sin excepciones
- Fácil de testear y verificar
- Comportamiento consistente

---

## Comparación: ANTES vs DESPUÉS

| Aspecto | ANTES (INSERT) | DESPUÉS (UPSERT) |
|---------|---------------|------------------|
| **Documentos en DB** | Cientos (767 en un caso real) | Siempre 2 |
| **Limpieza automática** | Necesaria (cada 24 horas) | No necesaria |
| **Espacio en disco** | Crece constantemente | Fijo (2 documentos) |
| **Query complexity** | Requiere sort y filtrado | Directo por timeframe |
| **Riesgo de datos viejos** | Alto (sort incorrecto) | Cero |
| **Mantenimiento** | Alto | Mínimo |
| **Historial** | Disponible (pero causaba problemas) | No disponible (no necesario) |

---

## Preguntas Frecuentes

### ¿Qué pasa si quiero historial en el futuro?

**Opción 1**: Crear colección separada para historial
```python
# Después del análisis exitoso
await history_collection.insert_one({
    'timeframe': timeframe,
    'market_status': status,
    'timestamp': datetime.now(),
    ...
})
```

**Opción 2**: Usar MongoDB Change Streams para capturar cambios
```python
# Escuchar cambios en marketAnalysis
with collection.watch() as stream:
    for change in stream:
        # Guardar en marketAnalysisHistory
        await history_collection.insert_one(change['fullDocument'])
```

### ¿Cómo pruebo en local que UPSERT funciona?

```bash
cd backend
python scripts/test_upsert.py
```

Deberías ver:
```
[SUCCESS] Exactly 2 records exist (1 per timeframe)
```

### ¿Qué pasa si borro manualmente los documentos?

La próxima vez que se ejecute `analyze_and_save()`, los documentos se **recrearán automáticamente**. UPSERT maneja tanto creación como actualización.

### ¿El Event-Driven system sigue funcionando?

Sí, completamente compatible. El event system llama a `analyze_and_save()`, que internamente usa `insert_analysis()`, que ahora hace UPSERT. Todo funciona igual, pero sin acumular documentos.

---

## Conclusión

✅ **UPSERT implementado exitosamente**
✅ **Siempre 2 documentos en base de datos**
✅ **No requiere limpieza automática**
✅ **Sistema simplificado y eficiente**
✅ **Compatible con event-driven architecture**

**Resultado**: Sistema más limpio, rápido y confiable que garantiza información siempre actualizada sin acumulación de datos históricos.
