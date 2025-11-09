# Lógica de Dirección del Mercado

## 📊 Cómo se determina la dirección del mercado

El sistema analiza TODOS los tokens en TODOS los timeframes y calcula el porcentaje de tokens con rendimiento positivo (bullish) vs negativo (bearish).

## 🎯 Umbrales de Decisión

| Condición | Dirección | directionNumber | Descripción |
|-----------|-----------|-----------------|-------------|
| **>= 60%** tokens alcistas | **LONG** | `1` | Mercado ALCISTA |
| **<= 40%** tokens alcistas | **SHORT** | `0` | Mercado BAJISTA |
| **40% - 60%** tokens alcistas | **FLAT** | `0.5` | Mercado LATERAL |

## 📐 Cálculo

```python
# Contar tokens por todos los timeframes
total_bullish = 0
total_bearish = 0
total_count = 0

for timeframe in ['15m', '30m', '1h', '4h', '12h', '1d']:
    for candle in candles[timeframe]:
        if candle.performance > 0:
            total_bullish += 1
        elif candle.performance < 0:
            total_bearish += 1
        total_count += 1

# Calcular porcentaje
bullish_percentage = (total_bullish / total_count) * 100

# Determinar dirección
if bullish_percentage >= 60:
    direction = "LONG"
    direction_number = 1
elif bullish_percentage <= 40:
    direction = "SHORT"
    direction_number = 0
else:
    direction = "FLAT"
    direction_number = 0.5
```

## 📈 Ejemplos

### Ejemplo 1: Mercado ALCISTA
```
Total tokens: 438 (73 tokens × 6 timeframes)
Tokens alcistas: 303 (69.2%)
Tokens bajistas: 135 (30.8%)

Resultado:
- direction: "LONG"
- directionNumber: 1
- directionNumberReal: 0.692
```

### Ejemplo 2: Mercado BAJISTA
```
Total tokens: 438
Tokens alcistas: 131 (29.9%)
Tokens bajistas: 307 (70.1%)

Resultado:
- direction: "SHORT"
- directionNumber: 0
- directionNumberReal: 0.299
```

### Ejemplo 3: Mercado FLAT
```
Total tokens: 438
Tokens alcistas: 219 (50%)
Tokens bajistas: 219 (50%)

Resultado:
- direction: "FLAT"
- directionNumber: 0.5
- directionNumberReal: 0.50
```

## 🔢 Campos de respuesta

### `direction`
**Tipo:** string
**Valores:** `"SHORT"`, `"FLAT"`, `"LONG"`
**Descripción:** Dirección general del mercado basada en los umbrales

### `directionNumber`
**Tipo:** float
**Valores:** `0`, `0.5`, `1`
**Descripción:** Representación numérica discreta de la dirección
- `0` = SHORT (bajista)
- `0.5` = FLAT (lateral)
- `1` = LONG (alcista)

### `directionNumberReal`
**Tipo:** float
**Rango:** `0.0 - 1.0`
**Descripción:** Porcentaje real de tokens alcistas dividido por 100
- `0.0` = 0% tokens alcistas
- `0.5` = 50% tokens alcistas
- `1.0` = 100% tokens alcistas

## 🎨 Uso en Frontend

### Código de ejemplo:
```typescript
interface MarketAnalysis {
  direction: 'SHORT' | 'FLAT' | 'LONG';
  directionNumber: 0 | 0.5 | 1;
  directionNumberReal: number;
}

function getMarketColor(direction: string): string {
  switch(direction) {
    case 'LONG':
      return '#22c55e';  // Verde (alcista)
    case 'SHORT':
      return '#ef4444';  // Rojo (bajista)
    case 'FLAT':
      return '#eab308';  // Amarillo (lateral)
    default:
      return '#6b7280';  // Gris
  }
}

function getMarketIcon(direction: string): string {
  switch(direction) {
    case 'LONG':
      return '📈';  // Tendencia alcista
    case 'SHORT':
      return '📉';  // Tendencia bajista
    case 'FLAT':
      return '➡️';  // Lateral
  }
}
```

## 📊 Visualización Recomendada

```
LONG (>=60%)  ████████████░░ 69.2% ▲ ALCISTA
FLAT (40-60%) ██████░░░░░░░░ 50.0% → LATERAL
SHORT (<=40%) ███░░░░░░░░░░░ 29.9% ▼ BAJISTA
```

## ⚙️ Configuración

Los umbrales están definidos en:
- Archivo: `backend/services/market_analysis_service.py`
- Líneas: ~111-130

Para modificar los umbrales, edita estas líneas:
```python
if bullish_percentage >= 60:  # Cambiar 60 a tu valor
    direction = "LONG"
elif bullish_percentage <= 40:  # Cambiar 40 a tu valor
    direction = "SHORT"
```

## 🔄 Actualización

El análisis se actualiza cada vez que se ejecuta:
```bash
POST /api/market-analysis/analyze
```

Este endpoint:
1. Analiza los 6 timeframes
2. Cuenta tokens alcistas/bajistas
3. Calcula porcentaje global
4. Determina dirección según umbrales
5. Guarda en ambas bases de datos
6. Retorna estructura completa

---

**Última actualización:** 2025-11-08
**Versión:** 2.0 (nueva estructura anidada)
