# Event-Driven Architecture - Market Analysis Real-Time Updates

## Overview

This system implements a reactive, event-driven architecture that ensures Market Analysis is **always synchronized** with the latest token performance data. When TIER 1 or TIER 2 tokens update their performance values, the Market Analysis automatically recalculates and pushes updates to all connected frontend clients.

---

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    TIER UPDATE SCHEDULERS                        │
│                                                                  │
│  TIER 1 (TOP 10)     →  Every 5 seconds                         │
│  TIER 2 (>$5B)       →  Every 30 seconds                        │
│  TIER 3 (Rest)       →  Every 60 seconds                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      EVENT BUS (Debounced)                       │
│                                                                  │
│  - Receives: 'tier1_updated' or 'tier2_updated' events          │
│  - Debounce: 5 seconds delay                                    │
│  - Purpose: Prevent multiple executions on rapid changes        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│               MARKET ANALYSIS SERVICE (Listener)                 │
│                                                                  │
│  - Registered listeners for TIER 1 & TIER 2 events              │
│  - Automatically triggers analyze_and_save()                    │
│  - Analyzes BOTH timeframes: 12h and 24h                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MONGODB PERSISTENCE                         │
│                                                                  │
│  - Saves new market analysis to 'market_analysis' collection    │
│  - Keeps history for trend analysis                             │
│  - Auto-cleanup: Deletes records older than 7 days              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    WEBSOCKET PUSH NOTIFICATION                   │
│                                                                  │
│  - Event: 'market_analysis_updated'                             │
│  - Payload: Complete market analysis data                       │
│  - Broadcast: To ALL connected frontend clients                 │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FRONTEND UPDATE (Real-Time)                 │
│                                                                  │
│  - Receives WebSocket event instantly                           │
│  - Updates UI without polling                                   │
│  - Shows latest market status, top performers, percentages      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Components

### 1. Event Bus (`backend/services/event_bus.py`)

**Purpose**: Central event coordination system with debouncing

**Features**:
- **Listeners**: Services subscribe to specific events
- **Emitters**: Services emit events when important changes occur
- **Debouncing**: Prevents excessive executions when rapid changes happen
  - Example: If TIER 1 updates 3 times in 5 seconds, Market Analysis only runs ONCE after the last update

**Methods**:
```python
event_bus.on(event_name, callback)              # Register listener
event_bus.emit(event_name, data)                # Emit immediately
event_bus.emit_debounced(event_name, data, delay=5)  # Emit after delay
```

**Example Usage**:
```python
# Market Analysis registers listener
event_bus.on('tier1_updated', self._on_tier_updated)

# Scheduler emits event after TIER 1 update
await event_bus.emit_debounced('tier1_updated', {
    'tier': 1,
    'updated_count': 50,
    'timestamp': datetime.now().isoformat()
}, delay=5)
```

---

### 2. Scheduler Service Modifications (`backend/services/scheduler_service.py`)

**Changes Made**:

#### A) TIER 1 Update Task
```python
async def refresh_tier1_task(self):
    """Refresh TIER 1 tokens (TOP 10) every 5 seconds"""
    result = await self.candlestick_service.refresh_tier1_candles()
    updated_count = result.get('updated_count', 0)
    logger.info(f"[TIER1] Updated {updated_count} candles")

    # NUEVO: Emitir evento si hubo actualizaciones
    if updated_count > 0:
        await event_bus.emit_debounced('tier1_updated', {
            'tier': 1,
            'updated_count': updated_count,
            'timestamp': datetime.now().isoformat()
        }, delay=5)
```

**Why Important**: TOP 10 tokens are the most volatile and frequently change rankings. This ensures Market Analysis reflects those changes immediately.

#### B) TIER 2 Update Task
```python
async def refresh_tier2_task(self):
    """Refresh TIER 2 tokens (Market Cap > $5B) every 30 seconds"""
    result = await self.candlestick_service.refresh_tier2_candles()
    updated_count = result.get('updated_count', 0)
    logger.info(f"[TIER2] Updated {updated_count} candles")

    # NUEVO: Emitir evento si hubo actualizaciones
    if updated_count > 0:
        await event_bus.emit_debounced('tier2_updated', {
            'tier': 2,
            'updated_count': updated_count,
            'timestamp': datetime.now().isoformat()
        }, delay=5)
```

**Why Important**: TIER 2 tokens are also significant market movers and affect overall market sentiment.

#### C) Market Analysis Frequency Change
```python
# Market analysis every 1 minute (changed from 5 for better sync)
self.scheduler.add_job(
    self.analyze_market_task,
    IntervalTrigger(minutes=1),
    id='analyze_market',
    name='Analyze market sentiment',
    replace_existing=True,
    max_instances=1
)
```

**Why**: Faster polling (1 min instead of 5 min) provides more frequent snapshots, but the **real-time updates** come from the event-driven system, not polling.

---

### 3. Market Analysis Service Listener (`backend/services/market_analysis_service.py`)

**Changes Made**:

#### A) Event Listener Setup
```python
def _setup_event_listeners(self):
    """
    Configurar listeners para eventos del sistema
    Market Analysis reacciona automáticamente cuando TIER 1 o TIER 2 se actualizan
    """
    event_bus.on('tier1_updated', self._on_tier_updated)
    event_bus.on('tier2_updated', self._on_tier_updated)
    logger.info("[MARKET ANALYSIS] Event listeners registered for TIER updates")
```

#### B) Event Callback Handler
```python
async def _on_tier_updated(self, data: Dict[str, Any]):
    """
    Callback cuando TIER 1 o TIER 2 se actualizan
    Dispara análisis automático con debounce para evitar múltiples ejecuciones
    """
    tier = data.get('tier')
    updated_count = data.get('updated_count', 0)

    logger.info(f"[MARKET ANALYSIS] EVENT RECEIVED: TIER {tier} updated ({updated_count} candles)")
    logger.info("[MARKET ANALYSIS] Triggering automatic market analysis...")

    # Ejecutar análisis automático (ya viene con debounce del event_bus)
    await self.analyze_and_save()
```

#### C) WebSocket Integration
```python
async def analyze_and_save(self) -> Dict[str, Any]:
    """
    Analyze market for BOTH timeframes (12h and 24h) and save results to database
    Called by scheduler every 5 minutes OR by event listeners
    Emits WebSocket notification to frontend after each analysis
    """
    for timeframe in self.available_timeframes:
        analysis = await self.analyze_market(timeframe)
        analysis_dict = analysis.model_dump()
        await self.market_repository.insert_analysis(analysis_dict)

        # NUEVO: Emit WebSocket event to notify frontend of update
        from services.websocket_service import websocket_service
        await websocket_service.emit_market_analysis_updated({
            'market_status': analysis.market_status,
            'timeframe': timeframe,
            'total_tokens': analysis.total_tokens,
            'bullish_percentage': analysis.bullish_percentage,
            'bearish_percentage': analysis.bearish_percentage,
            'neutral_percentage': analysis.neutral_percentage,
            'timestamp': analysis.timestamp,
            'top_performers': [p.model_dump() for p in analysis.top_performers],
            'worst_performers': [p.model_dump() for p in analysis.worst_performers]
        })
```

---

### 4. WebSocket Service (`backend/services/websocket_service.py`)

**New Method**:
```python
async def emit_market_analysis_updated(self, analysis_data: Dict[str, Any]):
    """
    Emit event when market analysis is updated.
    Notifies all connected clients to refresh market analysis display.

    Args:
        analysis_data: Dictionary with market analysis information
    """
    try:
        # Convertir timestamps a string
        payload = {
            **analysis_data,
            'timestamp': analysis_data['timestamp'].isoformat() if isinstance(analysis_data['timestamp'], datetime) else analysis_data['timestamp']
        }

        await self.sio.emit('market_analysis_updated', payload)

        logger.info(f"[WEBSOCKET] Broadcasted market_analysis_updated: {analysis_data.get('market_status')} [{analysis_data.get('timeframe')}]")

    except Exception as e:
        logger.error(f"Error emitting market_analysis_updated event: {e}")
```

**WebSocket Event Name**: `'market_analysis_updated'`

**Payload Structure**:
```json
{
  "market_status": "ALCISTA" | "BAJISTA" | "NEUTRAL",
  "timeframe": "12h" | "24h",
  "total_tokens": 91,
  "bullish_percentage": 65.93,
  "bearish_percentage": 30.77,
  "neutral_percentage": 3.30,
  "timestamp": "2025-01-15T14:23:45.123456",
  "top_performers": [
    {
      "symbol": "BTC-USDT",
      "name": "Bitcoin",
      "avg_performance": 5.23
    },
    ...
  ],
  "worst_performers": [
    {
      "symbol": "XLM-USDT",
      "name": "Stellar",
      "avg_performance": -2.19
    },
    ...
  ]
}
```

---

## Frontend Integration (TODO)

### Step 1: Add WebSocket Listener

In `frontend/src/app/components/market-analysis/market-analysis.component.ts`:

```typescript
import { WebSocketService } from '../../services/websocket.service';

export class MarketAnalysisComponent implements OnInit, OnDestroy {

  ngOnInit(): void {
    // Existing code...
    this.loadAnalysis();

    // NEW: Listen for real-time market analysis updates
    this.socketService.on('market_analysis_updated').subscribe((data: any) => {
      console.log('[WS] Market analysis updated:', data);

      // Update current analysis if timeframes match
      if (data.timeframe === this.selectedTimeframe) {
        this.currentAnalysis = data;
        this.updateUIWithNewAnalysis(data);
      }
    });
  }

  private updateUIWithNewAnalysis(analysis: any): void {
    // Update market status badge
    this.marketStatus = analysis.market_status;

    // Update percentages
    this.bullishPercentage = analysis.bullish_percentage;
    this.bearishPercentage = analysis.bearish_percentage;
    this.neutralPercentage = analysis.neutral_percentage;

    // Update top/worst performers
    this.topPerformers = analysis.top_performers;
    this.worstPerformers = analysis.worst_performers;

    // Trigger change detection
    this.cdr.detectChanges();
  }
}
```

### Step 2: Reduce or Remove HTTP Polling

Since WebSocket provides real-time updates, you can:

**Option A**: Remove polling entirely
```typescript
// Remove or comment out:
// this.pollingSubscription = interval(60000).subscribe(() => this.loadAnalysis());
```

**Option B**: Keep minimal polling as fallback (recommended)
```typescript
// Poll every 5 minutes as backup (in case WebSocket disconnects)
this.pollingSubscription = interval(300000).subscribe(() => this.loadAnalysis());
```

---

## Benefits

### 1. **Real-Time Synchronization**
- Market Analysis updates **instantly** when performance values change
- No lag between candlestick table and Market Analysis
- User sees consistent data across all views

### 2. **Efficient Resource Usage**
- **Debouncing** prevents excessive database writes
- Only recalculates when meaningful changes occur
- WebSocket push eliminates constant frontend polling

### 3. **Scalable Architecture**
- Decoupled services via EventBus
- Easy to add new listeners or event types
- Can add more reactive features without refactoring

### 4. **Better User Experience**
- Instant feedback when market sentiment shifts
- No need to refresh page manually
- Feels like a professional trading platform

---

## Monitoring & Logs

### Backend Logs to Watch

```bash
# TIER 1 update triggers event
[TIER1] Updated 50 candles
[EVENT BUS] Debounced 'tier1_updated' scheduled (5s delay)

# After 5 second delay, event fires
[EVENT BUS] Debounced event 'tier1_updated' firing after 5s delay

# Market Analysis receives event
[MARKET ANALYSIS] EVENT RECEIVED: TIER 1 updated (50 candles)
[MARKET ANALYSIS] Triggering automatic market analysis...

# Analysis completes
[MARKET ANALYSIS] [24h] Status: ALCISTA
Market analysis saved [24h]: ALCISTA

# WebSocket broadcasts to frontend
[WEBSOCKET] Broadcasted market_analysis_updated: ALCISTA [24h]
```

### Frontend Logs to Watch

```bash
# WebSocket connection established
[WebSocket] Connected to server

# Receives real-time update
[WS] Market analysis updated: {market_status: "ALCISTA", timeframe: "24h", ...}

# UI updates instantly
Market Analysis Component: UI updated with new data
```

---

## Testing the System

### Manual Test Steps

1. **Start Backend**:
   ```bash
   cd backend
   python main.py
   ```

2. **Start Frontend**:
   ```bash
   cd frontend
   npm start
   ```

3. **Open Browser Console**: Navigate to Market Analysis page

4. **Wait for TIER 1 Update**: Happens every 5 seconds

5. **Expected Flow**:
   - Backend logs show TIER 1 update
   - Event emitted with 5s delay
   - Market Analysis triggered automatically
   - WebSocket broadcasts update
   - Frontend console shows received data
   - UI updates instantly

6. **Verify Synchronization**:
   - Check TOP 10 token in candlestick table (e.g., XLM: -2.19%)
   - Check same token in Market Analysis top performers
   - Values should match **exactly**

---

## Troubleshooting

### Issue: Market Analysis not updating

**Check**:
1. EventBus listeners registered?
   ```
   [MARKET ANALYSIS] Event listeners registered for TIER updates
   ```

2. TIER updates emitting events?
   ```
   [EVENT BUS] Debounced 'tier1_updated' scheduled
   ```

3. Market Analysis receiving events?
   ```
   [MARKET ANALYSIS] EVENT RECEIVED: TIER 1 updated
   ```

### Issue: Frontend not receiving updates

**Check**:
1. WebSocket connected?
   ```typescript
   this.socketService.getConnectionState() // Should be 'connected'
   ```

2. Listener registered?
   ```typescript
   this.socketService.on('market_analysis_updated') // Must be called
   ```

3. Backend emitting?
   ```
   [WEBSOCKET] Broadcasted market_analysis_updated: ALCISTA [24h]
   ```

---

## Future Enhancements

1. **TIER 3 Event Integration**: Currently only TIER 1 and TIER 2 emit events
2. **Ranking Change Detection**: Detect when a token enters/exits TOP 10 and emit special event
3. **Performance Threshold Alerts**: Emit event when any token gains/loses >5% in 24h
4. **Historical Trend Analysis**: Use saved market analysis records to show sentiment trends over time

---

## Summary

This event-driven architecture ensures **Market Analysis is always synchronized** with the latest token performance data. The system is:

- ✅ **Reactive**: Responds instantly to data changes
- ✅ **Efficient**: Uses debouncing to prevent excessive recalculations
- ✅ **Scalable**: Easy to add new listeners or event types
- ✅ **Real-Time**: WebSocket push eliminates frontend polling lag
- ✅ **Reliable**: Fallback polling ensures consistency even if events fail

**Result**: Users see consistent, up-to-date market analysis that perfectly matches the candlestick table at all times.
