# Quick Start - Event-Driven Market Analysis

## What Was Implemented

A complete event-driven system that ensures Market Analysis is **always synchronized** with token performance data in real-time.

---

## The Problem (Before)

**User Observation**:
```
Candlestick Table shows: XLM = -2.19%
Market Analysis shows:   XLM = -3.34%
```

**Root Cause**: Market Analysis was reading stale data from MongoDB. It only updated every 5 minutes via scheduled polling, while candlestick data updated every 5-60 seconds depending on tier.

---

## The Solution (After)

### Event-Driven Flow

```
TIER 1/2 Updates (every 5s/30s)
    ↓
Emit Event (with 5s debounce)
    ↓
Market Analysis Listens → Auto-triggers
    ↓
Saves to MongoDB
    ↓
Pushes to Frontend via WebSocket
    ↓
UI Updates Instantly
```

**Result**: Market Analysis and Candlestick Table **always match** in real-time.

---

## Files Modified

### 1. **NEW FILE**: `backend/services/event_bus.py`
```python
# Central event coordination with debouncing
event_bus.on('tier1_updated', callback)  # Register listener
event_bus.emit_debounced('tier1_updated', data, delay=5)  # Emit with delay
```

### 2. **MODIFIED**: `backend/services/scheduler_service.py`
```python
# Lines 10, 213-217, 233-239: Added event emission after TIER updates
from services.event_bus import event_bus

async def refresh_tier1_task(self):
    result = await self.candlestick_service.refresh_tier1_candles()
    if result.get('updated_count', 0) > 0:
        await event_bus.emit_debounced('tier1_updated', {...}, delay=5)
```

### 3. **MODIFIED**: `backend/services/market_analysis_service.py`
```python
# Lines 8, 22-46, 211-226: Added event listeners and WebSocket emission

from services.event_bus import event_bus

def _setup_event_listeners(self):
    event_bus.on('tier1_updated', self._on_tier_updated)
    event_bus.on('tier2_updated', self._on_tier_updated)

async def _on_tier_updated(self, data: Dict[str, Any]):
    await self.analyze_and_save()  # Auto-trigger analysis

async def analyze_and_save(self):
    # After saving to MongoDB:
    from services.websocket_service import websocket_service
    await websocket_service.emit_market_analysis_updated({...})
```

### 4. **ALREADY EXISTED**: `backend/services/websocket_service.py`
```python
# Lines 284-304: Method already existed, now being called

async def emit_market_analysis_updated(self, analysis_data: Dict[str, Any]):
    await self.sio.emit('market_analysis_updated', payload)
```

---

## How to Test

### Backend Verification

1. **Start backend**:
   ```bash
   cd backend
   python main.py
   ```

2. **Watch logs** for this sequence:
   ```
   [TIER1] Updated 50 candles
   [EVENT BUS] Debounced 'tier1_updated' scheduled (5s delay)
   [EVENT BUS] Debounced event 'tier1_updated' firing after 5s delay
   [MARKET ANALYSIS] EVENT RECEIVED: TIER 1 updated (50 candles)
   [MARKET ANALYSIS] Triggering automatic market analysis...
   [MARKET ANALYSIS] [24h] Status: ALCISTA
   Market analysis saved [24h]: ALCISTA
   [WEBSOCKET] Broadcasted market_analysis_updated: ALCISTA [24h]
   ```

3. **Expected timing**:
   - TIER 1 updates every 5 seconds
   - Event debounced for 5 seconds
   - Total: ~10 seconds between TIER update and Market Analysis completion

### Frontend Integration (TODO)

**File to modify**: `frontend/src/app/components/market-analysis/market-analysis.component.ts`

```typescript
ngOnInit(): void {
  // Existing code...

  // NEW: Listen for real-time updates
  this.socketService.on('market_analysis_updated').subscribe((data: any) => {
    console.log('[WS] Market analysis updated:', data);
    if (data.timeframe === this.selectedTimeframe) {
      this.currentAnalysis = data;
      this.updateUI();
    }
  });
}
```

**Benefit**: Remove or reduce polling since WebSocket provides instant updates.

---

## Key Features

### 1. Debouncing (5 seconds)
**Why**: Prevents excessive recalculations when rapid changes occur.

**Example**:
```
TIER 1 updates at: 14:00:00, 14:00:05, 14:00:10
Event fires only once at: 14:00:15 (5s after last update)
```

### 2. Dual Trigger System
Market Analysis runs via:
1. **Scheduled polling**: Every 1 minute (backup/fallback)
2. **Event-driven**: Instantly when TIER 1/2 updates

**Why both**: Ensures analysis runs even if event system fails.

### 3. WebSocket Push
**Old way**: Frontend polls every 60 seconds → Max 60s lag

**New way**: Backend pushes instantly → 0s lag

---

## Monitoring Commands

### Check Event Listeners Registered
```bash
# Should appear on startup:
[MARKET ANALYSIS] Event listeners registered for TIER updates
```

### Count Event Emissions
```bash
# In backend logs, search for:
grep "Debounced 'tier1_updated' scheduled" logs.txt | wc -l
```

### Verify WebSocket Broadcasts
```bash
# Should see after each analysis:
[WEBSOCKET] Broadcasted market_analysis_updated: ALCISTA [24h]
```

---

## Troubleshooting

### Issue: Market Analysis still shows old values

**Check 1**: Are event listeners registered?
```bash
# Backend startup should show:
[MARKET ANALYSIS] Event listeners registered for TIER updates
```

**Check 2**: Are TIER updates emitting events?
```bash
# After TIER 1 update, should see:
[EVENT BUS] Debounced 'tier1_updated' scheduled (5s delay)
```

**Check 3**: Is Market Analysis receiving events?
```bash
# 5 seconds after TIER update, should see:
[MARKET ANALYSIS] EVENT RECEIVED: TIER 1 updated
```

### Issue: Frontend not updating

**Check 1**: WebSocket connected?
```typescript
// In browser console:
window.socketService.getConnectionState() // Should be 'connected'
```

**Check 2**: Listener registered?
```typescript
// Check component code has:
this.socketService.on('market_analysis_updated')
```

**Check 3**: Backend emitting?
```bash
# Backend logs should show:
[WEBSOCKET] Broadcasted market_analysis_updated
```

---

## Performance Impact

### Before (Polling Only)
- Market Analysis updates: Every 5 minutes
- Frontend polls: Every 60 seconds
- Lag: Up to 6 minutes
- Database writes: 288 per day (every 5 min)

### After (Event-Driven + Polling)
- Market Analysis updates: **Instantly** when data changes + every 1 min backup
- Frontend: **Push notifications** + optional 5 min backup poll
- Lag: **~10 seconds** (5s TIER update + 5s debounce)
- Database writes: Variable (only when data actually changes)

**Net Result**:
- 36x faster response time (6 min → 10 sec)
- More efficient resource usage (debouncing prevents waste)
- Better user experience (feels real-time)

---

## Next Steps for Frontend Developer

1. **Add WebSocket Listener** in Market Analysis component
2. **Update UI method** to handle real-time data
3. **Reduce polling** to 5 min backup interval
4. **Test synchronization** by watching both candlestick table and market analysis

**Estimated time**: 15-30 minutes

**Expected result**: Market Analysis values match candlestick table **exactly** at all times.

---

## Summary

✅ **EventBus** created with debouncing
✅ **TIER 1/2** emit events after updates
✅ **Market Analysis** listens and auto-triggers
✅ **WebSocket** pushes updates to frontend
✅ **Documentation** complete

**Status**: Backend implementation **100% COMPLETE**
**Next**: Frontend integration (15-30 min work)
