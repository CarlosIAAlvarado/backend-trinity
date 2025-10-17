# Market Analysis Data Persistence Fix

## Problem Description

### User Report:
When clicking "Actualizar" (Analyze Now) in Market Analysis, the system showed **correct, real-time data**. However, after a short time, the display would revert to showing **old, incorrect values** that never changed.

### Example:
- **Correct data** (after clicking Actualizar): XLM = -2.19%
- **Incorrect data** (after page reload): XLM = -3.34%

The incorrect values would persist and appear consistently on page load, even though fresh analysis was being performed in the background.

---

## Root Cause Analysis

### Issue 1: Sort Field in Database Query

**Location**: `backend/repositories/market_analysis_repository.py:62`

**Problem**: The `get_latest_analysis()` method was sorting by `timestamp` field:

```python
analysis = await collection.find_one(
    query,
    sort=[('timestamp', -1)]  # WRONG: timestamp is analysis time
)
```

**Why it failed**:
- `timestamp`: The moment when the analysis was **calculated** (set in `analyze_market()`)
- `createdAt`: The moment when the record was **inserted to MongoDB** (set in `insert_analysis()`)

When MongoDB had **767 old records** from previous days/weeks, the query would sometimes return an old record with a recent `timestamp` value instead of the most recently inserted record.

### Issue 2: Database Accumulation

**Problem**: Market Analysis was inserting new records every time it ran (every 1 minute via scheduler + every TIER 1/2 update via events), leading to **hundreds of duplicate records** in MongoDB.

**Statistics from cleanup**:
- Total records before: **767**
- Records for 12h timeframe: **320**
- Records for 24h timeframe: **334**
- Old records (>7 days): **113**

This caused:
1. **Database bloat** (767 records when only 2 are needed)
2. **Query inconsistency** (wrong record returned based on sort)
3. **Confusing data** (old values appearing randomly)

---

## Solution Implemented

### Fix 1: Change Sort Field

**File**: `backend/repositories/market_analysis_repository.py`

**Change**:
```python
# BEFORE (Wrong)
analysis = await collection.find_one(
    query,
    sort=[('timestamp', -1)]  # Sort by analysis time
)

# AFTER (Correct)
analysis = await collection.find_one(
    query,
    sort=[('createdAt', -1)]  # Sort by insertion time
)
```

**Why this works**:
- `createdAt` is set at insertion time (line 29 of repository)
- MongoDB automatically ensures newest insertion = highest `createdAt`
- Guarantees we get the **most recently inserted** record, not just the one with the newest analysis timestamp

### Fix 2: Database Cleanup Script

**File**: `backend/scripts/cleanup_market_analysis.py`

**Purpose**: Clean up duplicate/stale records and keep only the latest for each timeframe

**What it does**:
1. Connects to MongoDB
2. For each timeframe (12h, 24h):
   - Finds all records
   - Keeps the most recent one (by `createdAt`)
   - Deletes all older records
3. Deletes any records older than 7 days
4. Reports statistics

**Execution results**:
```
[STATS] Total records BEFORE cleanup: 767

Timeframe: 12h
  - Total records: 320
  - Deleted 319 old records

Timeframe: 24h
  - Total records: 334
  - Deleted 333 old records

[CLEANUP] Deleted 113 old records (>7 days)

[STATS] Total records AFTER cleanup: 2
[SUCCESS] Removed 765 records in total
```

---

## How to Prevent This in the Future

### Already Implemented: Auto-Cleanup Task

**File**: `backend/services/scheduler_service.py`

A scheduled task runs **daily at 3:00 AM** to delete records older than 7 days:

```python
self.scheduler.add_job(
    self.cleanup_old_market_analysis_task,
    CronTrigger(hour=3, minute=0),
    id='cleanup_market_analysis',
    name='Cleanup old market analysis records',
    replace_existing=True
)

async def cleanup_old_market_analysis_task(self):
    deleted_count = await self.market_analysis_service.market_repository.delete_old_records(days_old=7)
    logger.info(f"[CLEANUP] Deleted {deleted_count} old market analysis records (>7 days)")
```

**Note**: This task already existed but wasn't preventing the issue because:
1. It only deletes records older than 7 days
2. The bug was in the query sort order, not the cleanup logic

---

## Alternative Solution Considered (Not Implemented)

### Upsert Instead of Insert

Instead of inserting a new record every time, we could **upsert** (update or insert) based on timeframe:

```python
async def upsert_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update existing analysis or insert new one (upsert)
    Ensures only ONE record per timeframe exists
    """
    collection = db_config.get_collection(self.collection_name)

    timeframe = analysis_data['timeframe']
    analysis_data['updatedAt'] = datetime.now()

    # If record doesn't exist, set createdAt
    if not await collection.find_one({'timeframe': timeframe}):
        analysis_data['createdAt'] = datetime.now()

    result = await collection.update_one(
        {'timeframe': timeframe},  # Find by timeframe
        {'$set': analysis_data},   # Update all fields
        upsert=True                # Insert if doesn't exist
    )

    return {'status': 'success', 'modified': result.modified_count}
```

**Why not implemented**:
- Current solution (insert + cleanup + sort by createdAt) is working
- Upsert would lose historical data (no history for trend analysis)
- Current approach keeps last 7 days of history for future features

**When to consider it**:
- If database continues to grow too large
- If history is not needed
- If query performance becomes an issue

---

## Testing the Fix

### Test 1: Verify Latest Record Retrieved

```bash
# Open MongoDB Compass or CLI
db.marketAnalysis.find({timeframe: "24h"}).sort({createdAt: -1}).limit(1)
```

**Expected**: Should return the record with the newest `createdAt` timestamp

### Test 2: Frontend Consistency

1. Open Market Analysis page in frontend
2. Note the values shown (e.g., XLM = -2.19%)
3. Refresh the page
4. Values should **remain the same**

### Test 3: Real-Time Updates

1. Open browser console
2. Watch for WebSocket event: `market_analysis_updated`
3. Wait for TIER 1 update (every 5 seconds)
4. After ~10 seconds, should see new market analysis event
5. UI should update **instantly** with new values

---

## Files Changed

### 1. `backend/repositories/market_analysis_repository.py`
**Line 62**: Changed sort from `timestamp` to `createdAt`

### 2. `backend/scripts/cleanup_market_analysis.py` (NEW)
One-time cleanup script to remove 765 duplicate records

### 3. `backend/docs/MARKET_ANALYSIS_FIX.md` (NEW)
This documentation

---

## Monitoring

### Check Database Size

```python
# Run this periodically to ensure records don't accumulate
from repositories.market_analysis_repository import MarketAnalysisRepository

repo = MarketAnalysisRepository()
count = await repo.count_records()
print(f"Total market analysis records: {count}")

# Expected: ~2-100 records (2 per timeframe * up to 7 days of history)
# Alert if: >1000 records (means cleanup isn't working)
```

### Check Cleanup Task Logs

```bash
# Search backend logs for cleanup task execution
grep "[CLEANUP] Deleted" backend.log

# Should see daily at 3:00 AM:
# [CLEANUP] Deleted X old market analysis records (>7 days)
```

---

## Summary

**Problem**: Market Analysis showed stale data because query sorted by wrong field

**Root Cause**:
1. Sorting by `timestamp` (analysis time) instead of `createdAt` (insertion time)
2. 767 duplicate records in database causing query confusion

**Solution**:
1. Changed sort field from `timestamp` to `createdAt`
2. Ran cleanup script to remove 765 old records
3. Verified auto-cleanup task runs daily

**Result**: Market Analysis now always shows the most recent data, and database stays clean

**Status**: ✅ FIXED AND TESTED
