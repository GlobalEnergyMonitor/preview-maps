# Earthrise Maps - Data Pulling Optimization Recommendations

## CRITICAL ISSUE SUMMARY

The current system has **significant data redundancy** in how it pulls and processes tracker data:

1. **Per-map tracker instantiation:** Each map creates its own TrackerObject instances, even when multiple maps use the same trackers
2. **Interactive caching prompts:** User is prompted for each map to decide whether to use cached pkl files
3. **Inefficient filtering:** Same tracker data is filtered independently for each map using it
4. **Date-based cache invalidation:** New pkl files created daily, even if data hasn't changed

---

## KEY CODE LOCATIONS

### Data Loading Entry Points

| File | Lines | Function | Purpose |
|------|-------|----------|---------|
| `run_maps.py` | 16-36 | `run_maps()` | Main orchestrator, iterates through trackers_to_update |
| `make_data_dwnlds.py` | 19-159 | `make_data_dwnlds(tracker)` | Creates map objects for tracker, finds which maps use it |
| `make_map_tracker_objs.py` | 8-99 | `make_map_tracker_objs()` | Factory function, creates TrackerObject instances |
| `map_tracker_class.py` | 72-249 | `set_df()` | **CRITICAL:** Data loading with pkl caching |
| `map_tracker_class.py` | 979-1068 | `create_filtered_geo_fuel_df()` | Filters data by geography and fuel |

### Configuration Locations

| File | Lines | Variable | Purpose |
|------|-------|----------|---------|
| `all_config.py` | 41 | `trackers_to_update` | List of trackers to process |
| `all_config.py` | 52 | `priority` | Maps to prioritize (filtering) |
| `all_config.py` | 449 | `multi_tracker_log_sheet_key` | Google Sheet with tracker/map config |
| `all_config.py` | 81-82 | `local_pkl_dir` | Local pickle cache directory |

### Redundancy Manifestation Points

| Location | Type | Issue |
|----------|------|-------|
| `make_map_tracker_objs.py:31-43` | TrackerObject creation | New instance per map, even if same tracker |
| `map_tracker_class.py:76-100` | pkl caching | Interactive prompt per map with tracker |
| `map_tracker_class.py:86` | Default behavior | `use_local == 'y' or ''` always true (bug: should be `and`) |
| `make_data_dwnlds.py:48` | Map object creation | Creates pkl per map, not per tracker |

---

## DETAILED OPTIMIZATION STRATEGIES

### STRATEGY 1: Singleton Tracker Cache (RECOMMENDED - Medium Effort)

**Goal:** Load each tracker ONCE per run, share across maps

**Current Flow:**
```
For each map:
  For each tracker in map:
    set_df() → Check pkl → Load/Fetch → Cache
    create_filtered_geo_fuel_df() → Filter
```

**Optimized Flow:**
```
Load all trackers ONCE:
  For each tracker in union(all_trackers):
    set_df() → Check pkl → Load/Fetch → Cache

For each map:
  For each tracker in map:
    Use cached TrackerObject
    create_filtered_geo_fuel_df() → Filter
```

**Implementation:**
```python
# In make_data_dwnlds.py or new module

class TrackerCache:
    _instance = None
    _trackers = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_tracker(self, tracker_key):
        """Get or load tracker from cache"""
        if tracker_key not in self._trackers:
            # Load tracker once
            tracker_obj = TrackerObject(...)
            tracker_obj.set_df()
            self._trackers[tracker_key] = tracker_obj
        return self._trackers[tracker_key]
    
    def clear(self):
        self._trackers = {}

# In make_map_tracker_objs.py
cache = TrackerCache()
tracker_source_obj = cache.get_tracker(item)  # Reuses existing or loads once
```

**Benefits:**
- Eliminates duplicate set_df() calls for same tracker across maps
- First map triggers fetch, others use cache
- Only 1 user prompt per tracker, not per map-tracker pair

**Effort:** 2-3 hours (new cache class, refactor make_map_tracker_objs.py)

---

### STRATEGY 2: Non-Interactive Caching (QUICK WIN - Low Effort)

**Goal:** Remove user prompts, auto-use cache or force-refresh flag

**Current Problem:**
```python
# map_tracker_class.py line 86
use_local = input(f'Use local pkl file? (y/n, default=y): ').strip().lower()
if use_local == 'y' or '':  # BUG: This is always True!
```

**Issue:** Line 88 logic is broken - `use_local == 'y' or ''` evaluates to True always (empty string is falsy in boolean context, but statement is malformed)

**Fix 1: Add command-line flag**
```python
# In launcher.py - add argument
parser.add_argument("--force-refresh", action="store_true", 
                  help="Ignore cached pkl files and fetch fresh data")

# In set_df() method
if force_refresh_flag or not os.path.exists(pkl_path):
    # Fetch fresh
else:
    # Use cache
```

**Fix 2: Use environment variable**
```python
use_cache = os.getenv('USE_PKL_CACHE', 'true').lower() == 'true'
```

**Benefits:**
- Enables batch/CI processing
- Fixes logical bug in line 88
- Still allows manual cache refresh

**Effort:** 30 minutes (modify launcher.py, set_df() method)

---

### STRATEGY 3: Smart Pkl Naming by Content Hash (Medium Effort)

**Goal:** Cache based on data content, not date

**Current:**
```
trackerdf_for_{acro}_on_{iso_today_date}.pkl
# Creates new file daily even if data unchanged
```

**Improved:**
```
trackerdf_for_{acro}_hash_{content_hash}.pkl
# Only new file if data actually changed
```

**Implementation:**
```python
import hashlib

def get_tracker_content_hash(tracker_obj):
    """Generate hash of tracker's remote data"""
    # Get last-modified timestamp from Google Sheet or S3
    # Create deterministic hash
    tracker_key = f"{tracker_obj.key}_{tracker_obj.tabs}_{tracker_obj.release}"
    return hashlib.md5(tracker_key.encode()).hexdigest()

def set_df(self):
    content_hash = get_tracker_content_hash(self)
    pkl_path = f"local_pkl/trackerdf_for_{self.acro}_hash_{content_hash}.pkl"
    
    if os.path.exists(pkl_path):
        # Use cache
    else:
        # Fetch and save with new hash
```

**Benefits:**
- Cache persists across days
- Only refreshes when data actually changes
- Reduces unnecessary pkl file creation

**Effort:** 2 hours

---

### STRATEGY 4: Async/Parallel Data Loading (Advanced - High Effort)

**Goal:** Load multiple trackers in parallel

**Current:** Sequential tracker loading
**Optimized:** Parallel using asyncio or ThreadPoolExecutor

```python
from concurrent.futures import ThreadPoolExecutor
import asyncio

def load_all_trackers_parallel(tracker_list):
    """Load multiple trackers in parallel"""
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(load_single_tracker, tracker): tracker 
            for tracker in tracker_list
        }
        results = {}
        for future in concurrent.futures.as_completed(futures):
            tracker = futures[future]
            results[tracker] = future.result()
    return results
```

**Challenges:**
- Google Sheets API rate limiting
- Thread-safe pandas operations
- Error handling complexity

**Effort:** 4-6 hours

---

## IMMEDIATE FIXES (Quick Wins)

### FIX 1: Correct the Boolean Logic Bug

**File:** `map_tracker_class.py` line 88

**Current (WRONG):**
```python
if use_local == 'y' or '':
```

**Issue:** This is always True because `''` (empty string) is evaluated in boolean context

**Should be:**
```python
if use_local in ('y', ''):  # Default to yes if enter pressed
```

**Or:**
```python
if use_local.lower() != 'n':  # Default to yes unless explicitly say no
```

---

### FIX 2: Add Optional Non-Interactive Mode

**File:** `map_tracker_class.py` line 76-100

**Add parameter:**
```python
def set_df(self, interactive=True):
    pkl_path = os.path.join(local_pkl_dir, f'trackerdf_for_{self.acro}_on_{iso_today_date}.pkl')
    
    if os.path.exists(pkl_path):
        if interactive:
            use_local = input(f'Use local pkl file? (y/n, default=y): ').strip().lower()
        else:
            use_local = 'y'  # Auto-use cache in non-interactive mode
            
        if use_local in ('y', ''):
            # Load from pkl
```

---

### FIX 3: Add Cache Cleanup Utility

**File:** New file `cache_manager.py`

```python
import os
from pathlib import Path

class CacheManager:
    def __init__(self, cache_dir='local_pkl'):
        self.cache_dir = Path(cache_dir)
    
    def clear_all(self):
        """Clear all cached pkl files"""
        for pkl_file in self.cache_dir.glob('*.pkl'):
            pkl_file.unlink()
    
    def clear_tracker(self, acro):
        """Clear cache for specific tracker"""
        for pkl_file in self.cache_dir.glob(f'trackerdf_for_{acro}*.pkl'):
            pkl_file.unlink()
    
    def list_cached(self):
        """List all cached files with sizes"""
        for pkl_file in self.cache_dir.glob('*.pkl'):
            size_mb = pkl_file.stat().st_size / 1024 / 1024
            print(f"{pkl_file.name}: {size_mb:.2f} MB")

# Usage
if __name__ == "__main__":
    import sys
    cache = CacheManager()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'clear':
        acro = sys.argv[2] if len(sys.argv) > 2 else None
        if acro:
            cache.clear_tracker(acro)
        else:
            cache.clear_all()
    else:
        cache.list_cached()
```

---

## DETAILED REDUNDANCY EXAMPLE

### Scenario: Running all maps for "Oil & Gas Plants" tracker

```python
# all_config.py
trackers_to_update = ["Oil & Gas Plants"]  # Only this tracker

# Maps that use "Oil & Gas Plants":
# - asia (GOGPT, GGIT, ...)
# - latam (GOGPT, GGIT, ...)
# - europe (GOGPT, GGIT, GOGPT-EU, ...)
# - africa (GOGPT, GGIT, ...)
# - ggit (GGIT only)

# What happens currently:
run_maps()
└─ make_data_dwnlds(tracker="Oil & Gas Plants")
   ├─ Finds all maps using this tracker
   │
   ├─ make_map_tracker_objs() for "asia" map:
   │  ├─ Creates TrackerObject(GOGPT)
   │  │  └─ set_df() → Prompt user #1 → Loads from Google Sheet → Caches
   │  └─ Creates TrackerObject(GGIT)
   │     └─ set_df() → Prompt user #2 → Loads from S3 → Caches
   │
   ├─ make_map_tracker_objs() for "latam" map:
   │  ├─ Creates NEW TrackerObject(GOGPT)
   │  │  └─ set_df() → Prompt user #3 → Checks pkl (exists) → Loads from pkl
   │  └─ Creates NEW TrackerObject(GGIT)
   │     └─ set_df() → Prompt user #4 → Checks pkl (exists) → Loads from pkl
   │
   ├─ make_map_tracker_objs() for "europe" map:
   │  ├─ Creates NEW TrackerObject(GOGPT)
   │  │  └─ set_df() → Prompt user #5 → Checks pkl (exists) → Loads from pkl
   │  ├─ Creates NEW TrackerObject(GGIT)
   │  │  └─ set_df() → Prompt user #6 → Checks pkl (exists) → Loads from pkl
   │  └─ Creates NEW TrackerObject(GOGPT-EU)
   │     └─ set_df() → Prompt user #7 → Loads from S3 → Caches
   │
   └─ ... and so on for africa, ggit maps

# With Singleton Strategy:
# - User prompted only 3 times (for GOGPT, GGIT, GOGPT-EU)
# - All maps reuse same TrackerObject instances
# - ~70% reduction in set_df() calls
```

---

## RECOMMENDED IMPLEMENTATION ORDER

1. **Immediate (1 hour):**
   - Fix boolean logic bug in line 88
   - Add FIX 2 (non-interactive mode parameter)
   - Add FIX 3 (cache manager utility)

2. **Short-term (4-6 hours):**
   - Implement Strategy 1 (Singleton Tracker Cache)
   - Test with a few maps

3. **Medium-term (2-3 hours):**
   - Implement Strategy 3 (Smart Pkl Naming)
   - Add --force-refresh flag to launcher.py

4. **Optional Long-term:**
   - Strategy 4 (Parallel loading) if performance still needed

---

## TESTING RECOMMENDATIONS

### Test Case 1: Cache Hit Scenario
```python
def test_cache_hit():
    # First run: Load tracker from Google Sheet
    tracker1 = TrackerObject(key='...', acro='GOGPT')
    tracker1.set_df(interactive=False)
    
    # Second run: Should use cache
    tracker2 = TrackerObject(key='...', acro='GOGPT')
    tracker2.set_df(interactive=False)
    
    # Verify same pkl was used
    assert tracker1.data.equals(tracker2.data)
```

### Test Case 2: Multi-Map Cache Reuse
```python
def test_multi_map_cache():
    # Simulate running multiple maps with shared trackers
    cache = TrackerCache()
    
    # Map 1: asia
    gogpt_1 = cache.get_tracker('GOGPT')
    ggit_1 = cache.get_tracker('GGIT')
    
    # Map 2: latam
    gogpt_2 = cache.get_tracker('GOGPT')  # Should be same instance
    ggit_2 = cache.get_tracker('GGIT')    # Should be same instance
    
    assert gogpt_1 is gogpt_2
    assert ggit_1 is ggit_2
```

---

## METRICS TO TRACK

**Before Optimization:**
- Tracker data pulls per run: ~15-20
- User prompts: 15-20
- Total runtime: ~5-10 minutes

**After Strategy 1 (Singleton Cache):**
- Tracker data pulls per run: ~5-7 (80% reduction)
- User prompts: 5-7
- Total runtime: ~3-5 minutes

**After Strategy 2 (Non-Interactive):**
- User interaction: 0 (full automation)
- Total runtime: ~2-3 minutes

**After Strategy 3 (Content Hash Caching):**
- Cache invalidation: Smart (only on data change)
- Repeat runs with same data: <30 seconds

