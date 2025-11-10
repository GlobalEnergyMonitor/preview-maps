# Earthrise Maps Codebase Structure and Data Flow Analysis

## Overview
The earthrise-maps system creates interactive web maps from various global energy tracker data. It pulls data from multiple trackers (like Oil & Gas Plants, Solar, Wind, etc.), filters by geography and fuel type, and generates map files for deployment.

---

## 1. HIGH-LEVEL MAP CREATION WORKFLOW

### Main Entry Point: `run_maps.py`
The workflow starts with **`run_maps()`** function that:

```
run_maps() 
├─ Iterates through trackers_to_update (list of official tracker names)
├─ For each tracker in trackers_to_update:
│  ├─ If official tracker: calls make_data_dwnlds(tracker)
│  ├─ If 'Integrated': special handling for integrated map
│  ├─ If 'Plumes': special handling for GMET
│  └─ If 'LNG Terminals': special handling for LNG
└─ Returns map objects to be processed further
```

**Current trackers_to_update (from all_config.py):**
```python
trackers_to_update = ["LNG Terminals"]  # Official tracker tab name in map tracker log sheet
```

---

## 2. DATA PULLING AND LOADING FLOW

### Step 1: Generate Map Objects -> `make_data_dwnlds(tracker)`

**File:** `make_data_dwnlds.py` (lines 19-159)

```
make_data_dwnlds(tracker)
├─ Reads source_tab from multi_tracker_log_sheet_key (Google Sheet: '15l2fcUBADkNVHw...')
├─ Reads map_tab from same Google Sheet
├─ Creates prep_dict: {tracker_name: tracker_config}
│  ├─ gspread_key
│  ├─ official name
│  ├─ tab name
│  ├─ gspread_tabs (list of tabs in Google Sheet)
│  ├─ latest release
│  ├─ tracker-acro
│  ├─ geocol (geography column name)
│  ├─ fuelcol (fuel column name)
│  └─ about_key
│
├─ PRIORITY FILTERING:
│  └─ If mapname in priority list: create map object
│     (Default priority = ["ggit", "asia", "latam", "africa", "europe"])
│
├─ For each row in map_tab:
│  └─ If tracker in map source:
│     └─ Create MapObject via make_map_tracker_objs()
│        └─ SAVE TO LOCAL PKL FILE: map_obj_for_{mapname}_on_{iso_today_date}.pkl
│
└─ Return map_obj_list
```

### Step 2: Create Tracker Objects -> `make_map_tracker_objs(map_tab_df, row, prep_dict)`

**File:** `make_map_tracker_objs.py` (lines 8-99)

For each map, creates tracker objects for each tracker source:

```
make_map_tracker_objs()
├─ Create MapObject with:
│  ├─ mapname (e.g., "asia", "europe", "ggit")
│  ├─ source (list of tracker acronyms, e.g., "GOGPT, GGIT")
│  ├─ geo (geography filter, e.g., "Asia")
│  ├─ fuel (fuel types)
│  ├─ trackers = [] (empty list, will populate with TrackerObjects)
│
├─ For each tracker source item:
│  └─ Create TrackerObject with:
│     ├─ key (Google Sheet key)
│     ├─ off_name (official tracker name)
│     ├─ tab_name (release tab name)
│     ├─ tabs (list of spreadsheet tabs)
│     ├─ release (latest release date)
│     ├─ acro (tracker acronym)
│     ├─ geocol (geography column)
│     ├─ fuelcol (fuel column)
│     ├─ data = pd.DataFrame() (empty, will be populated)
│     └─ about = pd.DataFrame() (empty)
│
│  ===== KEY DATA PULL STEP =====
│  ├─ Call tracker_source_obj.set_df()  <-- DATA LOADING HAPPENS HERE
│  ├─ Call tracker_source_obj.get_about()
│  └─ Append to map_obj.trackers list
│
├─ FILTERING BY GEO AND FUEL:
│  └─ For each tracker in map_obj.trackers:
│     └─ Call tracker.create_filtered_geo_fuel_df(geo, fuel)
│
└─ Return map_obj
```

### Step 3: Data Loading -> `TrackerObject.set_df()` (CRITICAL METHOD)

**File:** `map_tracker_class.py` (lines 72-249)

This is where the actual data pulling happens:

```
TrackerObject.set_df()

├─ CHECK FOR LOCAL PKL CACHE:
│  ├─ pkl_path = local_pkl/trackerdf_for_{acro}_on_{iso_today_date}.pkl
│  ├─ If pkl exists:
│  │  ├─ Ask user: "Use local pkl file? (y/n, default=y)"
│  │  ├─ If yes: Load from pkl and RETURN EARLY
│  │  └─ If no: Continue to fetch from remote
│  │
│  ├─ PROBLEM: This is per-tracker, not per-map
│  │  └─ Multiple maps using same tracker will use SAME pkl
│  │  └─ Causes redundant data pulling if pkl not found
│  │
│
├─ FOR SPECIAL TRACKERS (FETCH FROM S3):
│  ├─ If tab_name in ['Oil Pipelines']:
│  │  └─ Fetch parquet from S3, read as GeoDataFrame
│  ├─ If tab_name in ['Gas Pipelines']:
│  │  └─ Fetch parquet from S3, read as GeoDataFrame
│  ├─ If tab_name in ['Gas Pipelines EU']:
│  │  └─ Fetch GeoJSON from S3
│  └─ (Other special cases for various trackers)
│
├─ FOR MULTI-TAB TRACKERS:
│  ├─ If tab_name in ['Oil & Gas Extraction'] (GOGET):
│  │  └─ self.create_df_goget()
│  │     ├─ Pulls 'Main data' tab from Google Sheet
│  │     ├─ Pulls 'Production & reserves' tab from Google Sheet
│  │     └─ Returns tuple: (main_df, prod_df)
│  │
│  └─ For Iron and Steel and others:
│     ├─ self.create_df()
│        ├─ For each tab in self.tabs:
│        │  ├─ Connect to Google Sheet via gspread
│        │  └─ Fetch all records from worksheet
│        └─ Concat all dfs
│
├─ FOR NORMAL TRACKERS:
│  └─ self.create_df()
│     └─ Fetches data from Google Sheets
│
├─ SAVE TO LOCAL PKL:
│  └─ pickle.dump(self.data, pkl_file)
│     └─ Saved to: local_pkl/trackerdf_for_{acro}_on_{iso_today_date}.pkl
│
└─ Return (data is stored in self.data)
```

### Step 4: Filtering by Geography and Fuel

**File:** `map_tracker_class.py` (lines 979-1068)

```
TrackerObject.create_filtered_geo_fuel_df(geo, fuel)

├─ Get needed_geo from geo_mapping dict
├─
├─ FOR NORMAL TRACKERS (acro != 'GOGET'):
│  ├─ Strip whitespace from columns
│  ├─ Add 'country_to_check' column
│  ├─ Parse geography column(s) via split_countries()
│  ├─ Filter: self.data = self.data[check_list(country_to_check, needed_geo)]
│  └─ If fuel filter: apply fuel filtering
│
└─ FOR GOGET (tuple handling):
   ├─ Unpack: main, prod = self.data (tuple)
   ├─ Apply geo filter to both
   ├─ Apply fuel filter if needed
   └─ Re-pack: self.data = (filtered_main, filtered_prod)
```

### Step 5: Data Export

**File:** `make_data_dwnlds.py` (lines 90-156)

```
For each map_obj in map_obj_list:
├─ Create Excel files for data downloads:
│  ├─ {mapname}-data-download_{date}_{iso_date}.xlsx
│  └─ {mapname}-data-download_{date}_{iso_date}_test.xlsx
│
├─ For each tracker_obj in map_obj.trackers:
│  ├─ Set data_official = data (copy without internal columns)
│  ├─ Write tracker about page to Excel
│  └─ Write tracker data to Excel sheet
│
└─ Save Excel files to S3 (Digital Ocean)
```

---

## 3. THE REDUNDANCY ISSUE

### Current Problem:

**Multiple maps can use the same tracker but trigger multiple data pulls**

Example:
- Map "asia" uses trackers: [GOGPT, GGIT]
- Map "latam" uses trackers: [GOGPT, GGIT]
- Map "europe" uses trackers: [GOGPT, GGIT, GGPT-EU]

**Data Flow with Current Architecture:**

```
run_maps() for "all trackers"
│
├─ make_data_dwnlds(tracker="Oil & Gas Plants")
│  ├─ Finds maps: asia, latam, europe, africa, ggit
│  │
│  └─ make_map_tracker_objs() for each map:
│     ├─ asia:
│     │  └─ TrackerObject(GOGPT).set_df() → Fetches Google Sheet → Caches to pkl
│     │
│     ├─ latam:
│     │  └─ TrackerObject(GOGPT).set_df() → Checks pkl cache (FOUND) → Loads from pkl ✓
│     │
│     ├─ europe:
│     │  └─ TrackerObject(GOGPT).set_df() → Checks pkl cache (FOUND) → Loads from pkl ✓
│     │
│     ├─ africa:
│     │  └─ TrackerObject(GOGPT).set_df() → Checks pkl cache (FOUND) → Loads from pkl ✓
│     │
│     └─ ggit:
│        └─ TrackerObject(GOGPT).set_df() → Checks pkl cache (FOUND) → Loads from pkl ✓
```

**The actual redundancy happens:**
1. **Per-map data loading:** Each map independently loads its trackers
2. **Per-tracker pkl naming:** pkl_path = f'trackerdf_for_{acro}_on_{iso_today_date}.pkl'
3. **No cross-map caching:** Maps don't share tracker instances
4. **User prompt issue:** For EACH map with a tracker, user is prompted about using pkl
5. **Re-filtering:** Each map filters the same data by geo/fuel independently

### Example of Real Redundancy:

If tracker pkl doesn't exist:
```
asia map creation:
├─ set_df() on GOGPT → Pulls from Google Sheet (SLOW)
├─ create_filtered_geo_fuel_df(geo="Asia", fuel=...) → Filters
│
latam map creation:
├─ set_df() on GOGPT → Pkl exists, loads from cache ✓
├─ create_filtered_geo_fuel_df(geo="Latin America", fuel=...) → Filters differently
│
BUT if pkl is deleted or timestamp mismatch:
├─ set_df() on GOGPT → Pulls from Google Sheet AGAIN (REDUNDANT)
```

---

## 4. TRACKER TYPES AND CONFIGURATION

### Official Trackers (from all_config.py)

```python
list_of_all_official = [
    "Oil & Gas Plants",         # GOGPT
    "Coal Plants",              # GCPT
    "Solar",                    # GSPT
    "Wind",                     # GWPT
    "Nuclear",                  # GNPT
    "Hydropower",               # GHPT
    "Bioenergy",                # GBPT
    "Geothermal",               # GGPT
    "Coal Terminals",           # GCTT
    "Oil & Gas Extraction",     # GOGET (special: 2-tab tuple)
    "Coal Mines",               # GCMT
    "LNG Terminals",            # GGIT-LNG
    "Gas Pipelines",            # GGIT (S3 source)
    "Oil Pipelines",            # GOIT (S3 source)
    "Gas Pipelines EU",         # EGT-GAS (S3 source)
    "LNG Terminals EU",         # EGT-TERM (S3 source)
    "Iron & Steel",             # GIST (multi-tab)
    "Iron Ore Mines",           # GIOMT
    "Chemicals",                # (unknown acronym)
    "Cement and Concrete",      # GCCT
]
```

### Tracker Data Sources

**From Google Sheets:**
- Most trackers pull from live Google Sheets
- Configuration key: `multi_tracker_log_sheet_key = '15l2fcUBADkNVHw-Gld_kk7EaMiFFi8ysWt6aXVW26n8'`
- Tabs in sheet: ['source'] and ['map']
- Each tracker in 'source' tab has: key, official name, tab name, tabs list, release date, geocol, fuelcol

**From S3 (Digital Ocean):**
- Gas Pipelines
- Oil Pipelines
- Gas Pipelines EU
- LNG Terminals EU (commented out)

**Special Cases:**
- **GOGET** (Oil & Gas Extraction): Returns TUPLE (main_df, prod_df)
- **Iron & Steel**: Multiple tabs concatenated
- **Regular trackers**: Single tab or multiple tabs concatenated

### Tracker Map to Acronym Mapping (all_config.py lines 164-189)

```python
official_tracker_name_to_mapname = {
    "Oil & Gas Plants": "gogpt",
    "Coal Plants": "gcpt",
    "Solar": "gspt",
    "Wind": "gwpt",
    "Nuclear": "gnpt",
    "Hydropower": "ghpt",
    "Bioenergy": "gbpt",
    "Geothermal": "ggpt",
    "Coal Terminals": "gctt",
    "Oil & Gas Extraction": "goget",
    "Coal Mines": "gcmt",
    "LNG Terminals": "ggit-lng",
    "Gas Pipelines": "ggit",
    "Oil Pipelines": "goit",
    "Gas Pipelines EU": "egt-gas",
    "LNG Terminals EU": "egt-term",
    "GOGPT EU": "gogpt-eu",
    "Iron & Steel": "gist",
    "Plumes": "gmet",
    "Iron Ore Mines": "giomt",
    "Cement and Concrete": "gcct",
}
```

---

## 5. FILE STRUCTURE AND KEY CLASSES

### Core Files

```
gem_tracker_maps/
├─ all_config.py                 # Configuration (trackers, folders, Google Sheet keys)
├─ launcher.py                   # CLI argument parsing
├─ run_maps.py                   # Main entry point orchestrates the workflow
├─ make_data_dwnlds.py           # Creates data downloads and map objects
├─ make_map_tracker_objs.py      # Factory for creating tracker objects
├─ map_tracker_class.py          # TrackerObject class (SET_DF HAPPENS HERE)
├─ map_class.py                  # MapObject class
├─ make_map_file.py              # Creates final map files from map objects
├─ helper_functions.py           # Utility functions (geo filtering, data cleaning)
│
├─ local_pkl/                    # Local pickle file cache directory
│  └─ trackerdf_for_{acro}_on_{date}.pkl
│
└─ trackers/
   ├─ integrated/compilation_output/
   ├─ giomt/
   ├─ gist/
   ├─ gmet/
   ├─ solar/, wind/, nuclear/, etc.
```

### Key Classes

**TrackerObject (map_tracker_class.py)**
```python
class TrackerObject:
    def __init__(self,
                 off_name="",           # e.g., "Oil & Gas Plants"
                 tab_name="",           # Release tab name
                 acro="",               # e.g., "GOGPT"
                 key="",                # Google Sheet key
                 tabs=[],               # List of worksheet tabs
                 release="",            # Release date
                 geocol=[],             # Geography column name(s)
                 fuelcol="",            # Fuel column name
                 about_key="",          # About page sheet key
                 about=pd.DataFrame(),  # About page content
                 data=pd.DataFrame()):  # Tracker data (MAIN STORAGE)
                 
    def set_df(self):                  # LOADS DATA (with pkl caching)
    def create_df(self):               # Fetches from Google Sheets
    def create_df_goget(self):         # Special: Returns tuple
    def create_filtered_geo_fuel_df(): # Filters by geography and fuel
    def get_about(self):               # Loads about page
```

**MapObject (map_class.py)**
```python
class MapObject:
    def __init__(self,
                 mapname="",            # e.g., "asia"
                 source="",             # Comma-separated tracker names
                 geo="",                # Geography filter
                 needed_geo=[],         # List of countries
                 fuel="",               # Fuel types
                 trackers=[],           # List of TrackerObjects
                 about=pd.DataFrame()):
```

---

## 6. PKL CACHING MECHANISM

### How Caching Works

**Location:** `local_pkl/` directory (created in all_config.py)

**File Naming:**
```
trackerdf_for_{acro}_on_{iso_today_date}.pkl
Examples:
- trackerdf_for_GOGPT_on_2025-11-05.pkl
- trackerdf_for_GGIT_on_2025-11-05.pkl
```

**Caching Logic (set_df method, lines 78-100):**

1. Check if pkl file exists
2. If exists:
   - Show creation time to user
   - Prompt: "Use local pkl file? (y/n, default=y)"
   - If yes: Load from pkl and RETURN EARLY
   - If error: Log and continue to fetch

3. If doesn't exist or user says no:
   - Fetch from Google Sheets (or S3)
   - Process data (create_df or create_df_goget)
   - Save to pkl file

### Caching Issues

1. **Scope is per-tracker, not per-map**
   - Only prevents re-fetching same tracker for SAME date
   - If any map is run, pkl is created
   - If another map uses same tracker, it reuses pkl

2. **Interactive prompts cause delays**
   - For each map using a tracker, user sees "Use local pkl?" prompt
   - Can't batch-run without manual input

3. **Date-dependent**
   - New pkl created every day (even if no new data)
   - `iso_today_date` changes daily

4. **No version control**
   - If tracker data updates mid-run, pkl still used
   - Could cause inconsistency between maps

---

## 7. MAP ITERATION LOGIC

### How Maps Are Iterated

**Entry: `run_maps()` (run_maps.py)**
```python
def run_maps():
    for tracker in tqdm(trackers_to_update, desc='Running'):
        # trackers_to_update = ["LNG Terminals"] (from all_config.py)
        
        map_obj_list = make_data_dwnlds(tracker)
        # Returns list of MapObjects that use this tracker
```

**Inside `make_data_dwnlds(tracker)` (lines 19-159):**
```python
# Priority filtering
for row in map_tab_df.index:
    if map_tab_df.loc[row, 'mapname'] in priority:
        if tracker in map_tab_df.loc[row, 'source']:
            # This map uses this tracker
            map_obj = make_map_tracker_objs(map_tab_df, row, prep_dict)
            map_obj_list.append(map_obj)

# Write data for all maps
for map_obj in map_obj_list:
    for tracker_obj in map_obj.trackers:
        # Create Excel files, write to S3
```

### Priority-based Filtering

```python
priority = ["ggit", "asia", "latam", "africa", "europe"]

# In make_data_dwnlds:
if map_tab_df.loc[row, 'mapname'] in priority:
    # Create map object
elif priority == [''] or None:
    # Create all maps
else:
    # Skip this map
    continue
```

This allows running only specific maps for faster testing/debugging.

---

## 8. SUMMARY OF KEY METHODS

### Data Loading Path

```
run_maps()
  ↓
make_data_dwnlds(tracker)
  ↓
make_map_tracker_objs(map_tab_df, row, prep_dict)
  ├─ MapObject.get_about()
  ├─ For each tracker source:
  │   └─ TrackerObject.set_df()  ← DATA PULLED HERE
  │       ├─ Check local pkl cache
  │       ├─ If not found: create_df() or create_df_goget()
  │       └─ Save to pkl
  │
  └─ TrackerObject.create_filtered_geo_fuel_df()
      ├─ Add country_to_check column
      ├─ Filter by geography
      └─ Filter by fuel
```

### Filtering Path

```
create_filtered_geo_fuel_df(geo, fuel)
  ├─ Get needed_geo from geo_mapping[geo]
  ├─ For NORMAL trackers:
  │   ├─ Parse geocol(s)
  │   ├─ Add country_to_check column
  │   └─ Filter: check_list(country_to_check, needed_geo)
  │
  └─ For GOGET (tuple):
      ├─ Unpack (main, prod)
      ├─ Apply filters to both
      └─ Re-pack (filtered_main, filtered_prod)
```

---

## 9. CURRENT DATA FLOW DIAGRAM

```
┌─────────────────────────────────────────────────────┐
│ run_maps()                                          │
│ trackers_to_update = ["LNG Terminals"]              │
└────────────────────┬────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────┐
│ make_data_dwnlds(tracker="LNG Terminals")            │
│ Reads Google Sheet: multi_tracker_log_sheet_key     │
│ Finds all maps that use "LNG Terminals"             │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        ↓            ↓            ↓
    ggit       ggit-lng      (others)
    
    For each map:
    └─────────────────────────────────────────────────┐
      │ make_map_tracker_objs(map_tab_df, row, prep_dict)
      │
      ├─ Create MapObject(mapname="ggit", ...)
      │
      ├─ For each tracker source (e.g., "GGIT, GOGPT"):
      │  │
      │  ├─ Create TrackerObject(off_name="LNG Terminals", acro="GGIT", ...)
      │  │
      │  ├─ set_df():
      │  │  ├─ Check: local_pkl/trackerdf_for_GGIT_on_2025-11-05.pkl
      │  │  ├─ If exists: Load from pkl
      │  │  ├─ If not exists: Fetch from Google Sheet or S3
      │  │  └─ Save to pkl
      │  │
      │  ├─ get_about()
      │  │
      │  └─ Append to map_obj.trackers
      │
      ├─ create_filtered_geo_fuel_df(geo="Global", fuel=...)
      │  ├─ Filter by geography
      │  └─ Filter by fuel
      │
      └─ Return map_obj
      
    Write to Excel and S3
```

---

## 10. OPTIMIZATION OPPORTUNITIES

### Current Inefficiencies

1. **Redundant Google Sheet Fetches**
   - Each tracker is fetched independently for each map
   - If pkl cache is cleared, fetches multiply

2. **User Prompts in Automation**
   - "Use local pkl?" prompt requires interactive input
   - Blocks batch processing

3. **Per-map Filtering**
   - Same tracker filtered independently for each map
   - Could share filtered data

4. **No Shared TrackerObject Instances**
   - Each map creates new TrackerObject instances
   - Even for same tracker

5. **Date-based Cache Invalidation**
   - New pkl every day, even if data hasn't changed
   - Could improve with content-based caching

### Proposed Solutions

**See optimization recommendations in the analysis summary**

