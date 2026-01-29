# Data Directory Structure

This directory contains all input and output data for the Cyclone Impact Toolkit.

---

## Complete Directory Layout

```
data/
├── inputs/                                    # Input data required for analysis
│   ├── viirs/                                # VIIRS satellite data
│   │   ├── phl/
│   │   │   ├── 2023/                        # VIIRS CSV files by year
│   │   │   ├── 2024/
│   │   │   └── 2025/
│   │   ├── vnm/
│   │   └── idn/
│   │
│   ├── gis/                                  # GIS reference data (shared)
│   │   ├── cyclone_tracks/                  # IBTrACS cyclone track data
│   │   │   └── cache/                       # Cached downloaded tracks
│   │   │       ├── IBTrACS.last3years.*.zip
│   │   │       └── IBTrACS.last3years.*.shp
│   │   │
│   │   ├── countries/                       # Country-specific GIS data
│   │   │   ├── phl/
│   │   │   │   ├── eez/                    # EEZ boundaries
│   │   │   │   │   ├── phl_eez.shp
│   │   │   │   │   ├── phl_eez.shx
│   │   │   │   │   ├── phl_eez.dbf
│   │   │   │   │   └── phl_eez.prj
│   │   │   │   ├── fishing_grounds/        # Fishing ground polygons
│   │   │   │   │   └── phl_merged_dense_area_polygons_2023.geojson
│   │   │   │   ├── centroids/              # Polygon centroids
│   │   │   │   │   └── polygon_centroids_historical.csv
│   │   │   │   └── baselines/              # Baseline reference data
│   │   │   │       ├── baseline_nowcast.csv
│   │   │   │       ├── avg_daily_boats_noty_phl_2023.csv
│   │   │   │       └── no_ty_file_pivoted_avg_per_contour.csv
│   │   │   ├── vnm/
│   │   │   └── idn/
│   │   │
│   │   ├── wrddsf.shp                       # Shared reference shapefiles
│   │   ├── wrddsf.shx
│   │   ├── wrdph.shp
│   │   └── wrdph.shx
│   │
│   └── uploads/                              # User-uploaded cyclone tracks
│       └── temp/                            # Temporary upload storage
│
└── outputs/                                  # Generated analysis results
    ├── historical/                           # Historical analysis outputs
    │   ├── phl/
    │   │   ├── 2023/
    │   │   │   ├── intermediate/            # Processing files (~30 CSVs)
    │   │   │   │   ├── t_processed.csv     # Processed VIIRS data
    │   │   │   │   ├── td_phl_2023.csv     # Typhoon day data
    │   │   │   │   ├── lin11d_phl_2023.csv # Filtered cyclone data
    │   │   │   │   ├── all_filtered_2023.csv
    │   │   │   │   ├── filtered_2023.csv
    │   │   │   │   ├── clipped_original_data_phl_2023.csv
    │   │   │   │   ├── phl_merged_dense_area_polygons_2023.geojson
    │   │   │   │   ├── boats_fishing_grounds_phl_2023.csv
    │   │   │   │   └── ... (20+ more processing files)
    │   │   │   ├── analysis/                # Final results for dashboards
    │   │   │   │   ├── phl_boatdiff_2023.csv
    │   │   │   │   ├── phl_boatdiff2_2023.csv
    │   │   │   │   ├── phl_logdatadf_py_new_2023_all.csv
    │   │   │   │   └── phl_logdatadf0_py_new_2023_all.csv
    │   │   │   └── visualizations/          # Maps and animations
    │   │   │       ├── phl_2023_map.png    # Main fishing grounds map
    │   │   │       ├── gifs/                # Animated cyclone progressions
    │   │   │       │   ├── gif_Doksuri.gif
    │   │   │       │   ├── gif_Saola.gif
    │   │   │       │   └── ...
    │   │   │       └── maps/                # Individual cyclone frames
    │   │   │           ├── Doksuri_2023-07-22.png
    │   │   │           ├── Doksuri_2023-07-23.png
    │   │   │           └── ...
    │   │   ├── 2024/
    │   │   └── 2025/
    │   ├── vnm/
    │   └── idn/
    │
    └── nowcast/                              # Real-time nowcast outputs
        ├── phl/
        │   ├── runs/                        # Date-stamped nowcast runs
        │   │   ├── 2025-01-15/
        │   │   │   ├── storm_speed_stats.csv
        │   │   │   ├── phl_logdatadf_py_new_2025.csv
        │   │   │   ├── pivot_table_test.csv
        │   │   │   └── phl_2025_map.png
        │   │   └── 2025-01-20/
        │   └── latest/                      # Symlink → most recent run
        ├── vnm/
        └── idn/
```

---

## Input Data Details

### 1. VIIRS Satellite Data
**Path:** `data/inputs/viirs/{country}/{year}/`

- **Content:** VIIRS-DNB (Day/Night Band) boat detection data
- **Format:** CSV files (may be gzipped)
- **Size:** Can be large (100MB - 2GB per year)
- **Usage:** Historical analysis only
- **Organization:** By country ISO3 code and year

**Example:**
```
viirs/phl/2024/
├── VBD_npp_d20240101_*.csv.gz
├── VBD_npp_d20240102_*.csv.gz
└── ... (365 daily files)
```

### 2. GIS Reference Data

#### A. Cyclone Tracks
**Path:** `data/inputs/gis/cyclone_tracks/cache/`

- **Content:** IBTrACS (International Best Track Archive) cyclone data
- **Format:** Shapefiles (.shp, .shx, .dbf, .prj)
- **Source:** NOAA IBTrACS database
- **Updates:** Downloaded automatically when needed

#### B. Country-Specific Data
**Path:** `data/inputs/gis/countries/{country}/`

**EEZ Boundaries** (`eez/`)
- Exclusive Economic Zone shapefiles
- Defines maritime boundaries for analysis
- Format: Shapefile set (`.shp`, `.shx`, `.dbf`, `.prj`)

**Fishing Grounds** (`fishing_grounds/`)
- Delineated fishing ground polygons
- Generated from historical boat density analysis
- Format: GeoJSON

**Centroids** (`centroids/`)
- Centroid points of fishing ground polygons
- Used for distance calculations
- Format: CSV with lat/lon columns

**Baselines** (`baselines/`)
- Historical baseline boat counts
- Used for comparative analysis
- Files:
  - `baseline_nowcast.csv` - Daily baseline counts by fishing ground
  - `avg_daily_boats_noty_phl_{year}.csv` - Average daily boats (no typhoon)
  - `no_ty_file_pivoted_avg_per_contour.csv` - Pivot table of baseline data

#### C. Shared Reference Shapefiles
**Path:** `data/inputs/gis/`

- `wrddsf.shp` - World reference data (shared)
- `wrdph.shp` - World reference data (shared)

### 3. User Uploads
**Path:** `data/inputs/uploads/temp/`

- **Content:** User-uploaded custom cyclone track files
- **Usage:** Nowcast analysis with custom forecasts
- **Lifespan:** Temporary (cleaned periodically)
- **Format:** Shapefile or GeoJSON

---

## Output Data Details

### 1. Historical Analysis Outputs

#### A. Intermediate Files
**Path:** `data/outputs/historical/{country}/{year}/intermediate/`

**Purpose:** Processing files generated during the analysis pipeline

**Key Files:**
- `t_processed.csv` (large) - Processed VIIRS boat detection data
- `td_{country}_{year}.csv` - Typhoon day filtered data
- `lin11d_{country}_{year}.csv` - Processed cyclone track points
- `all_filtered_{year}.csv` - All filtered cyclone data
- `filtered_{year}.csv` - Filtered cyclone points in EEZ
- `clipped_original_data_{country}_{year}.csv` - Boats clipped to fishing grounds
- `{country}_merged_dense_area_polygons_{year}.geojson` - Fishing ground boundaries
- `boats_fishing_grounds_{country}_{year}.csv` - Boats by fishing ground
- `phl_boatdiff_{year}.csv` - Boat differences (preliminary)
- Various pivot tables, statistics, and intermediate calculations

**Size:** Can be several hundred MB per year

#### B. Analysis Files
**Path:** `data/outputs/historical/{country}/{year}/analysis/`

**Purpose:** Final results ready for dashboard consumption

**Key Files:**
- `{country}_boatdiff_{year}.csv` - Final boat count differences
- `{country}_boatdiff2_{year}.csv` - Enhanced boat differences with statistics
- `{country}_logdatadf_py_new_{year}_all.csv` - Complete log-transformed analysis
- `{country}_logdatadf0_py_new_{year}_all.csv` - Alternative analysis format

**Usage:** These files feed directly into the dashboard visualizations

#### C. Visualizations
**Path:** `data/outputs/historical/{country}/{year}/visualizations/`

**Main Map:**
- `{country}_{year}_map.png` - Overview map of fishing grounds and cyclone activity

**Cyclone Frame Maps** (`maps/`)
- Individual PNG frames for each cyclone date
- Format: `{CycloneName}_{YYYY-MM-DD}.png`
- Resolution: 300 DPI
- Example: `Gaemi_2024-07-22.png`

**Animated GIFs** (`gifs/`)
- Animated cyclone progression for each storm
- Format: `gif_{CycloneName}.gif`
- Frame rate: 10 FPS
- Example: `gif_Gaemi.gif`

**Typical Output:**
- 1 main map
- 10-15 GIF animations
- 30-60 individual PNG frames

### 2. Nowcast Analysis Outputs

#### A. Timestamped Runs
**Path:** `data/outputs/nowcast/{country}/runs/{YYYY-MM-DD}/`

**Purpose:** Each real-time forecast is saved with a timestamp

**Files per run:**
- `storm_speed_stats.csv` - Cyclone speed statistics
- `{country}_logdatadf_py_new_{year}.csv` - Analysis results
- `pivot_table_test.csv` - Distance calculations
- `{country}_{year}_map.png` - Forecast map

#### B. Latest Symlink
**Path:** `data/outputs/nowcast/{country}/latest/`

**Purpose:** Always points to the most recent nowcast run

**Usage:** Dashboards can always read from `latest/` to get current forecasts

---

## File Size Guidelines

### Input Data
| Type | Size Range | Notes |
|------|-----------|-------|
| VIIRS (per year) | 100 MB - 2 GB | Compressed: ~50-500 MB |
| EEZ Shapefiles | 1-10 MB | Per country |
| Fishing Grounds | 10-100 KB | GeoJSON |
| Baselines | 10-50 KB | CSV files |
| Cyclone Tracks | 5-50 MB | Cached shapefiles |

### Output Data
| Type | Size Range | Notes |
|------|-----------|-------|
| Intermediate CSVs | 200-500 MB | Per year |
| Analysis CSVs | 1-5 MB | Per year |
| Visualizations | 50-100 MB | PNGs + GIFs per year |
| Nowcast Run | 1-5 MB | Per run |

### Total Storage Estimates
- **Per Country Per Year:** ~300-600 MB (with intermediate files)
- **3 Countries, 3 Years:** ~2.5-5 GB
- **Add 20 Nowcast Runs:** +20-100 MB

---

## Data Management

### Git Exclusions

Large data files are excluded from version control (see `.gitignore`):

✅ **Tracked:**
- Folder structure (`.gitkeep` files)
- Small baseline CSV files
- Documentation

❌ **Ignored:**
- VIIRS satellite data
- Large shapefiles
- All intermediate CSV files
- All output files (CSVs, PNGs, GIFs)
- Cached cyclone tracks

### Backup Recommendations

**Critical to backup:**
1. Baseline reference files (`baselines/`)
2. Fishing ground polygons (`fishing_grounds/`)
3. Final analysis results (`analysis/`)

**Can be regenerated:**
- Intermediate processing files
- Visualization PNGs and GIFs
- Cached cyclone tracks

### Cleanup Tasks

**Regular maintenance:**
```bash
# Remove old nowcast runs (keep last 30 days)
find data/outputs/nowcast/*/runs/* -mtime +30 -delete

# Archive old intermediate files
tar -czf archive_phl_2023.tar.gz data/outputs/historical/phl/2023/intermediate/
rm -rf data/outputs/historical/phl/2023/intermediate/*.csv

# Clean cache
rm -rf data/inputs/gis/cyclone_tracks/cache/*
```

### Migration Script

Use `scripts/manage_data.py` to:
- Migrate data from old project structure
- Validate folder organization
- Generate storage reports
- Clean up old files

---

## Usage in Code

### Historical Analysis
```python
from backend.services.historical import Config

config = Config.from_defaults(
    country="phl",
    year_selected=2024,
    cyclone_seasons=cyclone_seasons
)
# Paths are automatically set:
# - viirs_path: data/inputs/viirs/phl/2024/
# - gis_path: data/inputs/gis/
# - output_path: data/outputs/historical/phl/2024/intermediate/
# - graphs_path: data/outputs/historical/phl/2024/visualizations/
```

### Nowcast Analysis
```python
from backend.services.nowcast import NowcastConfig

config = NowcastConfig.from_defaults(
    country="phl",
    year_selected=2025
)
# Paths are automatically set:
# - gis_path: data/inputs/gis/
# - output_path: data/outputs/nowcast/phl/runs/{timestamp}/
# - baseline_csv_path: data/inputs/gis/countries/phl/baselines/baseline_nowcast.csv
```

---

## Storage Best Practices

1. **Keep baselines in version control** - Small and essential
2. **Archive old intermediate files** - Can be large, regenerate if needed
3. **Retain final analysis files** - Critical for dashboards
4. **Clean nowcast runs regularly** - Keep last 30 days only
5. **Document data sources** - Track when/where data was obtained
6. **Use compression** - Compress large CSV files with gzip

---

## Questions & Support

For issues with:
- **Missing data:** Check `data/inputs/gis/countries/{country}/`
- **Large files:** Review `.gitignore` and cleanup old runs
- **Folder structure:** Run `scripts/manage_data.py validate`
