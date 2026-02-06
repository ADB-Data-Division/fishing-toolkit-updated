# Cyclone Impact Toolkit

A unified desktop application for analyzing tropical cyclone impacts on fishing activities, combining historical analysis and real-time nowcast capabilities.

## Overview

The Cyclone Impact Toolkit provides comprehensive analysis of typhoon impacts on fishing grounds through two distinct modes:

- **Historical Mode**: Analyze past typhoon events and their long-term impacts on fishing activity
- **Nowcast Mode**: Real-time typhoon tracking and immediate impact assessment

## Data Source
1. Fishing activity data are retrieved from the Visible Infrared Imaging Radiometer Suite (VIIRS) satellite imagery of the [Colorado School of Mines](https://eogauth-new.mines.edu/realms/eog/protocol/openid-connect/auth?response_type=code&scope=openid%20email&client_id=eogdata-new-apache&state=_YbW0R8Gcy5ChuupI3wDGUi3oJY&redirect_uri=https://eogdata.mines.edu/oauth2callback&nonce=yiVqlLQWyMEHEHrLORMdiOuTbnHVhc3xvODFcx6eEpA).
2. Cyclone track and intensity data are sourced from the [International Best Track Archive for Climate Stewardship (IBTrACS)](https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/shapefile/). 

## Features

### Historical Analysis
- Year-based filtering (2023-2025)
- Comprehensive typhoon impact data
- Fishing ground analysis per typhoon
- Interactive charts and visualizations
- Data export capabilities

### Nowcast Analysis
- Real-time typhoon tracking
- CSV and Shapefile data upload
- Dynamic fishing ground impact assessment
- Live typhoon track visualization
- Immediate decision support

### Unified Interface
- Welcome screen with mode selection
- Consistent user experience across modes
- Shared fishing grounds data (where applicable)
- Single application deployment

## Architecture

The application follows a clean architecture pattern with unified API initialization:

```
backend/
├── api/           # PyWebView API bridges (unified initialization)
├── repositories/  # Data access layer (TinyDB)
├── services/      # Business logic (ready for future expansion)
└── models/        # Data models (ready for future expansion)

frontend/
├── static/
│   ├── welcome/   # Landing page with mode selection
│   ├── nowcast/   # Nowcast dashboard
│   ├── historical/ # Historical dashboard
│   └── shared/    # Common assets
```

### Key Features
- **Single Page Application**: All screens in one HTML file with JavaScript navigation
- **Unified API**: Single API instance handles both modes
- **No Threading Issues**: Uses client-side navigation instead of window creation
- **Configuration Screens**: Dedicated setup screens for each mode
- **Seamless Navigation**: Smooth transitions between all screens
- **Back Navigation**: Return to welcome screen from any mode

## Installation

### Prerequisites
- Python 3.12+
- `uv` package manager (recommended)

### Development Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   uv sync
   ```

3. Create historical database:
   ```bash
   uv run python scripts/create_historical_db.py
   ```

4. Add sample nowcast data (optional):
   ```bash
   uv run python scripts/add_sample_typhoons.py
   ```

5. Run the application:
   ```bash
   uv run python main.py
   ```

### Building for Distribution

1. Navigate to build directory:
   ```bash
   cd build/
   ```

2. Run build script:
   ```bash
   python build.py
   ```

3. The packaged application will be in `dist/cyclone_toolkit/`

## Usage

### User Journey

1. **Welcome Screen**: Choose between Historical or Nowcast analysis
2. **Configuration Screen**: Set up analysis parameters (country, year, data source)
3. **Analysis Processing**: Loading screen with progress indicators
4. **Dashboard Screen**: View results and interact with data
5. **Navigation**: Use "← Home" button to return to welcome screen

### Historical Mode

1. Click "Historical" → Configuration screen
2. Select country and year (2023-2025)
3. Click "Run Historical Analysis"
4. View dashboard with typhoon data and impact analysis

### Nowcast Mode

1. Click "Nowcast" → Configuration screen
2. Select country and data source (IBTACS or Synthetic)
3. For synthetic: upload CSV/Shapefile data
4. Click "Run Nowcast Analysis"
5. View dashboard with real-time typhoon tracking

## Data Sources

### Historical Data
- Pre-loaded CSV files with typhoon impact data
- Track data from sample files
- Fishing ground definitions per typhoon

### Nowcast Data
- User-uploaded CSV files with daily predictions
- Shapefile track data
- Static fishing grounds GeoJSON

## Database Structure

The application uses TinyDB for data storage:

- `database/nowcast.json`: Real-time typhoon data
- `database/historical.json`: Historical analysis data

## Development

### Project Structure
```
cyclone-impact-toolkit/
├── main.py                    # Application entry point
├── backend/                   # Backend logic
│   ├── api/                  # PyWebView APIs
│   ├── repositories/         # Data access
│   └── services/             # Business logic
├── frontend/static/          # Frontend assets
├── scripts/                  # Utility scripts
├── build/                    # Build configuration
└── tests/                    # Test suite
```

### Adding New Features

1. **New API Methods**: Add to appropriate API class in `backend/api/`
2. **Data Operations**: Extend repository classes in `backend/repositories/`
3. **Frontend Changes**: Modify HTML/CSS/JS in `frontend/static/`
4. **Business Logic**: Add to service classes in `backend/services/`

### Testing

Run the test suite:
```bash
uv run python -m pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This research was funded by the Japan Fund for Prosperous and Resilient Asia and the Pacific (Government of Japan) through the Asian Development Bank.

## Support

For issues and questions:
1. Check the troubleshooting section in `build/BUILD_README.md`
2. Review the console output for error messages
3. Ensure all dependencies are properly installed

## Future Enhancements

- Side-by-side comparison mode
- Advanced data visualization
- Machine learning predictions
- Multi-country support
- Cloud data integration
- Mobile companion app
