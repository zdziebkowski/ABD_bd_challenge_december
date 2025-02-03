# Weather Data Pipeline Project

## Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline for weather data processing. It fetches weather data from the Open-Meteo API for multiple cities, processes it through various stages, and ultimately presents it in a format suitable for analysis and visualization.

## Tools and Technologies

Core Technologies:
- Python - Main programming language
- Apache Spark - Big data processing engine
- PostgreSQL - Relational database for data storage
- Shiny for Python - Interactive dashboard for data visualization

Python Libraries:
- PySpark - Python API for Apache Spark
- Requests - For API calls to Open-Meteo
- Typer - CLI interface creation
- Logging - Built-in logging functionality
- psycopg2 - PostgreSQL adapter for Python
- plotly - Interactive charting for dashboards 
- Folium - Map visualization

Data Formats:
- JSON - Raw data storage
- Parquet - Optimized columnar storage

APIs:
- Open-Meteo API - Weather data source

Development Tools:
- Git - Version control
- Virtual Environment - Python environment management

Monitoring & Logging:
- Custom logging system with file and console outputs
- Error tracking and retry mechanisms

## Project Structure
```
ABD_bd_challenge_december/
├── analysis_results/
│   ├── map_avg_temp/
│   ├── ranking_temperature/
│   ├── top_weather_code/
├── data/
├── fetchers/
│   ├── current_fetcher.py
│   ├── historical_fetcher.py
├── lib/
├── logs/
├── parquet/
├── utils/
│   ├── __init__.py
│   ├── config.py
│   ├── database_connection.py
│   ├── db_config.py
│   ├── file_manager.py
├── fetch_weather.bat
├── json_to_parquet.py
├── postgres_setup.py
├── shiny_dashboard.py
├── transform_data.py
├── requirements.txt
├── .gitignore
├── README.md

```

## Features

### 1. Data Extraction (E)
- Fetches current and historical weather data from Open-Meteo API
- Supports multiple cities (26 cities configured)
- Handles both current weather and historical data retrieval
- Includes robust error handling and logging
- CLI interface for easy data fetching

### 2. Data Transformation (T)
- Converts JSON files to Parquet format
- Processes raw weather data into analytical insights:
  - Temperature rankings across cities
  - Wind analysis and patterns
  - Day/night weather patterns
  - Weather code frequency analysis
- Implements data cleaning and standardization
- Uses Apache Spark for efficient data processing

### 3. Data Loading (L)
- Loads processed data into PostgreSQL database
- Creates optimized table schemas
- Implements batch processing for large datasets
- Includes automatic backup management
- Handles data versioning and updates

### 4. Data Visualization
- Interactive **Shiny for Python** dashboard for real-time weather data analysis
- **Plotly charts** for temperature ranking and weather code frequency
- **Folium-based map** for visualizing city temperature distribution
- Integrated data retrieval from PostgreSQL using `utils/database_connection.py`

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Configure PostgreSQL:
- Update `utils/db_config.py` with your database credentials
- Ensure PostgreSQL is installed and running

## Usage

### Fetching Weather Data
```bash
# Fetch current weather for all cities
python -m fetchers.current_fetcher --output-dir "data/current"

# Fetch historical weather data
python -m fetchers.historical_fetcher --city "Warsaw" --start-date "2024-01-01" --end-date "2024-01-31" --output-dir "data/historical"
```

### Processing Data
```bash
# Convert JSON to Parquet
python json_to_parquet.py

# Transform data
python transform_data.py

# Load data to PostgreSQL
python postgres_setup.py
```

### Running the Dashboard
```bash
# Start the interactive Shiny dashboard
shiny run shiny_dashboard.py
```
## Configuration

### Cities
The project includes predefined cities with their coordinates in `utils/config.py`. Cities include:
- Major Polish cities (Warsaw, Krakow, etc.)
- Selected European locations
- Custom locations can be added to the configuration

### Database
Default database configuration in `utils/db_config.py`:
- Database name: weather_db
- Default port: 5432
- Configurable user credentials

### Database Connection
The database_connection.py module provides utility functions to fetch data directly from the PostgreSQL database. It retrieves:
- City temperature rankings
- Weather code frequency
- Map data for visualizations

## Data Flow
1. Raw weather data is fetched from the API (JSON format)
2. Data is converted to Parquet format for efficient processing
3. Spark transformations create analytical datasets
4. Results are stored in PostgreSQL for analysis
5. Data can be visualized using Shiny for Python

## Error Handling and Logging
- Comprehensive logging system
- Automatic retry mechanism for API calls
- Database operation monitoring
- Backup management for data safety

## Future Enhancements
- Additional weather metrics analysis
- Advanced visualization capabilities
- Advanced analytics features

## Contributing
Contributions are welcome! Please feel free to submit pull requests.

## Acknowledgments
- Weather data provided by Open-Meteo API
- Project developed as part of the Big Data December Challenge
