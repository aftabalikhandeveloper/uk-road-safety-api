# UK Road Safety Data Platform
## Comprehensive Data Integration & Execution Plan

**Version:** 1.0  
**Date:** January 2026  
**Objective:** Build an integrated, auto-updating database combining UK road accident data with enrichment sources

---

## Executive Summary

This plan outlines the integration of 8+ data sources into a unified PostGIS database with automated daily/weekly/annual update pipelines. The platform will power dashboard visualizations and API services for insurance, fleet, legal, and government clients.

---

## Part 1: Data Sources Inventory

### 1.1 Core Data: STATS19 Road Accidents

| Attribute | Details |
|-----------|---------|
| **Source** | Department for Transport via data.gov.uk |
| **URL** | https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data |
| **Format** | CSV files (Collisions, Casualties, Vehicles) |
| **Coverage** | 1979 to present (currently through 2024) |
| **Size** | ~1.4GB (all years), ~18MB (single year) |
| **Update Frequency** | Annual (September) + Provisional mid-year (November) |
| **License** | Open Government Licence v3.0 |
| **Key Fields** | accident_index, longitude, latitude, date, time, severity, LSOA, police_force, 36+ attributes |

**Files to Download:**
```
Road Safety Data - Collisions - 1979 - Latest Published Year.csv (1.4GB)
Road Safety Data - Vehicles - 1979 - Latest Published Year.csv (1.6GB)
Road Safety Data - Casualties - 1979 - Latest Published Year.csv (911MB)
```

**Alternative: R stats19 Package**
```r
# Automated download using stats19 R package
library(stats19)
crashes_2024 <- get_stats19(year = 2024, type = "collision")
casualties_2024 <- get_stats19(year = 2024, type = "casualty")
vehicles_2024 <- get_stats19(year = 2024, type = "vehicle")
```

---

### 1.2 Geographic Boundaries: LSOA/MSOA

| Attribute | Details |
|-----------|---------|
| **Source** | ONS Open Geography Portal |
| **URL** | https://geoportal.statistics.gov.uk/ |
| **Format** | GeoJSON, Shapefile, KML |
| **Coverage** | England & Wales (34,753 LSOAs for 2021 census) |
| **Update Frequency** | Decennial (census years) + boundary changes |
| **License** | Open Government Licence v3.0 |
| **API Endpoint** | ArcGIS REST API |

**API Query Example:**
```python
ENDPOINT = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"

params = {
    "where": "1=1",
    "outFields": "*",
    "returnGeometry": "true",
    "f": "geojson",
    "resultRecordCount": 2000,
    "resultOffset": 0
}
```

**Key Datasets:**
- LSOA 2021 Boundaries (England & Wales)
- MSOA 2021 Boundaries
- Local Authority District Boundaries
- Police Force Area Boundaries

---

### 1.3 Traffic Volume: DfT Traffic Counts

| Attribute | Details |
|-----------|---------|
| **Source** | Department for Transport Road Traffic Statistics |
| **URL** | https://roadtraffic.dft.gov.uk/downloads |
| **API** | https://roadtraffic.dft.gov.uk/api |
| **Format** | CSV, REST API (JSON) |
| **Coverage** | ~45,000 count points across GB |
| **Update Frequency** | Annual (June) |
| **License** | Open Government Licence v3.0 |
| **Key Fields** | count_point_id, AADF, road_name, coordinates, vehicle_type breakdown |

**API Endpoints:**
```
GET /api/count-points              # All count point locations
GET /api/count-points/{id}         # Single count point
GET /api/count-points/{id}/aadf    # Annual average daily flows
GET /api/raw-counts                # Raw manual count data
```

**Files:**
```
dft_traffic_counts_aadf.csv        # Annual Average Daily Flows
dft_traffic_counts_raw.csv         # Raw count data
count_point_locations.csv          # Count point metadata
```

---

### 1.4 Real-Time Traffic: National Highways NTIS

| Attribute | Details |
|-----------|---------|
| **Source** | National Highways |
| **URL** | https://www.trafficengland.com/services-info |
| **API** | WebTRIS API (free, no registration) |
| **Format** | DATEX II (XML), JSON via WebTRIS |
| **Coverage** | Strategic Road Network (motorways + major A roads) |
| **Update Frequency** | Real-time (1-5 minute intervals) |
| **License** | Acceptable Use Policy |
| **Data Types** | Incidents, Journey Times, Speeds, Flows, VMS settings |

**WebTRIS API (Free, No Auth):**
```
Base URL: https://webtris.highwaysengland.co.uk/api

GET /v{version}/sites                    # All monitoring sites
GET /v{version}/sites/{site_id}          # Site details
GET /v{version}/reports/{site_ids}/...   # Traffic reports
GET /v{version}/sitetypes                # Site type lookup
GET /v{version}/areas                    # Geographic areas
```

**DATEX II Subscription (For Production):**
- Requires registration at trafficengland.com/subscribers
- Push-based data delivery via web services
- Includes incidents, roadworks, events

---

### 1.5 Weather Data: Met Office DataHub

| Attribute | Details |
|-----------|---------|
| **Source** | Met Office Weather DataHub |
| **URL** | https://datahub.metoffice.gov.uk/ |
| **Format** | JSON (GeoJSON), GRIB2 |
| **Coverage** | UK-wide |
| **Update Frequency** | Hourly observations, 3-hourly forecasts |
| **License** | Met Office DataHub Terms (attribution required) |
| **Free Tier** | Available (limited requests) |

**API Products:**
- Site-Specific Forecasts (Global Spot)
- Land Observations (historical weather stations)
- Atmospheric Model Data

**Python Integration:**
```python
import requests

API_KEY = "your_api_key"
BASE_URL = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0"

# Get forecast for location
response = requests.get(
    f"{BASE_URL}/point/hourly",
    params={"latitude": 51.5, "longitude": -0.1},
    headers={"apikey": API_KEY}
)
```

---

### 1.6 Schools Data: Get Information About Schools (GIAS)

| Attribute | Details |
|-----------|---------|
| **Source** | Department for Education |
| **URL** | https://get-information-schools.service.gov.uk/ |
| **Format** | CSV (bulk download) |
| **Coverage** | All schools in England |
| **Update Frequency** | Daily |
| **License** | Open Government Licence v3.0 |
| **Key Fields** | URN, name, postcode, lat/long, school_type, age_range |

**Download URL:**
```
https://get-information-schools.service.gov.uk/Downloads
→ "Establishment fields" → Select all → Download CSV
```

**Key Fields for Road Safety:**
- Postcode (can geocode if lat/long missing)
- PhaseOfEducation (Primary, Secondary, etc.)
- TypeOfEstablishment
- StatutoryLowAge, StatutoryHighAge
- NumberOfPupils

---

### 1.7 Speed Camera Locations

| Attribute | Details |
|-----------|---------|
| **Source** | Various (no single national dataset) |
| **Coverage** | Fragmented by region/police force |
| **Update Frequency** | Varies |
| **License** | Varies |

**Available Sources:**
1. **Regional Open Data** (e.g., Greater Manchester)
   - https://www.data.gov.uk/dataset/c428ea7f-af17-451c-8657-8e15621490ab/speed-camera-locations-in-greater-manchester

2. **Community-sourced** (for reference only)
   - speedcamerasuk.com (~4,400 locations)
   - Note: Use for validation, not primary data

3. **FOI Requests** to individual police forces

**Recommendation:** Build incrementally from regional open data + FOI requests

---

### 1.8 Road Network: OS Open Roads

| Attribute | Details |
|-----------|---------|
| **Source** | Ordnance Survey |
| **URL** | https://osdatahub.os.uk/downloads/open/OpenRoads |
| **Format** | GeoPackage, Shapefile, GML |
| **Coverage** | Great Britain |
| **Update Frequency** | Quarterly |
| **License** | Open Government Licence v3.0 |
| **Key Fields** | Road geometry, classification, names, one-way info |

---

## Part 2: Database Architecture

### 2.1 Technology Stack

```
┌─────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                     │
├─────────────────────────────────────────────────────────┤
│  FastAPI/Flask │ React Dashboard │ API Documentation    │
├─────────────────────────────────────────────────────────┤
│                     API LAYER                            │
├─────────────────────────────────────────────────────────┤
│  REST API │ GraphQL (optional) │ WebSocket (real-time)  │
├─────────────────────────────────────────────────────────┤
│                    DATA LAYER                            │
├─────────────────────────────────────────────────────────┤
│  PostgreSQL 15+ with PostGIS 3.x                        │
│  ├── Accident data (partitioned by year)                │
│  ├── Geographic boundaries (LSOA, police areas)         │
│  ├── Traffic counts                                     │
│  ├── Weather observations                               │
│  ├── Schools                                            │
│  └── Derived tables (risk scores, aggregations)         │
├─────────────────────────────────────────────────────────┤
│                   ETL LAYER                              │
├─────────────────────────────────────────────────────────┤
│  Python ETL Scripts │ Apache Airflow (optional)         │
│  GitHub Actions (scheduling)                            │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Database Schema

```sql
-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================
-- CORE TABLES
-- ============================================

-- Accidents (partitioned by year for performance)
CREATE TABLE accidents (
    accident_id VARCHAR(20) PRIMARY KEY,
    accident_year INT NOT NULL,
    accident_date DATE NOT NULL,
    accident_time TIME,
    day_of_week INT,
    
    -- Location
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    location_easting INT,
    location_northing INT,
    geom GEOMETRY(Point, 4326),
    lsoa_code VARCHAR(15),
    police_force INT,
    local_authority_district VARCHAR(10),
    
    -- Accident details
    severity INT, -- 1=Fatal, 2=Serious, 3=Slight
    number_of_vehicles INT,
    number_of_casualties INT,
    
    -- Road conditions
    road_type INT,
    speed_limit INT,
    junction_detail INT,
    road_surface_conditions INT,
    weather_conditions INT,
    light_conditions INT,
    
    -- Additional fields
    urban_or_rural INT,
    carriageway_hazards INT,
    special_conditions INT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (accident_year);

-- Create partitions for each year
CREATE TABLE accidents_2017 PARTITION OF accidents 
    FOR VALUES FROM (2017) TO (2018);
CREATE TABLE accidents_2018 PARTITION OF accidents 
    FOR VALUES FROM (2018) TO (2019);
-- ... continue for each year through 2025

-- Spatial index
CREATE INDEX idx_accidents_geom ON accidents USING GIST (geom);
CREATE INDEX idx_accidents_lsoa ON accidents (lsoa_code);
CREATE INDEX idx_accidents_date ON accidents (accident_date);
CREATE INDEX idx_accidents_severity ON accidents (severity);

-- Casualties
CREATE TABLE casualties (
    casualty_id SERIAL PRIMARY KEY,
    accident_id VARCHAR(20) REFERENCES accidents(accident_id),
    vehicle_reference INT,
    casualty_reference INT,
    casualty_class INT, -- 1=Driver, 2=Passenger, 3=Pedestrian
    sex INT,
    age INT,
    age_band INT,
    severity INT,
    pedestrian_location INT,
    pedestrian_movement INT,
    casualty_type INT,
    casualty_home_area_type INT,
    casualty_imd_decile INT
);

CREATE INDEX idx_casualties_accident ON casualties (accident_id);

-- Vehicles
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    accident_id VARCHAR(20) REFERENCES accidents(accident_id),
    vehicle_reference INT,
    vehicle_type INT,
    towing_and_articulation INT,
    vehicle_manoeuvre INT,
    vehicle_direction_from INT,
    vehicle_direction_to INT,
    junction_location INT,
    skidding_and_overturning INT,
    first_point_of_impact INT,
    driver_sex INT,
    driver_age INT,
    driver_age_band INT,
    engine_capacity_cc INT,
    propulsion_code INT,
    vehicle_age INT,
    driver_imd_decile INT
);

CREATE INDEX idx_vehicles_accident ON vehicles (accident_id);
CREATE INDEX idx_vehicles_type ON vehicles (vehicle_type);

-- ============================================
-- GEOGRAPHIC BOUNDARIES
-- ============================================

-- LSOA boundaries
CREATE TABLE lsoa_boundaries (
    lsoa_code VARCHAR(15) PRIMARY KEY,
    lsoa_name VARCHAR(100),
    local_authority_code VARCHAR(10),
    local_authority_name VARCHAR(100),
    region_code VARCHAR(10),
    region_name VARCHAR(50),
    area_hectares DECIMAL(12, 2),
    population_2021 INT,
    geom GEOMETRY(MultiPolygon, 4326)
);

CREATE INDEX idx_lsoa_geom ON lsoa_boundaries USING GIST (geom);

-- Police force areas
CREATE TABLE police_force_areas (
    police_force_code INT PRIMARY KEY,
    police_force_name VARCHAR(100),
    geom GEOMETRY(MultiPolygon, 4326)
);

CREATE INDEX idx_police_geom ON police_force_areas USING GIST (geom);

-- ============================================
-- ENRICHMENT TABLES
-- ============================================

-- Traffic counts
CREATE TABLE traffic_counts (
    count_point_id INT PRIMARY KEY,
    road_name VARCHAR(100),
    road_category VARCHAR(10),
    road_type VARCHAR(50),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    local_authority_code VARCHAR(10),
    region_name VARCHAR(50),
    link_length_km DECIMAL(6, 3)
);

CREATE TABLE traffic_aadf (
    id SERIAL PRIMARY KEY,
    count_point_id INT REFERENCES traffic_counts(count_point_id),
    year INT,
    all_motor_vehicles INT,
    cars_and_taxis INT,
    buses_and_coaches INT,
    lgvs INT,  -- Light goods vehicles
    hgvs_2_rigid INT,
    hgvs_3_rigid INT,
    hgvs_4_or_more_rigid INT,
    hgvs_3_or_4_articulated INT,
    hgvs_5_articulated INT,
    hgvs_6_articulated INT,
    pedal_cycles INT,
    motorcycles INT,
    estimation_method VARCHAR(20),
    UNIQUE(count_point_id, year)
);

CREATE INDEX idx_traffic_counts_geom ON traffic_counts USING GIST (geom);

-- Schools
CREATE TABLE schools (
    urn INT PRIMARY KEY,  -- Unique Reference Number
    name VARCHAR(200),
    type_of_establishment VARCHAR(100),
    phase_of_education VARCHAR(50),
    street VARCHAR(200),
    town VARCHAR(100),
    postcode VARCHAR(10),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    statutory_low_age INT,
    statutory_high_age INT,
    number_of_pupils INT,
    establishment_status VARCHAR(50)
);

CREATE INDEX idx_schools_geom ON schools USING GIST (geom);
CREATE INDEX idx_schools_phase ON schools (phase_of_education);

-- Weather observations (for historical enrichment)
CREATE TABLE weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20),
    observation_time TIMESTAMP,
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    temperature_c DECIMAL(4, 1),
    feels_like_c DECIMAL(4, 1),
    wind_speed_mph DECIMAL(5, 1),
    wind_direction VARCHAR(10),
    humidity_pct INT,
    pressure_hpa DECIMAL(6, 1),
    visibility_m INT,
    weather_type VARCHAR(50),
    precipitation_mm DECIMAL(5, 2)
);

CREATE INDEX idx_weather_geom ON weather_observations USING GIST (geom);
CREATE INDEX idx_weather_time ON weather_observations (observation_time);

-- Speed cameras (when available)
CREATE TABLE speed_cameras (
    camera_id SERIAL PRIMARY KEY,
    camera_type VARCHAR(50),  -- Gatso, SPECS, HADECS, Mobile
    road_name VARCHAR(100),
    speed_limit INT,
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    direction VARCHAR(50),
    operational_since DATE,
    data_source VARCHAR(50),
    last_verified DATE
);

CREATE INDEX idx_cameras_geom ON speed_cameras USING GIST (geom);

-- ============================================
-- DERIVED/ANALYTICS TABLES
-- ============================================

-- Pre-computed LSOA risk scores
CREATE TABLE lsoa_risk_scores (
    lsoa_code VARCHAR(15) PRIMARY KEY,
    year INT,
    total_accidents INT,
    fatal_accidents INT,
    serious_accidents INT,
    slight_accidents INT,
    pedestrian_accidents INT,
    cyclist_accidents INT,
    motorcycle_accidents INT,
    risk_score DECIMAL(5, 2),  -- Normalized 0-100
    risk_category VARCHAR(20),  -- Low, Medium, High, Very High
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Route risk segments (for API queries)
CREATE TABLE route_risk_segments (
    segment_id SERIAL PRIMARY KEY,
    geom GEOMETRY(LineString, 4326),
    road_name VARCHAR(100),
    accident_count_1yr INT,
    accident_count_3yr INT,
    accident_count_5yr INT,
    avg_severity DECIMAL(3, 2),
    risk_score DECIMAL(5, 2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_route_segments_geom ON route_risk_segments USING GIST (geom);

-- ============================================
-- METADATA TABLES
-- ============================================

-- Data source tracking
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100),
    source_url TEXT,
    last_checked TIMESTAMP,
    last_updated TIMESTAMP,
    latest_data_date DATE,
    update_frequency VARCHAR(50),
    notes TEXT
);

-- ETL job history
CREATE TABLE etl_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20),  -- Running, Completed, Failed
    records_processed INT,
    records_inserted INT,
    records_updated INT,
    error_message TEXT
);

-- Lookup tables for coded values
CREATE TABLE lookup_severity (
    code INT PRIMARY KEY,
    description VARCHAR(50)
);

INSERT INTO lookup_severity VALUES 
    (1, 'Fatal'),
    (2, 'Serious'),
    (3, 'Slight');

CREATE TABLE lookup_vehicle_type (
    code INT PRIMARY KEY,
    description VARCHAR(100)
);

INSERT INTO lookup_vehicle_type VALUES
    (1, 'Pedal cycle'),
    (2, 'Motorcycle 50cc and under'),
    (3, 'Motorcycle 125cc and under'),
    (4, 'Motorcycle over 125cc and up to 500cc'),
    (5, 'Motorcycle over 500cc'),
    (8, 'Taxi/Private hire car'),
    (9, 'Car'),
    (10, 'Minibus (8 - 16 passengers)'),
    (11, 'Bus or coach (17 or more pass seats)'),
    (16, 'Ridden horse'),
    (17, 'Agricultural vehicle'),
    (18, 'Tram'),
    (19, 'Van / Goods 3.5 tonnes mgw or under'),
    (20, 'Goods over 3.5t. and under 7.5t'),
    (21, 'Goods 7.5 tonnes mgw and over'),
    (22, 'Mobility scooter'),
    (23, 'Electric motorcycle'),
    (90, 'Other vehicle'),
    (97, 'Motorcycle - unknown cc'),
    (98, 'Goods vehicle - unknown weight'),
    (-1, 'Data missing or out of range');

-- Add more lookup tables as needed for other coded fields
```

### 2.3 Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  database:
    image: postgis/postgis:15-3.3
    container_name: road_safety_db
    environment:
      POSTGRES_USER: roadsafety
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: road_safety
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U roadsafety -d road_safety"]
      interval: 10s
      timeout: 5s
      retries: 5

  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: road_safety_pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@roadsafety.local
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_PASSWORD}
    ports:
      - "5050:80"
    depends_on:
      - database

  api:
    build:
      context: ./api
      dockerfile: Dockerfile
    container_name: road_safety_api
    environment:
      DATABASE_URL: postgresql://roadsafety:${DB_PASSWORD}@database:5432/road_safety
      API_KEY: ${API_KEY}
    ports:
      - "8000:8000"
    depends_on:
      database:
        condition: service_healthy
    volumes:
      - ./api:/app

  etl:
    build:
      context: ./etl
      dockerfile: Dockerfile
    container_name: road_safety_etl
    environment:
      DATABASE_URL: postgresql://roadsafety:${DB_PASSWORD}@database:5432/road_safety
      MET_OFFICE_API_KEY: ${MET_OFFICE_API_KEY}
    depends_on:
      database:
        condition: service_healthy
    volumes:
      - ./etl:/app
      - ./data:/data

volumes:
  postgres_data:
```

---

## Part 3: ETL Pipeline Architecture

### 3.1 Python ETL Structure

```
road_safety_platform/
├── docker-compose.yml
├── .env
├── .github/
│   └── workflows/
│       ├── daily_update.yml
│       ├── weekly_update.yml
│       └── annual_update.yml
├── etl/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.py
│   ├── main.py
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── stats19.py          # STATS19 accident data
│   │   ├── ons_geography.py    # LSOA boundaries
│   │   ├── traffic_counts.py   # DfT traffic counts
│   │   ├── met_office.py       # Weather data
│   │   ├── schools.py          # GIAS schools data
│   │   └── webtris.py          # National Highways
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── geocoding.py
│   │   ├── risk_scoring.py
│   │   └── aggregations.py
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── postgres.py
│   └── utils/
│       ├── __init__.py
│       ├── logging.py
│       └── notifications.py
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── routers/
│       ├── accidents.py
│       ├── risk.py
│       └── analytics.py
├── dashboard/
│   └── ... (React/Next.js app)
└── data/
    └── raw/
        └── ... (downloaded files)
```

### 3.2 Core ETL Scripts

**etl/sources/stats19.py**
```python
"""
STATS19 Road Accident Data ETL
Downloads and processes UK road accident data from DfT
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, Tuple
import geopandas as gpd
from shapely.geometry import Point

from config import DATA_DIR, STATS19_BASE_URL
from loaders.postgres import PostgresLoader

logger = logging.getLogger(__name__)

class Stats19Extractor:
    """Extract STATS19 data from DfT"""
    
    BASE_URL = "https://data.dft.gov.uk/road-accidents-safety-data"
    
    FILES = {
        'collisions': 'dft-road-casualty-statistics-collision-{year}.csv',
        'casualties': 'dft-road-casualty-statistics-casualty-{year}.csv',
        'vehicles': 'dft-road-casualty-statistics-vehicle-{year}.csv',
    }
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def download_year(self, year: int) -> dict:
        """Download all files for a specific year"""
        downloaded = {}
        
        for data_type, filename_template in self.FILES.items():
            filename = filename_template.format(year=year)
            url = f"{self.BASE_URL}/{filename}"
            filepath = self.data_dir / filename
            
            logger.info(f"Downloading {url}")
            
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                downloaded[data_type] = filepath
                logger.info(f"Downloaded {filename}")
                
            except requests.RequestException as e:
                logger.error(f"Failed to download {filename}: {e}")
                downloaded[data_type] = None
        
        return downloaded
    
    def download_latest(self) -> dict:
        """Download latest available year"""
        current_year = datetime.now().year
        
        # Try current year, then previous years
        for year in range(current_year, current_year - 3, -1):
            try:
                files = self.download_year(year)
                if all(f is not None for f in files.values()):
                    return files
            except Exception as e:
                logger.warning(f"Year {year} not available: {e}")
        
        raise Exception("Could not download any recent data")
    
    def download_all_years(self, start_year: int = 2017, end_year: int = None) -> dict:
        """Download all years in range"""
        end_year = end_year or datetime.now().year
        all_files = {}
        
        for year in range(start_year, end_year + 1):
            try:
                all_files[year] = self.download_year(year)
            except Exception as e:
                logger.warning(f"Could not download {year}: {e}")
        
        return all_files


class Stats19Transformer:
    """Transform STATS19 data for loading"""
    
    COLUMN_MAPPING = {
        'collision': {
            'accident_index': 'accident_id',
            'accident_year': 'accident_year',
            'longitude': 'longitude',
            'latitude': 'latitude',
            'accident_severity': 'severity',
            'number_of_vehicles': 'number_of_vehicles',
            'number_of_casualties': 'number_of_casualties',
            'date': 'accident_date',
            'time': 'accident_time',
            'day_of_week': 'day_of_week',
            'road_type': 'road_type',
            'speed_limit': 'speed_limit',
            'light_conditions': 'light_conditions',
            'weather_conditions': 'weather_conditions',
            'road_surface_conditions': 'road_surface_conditions',
            'urban_or_rural_area': 'urban_or_rural',
            'police_force': 'police_force',
            'lsoa_of_accident_location': 'lsoa_code',
            'local_authority_ons_district': 'local_authority_district',
        }
    }
    
    def transform_collisions(self, filepath: Path) -> gpd.GeoDataFrame:
        """Transform collision data with geometry"""
        logger.info(f"Transforming collisions from {filepath}")
        
        df = pd.read_csv(filepath, low_memory=False)
        
        # Rename columns
        df = df.rename(columns=self.COLUMN_MAPPING['collision'])
        
        # Parse date and time
        df['accident_date'] = pd.to_datetime(df['accident_date'], format='%d/%m/%Y')
        df['accident_time'] = pd.to_datetime(df['accident_time'], format='%H:%M').dt.time
        
        # Create geometry
        df = df.dropna(subset=['longitude', 'latitude'])
        geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        gdf = gdf.rename(columns={'geometry': 'geom'})
        gdf = gdf.set_geometry('geom')
        
        logger.info(f"Transformed {len(gdf)} collision records")
        return gdf
    
    def transform_casualties(self, filepath: Path) -> pd.DataFrame:
        """Transform casualty data"""
        logger.info(f"Transforming casualties from {filepath}")
        
        df = pd.read_csv(filepath, low_memory=False)
        
        # Column mapping and transformations
        df = df.rename(columns={
            'accident_index': 'accident_id',
            'casualty_severity': 'severity',
        })
        
        logger.info(f"Transformed {len(df)} casualty records")
        return df
    
    def transform_vehicles(self, filepath: Path) -> pd.DataFrame:
        """Transform vehicle data"""
        logger.info(f"Transforming vehicles from {filepath}")
        
        df = pd.read_csv(filepath, low_memory=False)
        
        df = df.rename(columns={
            'accident_index': 'accident_id',
        })
        
        logger.info(f"Transformed {len(df)} vehicle records")
        return df


class Stats19Loader:
    """Load transformed STATS19 data to PostgreSQL"""
    
    def __init__(self, connection_string: str):
        self.loader = PostgresLoader(connection_string)
    
    def load_collisions(self, gdf: gpd.GeoDataFrame, year: int):
        """Load collision data to appropriate partition"""
        table_name = f'accidents_{year}'
        
        # Upsert logic
        self.loader.upsert_geodataframe(
            gdf, 
            table_name,
            if_exists='replace',
            index=False
        )
        
        logger.info(f"Loaded {len(gdf)} collisions to {table_name}")
    
    def load_casualties(self, df: pd.DataFrame):
        """Load casualty data"""
        self.loader.upsert_dataframe(df, 'casualties')
        logger.info(f"Loaded {len(df)} casualties")
    
    def load_vehicles(self, df: pd.DataFrame):
        """Load vehicle data"""
        self.loader.upsert_dataframe(df, 'vehicles')
        logger.info(f"Loaded {len(df)} vehicles")


def run_stats19_etl(year: Optional[int] = None, full_refresh: bool = False):
    """Main ETL entry point"""
    from config import DATABASE_URL
    
    extractor = Stats19Extractor()
    transformer = Stats19Transformer()
    loader = Stats19Loader(DATABASE_URL)
    
    if full_refresh:
        # Download and load all years
        all_files = extractor.download_all_years()
        for year, files in all_files.items():
            if files['collisions']:
                gdf = transformer.transform_collisions(files['collisions'])
                loader.load_collisions(gdf, year)
            if files['casualties']:
                df = transformer.transform_casualties(files['casualties'])
                loader.load_casualties(df)
            if files['vehicles']:
                df = transformer.transform_vehicles(files['vehicles'])
                loader.load_vehicles(df)
    else:
        # Download specific year or latest
        if year:
            files = extractor.download_year(year)
        else:
            files = extractor.download_latest()
        
        gdf = transformer.transform_collisions(files['collisions'])
        loader.load_collisions(gdf, gdf['accident_year'].iloc[0])


if __name__ == '__main__':
    run_stats19_etl(full_refresh=True)
```

**etl/sources/traffic_counts.py**
```python
"""
DfT Traffic Counts ETL
Downloads traffic count data via API
"""

import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import logging

logger = logging.getLogger(__name__)

class TrafficCountsExtractor:
    """Extract traffic count data from DfT API"""
    
    BASE_URL = "https://roadtraffic.dft.gov.uk/api"
    
    def get_count_points(self, page_size: int = 1000) -> pd.DataFrame:
        """Get all count point locations"""
        all_data = []
        page = 1
        
        while True:
            url = f"{self.BASE_URL}/count-points"
            params = {'page': page, 'page_size': page_size}
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if not data.get('rows'):
                break
            
            all_data.extend(data['rows'])
            page += 1
            
            logger.info(f"Fetched page {page}, total records: {len(all_data)}")
        
        return pd.DataFrame(all_data)
    
    def get_aadf(self, count_point_id: int) -> pd.DataFrame:
        """Get Annual Average Daily Flow for a count point"""
        url = f"{self.BASE_URL}/count-points/{count_point_id}/aadf"
        response = requests.get(url)
        return pd.DataFrame(response.json())
    
    def download_bulk_aadf(self) -> pd.DataFrame:
        """Download bulk AADF data from CSV"""
        url = "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/aadf/dft_aadf_count_point_id.csv"
        return pd.read_csv(url)


def run_traffic_counts_etl():
    """Run traffic counts ETL"""
    from config import DATABASE_URL
    from loaders.postgres import PostgresLoader
    
    extractor = TrafficCountsExtractor()
    loader = PostgresLoader(DATABASE_URL)
    
    # Get count points
    count_points = extractor.get_count_points()
    
    # Create geometry
    count_points['geom'] = count_points.apply(
        lambda row: Point(row['longitude'], row['latitude']),
        axis=1
    )
    gdf = gpd.GeoDataFrame(count_points, geometry='geom', crs='EPSG:4326')
    
    # Load to database
    loader.upsert_geodataframe(gdf, 'traffic_counts')
    
    # Get AADF data
    aadf_df = extractor.download_bulk_aadf()
    loader.upsert_dataframe(aadf_df, 'traffic_aadf')
    
    logger.info("Traffic counts ETL complete")
```

### 3.3 GitHub Actions Workflows

**.github/workflows/daily_update.yml**
```yaml
name: Daily Data Update

on:
  schedule:
    # Run at 6:00 AM UTC every day
    - cron: '0 6 * * *'
  workflow_dispatch:  # Allow manual trigger

env:
  DATABASE_URL: ${{ secrets.DATABASE_URL }}
  MET_OFFICE_API_KEY: ${{ secrets.MET_OFFICE_API_KEY }}

jobs:
  daily-update:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r etl/requirements.txt
      
      - name: Run weather data update
        run: python etl/sources/met_office.py --mode daily
      
      - name: Run real-time traffic incidents
        run: python etl/sources/webtris.py --mode daily
      
      - name: Update risk scores
        run: python etl/transformers/risk_scoring.py
      
      - name: Notify on success
        if: success()
        run: echo "Daily update completed successfully"
      
      - name: Notify on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: 'Daily ETL Job Failed',
              body: 'The daily data update workflow failed. Please check the logs.'
            })
```

**.github/workflows/weekly_update.yml**
```yaml
name: Weekly Data Update

on:
  schedule:
    # Run at 3:00 AM UTC every Monday
    - cron: '0 3 * * 1'
  workflow_dispatch:

env:
  DATABASE_URL: ${{ secrets.DATABASE_URL }}

jobs:
  weekly-update:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r etl/requirements.txt
      
      - name: Update schools data
        run: python etl/sources/schools.py
      
      - name: Update traffic counts (if new data)
        run: python etl/sources/traffic_counts.py --check-updates
      
      - name: Refresh LSOA aggregations
        run: python etl/transformers/aggregations.py --lsoa
      
      - name: Generate weekly report
        run: python etl/utils/reports.py --weekly
```

**.github/workflows/annual_update.yml**
```yaml
name: Annual STATS19 Update

on:
  schedule:
    # Run at 2:00 AM UTC on October 1st (after Sept release)
    - cron: '0 2 1 10 *'
  workflow_dispatch:
    inputs:
      year:
        description: 'Year to download (leave empty for latest)'
        required: false
        type: string
      full_refresh:
        description: 'Download all years'
        required: false
        type: boolean
        default: false

env:
  DATABASE_URL: ${{ secrets.DATABASE_URL }}

jobs:
  annual-update:
    runs-on: ubuntu-latest
    timeout-minutes: 120  # Allow 2 hours for full refresh
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r etl/requirements.txt
      
      - name: Download and load STATS19 data
        run: |
          if [ "${{ github.event.inputs.full_refresh }}" = "true" ]; then
            python etl/sources/stats19.py --full-refresh
          elif [ -n "${{ github.event.inputs.year }}" ]; then
            python etl/sources/stats19.py --year ${{ github.event.inputs.year }}
          else
            python etl/sources/stats19.py --latest
          fi
      
      - name: Update geographic boundaries
        run: python etl/sources/ons_geography.py
      
      - name: Rebuild indexes
        run: python etl/utils/maintenance.py --rebuild-indexes
      
      - name: Generate annual statistics
        run: python etl/transformers/aggregations.py --annual
      
      - name: Notify completion
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: 'Annual STATS19 Update Complete',
              body: 'The annual STATS19 data update has been completed. Please verify the data.'
            })
```

---

## Part 4: Update Schedule Summary

| Data Source | Update Frequency | Schedule (Cron) | Method |
|-------------|------------------|-----------------|--------|
| **STATS19 Accidents** | Annual (September) | `0 2 1 10 *` | GitHub Actions |
| **STATS19 Provisional** | Semi-annual (Nov, May) | `0 2 1 12,6 *` | GitHub Actions |
| **Traffic Counts (AADF)** | Annual (June) | `0 3 15 7 *` | GitHub Actions |
| **LSOA Boundaries** | Rarely (census years) | Manual | GitHub Actions (manual) |
| **Schools Data** | Weekly | `0 3 * * 1` | GitHub Actions |
| **Weather Observations** | Daily | `0 6 * * *` | GitHub Actions |
| **Real-time Traffic** | Continuous | - | Webhook/Stream |
| **Risk Score Refresh** | Daily | `0 7 * * *` | GitHub Actions |

---

## Part 5: Execution Timeline

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up PostgreSQL/PostGIS database (Docker)
- [ ] Create database schema
- [ ] Download and load STATS19 data (2017-2024)
- [ ] Download and load LSOA boundaries
- [ ] Set up GitHub repository structure

### Phase 2: Core ETL (Weeks 3-4)
- [ ] Build STATS19 ETL pipeline
- [ ] Build traffic counts ETL
- [ ] Build schools data ETL
- [ ] Create lookup tables
- [ ] Test all ETL scripts locally

### Phase 3: Enrichment (Weeks 5-6)
- [ ] Integrate Met Office weather API
- [ ] Integrate WebTRIS traffic API
- [ ] Build geographic enrichment (nearest school, traffic point)
- [ ] Create spatial indexes

### Phase 4: Analytics (Weeks 7-8)
- [ ] Build risk scoring algorithm
- [ ] Create LSOA aggregation views
- [ ] Build route risk analysis functions
- [ ] Create pre-computed analytics tables

### Phase 5: Automation (Weeks 9-10)
- [ ] Set up GitHub Actions workflows
- [ ] Configure secrets and environment variables
- [ ] Test automated updates
- [ ] Set up monitoring and alerting

### Phase 6: API Development (Weeks 11-12)
- [ ] Build FastAPI REST endpoints
- [ ] Implement authentication
- [ ] Create API documentation
- [ ] Deploy to cloud (Railway/Render/AWS)

### Phase 7: Dashboard (Weeks 13-16)
- [ ] Design dashboard wireframes
- [ ] Build React/Next.js frontend
- [ ] Implement visualizations (maps, charts)
- [ ] User authentication
- [ ] Deploy frontend

---

## Part 6: Cost Estimates

### Infrastructure (Monthly)
| Service | Provider | Estimated Cost |
|---------|----------|----------------|
| PostgreSQL Database | Railway/Render | £15-50/month |
| API Hosting | Railway/Render | £10-30/month |
| GitHub Actions | GitHub (Free tier) | £0 |
| Domain | Namecheap | £1/month |
| **Total** | | **£26-81/month** |

### Data Sources
| Source | Cost |
|--------|------|
| STATS19 | Free (Open Government Licence) |
| DfT Traffic Counts | Free (Open Government Licence) |
| ONS Geography | Free (Open Government Licence) |
| Met Office DataHub | Free tier available |
| Schools Data | Free (Open Government Licence) |
| National Highways WebTRIS | Free (no auth required) |

---

## Part 7: Next Steps

1. **Immediate Action:** Clone/create repository with structure above
2. **Day 1:** Set up Docker environment and PostgreSQL
3. **Day 2-3:** Download all STATS19 data and create initial load
4. **Day 4-5:** Add geographic boundaries
5. **Week 2:** Build automated ETL scripts
6. **Week 3:** Set up GitHub Actions

Would you like me to create any specific component in detail, such as:
- Complete Python ETL package with all scripts?
- API endpoints with FastAPI?
- Dashboard specifications?
- Deployment configuration?
