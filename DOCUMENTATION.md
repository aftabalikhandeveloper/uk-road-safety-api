# UK Road Safety Platform - Complete Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Data Sources & Database Schema](#data-sources--database-schema)
3. [API Reference](#api-reference)
4. [Dashboard & Visualizations](#dashboard--visualizations)
5. [Setup & Deployment](#setup--deployment)

---

## Project Overview

The UK Road Safety Platform is a comprehensive data analytics system for analyzing road traffic accidents across the United Kingdom. It combines government open data sources with modern visualization tools to provide insights into road safety patterns, hotspots, and trends.

### Tech Stack

| Layer | Technology |
|-------|------------|
| **Database** | PostgreSQL 18 with PostGIS extension |
| **Backend API** | Python FastAPI |
| **Frontend** | React + Vite + Tailwind CSS |
| **Maps** | Leaflet.js with OpenStreetMap |
| **Deployment** | Docker + nginx |

### Key Features
- 📊 Interactive analytics dashboard with year-over-year comparisons
- 🗺️ Geographic accident mapping with clustering
- 🔥 Hotspot identification and risk scoring
- 🏫 School proximity safety analysis
- 📈 Time-series analysis (hourly, daily, yearly patterns)
- 🚗 Vehicle type breakdown
- 👮 Police force area statistics

---

## Data Sources & Database Schema

### Data Overview

| Dataset | Records | Description |
|---------|---------|-------------|
| **Accidents** | 503,410 | Road traffic accidents (2019-2023) |
| **Casualties** | 1,152,626 | Individual casualties linked to accidents |
| **Vehicles** | 1,657,622 | Vehicles involved in accidents |
| **Schools** | 26,508 | UK educational establishments with locations |
| **LSOA Boundaries** | 35,672 | Lower Super Output Area geographic boundaries |
| **Traffic Count Points** | 46,251 | DfT traffic monitoring locations |
| **Traffic AADF** | 578,217 | Annual Average Daily Flow data |

### Data Sources

#### 1. Stats19 Accident Data
- **Source**: UK Department for Transport (DfT)
- **URL**: https://data.gov.uk/dataset/road-accidents-safety-data
- **Coverage**: 2019-2023
- **Contains**: Accident location, severity, conditions, date/time

#### 2. School Data
- **Source**: Get Information About Schools (GIAS)
- **URL**: https://get-information-schools.service.gov.uk/
- **Contains**: School name, type, location, pupil count

#### 3. LSOA Geographic Boundaries
- **Source**: ONS Open Geography Portal
- **URL**: https://geoportal.statistics.gov.uk/
- **Contains**: Lower Super Output Area polygons for England & Wales

#### 4. Traffic Data (AADF)
- **Source**: DfT Road Traffic Statistics
- **URL**: https://roadtraffic.dft.gov.uk/
- **Contains**: Annual Average Daily Flow by vehicle type

---

### Database Schema

#### Core Tables

##### `accidents`
Main table containing road accident records.

| Column | Type | Description |
|--------|------|-------------|
| `accident_id` | VARCHAR(20) | Unique accident reference (PK) |
| `accident_year` | INTEGER | Year of accident |
| `accident_date` | DATE | Date of accident |
| `accident_time` | TIME | Time of accident |
| `day_of_week` | INTEGER | 1=Sunday, 2=Monday, ..., 7=Saturday |
| `longitude` | DECIMAL(10,6) | WGS84 longitude |
| `latitude` | DECIMAL(10,6) | WGS84 latitude |
| `geom` | GEOMETRY(Point,4326) | PostGIS point geometry |
| `lsoa_code` | VARCHAR(15) | Lower Super Output Area code |
| `police_force` | INTEGER | Police force code |
| `severity` | INTEGER | 1=Fatal, 2=Serious, 3=Slight |
| `number_of_vehicles` | INTEGER | Vehicles involved |
| `number_of_casualties` | INTEGER | Total casualties |
| `road_type` | INTEGER | Road type code |
| `speed_limit` | INTEGER | Speed limit (mph) |
| `light_conditions` | INTEGER | Light conditions code |
| `weather_conditions` | INTEGER | Weather conditions code |
| `road_surface_conditions` | INTEGER | Road surface code |
| `urban_or_rural` | INTEGER | 1=Urban, 2=Rural |

##### `casualties`
Individual casualty records linked to accidents.

| Column | Type | Description |
|--------|------|-------------|
| `casualty_id` | SERIAL | Primary key |
| `accident_id` | VARCHAR(20) | Foreign key to accidents |
| `vehicle_reference` | INTEGER | Reference to vehicle |
| `casualty_class` | INTEGER | 1=Driver, 2=Passenger, 3=Pedestrian |
| `sex` | INTEGER | 1=Male, 2=Female |
| `age` | INTEGER | Age of casualty |
| `severity` | INTEGER | 1=Fatal, 2=Serious, 3=Slight |

##### `vehicles`
Vehicles involved in accidents.

| Column | Type | Description |
|--------|------|-------------|
| `vehicle_id` | SERIAL | Primary key |
| `accident_id` | VARCHAR(20) | Foreign key to accidents |
| `vehicle_type` | INTEGER | Vehicle type code |
| `age_of_driver` | INTEGER | Driver age |
| `sex_of_driver` | INTEGER | Driver sex |
| `age_of_vehicle` | INTEGER | Vehicle age in years |
| `engine_capacity_cc` | INTEGER | Engine size |

##### `schools`
UK educational establishments.

| Column | Type | Description |
|--------|------|-------------|
| `urn` | BIGINT | Unique Reference Number (PK) |
| `name` | TEXT | School name |
| `establishment_type` | TEXT | Type of establishment |
| `phase_of_education` | TEXT | Primary/Secondary/etc. |
| `longitude` | DOUBLE | WGS84 longitude |
| `latitude` | DOUBLE | WGS84 latitude |
| `geom` | GEOMETRY(Point,4326) | PostGIS point |
| `postcode` | TEXT | Postcode |
| `number_of_pupils` | DOUBLE | Pupil count |

##### `lsoa_boundaries`
Geographic boundaries for Lower Super Output Areas.

| Column | Type | Description |
|--------|------|-------------|
| `lsoa_code` | TEXT | LSOA code (e.g., E01000001) |
| `lsoa_name` | TEXT | LSOA name |
| `area_hectares` | DOUBLE | Area in hectares |
| `geom` | GEOMETRY | Polygon geometry |

---

#### Lookup Tables

| Table | Description |
|-------|-------------|
| `lookup_severity` | 1=Fatal, 2=Serious, 3=Slight |
| `lookup_vehicle_type` | 1=Pedal cycle, 9=Car, 11=Bus, etc. |
| `lookup_road_type` | 1=Roundabout, 3=Dual carriageway, 6=Single carriageway |
| `lookup_weather` | 1=Fine, 2=Raining, 3=Snowing, 7=Fog |
| `lookup_light_conditions` | 1=Daylight, 4=Darkness lit, 5=Darkness unlit |
| `lookup_road_surface` | 1=Dry, 2=Wet, 3=Snow, 4=Frost/Ice |
| `lookup_police_force` | Police force codes and names |
| `lookup_day_of_week` | 1=Sunday to 7=Saturday |

---

#### Database Views

| View | Purpose |
|------|---------|
| `v_accidents_decoded` | Accidents with lookup descriptions joined |
| `v_accident_hotspots` | LSOAs ranked by risk score |
| `v_lsoa_risk_summary` | Risk statistics per LSOA |
| `v_recent_accidents` | Last 30 days of accidents |
| `v_road_condition_risk` | Risk by road/weather conditions |
| `v_school_proximity_accidents` | Accidents within 500m of schools |
| `v_temporal_risk_patterns` | Hour/day patterns |

---

#### Spatial Indexes

```sql
CREATE INDEX idx_accidents_geom_gist ON accidents USING GIST(geom);
CREATE INDEX idx_schools_geom ON schools USING GIST(geom);
CREATE INDEX idx_lsoa_boundaries_geom ON lsoa_boundaries USING GIST(geom);
CREATE INDEX idx_accidents_year_severity ON accidents(accident_year, severity);
CREATE INDEX idx_accidents_day_week ON accidents(day_of_week);
```

---

## API Reference

**Base URL**: `http://localhost:8000/api/v1`

### Accidents Endpoints

#### `GET /accidents`
List accidents with filters.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `year` | int | Filter by year (1979-2030) |
| `severity` | int | 1=Fatal, 2=Serious, 3=Slight |
| `police_force` | int | Police force code |
| `lsoa` | string | LSOA code |
| `date_from` | date | Start date (YYYY-MM-DD) |
| `date_to` | date | End date (YYYY-MM-DD) |
| `page` | int | Page number (default: 1) |
| `page_size` | int | Results per page (1-1000, default: 100) |

**Response:**
```json
{
  "total": 503410,
  "page": 1,
  "page_size": 100,
  "data": [
    {
      "accident_id": "2023010012345",
      "accident_date": "2023-01-15",
      "accident_time": "14:30:00",
      "severity": 3,
      "severity_desc": "Slight",
      "location": {"latitude": 51.5074, "longitude": -0.1278},
      "lsoa_code": "E01000001",
      "number_of_casualties": 2,
      "number_of_vehicles": 2
    }
  ]
}
```

---

#### `GET /accidents/nearby`
Find accidents near a location (spatial query).

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | ✅ | Latitude (49.0-61.0) |
| `lon` | float | ✅ | Longitude (-9.0-2.0) |
| `radius` | int | | Radius in meters (50-10000, default: 500) |
| `years` | string | | Comma-separated years |
| `severity` | int | | Filter by severity |
| `limit` | int | | Max results (1-1000, default: 100) |

**Response:**
```json
{
  "center": {"latitude": 51.5074, "longitude": -0.1278},
  "radius_meters": 500,
  "total": 15,
  "data": [
    {
      "accident_id": "2023010012345",
      "distance_meters": 125.5,
      ...
    }
  ]
}
```

---

#### `GET /accidents/{accident_id}`
Get detailed accident information.

---

#### `GET /accidents/lsoa/{lsoa_code}/stats`
Get statistics for a specific LSOA.

---

### Analytics Endpoints

#### `GET /analytics/summary/bulk`
Get summary statistics for multiple years in one call (optimized).

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `years` | string | Comma-separated years (default: "2020,2021,2022,2023,2024") |

**Response:**
```json
[
  {
    "year": 2023,
    "total_accidents": 106000,
    "total_casualties": 130000,
    "fatalities": 1600,
    "serious_injuries": 25000,
    "severity_breakdown": {
      "fatal": 1500,
      "serious": 24000,
      "slight": 80500,
      "fatal_pct": 1.42,
      "serious_pct": 22.64,
      "slight_pct": 75.94
    }
  }
]
```

---

#### `GET /analytics/summary/{year}`
Get summary for a specific year.

---

#### `GET /analytics/summary`
Get combined summary for all years.

---

#### `GET /analytics/timeseries`
Get accident time series data.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `start_year` | int | Start year (default: 2018) |
| `end_year` | int | End year (default: 2023) |
| `granularity` | string | "year", "month", or "week" |

---

#### `GET /analytics/patterns/hourly`
Get accidents by hour of day.

**Response:**
```json
[
  {"hour": 0, "total_accidents": 12500, "avg_severity": 2.65},
  {"hour": 8, "total_accidents": 35000, "avg_severity": 2.78},
  {"hour": 17, "total_accidents": 42000, "avg_severity": 2.75}
]
```

---

#### `GET /analytics/patterns/daily`
Get accidents by day of week.

**Response:**
```json
[
  {"day": 1, "day_name": "Sunday", "total_accidents": 58000},
  {"day": 6, "day_name": "Friday", "total_accidents": 82000}
]
```

---

#### `GET /analytics/police-forces`
Get statistics by police force area.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `year` | int | Filter by year |
| `limit` | int | Number of results (default: 20) |

**Response:**
```json
[
  {
    "police_force_code": 1,
    "police_force_name": "Metropolitan Police",
    "year": 2023,
    "total_accidents": 25000,
    "fatal_accidents": 120,
    "serious_accidents": 4500,
    "ksi_rate": 18.48
  }
]
```

---

#### `GET /analytics/vehicle-types`
Get accidents by vehicle type.

**Response:**
```json
[
  {"vehicle_type_code": 9, "vehicle_type_name": "Car", "vehicle_count": 850000},
  {"vehicle_type_code": 1, "vehicle_type_name": "Pedal cycle", "vehicle_count": 95000}
]
```

---

#### `GET /analytics/hotspots`
Get high-risk accident locations (LSOA level).

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `year` | int | Filter by year |
| `min_accidents` | int | Minimum accidents (default: 5) |
| `limit` | int | Number of results (default: 50) |

---

#### `GET /analytics/accident-conditions`
Get breakdown by weather, light, and road conditions.

**Response:**
```json
{
  "weather": [
    {"code": 1, "name": "Fine no high winds", "count": 380000},
    {"code": 2, "name": "Raining no high winds", "count": 85000}
  ],
  "light": [...],
  "road_surface": [...],
  "road_type": [...]
}
```

---

#### `GET /analytics/heatmap-data`
Get coordinate data for heatmap visualization.

---

### Schools Endpoints

#### `GET /schools/nearby`
Find schools near a location.

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | ✅ | Latitude |
| `lon` | float | ✅ | Longitude |
| `radius` | int | | Radius in meters (default: 1000) |
| `phase` | string | | Filter by phase (Primary, Secondary) |

---

#### `GET /schools/{urn}/accidents`
Get accidents near a specific school.

---

### Health Endpoints

#### `GET /health`
API health check with database status.

---

## Dashboard & Visualizations

### Pages

#### 1. Dashboard (`/`)
Main overview page with key statistics.

**Components:**
- **Summary Cards**: Total accidents, casualties, fatalities, KSI rate
- **Year Comparison**: Side-by-side stats for 2019-2023
- **Severity Trend Chart**: Year-over-year severity breakdown
- **Quick Stats**: Fatal accidents percentage, avg casualties per accident

---

#### 2. Analytics (`/analytics`)
Detailed statistical analysis.

**Visualizations:**
- **Time Series Chart**: Accidents over time (year/month granularity)
- **Hourly Pattern Chart**: Accidents by hour of day (bar chart)
- **Day of Week Chart**: Accidents by day (bar chart)
- **Police Force Statistics**: Ranked list with gradient progress bars
- **Vehicle Types**: Horizontal bar chart with color coding
- **Conditions Breakdown**: Weather, light, road surface distributions

---

#### 3. Accident Map (`/map`)
Interactive geographic visualization.

**Features:**
- **Cluster Markers**: Grouped accident points with zoom-based expansion
- **Severity Color Coding**: Red=Fatal, Orange=Serious, Blue=Slight
- **Click Details**: Popup with accident information
- **Year Filter**: Dropdown to filter by year
- **Severity Filter**: Checkboxes for severity levels

---

#### 4. Hotspots (`/hotspots`)
High-risk area identification.

**Features:**
- **Top 50 Hotspots**: LSOAs ranked by risk score
- **Risk Score Calculation**: `(Fatal×10) + (Serious×3) + (Slight×1)`
- **Map View**: Heatmap overlay on map
- **LSOA Details**: Click for detailed breakdown

---

#### 5. School Safety (`/schools`)
School proximity analysis.

**Features:**
- **School Search**: Find schools by name or postcode
- **Accident Radius**: Accidents within 500m of each school
- **Risk Ranking**: Schools ranked by nearby accident severity
- **Interactive Map**: School locations with accident clusters

---

### Chart Components

| Component | Library | Usage |
|-----------|---------|-------|
| Line Charts | Recharts | Time series trends |
| Bar Charts | Recharts | Hourly/daily patterns, vehicle types |
| Progress Bars | Custom CSS | Police force rankings |
| Heatmaps | Leaflet.heat | Geographic density |
| Cluster Maps | react-leaflet-cluster | Accident point grouping |

---

## Setup & Deployment

### Prerequisites

- PostgreSQL 18+ with PostGIS extension
- Python 3.11+
- Node.js 18+
- Docker (optional)

### Database Setup

```bash
# Create database
createdb roadsafety

# Enable PostGIS
psql -d roadsafety -c "CREATE EXTENSION postgis;"

# Run schema creation
psql -d roadsafety -f scripts/init_db.sql
```

### API Setup

```bash
# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt

# Set environment variables
copy .env.example .env
# Edit .env with your DATABASE_URL

# Run API
uvicorn api.main:app --reload --port 8000
```

### Dashboard Setup

```bash
cd dashboard

# Install dependencies
npm install

# Set environment variables
copy .env.example .env
# Edit VITE_API_URL

# Run development server
npm run dev
```

### Docker Deployment

```bash
# Build and run all services
docker-compose -f docker-compose.prod.yml up -d

# Services:
# - API: http://localhost:8000
# - Dashboard: http://localhost (port 80)
# - PostgreSQL: localhost:5432
```

### Environment Variables

#### API (.env)
```env
DATABASE_URL=postgresql://postgres:password@localhost:5432/roadsafety
CORS_ORIGINS=http://localhost:5173,http://localhost
API_SECRET_KEY=your-secret-key
CACHE_TTL=300
```

#### Dashboard (.env)
```env
VITE_API_URL=http://localhost:8000/api/v1
VITE_MAP_CENTER_LAT=54.5
VITE_MAP_CENTER_LNG=-2
VITE_MAP_ZOOM=6
```

---

## GitHub Repositories

| Repository | URL |
|------------|-----|
| API | https://github.com/saqibstudent/uk-road-safety-api |
| Dashboard | https://github.com/saqibstudent/uk-road-safety-dashboard |

---

## Performance Optimizations

### Implemented
- ✅ Connection pooling with SQLAlchemy QueuePool
- ✅ In-memory caching with 5-minute TTL
- ✅ Composite database indexes (year+severity, day_of_week)
- ✅ Bulk API endpoints to reduce HTTP requests
- ✅ Optimized vehicle type query (removed slow JOIN)
- ✅ PostGIS spatial indexes for geographic queries

### Query Performance

| Endpoint | Before | After |
|----------|--------|-------|
| `/analytics/summary/bulk` | 5 calls × 200ms | 1 call × 150ms |
| `/analytics/vehicle-types` | 7.7s | 0.3s |
| `/schools/nearby` | 2.5s | 0.1s |

---

## License

This project uses UK Government Open Data under the Open Government Licence v3.0.

---

*Documentation generated: January 2026*
