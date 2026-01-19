# UK Road Safety Data Platform

A comprehensive platform for analyzing UK road accident data, providing APIs and analytics for road safety research, commercial applications, and policy-making.

## 🚀 Features

- **Complete ETL Pipeline**: Automated data ingestion from 8+ government sources
- **PostGIS Database**: Spatial database optimized for geographic queries
- **REST API**: FastAPI-based API with comprehensive endpoints
- **Automated Updates**: GitHub Actions for scheduled data refreshes
- **Risk Analytics**: Pre-computed risk scores and hotspot identification

## 📊 Data Sources

| Source | Description | Update Frequency |
|--------|-------------|------------------|
| **STATS19** | Road accident data from DfT | Annual (September) |
| **LSOA Boundaries** | Geographic boundaries from ONS | Decennial |
| **Traffic Counts** | Vehicle counts from DfT | Annual |
| **Schools (GIAS)** | School locations from DfE | Weekly |
| **WebTRIS** | Real-time traffic (motorways) | Real-time |
| **Met Office** | Weather observations | Hourly |

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
│  STATS19 │ ONS │ DfT Traffic │ Schools │ Weather │ WebTRIS      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        ETL Pipeline                              │
│  Extractors → Transformers → Loaders                            │
│  (Python + Pandas + GeoPandas)                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL + PostGIS                          │
│  accidents │ casualties │ vehicles │ lsoa_boundaries │ ...      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI                                   │
│  /accidents │ /analytics │ /nearby │ /hotspots                  │
└─────────────────────────────────────────────────────────────────┘
```

## 🚦 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/uk-road-safety-platform.git
cd uk-road-safety-platform

# Run setup script
chmod +x scripts/setup.sh
./scripts/setup.sh

# Or manually:

# 1. Copy environment file
cp .env.template .env

# 2. Start database
docker compose up -d database

# 3. Install Python dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Load initial data
python -m etl.main stats19 --start-year 2020
python -m etl.main geography --lsoa

# 5. Start API
docker compose up -d api
```

### Verify Installation

```bash
# Test database connection
python -m etl.main test-connection

# Access API docs
open http://localhost:8000/docs
```

## 📁 Project Structure

```
uk_road_safety_platform/
├── api/                          # FastAPI application
│   ├── main.py                   # API entry point
│   └── routers/                  # API endpoints
│       ├── accidents.py          # Accident queries
│       ├── analytics.py          # Statistics & patterns
│       └── health.py             # Health checks
├── etl/                          # ETL pipeline
│   ├── config.py                 # Configuration
│   ├── main.py                   # CLI entry point
│   ├── sources/                  # Data extractors
│   │   ├── stats19.py            # STATS19 accidents
│   │   ├── ons_geography.py      # LSOA boundaries
│   │   ├── traffic_counts.py     # Traffic data
│   │   └── schools.py            # School locations
│   ├── transformers/             # Data transformers
│   └── loaders/                  # Database loaders
│       └── postgres.py           # PostgreSQL loader
├── scripts/                      # Utility scripts
│   ├── init_db.sql               # Database schema
│   └── setup.sh                  # Setup script
├── .github/workflows/            # GitHub Actions
│   ├── daily_update.yml          # Daily refresh
│   ├── weekly_update.yml         # Weekly refresh
│   └── annual_update.yml         # Annual STATS19
├── data/raw/                     # Downloaded data files
├── logs/                         # ETL logs
├── docker-compose.yml            # Docker services
├── Dockerfile.api                # API container
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables

```bash
# Database
DB_USER=roadsafety
DB_PASSWORD=your_secure_password
DB_NAME=road_safety
DB_HOST=localhost
DB_PORT=5432

# API
API_SECRET_KEY=your-secret-key
API_HOST=0.0.0.0
API_PORT=8000

# External APIs (optional)
MET_OFFICE_API_KEY=your-api-key
```

### GitHub Secrets (for Actions)

- `DATABASE_URL`: Full PostgreSQL connection string
- `MET_OFFICE_API_KEY`: Met Office DataHub API key

## 📡 API Reference

### Accidents

```bash
# List accidents with filters
GET /api/v1/accidents?year=2023&severity=1&page=1

# Search nearby location
GET /api/v1/accidents/nearby?lat=51.5074&lon=-0.1278&radius=500

# Get specific accident
GET /api/v1/accidents/{accident_id}

# Get LSOA statistics
GET /api/v1/accidents/lsoa/{lsoa_code}/stats
```

### Analytics

```bash
# Year summary
GET /api/v1/analytics/summary/2023

# Time series
GET /api/v1/analytics/timeseries?start_year=2018&end_year=2023

# Hourly patterns
GET /api/v1/analytics/patterns/hourly?year=2023

# Hotspots
GET /api/v1/analytics/hotspots?year=2023&limit=50

# Police force stats
GET /api/v1/analytics/police-forces?year=2023
```

## 🔄 ETL Commands

```bash
# Load STATS19 data
python -m etl.main stats19 --start-year 2020 --end-year 2023

# Load geographic boundaries
python -m etl.main geography --lsoa --la --police

# Load schools data
python -m etl.main schools

# Load traffic counts
python -m etl.main traffic

# Full ETL pipeline
python -m etl.main full --stats19-years 2020-2023

# Test database connection
python -m etl.main test-connection

# Refresh statistics
python -m etl.main refresh-statistics --year 2023
```

## 📈 Database Schema

### Core Tables

| Table | Description | Records (typical) |
|-------|-------------|-------------------|
| `accidents` | Main accident records | ~120K/year |
| `casualties` | Casualty details | ~150K/year |
| `vehicles` | Vehicle details | ~210K/year |
| `lsoa_boundaries` | LSOA geographic boundaries | ~35K |
| `traffic_count_points` | Traffic monitoring points | ~45K |
| `schools` | School locations | ~25K |

### Key Indexes

- Spatial (GIST) on all geometry columns
- B-tree on `accident_date`, `severity`, `lsoa_code`
- Composite indexes for common query patterns

## 🚀 Deployment

### Railway / Render

```bash
# Deploy database
railway add postgres

# Set environment variables
railway variables set DATABASE_URL=...

# Deploy API
railway up
```

### Docker Production

```bash
# Build and run
docker compose -f docker-compose.prod.yml up -d

# Scale API
docker compose up -d --scale api=3
```

## 📊 Performance

### Query Performance (typical)

| Query Type | Response Time |
|------------|---------------|
| Point radius search (500m) | <50ms |
| Year summary | <100ms |
| LSOA statistics | <30ms |
| Hotspots (top 50) | <200ms |

### Data Volumes

| Dataset | Size |
|---------|------|
| STATS19 (per year) | ~50MB |
| LSOA boundaries | ~200MB |
| Full database | ~2-3GB |

## � Production Deployment

### Quick Deploy with Docker

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/uk-road-safety-platform.git
cd uk-road-safety-platform

# 2. Create environment file
cp .env.example .env
# Edit .env with secure passwords!

# 3. Deploy (Linux/Mac)
chmod +x deploy.sh
./deploy.sh

# Or Windows PowerShell:
.\deploy.ps1
```

### Manual Deployment

```bash
# Build and start all services
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d

# Check status
docker compose -f docker-compose.prod.yml ps

# View logs
docker compose -f docker-compose.prod.yml logs -f
```

### Cloud Deployment Options

#### AWS / DigitalOcean / Azure VM

1. Provision a VM (recommended: 2 CPU, 4GB RAM minimum)
2. Install Docker and Docker Compose
3. Clone repo and run `./deploy.sh`
4. Configure firewall to allow port 80/443
5. (Optional) Set up SSL with Certbot/Let's Encrypt

#### Railway / Render / Fly.io

These platforms support Docker deployment directly:
1. Connect your GitHub repository
2. Set environment variables in dashboard
3. Deploy from `docker-compose.prod.yml`

### Load Initial Data

After deployment, load the STATS19 data:

```bash
# Connect to API container
docker compose -f docker-compose.prod.yml exec api bash

# Run ETL
python -m etl.main stats19 --start-year 2020
python -m etl.main geography --lsoa
```

## �🔒 Security

- API rate limiting (configurable)
- Input validation on all endpoints
- SQL injection prevention (parameterized queries)
- CORS configuration for production

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## 📄 License

This project uses data under the Open Government Licence v3.0.

- STATS19 data: Crown Copyright
- ONS boundaries: Crown Copyright  
- Traffic data: Crown Copyright

## 📞 Support

- Issues: GitHub Issues
- Documentation: `/docs` endpoint
- Email: support@example.com

---

Built with ❤️ for road safety research and commercial applications.
