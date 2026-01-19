#!/bin/bash
# UK Road Safety Platform - Initial Setup Script
# Run this script to set up the complete platform

set -e  # Exit on error

echo "========================================"
echo "UK Road Safety Platform Setup"
echo "========================================"

# Check requirements
echo ""
echo "Checking requirements..."

command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v docker compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting."; exit 1; }

echo "✓ All requirements met"

# Create directories
echo ""
echo "Creating directories..."
mkdir -p data/raw
mkdir -p logs
echo "✓ Directories created"

# Copy environment file if not exists
if [ ! -f .env ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.template .env
    echo "✓ .env file created - please update with your credentials"
fi

# Start database
echo ""
echo "Starting PostgreSQL database..."
docker compose up -d database
echo "Waiting for database to be ready..."
sleep 10

# Check database health
until docker compose exec -T database pg_isready -U roadsafety -d road_safety > /dev/null 2>&1; do
    echo "Waiting for database..."
    sleep 2
done
echo "✓ Database is ready"

# Create Python virtual environment
echo ""
echo "Setting up Python environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ Python environment ready"

# Test database connection
echo ""
echo "Testing database connection..."
python -m etl.main test-connection

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Load initial data:"
echo "   python -m etl.main stats19 --start-year 2020"
echo ""
echo "2. Load geographic boundaries:"
echo "   python -m etl.main geography --lsoa"
echo ""
echo "3. Start the API:"
echo "   docker compose up -d api"
echo "   # Or locally: uvicorn api.main:app --reload"
echo ""
echo "4. Access the API docs:"
echo "   http://localhost:8000/docs"
echo ""
echo "5. Access pgAdmin (optional):"
echo "   docker compose up -d pgadmin"
echo "   http://localhost:5050"
echo ""
