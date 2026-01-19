#!/bin/bash
# UK Road Safety Platform - Production Deployment Script

set -e

echo "🚀 UK Road Safety Platform - Production Deployment"
echo "=================================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env file with your production values!"
    echo "   - Set strong DB_PASSWORD"
    echo "   - Set random API_SECRET_KEY"
    exit 1
fi

# Check Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "🔨 Building production images..."
docker compose -f docker-compose.prod.yml build --no-cache

echo "🗄️  Starting database..."
docker compose -f docker-compose.prod.yml up -d database
echo "⏳ Waiting for database to be healthy..."
sleep 10

echo "🚀 Starting all services..."
docker compose -f docker-compose.prod.yml up -d

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📊 Services running:"
docker compose -f docker-compose.prod.yml ps
echo ""
echo "🌐 Access the application at: http://localhost:${PORT:-80}"
echo ""
echo "📋 Useful commands:"
echo "   View logs:      docker compose -f docker-compose.prod.yml logs -f"
echo "   Stop services:  docker compose -f docker-compose.prod.yml down"
echo "   Restart:        docker compose -f docker-compose.prod.yml restart"
