# UK Road Safety Platform - Windows Deployment Script

Write-Host "🚀 UK Road Safety Platform - Production Deployment" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# Check if .env file exists
if (-not (Test-Path .env)) {
    Write-Host "⚠️  No .env file found. Creating from template..." -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host "📝 Please edit .env file with your production values!" -ForegroundColor Yellow
    Write-Host "   - Set strong DB_PASSWORD"
    Write-Host "   - Set random API_SECRET_KEY"
    exit 1
}

# Check Docker is installed
try {
    docker --version | Out-Null
} catch {
    Write-Host "❌ Docker is not installed. Please install Docker Desktop first." -ForegroundColor Red
    exit 1
}

Write-Host "🔨 Building production images..." -ForegroundColor Green
docker compose -f docker-compose.prod.yml build --no-cache

Write-Host "🗄️  Starting database..." -ForegroundColor Green
docker compose -f docker-compose.prod.yml up -d database
Write-Host "⏳ Waiting for database to be healthy..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "🚀 Starting all services..." -ForegroundColor Green
docker compose -f docker-compose.prod.yml up -d

Write-Host ""
Write-Host "✅ Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📊 Services running:" -ForegroundColor Cyan
docker compose -f docker-compose.prod.yml ps
Write-Host ""

$port = if ($env:PORT) { $env:PORT } else { "80" }
Write-Host "🌐 Access the application at: http://localhost:$port" -ForegroundColor Cyan
Write-Host ""
Write-Host "📋 Useful commands:" -ForegroundColor Yellow
Write-Host "   View logs:      docker compose -f docker-compose.prod.yml logs -f"
Write-Host "   Stop services:  docker compose -f docker-compose.prod.yml down"
Write-Host "   Restart:        docker compose -f docker-compose.prod.yml restart"
