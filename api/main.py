"""
UK Road Safety Platform - FastAPI Application

REST API for accessing road accident data and analytics.
Includes API key authentication and rate limiting.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from typing import Optional, List
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .routers import accidents, analytics, health, schools, usage, demographics, users
from .auth import RateLimitMiddleware, optional_api_key

# App metadata
APP_TITLE = "UK Road Safety API"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = """
## UK Road Safety Data Platform API

Access comprehensive UK road accident data, analytics, and risk assessments.

### Features

* **Accident Data**: Query historical accident records with geospatial filtering
* **Risk Analysis**: Get location-based risk scores and hotspot identification
* **Analytics**: Pre-computed statistics by region, time, and other dimensions
* **Route Risk**: Calculate safety scores for journey routes

### Data Sources

* STATS19 road accident data (Department for Transport)
* LSOA geographic boundaries (ONS)
* Traffic counts (DfT)
* School locations (GIAS)

### Rate Limits

* Free tier: 100 requests/hour
* Developer: 5,000 requests/hour
* Professional: 25,000 requests/hour

### Authentication

Include your API key in requests using:
* Header: `X-API-Key: your-api-key`
* Query parameter: `?api_key=your-api-key`

**Demo keys for testing:**
* Free tier: `demo-key-free`
* Developer tier: `demo-key-dev`
* Professional tier: `demo-key-pro`
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    print(f"Starting {APP_TITLE} v{APP_VERSION}")
    yield
    # Shutdown
    print("Shutting down API")


# Create FastAPI app with OpenAPI security scheme
app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    description=APP_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    swagger_ui_parameters={"persistAuthorization": True}
)

# Add security scheme to OpenAPI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    from fastapi.openapi.utils import get_openapi
    openapi_schema = get_openapi(
        title=APP_TITLE,
        version=APP_VERSION,
        description=APP_DESCRIPTION,
        routes=app.routes,
    )
    
    # Add API Key security scheme
    openapi_schema["components"]["securitySchemes"] = {
        "APIKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API key for authentication. Get one by signing up."
        }
    }
    
    # Apply security globally to all endpoints
    openapi_schema["security"] = [{"APIKeyHeader": []}]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting middleware
app.add_middleware(RateLimitMiddleware)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(accidents.router, prefix="/api/v1/accidents", tags=["Accidents"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(demographics.router, prefix="/api/v1/demographics", tags=["Demographics"])
app.include_router(schools.router, prefix="/api/v1/schools", tags=["Schools"])
app.include_router(usage.router, prefix="/api/v1/usage", tags=["Usage & Rate Limits"])
app.include_router(users.router, prefix="/api/v1/users", tags=["User Authentication"])


@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint - redirect to docs."""
    return {
        "name": APP_TITLE,
        "version": APP_VERSION,
        "docs": "/docs",
        "health": "/health"
    }


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "Not found", "detail": str(exc.detail)}
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        # from environment variable or default to 8080
        port=int(os.getenv("API_PORT", 8080)),
        reload=True
    )
