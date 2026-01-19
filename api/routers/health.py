"""
Health check endpoints
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import create_engine, text

router = APIRouter()

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database: str
    details: Dict[str, Any]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Check API and database health status.
    
    Returns service status and basic statistics.
    """
    details = {}
    db_status = "unknown"
    
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Test connection
            conn.execute(text("SELECT 1"))
            db_status = "healthy"
            
            # Get basic stats
            result = conn.execute(text("SELECT COUNT(*) FROM accidents"))
            details["accidents_count"] = result.scalar()
            
            result = conn.execute(text(
                "SELECT MAX(accident_date) FROM accidents"
            ))
            latest_date = result.scalar()
            details["latest_accident_date"] = str(latest_date) if latest_date else None
            
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
        details["error"] = str(e)
    
    return HealthResponse(
        status="healthy" if db_status == "healthy" else "degraded",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        database=db_status,
        details=details
    )


@router.get("/health/ready")
async def readiness_check():
    """Kubernetes readiness probe."""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Not ready: {e}")


@router.get("/health/live")
async def liveness_check():
    """Kubernetes liveness probe."""
    return {"status": "alive"}
