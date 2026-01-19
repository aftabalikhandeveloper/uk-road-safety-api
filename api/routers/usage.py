"""
Usage and API Management Endpoints

Provides endpoints for checking API usage, rate limits, and account info.
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import os
import time
from sqlalchemy import create_engine, text

from ..auth import require_api_key, RATE_LIMITS, get_usage_stats

router = APIRouter(dependencies=[Depends(require_api_key)])

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)


def get_db_engine():
    return create_engine(DATABASE_URL)


class RateLimitInfo(BaseModel):
    tier: str
    limit: int
    remaining: int
    reset_at: datetime
    reset_in_seconds: int


class UsageStats(BaseModel):
    total_requests: int
    period_hours: int
    unique_endpoints: int = 0
    avg_response_time_ms: float = 0
    error_count: int = 0


class ApiKeyInfo(BaseModel):
    tier: str
    name: str
    rate_limit: RateLimitInfo
    usage: UsageStats


@router.get("/rate-limit", response_model=RateLimitInfo)
async def get_rate_limit_status(request: Request):
    """
    Get current rate limit status for your API key.
    """
    tier = request.state.tier if hasattr(request.state, "tier") else "free"
    limit = RATE_LIMITS.get(tier, 100)
    remaining = request.state.rate_limit_remaining if hasattr(request.state, "rate_limit_remaining") else limit
    reset_at = request.state.rate_limit_reset if hasattr(request.state, "rate_limit_reset") else int(time.time() + 3600)
    
    return RateLimitInfo(
        tier=tier,
        limit=int(limit) if limit != float('inf') else 999999,
        remaining=remaining,
        reset_at=datetime.fromtimestamp(reset_at),
        reset_in_seconds=max(0, reset_at - int(time.time()))
    )


@router.get("/stats")
async def get_api_usage_stats(
    request: Request,
    hours: int = Query(24, ge=1, le=720, description="Hours to look back")
) -> Dict[str, Any]:
    """
    Get your API usage statistics.
    Tracks usage by user_id so stats persist across API key regeneration.
    """
    user_id = request.state.user_id if hasattr(request.state, "user_id") else None
    api_key = request.state.api_key if hasattr(request.state, "api_key") else None
    
    engine = get_db_engine()
    
    try:
        with engine.connect() as conn:
            # Build user filter - get all API keys for this user to track across key regeneration
            user_filter = ""
            if user_id:
                # Get all API keys that belong to this user (current and previous)
                user_filter = f"AND user_id = {user_id}"
            elif api_key and not api_key.startswith('anon_'):
                user_filter = f"AND api_key = '{api_key}'"
            
            # Total requests
            result = conn.execute(
                text(f"""
                    SELECT 
                        COUNT(*) as total_requests,
                        COUNT(DISTINCT endpoint) as unique_endpoints,
                        COALESCE(AVG(response_time_ms), 0) as avg_response_time,
                        SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count,
                        MIN(request_time) as first_request,
                        MAX(request_time) as last_request
                    FROM api_usage
                    WHERE request_time >= NOW() - INTERVAL '{hours} hours'
                    {user_filter}
                """)
            )
            row = result.fetchone()
            
            # Requests by endpoint
            endpoint_result = conn.execute(
                text(f"""
                    SELECT endpoint, COUNT(*) as count
                    FROM api_usage
                    WHERE request_time >= NOW() - INTERVAL '{hours} hours'
                    {user_filter}
                    GROUP BY endpoint
                    ORDER BY count DESC
                    LIMIT 10
                """)
            )
            endpoints = [{"endpoint": r[0], "count": r[1]} for r in endpoint_result.fetchall()]
            
            # Requests by hour
            hourly_result = conn.execute(
                text(f"""
                    SELECT 
                        DATE_TRUNC('hour', request_time) as hour,
                        COUNT(*) as count
                    FROM api_usage
                    WHERE request_time >= NOW() - INTERVAL '{min(hours, 48)} hours'
                    {user_filter}
                    GROUP BY DATE_TRUNC('hour', request_time)
                    ORDER BY hour DESC
                """)
            )
            hourly = [{"hour": r[0].isoformat() if r[0] else None, "count": r[1]} for r in hourly_result.fetchall()]
            
            tier = request.state.tier if hasattr(request.state, "tier") else "free"
            limit = RATE_LIMITS.get(tier, 100)
            remaining = request.state.rate_limit_remaining if hasattr(request.state, "rate_limit_remaining") else limit
            
            return {
                "api_key": api_key[:10] + "..." if api_key and len(api_key) > 10 else api_key,
                "tier": tier,
                "rate_limit": {
                    "limit": int(limit) if limit != float('inf') else 999999,
                    "remaining": remaining,
                    "period": "1 hour"
                },
                "usage": {
                    "total_requests": row[0] if row else 0,
                    "unique_endpoints": row[1] if row else 0,
                    "avg_response_time_ms": round(float(row[2] or 0), 2) if row else 0,
                    "error_count": row[3] if row else 0,
                    "first_request": row[4].isoformat() if row and row[4] else None,
                    "last_request": row[5].isoformat() if row and row[5] else None
                },
                "period_hours": hours,
                "top_endpoints": endpoints,
                "hourly_breakdown": hourly
            }
            
    except Exception as e:
        # Return basic info if database query fails
        tier = request.state.tier if hasattr(request.state, "tier") else "free"
        limit = RATE_LIMITS.get(tier, 100)
        remaining = request.state.rate_limit_remaining if hasattr(request.state, "rate_limit_remaining") else limit
        
        return {
            "api_key": api_key[:10] + "..." if api_key and len(api_key) > 10 else api_key,
            "tier": tier,
            "rate_limit": {
                "limit": int(limit) if limit != float('inf') else 999999,
                "remaining": remaining,
                "period": "1 hour"
            },
            "usage": {
                "total_requests": 0,
                "note": "Usage tracking initializing"
            },
            "period_hours": hours
        }


@router.get("/global-stats")
async def get_global_stats(
    hours: int = Query(24, ge=1, le=720, description="Hours to look back")
) -> Dict[str, Any]:
    """
    Get global API usage statistics (public).
    """
    engine = get_db_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT 
                        COUNT(*) as total_requests,
                        COUNT(DISTINCT api_key) as unique_users,
                        COUNT(DISTINCT endpoint) as unique_endpoints,
                        COALESCE(AVG(response_time_ms), 0) as avg_response_time
                    FROM api_usage
                    WHERE request_time >= NOW() - INTERVAL '{hours} hours'
                """)
            )
            row = result.fetchone()
            
            return {
                "total_requests": row[0] if row else 0,
                "unique_users": row[1] if row else 0,
                "unique_endpoints": row[2] if row else 0,
                "avg_response_time_ms": round(float(row[3] or 0), 2) if row else 0,
                "period_hours": hours
            }
    except Exception:
        return {
            "total_requests": 0,
            "unique_users": 0,
            "period_hours": hours,
            "note": "Usage tracking initializing"
        }


@router.get("/tiers")
async def get_available_tiers() -> Dict[str, Any]:
    """
    Get information about available API tiers.
    """
    return {
        "tiers": [
            {
                "name": "free",
                "rate_limit": 100,
                "period": "1 hour",
                "description": "Free tier for testing and small projects",
                "features": ["Basic accident data", "Analytics endpoints"]
            },
            {
                "name": "developer",
                "rate_limit": 5000,
                "period": "1 hour",
                "description": "For developers building applications",
                "features": ["All free features", "Higher rate limits", "Priority support"]
            },
            {
                "name": "professional",
                "rate_limit": 25000,
                "period": "1 hour",
                "description": "For production applications",
                "features": ["All developer features", "Highest rate limits", "Dedicated support"]
            }
        ],
        "demo_keys": {
            "free": "demo-key-free",
            "developer": "demo-key-dev",
            "professional": "demo-key-pro"
        }
    }
