"""
API Authentication and Rate Limiting

Provides API key authentication and rate limiting middleware.
"""

from fastapi import HTTPException, Security, Request, Depends
from fastapi.security import APIKeyHeader, APIKeyQuery
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import time
import os
import hashlib
from sqlalchemy import create_engine, text

# Configuration
API_KEY_NAME = "X-API-Key"
RATE_LIMIT_WINDOW = 3600  # 1 hour in seconds

# Rate limits by tier
RATE_LIMITS = {
    "free": 100,        # 100 requests/hour
    "developer": 5000,  # 5,000 requests/hour
    "professional": 25000,  # 25,000 requests/hour
    "unlimited": float('inf')  # No limit
}

# API key header/query parameter
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)

# In-memory storage for rate limiting and API keys
# In production, use Redis
_rate_limit_store: Dict[str, Dict] = defaultdict(lambda: {"count": 0, "reset_at": 0})
_api_keys_cache: Dict[str, Dict] = {}
_cache_ttl = 300  # 5 minutes

# Demo API keys for development (in production, use database)
DEMO_API_KEYS = {
    "demo-key-free": {"tier": "free", "name": "Demo Free", "active": True},
    "demo-key-dev": {"tier": "developer", "name": "Demo Developer", "active": True},
    "demo-key-pro": {"tier": "professional", "name": "Demo Professional", "active": True},
    "admin-key-unlimited": {"tier": "unlimited", "name": "Admin", "active": True},
}

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)


def get_db_engine():
    """Get database engine for API key validation."""
    return create_engine(DATABASE_URL)


def validate_api_key(api_key: str) -> Optional[Dict]:
    """
    Validate API key and return key info.
    Checks demo keys first, then database.
    """
    if not api_key:
        return None
    
    # Check demo keys first
    if api_key in DEMO_API_KEYS:
        key_info = DEMO_API_KEYS[api_key]
        if key_info["active"]:
            return {"key": api_key, **key_info}
        return None
    
    # Check cache
    cache_key = f"apikey_{api_key}"
    if cache_key in _api_keys_cache:
        cached, cached_at = _api_keys_cache[cache_key]
        if time.time() - cached_at < _cache_ttl:
            return cached
    
    # Check database - first try users table (for user-generated keys)
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Check users table first (user signup API keys start with 'rsk_')
            result = conn.execute(
                text("""
                    SELECT api_key, tier, name, is_active, id
                    FROM users 
                    WHERE api_key = :key AND is_active = true
                """),
                {"key": api_key}
            )
            row = result.fetchone()
            if row:
                key_info = {
                    "key": row[0],
                    "tier": row[1],
                    "name": row[2],
                    "active": row[3],
                    "user_id": row[4]
                }
                _api_keys_cache[cache_key] = (key_info, time.time())
                return key_info
            
            # Fallback to api_keys table if exists
            result = conn.execute(
                text("""
                    SELECT api_key, tier, name, active 
                    FROM api_keys 
                    WHERE api_key = :key AND active = true
                """),
                {"key": api_key}
            )
            row = result.fetchone()
            if row:
                key_info = {
                    "key": row[0],
                    "tier": row[1],
                    "name": row[2],
                    "active": row[3]
                }
                _api_keys_cache[cache_key] = (key_info, time.time())
                return key_info
    except Exception:
        # Table might not exist, that's ok
        pass
    
    return None


def check_rate_limit(rate_limit_key: str, tier: str) -> Tuple[bool, int, int]:
    """
    Check if request is within rate limit.
    Uses rate_limit_key (user_id for registered users, api_key for others) to track limits.
    This ensures rate limits persist across API key regeneration.
    Returns: (allowed, remaining, reset_at)
    """
    now = time.time()
    store = _rate_limit_store[rate_limit_key]
    
    # Reset if window expired
    if now > store["reset_at"]:
        store["count"] = 0
        store["reset_at"] = now + RATE_LIMIT_WINDOW
    
    limit = RATE_LIMITS.get(tier, RATE_LIMITS["free"])
    remaining = max(0, limit - store["count"])
    
    if store["count"] >= limit:
        return False, 0, int(store["reset_at"])
    
    store["count"] += 1
    return True, remaining - 1, int(store["reset_at"])


def log_api_usage(api_key: str, endpoint: str, method: str, status_code: int, response_time_ms: int, ip_address: str, user_id: int = None):
    """Log API usage to database (async-safe version). Includes user_id for tracking across key regeneration."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO api_usage (api_key, endpoint, method, status_code, response_time_ms, ip_address, request_time, user_id)
                    VALUES (:api_key, :endpoint, :method, :status_code, :response_time_ms, :ip_address, NOW(), :user_id)
                """),
                {
                    "api_key": api_key or "anonymous",
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": status_code,
                    "response_time_ms": response_time_ms,
                    "ip_address": ip_address,
                    "user_id": user_id
                }
            )
            conn.commit()
    except Exception as e:
        pass  # Don't fail request if logging fails


async def get_api_key(
    api_key_header: str = Security(api_key_header),
    api_key_query: str = Security(api_key_query)
) -> Optional[str]:
    """Extract API key from header or query parameter."""
    return api_key_header or api_key_query


async def require_api_key(
    request: Request,
    api_key: str = Depends(get_api_key)
) -> Dict:
    """
    Dependency that requires valid API key.
    Returns key info if valid, raises HTTPException otherwise.
    Dashboard requests (X-Dashboard: true) skip rate limiting.
    """
    # Allow health endpoints without auth
    if request.url.path in ["/health", "/", "/docs", "/redoc", "/openapi.json"]:
        return {"key": "public", "tier": "free", "name": "Public"}
    
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required. Include 'X-API-Key' header or 'api_key' query parameter.",
            headers={"WWW-Authenticate": "API key required"}
        )
    
    key_info = validate_api_key(api_key)
    if not key_info:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "API key invalid"}
        )
    
    # Check if this is a dashboard request (skip rate limiting for dashboard)
    is_dashboard = request.headers.get("X-Dashboard") == "true"
    
    # Store user info in request state
    request.state.api_key = api_key
    request.state.tier = key_info["tier"]
    request.state.user_id = key_info.get("user_id")
    request.state.is_dashboard = is_dashboard
    
    if is_dashboard:
        # Dashboard requests: no rate limiting, just set unlimited remaining
        limit = RATE_LIMITS.get(key_info["tier"], 100)
        request.state.rate_limit_remaining = int(limit) if limit != float('inf') else 999999
        request.state.rate_limit_reset = int(time.time() + 3600)
        request.state.rate_limit_limit = limit
        return key_info
    
    # External API requests: apply rate limiting
    rate_limit_key = f"user_{key_info.get('user_id')}" if key_info.get('user_id') else api_key
    allowed, remaining, reset_at = check_rate_limit(rate_limit_key, key_info["tier"])
    
    # Store rate limit info in request state for headers
    request.state.rate_limit_remaining = remaining
    request.state.rate_limit_reset = reset_at
    request.state.rate_limit_limit = RATE_LIMITS.get(key_info["tier"], 100)
    
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Resets at {datetime.fromtimestamp(reset_at).isoformat()}",
            headers={
                "X-RateLimit-Limit": str(request.state.rate_limit_limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(reset_at),
                "Retry-After": str(int(reset_at - time.time()))
            }
        )
    
    return key_info


async def optional_api_key(
    request: Request,
    api_key: str = Depends(get_api_key)
) -> Dict:
    """
    Dependency that accepts optional API key.
    Uses free tier rate limiting for anonymous requests.
    """
    if api_key:
        key_info = validate_api_key(api_key)
        if key_info:
            # Use user_id for rate limiting (persists across key regeneration)
            rate_limit_key = f"user_{key_info.get('user_id')}" if key_info.get('user_id') else api_key
            allowed, remaining, reset_at = check_rate_limit(rate_limit_key, key_info["tier"])
            request.state.rate_limit_remaining = remaining
            request.state.rate_limit_reset = reset_at
            request.state.rate_limit_limit = RATE_LIMITS.get(key_info["tier"], 100)
            request.state.api_key = api_key
            request.state.tier = key_info["tier"]
            
            if not allowed:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            return key_info
    
    # Anonymous user - use IP-based rate limiting
    client_ip = request.client.host if request.client else "unknown"
    anonymous_key = f"anon_{client_ip}"
    allowed, remaining, reset_at = check_rate_limit(anonymous_key, "free")
    
    request.state.rate_limit_remaining = remaining
    request.state.rate_limit_reset = reset_at
    request.state.rate_limit_limit = RATE_LIMITS["free"]
    request.state.api_key = anonymous_key
    request.state.tier = "free"
    
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Get an API key for higher limits."
        )
    
    return {"key": anonymous_key, "tier": "free", "name": "Anonymous"}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware to add rate limit headers to responses and log usage."""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Extract API key from header or query parameter
        api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
        
        # Check if this is a dashboard request (should not count against usage)
        is_dashboard = request.headers.get("X-Dashboard") == "true"
        
        response = await call_next(request)
        response_time = int((time.time() - start_time) * 1000)
        
        # Add rate limit headers if available
        if hasattr(request.state, "rate_limit_remaining"):
            response.headers["X-RateLimit-Limit"] = str(request.state.rate_limit_limit)
            response.headers["X-RateLimit-Remaining"] = str(request.state.rate_limit_remaining)
            response.headers["X-RateLimit-Reset"] = str(request.state.rate_limit_reset)
        
        # Log API usage only for external API calls (not dashboard)
        if api_key and not is_dashboard:
            # Get user_id from request state if available (set by require_api_key)
            user_id = getattr(request.state, 'user_id', None)
            log_api_usage(
                api_key,
                request.url.path,
                request.method,
                response.status_code,
                response_time,
                request.client.host if request.client else "unknown",
                user_id
            )
        
        return response


def get_usage_stats(api_key: str = None, hours: int = 24) -> Dict:
    """Get API usage statistics."""
    engine = get_db_engine()
    
    if api_key:
        sql = """
            SELECT 
                COUNT(*) as total_requests,
                COUNT(DISTINCT endpoint) as unique_endpoints,
                AVG(response_time_ms) as avg_response_time,
                SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count,
                MIN(request_time) as first_request,
                MAX(request_time) as last_request
            FROM api_usage
            WHERE api_key = :api_key
            AND request_time >= NOW() - INTERVAL ':hours hours'
        """
        params = {"api_key": api_key, "hours": hours}
    else:
        sql = """
            SELECT 
                COUNT(*) as total_requests,
                COUNT(DISTINCT api_key) as unique_keys,
                COUNT(DISTINCT endpoint) as unique_endpoints,
                AVG(response_time_ms) as avg_response_time,
                SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
            FROM api_usage
            WHERE request_time >= NOW() - INTERVAL ':hours hours'
        """
        params = {"hours": hours}
    
    try:
        with engine.connect() as conn:
            # Use a simpler query that definitely works
            result = conn.execute(
                text(f"""
                    SELECT 
                        COUNT(*) as total_requests,
                        COUNT(DISTINCT endpoint) as unique_endpoints,
                        COALESCE(AVG(response_time_ms), 0) as avg_response_time
                    FROM api_usage
                    WHERE request_time >= NOW() - INTERVAL '{hours} hours'
                    {"AND api_key = :api_key" if api_key else ""}
                """),
                {"api_key": api_key} if api_key else {}
            )
            row = result.fetchone()
            if row:
                return {
                    "total_requests": row[0],
                    "unique_endpoints": row[1],
                    "avg_response_time_ms": round(float(row[2] or 0), 2),
                    "period_hours": hours
                }
    except Exception as e:
        pass
    
    return {"total_requests": 0, "period_hours": hours}
