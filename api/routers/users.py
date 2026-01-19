"""
User Authentication Router
Handles signup, login, and user management
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime, timedelta
import secrets
import hashlib
import jwt
import os
import psycopg2

router = APIRouter(tags=["users"])

# JWT Configuration
JWT_SECRET = os.getenv("JWT_SECRET", "uk-road-safety-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:pass@localhost:5432/roadsafety')

def get_db_connection():
    """Get a psycopg2 database connection."""
    return psycopg2.connect(DATABASE_URL)

# Rate limit tiers
TIERS = {
    "free": {"requests_per_hour": 100, "name": "Free"},
    "developer": {"requests_per_hour": 5000, "name": "Developer"},
    "professional": {"requests_per_hour": 25000, "name": "Professional"},
}


class UserSignup(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, description="Minimum 8 characters")
    name: Optional[str] = None


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str]
    api_key: str
    tier: str
    created_at: datetime


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: UserResponse


class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    current_password: Optional[str] = None
    new_password: Optional[str] = Field(None, min_length=8)


def hash_password(password: str) -> str:
    """Hash password using SHA-256 with salt"""
    salt = secrets.token_hex(16)
    hashed = hashlib.sha256((password + salt).encode()).hexdigest()
    return f"{salt}:{hashed}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against stored hash"""
    try:
        salt, hashed = stored_hash.split(":")
        return hashlib.sha256((password + salt).encode()).hexdigest() == hashed
    except ValueError:
        return False


def generate_api_key() -> str:
    """Generate a unique API key"""
    return f"rsk_{secrets.token_hex(24)}"


def create_jwt_token(user_id: int, email: str) -> tuple[str, int]:
    """Create JWT token for user session"""
    expires = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": expires,
        "iat": datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, int(JWT_EXPIRATION_HOURS * 3600)


def verify_jwt_token(token: str) -> dict:
    """Verify and decode JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(request: Request) -> dict:
    """Dependency to get current user from JWT token"""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    
    token = auth_header.split(" ")[1]
    payload = verify_jwt_token(token)
    
    # Get user from database
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, email, name, api_key, tier, created_at FROM users WHERE id = %s AND is_active = TRUE",
                (payload["user_id"],)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="User not found")
            return {
                "id": row[0],
                "email": row[1],
                "name": row[2],
                "api_key": row[3],
                "tier": row[4],
                "created_at": row[5]
            }
    finally:
        conn.close()


@router.post("/signup", response_model=TokenResponse)
async def signup(user: UserSignup, request: Request):
    """
    Create a new user account
    
    - Automatically generates a unique API key
    - Starts with 'free' tier (100 requests/hour)
    - Returns JWT token for authentication
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if email already exists
            cur.execute("SELECT id FROM users WHERE email = %s", (user.email,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Email already registered")
            
            # Create user
            password_hash = hash_password(user.password)
            api_key = generate_api_key()
            
            cur.execute("""
                INSERT INTO users (email, password_hash, name, api_key, tier)
                VALUES (%s, %s, %s, %s, 'free')
                RETURNING id, email, name, api_key, tier, created_at
            """, (user.email, password_hash, user.name, api_key))
            
            row = cur.fetchone()
            conn.commit()
            
            user_data = UserResponse(
                id=row[0],
                email=row[1],
                name=row[2],
                api_key=row[3],
                tier=row[4],
                created_at=row[5]
            )
            
            token, expires_in = create_jwt_token(user_data.id, user_data.email)
            
            return TokenResponse(
                access_token=token,
                expires_in=expires_in,
                user=user_data
            )
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin, request: Request):
    """
    Login with email and password
    
    Returns JWT token for authentication
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, password_hash, name, api_key, tier, created_at, is_active
                FROM users WHERE email = %s
            """, (credentials.email,))
            
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid email or password")
            
            if not row[7]:  # is_active
                raise HTTPException(status_code=401, detail="Account is deactivated")
            
            if not verify_password(credentials.password, row[2]):
                raise HTTPException(status_code=401, detail="Invalid email or password")
            
            # Update last login
            cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (row[0],))
            conn.commit()
            
            user_data = UserResponse(
                id=row[0],
                email=row[1],
                name=row[3],
                api_key=row[4],
                tier=row[5],
                created_at=row[6]
            )
            
            token, expires_in = create_jwt_token(user_data.id, user_data.email)
            
            return TokenResponse(
                access_token=token,
                expires_in=expires_in,
                user=user_data
            )
    finally:
        conn.close()


@router.get("/me", response_model=UserResponse)
async def get_profile(current_user: dict = Depends(get_current_user)):
    """Get current user profile"""
    return UserResponse(**current_user)


@router.put("/me", response_model=UserResponse)
async def update_profile(
    update: ProfileUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update user profile (name or password)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            updates = []
            params = []
            
            if update.name is not None:
                updates.append("name = %s")
                params.append(update.name)
            
            if update.new_password:
                if not update.current_password:
                    raise HTTPException(status_code=400, detail="Current password required to change password")
                
                # Verify current password
                cur.execute("SELECT password_hash FROM users WHERE id = %s", (current_user["id"],))
                row = cur.fetchone()
                if not verify_password(update.current_password, row[0]):
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
                
                updates.append("password_hash = %s")
                params.append(hash_password(update.new_password))
            
            if not updates:
                raise HTTPException(status_code=400, detail="No updates provided")
            
            params.append(current_user["id"])
            cur.execute(f"""
                UPDATE users SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, email, name, api_key, tier, created_at
            """, params)
            
            row = cur.fetchone()
            conn.commit()
            
            return UserResponse(
                id=row[0],
                email=row[1],
                name=row[2],
                api_key=row[3],
                tier=row[4],
                created_at=row[5]
            )
    finally:
        conn.close()


@router.post("/regenerate-api-key", response_model=UserResponse)
async def regenerate_api_key(current_user: dict = Depends(get_current_user)):
    """Generate a new API key (invalidates the old one)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            new_api_key = generate_api_key()
            cur.execute("""
                UPDATE users SET api_key = %s
                WHERE id = %s
                RETURNING id, email, name, api_key, tier, created_at
            """, (new_api_key, current_user["id"]))
            
            row = cur.fetchone()
            conn.commit()
            
            return UserResponse(
                id=row[0],
                email=row[1],
                name=row[2],
                api_key=row[3],
                tier=row[4],
                created_at=row[5]
            )
    finally:
        conn.close()


@router.get("/usage-stats")
async def get_user_usage_stats(current_user: dict = Depends(get_current_user)):
    """Get usage statistics for the authenticated user"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            api_key = current_user["api_key"]
            tier = current_user["tier"]
            tier_info = TIERS.get(tier, TIERS["free"])
            
            # Get current hour usage
            cur.execute("""
                SELECT COUNT(*) FROM api_usage 
                WHERE api_key = %s AND request_time > NOW() - INTERVAL '1 hour'
            """, (api_key,))
            current_hour_usage = cur.fetchone()[0]
            
            # Get today's usage
            cur.execute("""
                SELECT COUNT(*) FROM api_usage 
                WHERE api_key = %s AND request_time > CURRENT_DATE
            """, (api_key,))
            today_usage = cur.fetchone()[0]
            
            # Get total usage
            cur.execute("SELECT COUNT(*) FROM api_usage WHERE api_key = %s", (api_key,))
            total_usage = cur.fetchone()[0]
            
            # Get hourly breakdown (last 24 hours)
            cur.execute("""
                SELECT 
                    DATE_TRUNC('hour', request_time) as hour,
                    COUNT(*) as count
                FROM api_usage
                WHERE api_key = %s AND request_time > NOW() - INTERVAL '24 hours'
                GROUP BY DATE_TRUNC('hour', request_time)
                ORDER BY hour
            """, (api_key,))
            hourly = [{"hour": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]
            
            # Get top endpoints
            cur.execute("""
                SELECT endpoint, COUNT(*) as count
                FROM api_usage
                WHERE api_key = %s
                GROUP BY endpoint
                ORDER BY count DESC
                LIMIT 10
            """, (api_key,))
            top_endpoints = [{"endpoint": row[0], "count": row[1]} for row in cur.fetchall()]
            
            return {
                "tier": tier,
                "tier_name": tier_info["name"],
                "rate_limit": tier_info["requests_per_hour"],
                "current_hour_usage": current_hour_usage,
                "remaining_requests": max(0, tier_info["requests_per_hour"] - current_hour_usage),
                "today_usage": today_usage,
                "total_usage": total_usage,
                "hourly_breakdown": hourly,
                "top_endpoints": top_endpoints
            }
    finally:
        conn.close()


@router.get("/verify-token")
async def verify_token(current_user: dict = Depends(get_current_user)):
    """Verify if the current JWT token is valid"""
    return {"valid": True, "user_id": current_user["id"], "email": current_user["email"]}
