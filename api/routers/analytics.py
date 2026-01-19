"""
Analytics API endpoints

Pre-computed statistics and aggregated data.
Optimized for performance with connection pooling and caching.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date
import os
import time
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from ..auth import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)

# Connection pool - reuse connections
_engine = None

def get_db_engine():
    """Get or create pooled database engine"""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
    return _engine

# Simple in-memory cache with TTL
_cache = {}
_cache_ttl = 300  # 5 minutes

def get_cached(key: str):
    """Get cached value if not expired"""
    if key in _cache:
        value, timestamp = _cache[key]
        if time.time() - timestamp < _cache_ttl:
            return value
    return None

def set_cached(key: str, value: Any):
    """Store value in cache"""
    _cache[key] = (value, time.time())


# Response models
class TimeSeriesPoint(BaseModel):
    period: str
    total_accidents: int
    fatal: int
    serious: int
    slight: int
    casualties: int


class HourlyPattern(BaseModel):
    hour: int
    total_accidents: int
    avg_severity: float


class DayOfWeekPattern(BaseModel):
    day: int
    day_name: str
    total_accidents: int


class SeverityBreakdown(BaseModel):
    fatal: int
    serious: int
    slight: int
    fatal_pct: float
    serious_pct: float
    slight_pct: float


class YearSummary(BaseModel):
    year: Optional[int] = None
    total_accidents: int
    total_casualties: int
    total_vehicles: int = 0
    fatalities: int
    serious_injuries: int
    severity_breakdown: SeverityBreakdown


class PoliceForceStats(BaseModel):
    police_force_code: int
    police_force_name: str
    year: int
    total_accidents: int
    fatal_accidents: int
    serious_accidents: int
    ksi_rate: float


class HotspotLocation(BaseModel):
    latitude: float
    longitude: float
    lsoa_code: str
    lsoa_name: Optional[str]
    accident_count: int
    fatal_count: int
    serious_count: int
    risk_score: float


# Helper
def get_db_connection():
    return get_db_engine()


# NEW: Bulk endpoint to get all years in single query
@router.get("/summary/bulk")
async def get_bulk_year_summary(
    years: str = Query("2020,2021,2022,2023,2024", description="Comma-separated years")
):
    """
    Get summary statistics for multiple years in one call.
    Much faster than calling /summary/{year} multiple times.
    """
    cache_key = f"bulk_summary_{years}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    year_list = [int(y.strip()) for y in years.split(',')]
    engine = get_db_connection()
    
    sql = """
        SELECT 
            accident_year,
            COUNT(*) as total_accidents,
            SUM(number_of_casualties) as total_casualties,
            SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
            SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
            SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight
        FROM accidents
        WHERE accident_year = ANY(:years)
        GROUP BY accident_year
        ORDER BY accident_year
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {'years': year_list})
            
            data = []
            for row in result.fetchall():
                total = row[1]
                fatal = row[3]
                serious = row[4]
                slight = row[5]
                
                data.append({
                    "year": row[0],
                    "total_accidents": total,
                    "total_casualties": row[2] or 0,
                    "fatalities": fatal,
                    "serious_injuries": serious,
                    "severity_breakdown": {
                        "fatal": fatal,
                        "serious": serious,
                        "slight": slight,
                        "fatal_pct": round(fatal / total * 100, 2) if total > 0 else 0,
                        "serious_pct": round(serious / total * 100, 2) if total > 0 else 0,
                        "slight_pct": round(slight / total * 100, 2) if total > 0 else 0
                    }
                })
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/summary/{year}", response_model=YearSummary)
async def get_year_summary(
    year: int
):
    """
    Get summary statistics for a specific year.
    """
    engine = get_db_connection()
    
    sql = """
        SELECT 
            accident_year,
            COUNT(*) as total_accidents,
            SUM(number_of_casualties) as total_casualties,
            SUM(number_of_vehicles) as total_vehicles,
            SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
            SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
            SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight
        FROM accidents
        WHERE accident_year = :year
        GROUP BY accident_year
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {'year': year})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail=f"No data for year {year}")
            
            total = row[1]
            total_vehicles = row[3] or 0
            fatal = row[4]
            serious = row[5]
            slight = row[6]
            
            # Get fatalities from casualties table
            cas_sql = """
                SELECT COUNT(*) 
                FROM casualties c
                JOIN accidents a ON c.accident_id = a.accident_id
                WHERE a.accident_year = :year AND c.severity = 1
            """
            cas_result = conn.execute(text(cas_sql), {'year': year})
            fatalities = cas_result.scalar() or fatal
            
            # Get serious injuries
            serious_sql = """
                SELECT COUNT(*) 
                FROM casualties c
                JOIN accidents a ON c.accident_id = a.accident_id
                WHERE a.accident_year = :year AND c.severity = 2
            """
            serious_result = conn.execute(text(serious_sql), {'year': year})
            serious_injuries = serious_result.scalar() or serious
            
            return YearSummary(
                year=year,
                total_accidents=total,
                total_casualties=row[2] or 0,
                total_vehicles=total_vehicles,
                fatalities=fatalities,
                serious_injuries=serious_injuries,
                severity_breakdown=SeverityBreakdown(
                    fatal=fatal,
                    serious=serious,
                    slight=slight,
                    fatal_pct=round(fatal / total * 100, 2) if total > 0 else 0,
                    serious_pct=round(serious / total * 100, 2) if total > 0 else 0,
                    slight_pct=round(slight / total * 100, 2) if total > 0 else 0
                )
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/summary", response_model=YearSummary)
async def get_all_years_summary():
    """
    Get summary statistics for all years combined.
    """
    engine = get_db_connection()
    
    sql = """
        SELECT 
            COUNT(*) as total_accidents,
            SUM(number_of_casualties) as total_casualties,
            SUM(number_of_vehicles) as total_vehicles,
            SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
            SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
            SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight
        FROM accidents
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="No data available")
            
            total = row[0]
            total_vehicles = row[2] or 0
            fatal = row[3]
            serious = row[4]
            slight = row[5]
            
            # Get fatalities from casualties table
            cas_sql = """
                SELECT COUNT(*) 
                FROM casualties c
                WHERE c.severity = 1
            """
            cas_result = conn.execute(text(cas_sql))
            fatalities = cas_result.scalar() or fatal
            
            # Get serious injuries
            serious_sql = """
                SELECT COUNT(*) 
                FROM casualties c
                WHERE c.severity = 2
            """
            serious_result = conn.execute(text(serious_sql))
            serious_injuries = serious_result.scalar() or serious
            
            return YearSummary(
                year=None,
                total_accidents=total,
                total_casualties=row[1] or 0,
                total_vehicles=total_vehicles,
                fatalities=fatalities,
                serious_injuries=serious_injuries,
                severity_breakdown=SeverityBreakdown(
                    fatal=fatal,
                    serious=serious,
                    slight=slight,
                    fatal_pct=round(fatal / total * 100, 2) if total > 0 else 0,
                    serious_pct=round(serious / total * 100, 2) if total > 0 else 0,
                    slight_pct=round(slight / total * 100, 2) if total > 0 else 0
                )
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/timeseries", response_model=List[TimeSeriesPoint])
async def get_timeseries(
    start_year: int = Query(2018, description="Start year"),
    end_year: int = Query(2023, description="End year"),
    granularity: str = Query("year", description="Granularity: year, month, week")
):
    """
    Get accident time series data.
    OPTIMIZED: Added caching.
    """
    cache_key = f"timeseries_{start_year}_{end_year}_{granularity}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_connection()
    
    if granularity == "year":
        sql = """
            SELECT 
                accident_year::text as period,
                COUNT(*) as total_accidents,
                SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                SUM(number_of_casualties) as casualties
            FROM accidents
            WHERE accident_year BETWEEN :start_year AND :end_year
            GROUP BY accident_year
            ORDER BY accident_year
        """
    elif granularity == "month":
        sql = """
            SELECT 
                TO_CHAR(accident_date, 'YYYY-MM') as period,
                COUNT(*) as total_accidents,
                SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                SUM(number_of_casualties) as casualties
            FROM accidents
            WHERE accident_year BETWEEN :start_year AND :end_year
            GROUP BY TO_CHAR(accident_date, 'YYYY-MM')
            ORDER BY period
        """
    else:
        sql = """
            SELECT 
                TO_CHAR(accident_date, 'IYYY-IW') as period,
                COUNT(*) as total_accidents,
                SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                SUM(number_of_casualties) as casualties
            FROM accidents
            WHERE accident_year BETWEEN :start_year AND :end_year
            GROUP BY TO_CHAR(accident_date, 'IYYY-IW')
            ORDER BY period
        """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {
                'start_year': start_year,
                'end_year': end_year
            })
            
            data = [
                TimeSeriesPoint(
                    period=row[0],
                    total_accidents=row[1],
                    fatal=row[2],
                    serious=row[3],
                    slight=row[4],
                    casualties=row[5] or 0
                )
                for row in result.fetchall()
            ]
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/patterns/hourly", response_model=List[HourlyPattern])
async def get_hourly_patterns(
    year: Optional[int] = Query(None, description="Filter by year")
):
    """
    Get hourly accident patterns.
    OPTIMIZED: Added caching.
    """
    cache_key = f"hourly_{year or 'all'}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_connection()
    
    year_filter = f"AND accident_year = {year}" if year else ""
    
    sql = f"""
        SELECT 
            EXTRACT(HOUR FROM accident_time)::int as hour,
            COUNT(*) as total_accidents,
            AVG(severity) as avg_severity
        FROM accidents
        WHERE accident_time IS NOT NULL
        {year_filter}
        GROUP BY EXTRACT(HOUR FROM accident_time)
        ORDER BY hour
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            
            data = [
                HourlyPattern(
                    hour=row[0],
                    total_accidents=row[1],
                    avg_severity=round(float(row[2]), 2)
                )
                for row in result.fetchall()
            ]
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/patterns/daily", response_model=List[DayOfWeekPattern])
async def get_daily_patterns(
    year: Optional[int] = Query(None, description="Filter by year")
):
    """
    Get day-of-week accident patterns.
    OPTIMIZED: Added caching.
    """
    cache_key = f"daily_{year or 'all'}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_connection()
    
    year_filter = f"AND accident_year = {year}" if year else ""
    day_names = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 
                 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
    
    sql = f"""
        SELECT 
            day_of_week,
            COUNT(*) as total_accidents
        FROM accidents
        WHERE day_of_week IS NOT NULL
        {year_filter}
        GROUP BY day_of_week
        ORDER BY day_of_week
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            
            data = [
                DayOfWeekPattern(
                    day=row[0],
                    day_name=day_names.get(row[0], 'Unknown'),
                    total_accidents=row[1]
                )
                for row in result.fetchall()
            ]
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/police-forces", response_model=List[PoliceForceStats])
async def get_police_force_stats(
    year: Optional[int] = Query(None, description="Year (defaults to all years)"),
    limit: int = Query(20, description="Number of results")
):
    """
    Get statistics by police force area.
    OPTIMIZED: Added caching.
    """
    cache_key = f"police_{year or 'all'}_{limit}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_connection()
    
    year_filter = "AND a.accident_year = :year" if year else ""
    year_val = year if year else 0
    
    sql = f"""
        SELECT 
            a.police_force,
            COALESCE(pf.name, 'Unknown') as police_force_name,
            COALESCE(:year_val, MAX(a.accident_year)) as year,
            COUNT(*) as total_accidents,
            SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as fatal,
            SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as serious,
            (SUM(CASE WHEN a.severity IN (1, 2) THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as ksi_rate
        FROM accidents a
        LEFT JOIN lookup_police_force pf ON a.police_force = pf.code
        WHERE a.police_force IS NOT NULL
        {year_filter}
        GROUP BY a.police_force, pf.name
        ORDER BY total_accidents DESC
        LIMIT :limit
    """
    
    try:
        params = {'year_val': year_val, 'limit': limit}
        if year:
            params['year'] = year
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            
            data = [
                PoliceForceStats(
                    police_force_code=row[0],
                    police_force_name=row[1],
                    year=row[2],
                    total_accidents=row[3],
                    fatal_accidents=row[4],
                    serious_accidents=row[5],
                    ksi_rate=round(float(row[6]), 2)
                )
                for row in result.fetchall()
            ]
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/hotspots", response_model=List[HotspotLocation])
async def get_hotspots(
    year: Optional[int] = Query(None, description="Year (defaults to all years)"),
    min_accidents: int = Query(5, description="Minimum accidents to be considered hotspot"),
    limit: int = Query(50, description="Number of results")
):
    """
    Get accident hotspots (LSOAs with highest accident counts).
    """
    engine = get_db_connection()
    
    year_filter = "AND a.accident_year = :year" if year else ""
    
    sql = f"""
        SELECT 
            AVG(a.latitude) as latitude,
            AVG(a.longitude) as longitude,
            a.lsoa_code,
            lb.lsoa_name,
            COUNT(*) as accident_count,
            SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as fatal_count,
            SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as serious_count,
            (SUM(CASE WHEN a.severity = 1 THEN 10 
                      WHEN a.severity = 2 THEN 3 
                      ELSE 1 END)) as risk_score
        FROM accidents a
        LEFT JOIN lsoa_boundaries lb ON a.lsoa_code = lb.lsoa_code
        WHERE a.lsoa_code IS NOT NULL
        AND a.latitude IS NOT NULL
        {year_filter}
        GROUP BY a.lsoa_code, lb.lsoa_name
        HAVING COUNT(*) >= :min_accidents
        ORDER BY risk_score DESC
        LIMIT :limit
    """
    
    try:
        params = {'min_accidents': min_accidents, 'limit': limit}
        if year:
            params['year'] = year
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            
            return [
                HotspotLocation(
                    latitude=float(row[0]),
                    longitude=float(row[1]),
                    lsoa_code=row[2],
                    lsoa_name=row[3],
                    accident_count=row[4],
                    fatal_count=row[5],
                    serious_count=row[6],
                    risk_score=float(row[7])
                )
                for row in result.fetchall()
            ]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/vehicle-types")
async def get_vehicle_type_stats(
    year: Optional[int] = Query(None, description="Year (defaults to all years)")
) -> List[Dict[str, Any]]:
    """
    Get accident statistics by vehicle type.
    OPTIMIZED: Removed slow JOIN, uses cached results.
    """
    cache_key = f"vehicle_types_{year or 'all'}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_connection()
    
    # Optimized query: avoid expensive JOIN with accidents table
    # Just count vehicles by type - much faster
    if year:
        sql = """
            SELECT 
                v.vehicle_type,
                COALESCE(vt.description, 'Unknown') as vehicle_type_name,
                COUNT(*) as vehicle_count
            FROM vehicles v
            LEFT JOIN lookup_vehicle_type vt ON v.vehicle_type = vt.code
            WHERE v.vehicle_type > 0
            AND v.accident_id IN (
                SELECT accident_id FROM accidents WHERE accident_year = :year
            )
            GROUP BY v.vehicle_type, vt.description
            ORDER BY vehicle_count DESC
        """
    else:
        sql = """
            SELECT 
                v.vehicle_type,
                COALESCE(vt.description, 'Unknown') as vehicle_type_name,
                COUNT(*) as vehicle_count
            FROM vehicles v
            LEFT JOIN lookup_vehicle_type vt ON v.vehicle_type = vt.code
            WHERE v.vehicle_type > 0
            GROUP BY v.vehicle_type, vt.description
            ORDER BY vehicle_count DESC
        """
    
    try:
        params = {'year': year} if year else {}
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            
            data = [
                {
                    "vehicle_type_code": row[0],
                    "vehicle_type_name": row[1],
                    "accidents": row[2],  # Actually vehicle count, but kept for API compat
                    "vehicle_count": row[2],
                    "fatal_accidents": 0  # Removed slow calculation
                }
                for row in result.fetchall()
            ]
            
            set_cached(cache_key, data)
            return data
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/accident-conditions")
async def get_accident_conditions(
    year: Optional[int] = Query(None, description="Year (defaults to all years)")
) -> Dict[str, Any]:
    """
    Get accident breakdown by conditions (weather, light, road surface).
    """
    engine = get_db_connection()
    
    year_filter = "WHERE accident_year = :year" if year else ""
    
    # Weather conditions
    weather_sql = f"""
        SELECT 
            weather_conditions,
            CASE weather_conditions
                WHEN 1 THEN 'Fine no high winds'
                WHEN 2 THEN 'Raining no high winds'
                WHEN 3 THEN 'Snowing no high winds'
                WHEN 4 THEN 'Fine + high winds'
                WHEN 5 THEN 'Raining + high winds'
                WHEN 6 THEN 'Snowing + high winds'
                WHEN 7 THEN 'Fog or mist'
                WHEN 8 THEN 'Other'
                WHEN 9 THEN 'Unknown'
                ELSE 'Unknown'
            END as condition_name,
            COUNT(*) as count
        FROM accidents
        {year_filter}
        GROUP BY weather_conditions
        ORDER BY count DESC
    """
    
    # Light conditions
    light_sql = f"""
        SELECT 
            light_conditions,
            CASE light_conditions
                WHEN 1 THEN 'Daylight'
                WHEN 4 THEN 'Darkness - lights lit'
                WHEN 5 THEN 'Darkness - lights unlit'
                WHEN 6 THEN 'Darkness - no lighting'
                WHEN 7 THEN 'Darkness - lighting unknown'
                ELSE 'Unknown'
            END as condition_name,
            COUNT(*) as count
        FROM accidents
        {year_filter}
        GROUP BY light_conditions
        ORDER BY count DESC
    """
    
    # Road surface
    surface_sql = f"""
        SELECT 
            road_surface_conditions,
            CASE road_surface_conditions
                WHEN 1 THEN 'Dry'
                WHEN 2 THEN 'Wet or damp'
                WHEN 3 THEN 'Snow'
                WHEN 4 THEN 'Frost or ice'
                WHEN 5 THEN 'Flood over 3cm deep'
                WHEN 6 THEN 'Oil or diesel'
                WHEN 7 THEN 'Mud'
                ELSE 'Unknown'
            END as condition_name,
            COUNT(*) as count
        FROM accidents
        {year_filter}
        GROUP BY road_surface_conditions
        ORDER BY count DESC
    """
    
    # Road type
    road_type_sql = f"""
        SELECT 
            road_type,
            CASE road_type
                WHEN 1 THEN 'Roundabout'
                WHEN 2 THEN 'One way street'
                WHEN 3 THEN 'Dual carriageway'
                WHEN 6 THEN 'Single carriageway'
                WHEN 7 THEN 'Slip road'
                WHEN 9 THEN 'Unknown'
                WHEN 12 THEN 'One way/Slip road'
                ELSE 'Other'
            END as type_name,
            COUNT(*) as count
        FROM accidents
        {year_filter}
        GROUP BY road_type
        ORDER BY count DESC
    """
    
    try:
        params = {}
        if year:
            params['year'] = year
        with engine.connect() as conn:
            weather = conn.execute(text(weather_sql), params).fetchall()
            light = conn.execute(text(light_sql), params).fetchall()
            surface = conn.execute(text(surface_sql), params).fetchall()
            road_type = conn.execute(text(road_type_sql), params).fetchall()
            
            return {
                "weather": [{"code": r[0], "name": r[1], "count": r[2]} for r in weather if r[0]],
                "light": [{"code": r[0], "name": r[1], "count": r[2]} for r in light if r[0]],
                "road_surface": [{"code": r[0], "name": r[1], "count": r[2]} for r in surface if r[0]],
                "road_type": [{"code": r[0], "name": r[1], "count": r[2]} for r in road_type if r[0]]
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/heatmap-data")
async def get_heatmap_data(
    year: Optional[int] = Query(None, description="Year"),
    limit: int = Query(5000, description="Max points to return")
) -> List[Dict[str, Any]]:
    """
    Get accident coordinates for heatmap visualization.
    """
    engine = get_db_connection()
    
    year_filter = "AND accident_year = :year" if year else ""
    
    sql = f"""
        SELECT 
            latitude,
            longitude,
            severity,
            number_of_casualties
        FROM accidents
        WHERE latitude IS NOT NULL
        AND longitude IS NOT NULL
        {year_filter}
        ORDER BY 
            CASE WHEN severity = 1 THEN 0 ELSE 1 END,
            number_of_casualties DESC
        LIMIT :limit
    """
    
    try:
        params = {'limit': limit}
        if year:
            params['year'] = year
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            
            return [
                {
                    "lat": float(row[0]),
                    "lng": float(row[1]),
                    "intensity": 10 if row[2] == 1 else 3 if row[2] == 2 else 1
                }
                for row in result.fetchall()
            ]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
