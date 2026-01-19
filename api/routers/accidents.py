"""
Accidents API endpoints

Query road accident data with various filters.
"""

from fastapi import APIRouter, HTTPException, Query, Path, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, time
from decimal import Decimal
import os
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import create_engine, text
import json
from ..auth import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)


# Response models
class Location(BaseModel):
    latitude: float
    longitude: float


class AccidentSummary(BaseModel):
    accident_id: str
    accident_date: date
    accident_time: Optional[str]
    severity: int
    severity_desc: Optional[str]
    location: Location
    lsoa_code: Optional[str]
    number_of_casualties: int
    number_of_vehicles: int
    distance_meters: Optional[float] = None


class AccidentDetail(AccidentSummary):
    day_of_week: Optional[int]
    police_force: Optional[int]
    police_force_name: Optional[str]
    road_type: Optional[int]
    speed_limit: Optional[int]
    light_conditions: Optional[int]
    weather_conditions: Optional[int]
    road_surface_conditions: Optional[int]
    urban_or_rural: Optional[int]


class AccidentListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: List[AccidentSummary]


class NearbySearchResponse(BaseModel):
    center: Location
    radius_meters: int
    total: int
    data: List[AccidentSummary]


class LSOAStatsResponse(BaseModel):
    lsoa_code: str
    lsoa_name: Optional[str]
    year: int
    total_accidents: int
    fatal_accidents: int
    serious_accidents: int
    slight_accidents: int
    total_casualties: int
    risk_score: Optional[float]
    risk_category: Optional[str]


# Helper functions
def get_db_connection():
    """Get database connection."""
    return create_engine(DATABASE_URL)


def severity_to_desc(code: int) -> str:
    """Convert severity code to description."""
    return {1: 'Fatal', 2: 'Serious', 3: 'Slight'}.get(code, 'Unknown')


# Endpoints
@router.get("", response_model=AccidentListResponse)
async def list_accidents(
    year: Optional[int] = Query(None, ge=1979, le=2030, description="Filter by year"),
    severity: Optional[int] = Query(None, ge=1, le=3, description="Filter by severity (1=Fatal, 2=Serious, 3=Slight)"),
    police_force: Optional[int] = Query(None, description="Filter by police force code"),
    lsoa: Optional[str] = Query(None, description="Filter by LSOA code"),
    date_from: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Results per page")
):
    """
    List accidents with optional filters.
    
    Returns paginated accident records matching the specified criteria.
    """
    engine = get_db_connection()
    
    # Build WHERE clause
    conditions = []
    params = {}
    
    if year:
        conditions.append("accident_year = :year")
        params['year'] = year
    if severity:
        conditions.append("severity = :severity")
        params['severity'] = severity
    if police_force:
        conditions.append("police_force = :police_force")
        params['police_force'] = police_force
    if lsoa:
        conditions.append("lsoa_code = :lsoa")
        params['lsoa'] = lsoa
    if date_from:
        conditions.append("accident_date >= :date_from")
        params['date_from'] = date_from
    if date_to:
        conditions.append("accident_date <= :date_to")
        params['date_to'] = date_to
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # Count total
    count_sql = f"SELECT COUNT(*) FROM accidents WHERE {where_clause}"
    
    # Get data
    offset = (page - 1) * page_size
    data_sql = f"""
        SELECT 
            accident_id,
            accident_date,
            accident_time::text,
            severity,
            latitude,
            longitude,
            lsoa_code,
            number_of_casualties,
            number_of_vehicles
        FROM accidents
        WHERE {where_clause}
        ORDER BY accident_date DESC, accident_time DESC
        LIMIT :limit OFFSET :offset
    """
    params['limit'] = page_size
    params['offset'] = offset
    
    try:
        with engine.connect() as conn:
            # Get total count
            result = conn.execute(text(count_sql), params)
            total = result.scalar()
            
            # Get data
            result = conn.execute(text(data_sql), params)
            rows = result.fetchall()
            
            data = [
                AccidentSummary(
                    accident_id=row[0],
                    accident_date=row[1],
                    accident_time=row[2],
                    severity=row[3],
                    severity_desc=severity_to_desc(row[3]),
                    location=Location(latitude=float(row[4]), longitude=float(row[5])),
                    lsoa_code=row[6],
                    number_of_casualties=row[7],
                    number_of_vehicles=row[8]
                )
                for row in rows
                if row[4] and row[5]  # Skip rows without coordinates
            ]
            
            return AccidentListResponse(
                total=total,
                page=page,
                page_size=page_size,
                data=data
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/nearby", response_model=NearbySearchResponse)
async def search_nearby(
    lat: float = Query(..., ge=49.0, le=61.0, description="Latitude"),
    lon: float = Query(..., ge=-9.0, le=2.0, description="Longitude"),
    radius: int = Query(500, ge=50, le=10000, description="Search radius in meters"),
    years: Optional[str] = Query(None, description="Comma-separated years to include"),
    severity: Optional[int] = Query(None, ge=1, le=3, description="Filter by severity"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results")
):
    """
    Search for accidents near a location.
    
    Returns accidents within the specified radius of the given coordinates.
    Uses PostGIS spatial indexing for efficient queries.
    """
    engine = get_db_connection()
    
    # Parse years
    year_filter = ""
    if years:
        year_list = [int(y.strip()) for y in years.split(',')]
        year_filter = f"AND accident_year = ANY(ARRAY{year_list})"
    
    severity_filter = ""
    if severity:
        severity_filter = f"AND severity = {severity}"
    
    sql = f"""
        SELECT 
            accident_id,
            accident_date,
            accident_time::text,
            severity,
            latitude,
            longitude,
            lsoa_code,
            number_of_casualties,
            number_of_vehicles,
            ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
            ) as distance_meters
        FROM accidents
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
            :radius
        )
        {year_filter}
        {severity_filter}
        ORDER BY distance_meters
        LIMIT :limit
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {
                'lat': lat,
                'lon': lon,
                'radius': radius,
                'limit': limit
            })
            rows = result.fetchall()
            
            data = [
                AccidentSummary(
                    accident_id=row[0],
                    accident_date=row[1],
                    accident_time=row[2],
                    severity=row[3],
                    severity_desc=severity_to_desc(row[3]),
                    location=Location(latitude=float(row[4]), longitude=float(row[5])),
                    lsoa_code=row[6],
                    number_of_casualties=row[7],
                    number_of_vehicles=row[8],
                    distance_meters=float(row[9]) if row[9] is not None else None
                )
                for row in rows
            ]
            
            return NearbySearchResponse(
                center=Location(latitude=lat, longitude=lon),
                radius_meters=radius,
                total=len(data),
                data=data
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{accident_id}", response_model=AccidentDetail)
async def get_accident(
    accident_id: str = Path(..., description="Accident ID")
):
    """
    Get detailed information about a specific accident.
    """
    engine = get_db_connection()
    
    sql = """
        SELECT 
            a.accident_id,
            a.accident_date,
            a.accident_time::text,
            a.severity,
            a.latitude,
            a.longitude,
            a.lsoa_code,
            a.number_of_casualties,
            a.number_of_vehicles,
            a.day_of_week,
            a.police_force,
            pf.name as police_force_name,
            a.road_type,
            a.speed_limit,
            a.light_conditions,
            a.weather_conditions,
            a.road_surface_conditions,
            a.urban_or_rural
        FROM accidents a
        LEFT JOIN lookup_police_force pf ON a.police_force = pf.code
        WHERE a.accident_id = :accident_id
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {'accident_id': accident_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Accident not found")
            
            return AccidentDetail(
                accident_id=row[0],
                accident_date=row[1],
                accident_time=row[2],
                severity=row[3],
                severity_desc=severity_to_desc(row[3]),
                location=Location(latitude=float(row[4]), longitude=float(row[5])),
                lsoa_code=row[6],
                number_of_casualties=row[7],
                number_of_vehicles=row[8],
                day_of_week=row[9],
                police_force=row[10],
                police_force_name=row[11],
                road_type=row[12],
                speed_limit=row[13],
                light_conditions=row[14],
                weather_conditions=row[15],
                road_surface_conditions=row[16],
                urban_or_rural=row[17]
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/lsoa/{lsoa_code}/stats", response_model=LSOAStatsResponse)
async def get_lsoa_stats(
    lsoa_code: str = Path(..., description="LSOA code"),
    year: Optional[int] = Query(None, description="Year (default: latest)")
):
    """
    Get accident statistics for an LSOA.
    """
    engine = get_db_connection()
    
    year_filter = f"AND ls.year = {year}" if year else ""
    
    sql = f"""
        SELECT 
            ls.lsoa_code,
            lb.lsoa_name,
            ls.year,
            ls.total_accidents,
            ls.fatal_accidents,
            ls.serious_accidents,
            ls.slight_accidents,
            ls.total_casualties,
            ls.risk_score,
            ls.risk_category
        FROM lsoa_statistics ls
        LEFT JOIN lsoa_boundaries lb ON ls.lsoa_code = lb.lsoa_code
        WHERE ls.lsoa_code = :lsoa_code
        {year_filter}
        ORDER BY ls.year DESC
        LIMIT 1
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), {'lsoa_code': lsoa_code})
            row = result.fetchone()
            
            if not row:
                # Calculate on the fly
                calc_sql = """
                    SELECT 
                        :lsoa_code as lsoa_code,
                        lb.lsoa_name,
                        MAX(accident_year) as year,
                        COUNT(*) as total_accidents,
                        SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                        SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                        SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                        SUM(number_of_casualties) as total_casualties
                    FROM accidents a
                    LEFT JOIN lsoa_boundaries lb ON a.lsoa_code = lb.lsoa_code
                    WHERE a.lsoa_code = :lsoa_code
                    GROUP BY lb.lsoa_name
                """
                result = conn.execute(text(calc_sql), {'lsoa_code': lsoa_code})
                row = result.fetchone()
                
                if not row or row[2] is None:
                    raise HTTPException(status_code=404, detail="LSOA not found or no accidents")
                
                return LSOAStatsResponse(
                    lsoa_code=row[0],
                    lsoa_name=row[1],
                    year=row[2],
                    total_accidents=row[3],
                    fatal_accidents=row[4],
                    serious_accidents=row[5],
                    slight_accidents=row[6],
                    total_casualties=row[7],
                    risk_score=None,
                    risk_category=None
                )
            
            return LSOAStatsResponse(
                lsoa_code=row[0],
                lsoa_name=row[1],
                year=row[2],
                total_accidents=row[3],
                fatal_accidents=row[4],
                serious_accidents=row[5],
                slight_accidents=row[6],
                total_casualties=row[7],
                risk_score=float(row[8]) if row[8] else None,
                risk_category=row[9]
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
