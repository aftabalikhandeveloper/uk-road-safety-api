"""
Schools API endpoints for UK Road Safety Platform.
Provides school data with nearby accident analysis.
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from datetime import date
import os
from ..auth import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:pass@localhost:5432/roadsafety"
)


class School(BaseModel):
    urn: int
    name: str
    establishment_type: Optional[str]
    establishment_type_group: Optional[str]
    phase_of_education: Optional[str]
    statutory_low_age: Optional[int]
    statutory_high_age: Optional[int]
    street: Optional[str]
    locality: Optional[str]
    town: Optional[str]
    county: Optional[str]
    postcode: Optional[str]
    latitude: float
    longitude: float
    local_authority_name: Optional[str]
    number_of_pupils: Optional[int]
    establishment_status: Optional[str]


class SchoolWithAccidents(School):
    accident_count: int = 0
    fatal_count: int = 0
    serious_count: int = 0
    slight_count: int = 0


class NearbyAccident(BaseModel):
    accident_id: str
    accident_date: date
    severity: int
    latitude: float
    longitude: float
    number_of_casualties: int
    distance_meters: float


class SchoolDetail(SchoolWithAccidents):
    accidents: List[NearbyAccident] = []


class SchoolsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: List[SchoolWithAccidents]


def get_db_connection():
    return create_engine(DATABASE_URL)


def meters_to_degrees(meters: float) -> float:
    """
    Convert meters to approximate degrees for UK latitude (~52°).
    At this latitude, 1 degree ≈ 111km, so we use a conservative factor.
    This is an approximation that works well for small distances in the UK.
    """
    return meters / 111000.0


@router.get("", response_model=SchoolsResponse)
async def get_schools(
    search: Optional[str] = Query(None, description="Search by school name"),
    phase: Optional[str] = Query(None, description="Filter by phase of education (Primary, Secondary, etc.)"),
    local_authority: Optional[str] = Query(None, description="Filter by local authority name"),
    town: Optional[str] = Query(None, description="Filter by town"),
    county: Optional[str] = Query(None, description="Filter by county"),
    radius: int = Query(500, description="Radius in meters to search for nearby accidents"),
    year: Optional[int] = Query(None, description="Year to filter accidents"),
    order_by: str = Query("name", description="Order by: name, number_of_pupils"),
    order_dir: str = Query("asc", description="Order direction: asc, desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200)
):
    """
    Get schools with nearby accident counts.
    For performance, uses geometry-based spatial query (faster than geography).
    Accident counts are computed only for the returned page of schools.
    """
    engine = get_db_connection()
    
    # Convert radius from meters to degrees for faster geometry-based query
    radius_degrees = meters_to_degrees(radius)
    
    # Build WHERE conditions for schools only (fast query)
    conditions = ["s.latitude IS NOT NULL", "s.longitude IS NOT NULL"]
    params = {'offset': (page - 1) * page_size, 'limit': page_size, 'radius_deg': radius_degrees}
    
    if search:
        conditions.append("LOWER(s.name) LIKE LOWER(:search)")
        params['search'] = f"%{search}%"
    
    if phase:
        conditions.append("s.phase_of_education = :phase")
        params['phase'] = phase
    
    if local_authority:
        conditions.append("LOWER(s.local_authority_name) LIKE LOWER(:local_authority)")
        params['local_authority'] = f"%{local_authority}%"
    
    if town:
        conditions.append("LOWER(s.town) LIKE LOWER(:town)")
        params['town'] = f"%{town}%"
    
    if county:
        conditions.append("LOWER(s.county) LIKE LOWER(:county)")
        params['county'] = f"%{county}%"
    
    where_clause = " AND ".join(conditions)
    year_filter = f"AND a.accident_year = {year}" if year else ""
    
    # Order by validation
    valid_order_cols = ['name', 'number_of_pupils', 'town', 'phase_of_education']
    if order_by not in valid_order_cols:
        order_by = 'name'
    order_dir = 'DESC' if order_dir.lower() == 'desc' else 'ASC'
    
    # Count query (fast - no spatial join)
    count_sql = f"""
        SELECT COUNT(*) 
        FROM schools s
        WHERE {where_clause}
    """
    
    # Main query - get schools first, then compute accident counts only for those schools
    # Uses geometry-based ST_DWithin (degrees) for much faster performance
    sql = f"""
        WITH filtered_schools AS (
            SELECT 
                s.urn, s.name, s.establishment_type, s.establishment_type_group,
                s.phase_of_education, s.statutory_low_age, s.statutory_high_age,
                s.street, s.locality, s.town, s.county, s.postcode,
                s.latitude, s.longitude, s.local_authority_name, s.number_of_pupils,
                s.establishment_status, s.geom
            FROM schools s
            WHERE {where_clause}
            ORDER BY {order_by} {order_dir}
            OFFSET :offset
            LIMIT :limit
        )
        SELECT 
            fs.urn, fs.name, fs.establishment_type, fs.establishment_type_group,
            fs.phase_of_education, fs.statutory_low_age, fs.statutory_high_age,
            fs.street, fs.locality, fs.town, fs.county, fs.postcode,
            fs.latitude, fs.longitude, fs.local_authority_name, fs.number_of_pupils,
            fs.establishment_status,
            COUNT(a.accident_id) as accident_count,
            SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as fatal_count,
            SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as serious_count,
            SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END) as slight_count
        FROM filtered_schools fs
        LEFT JOIN accidents a ON ST_DWithin(fs.geom, a.geom, :radius_deg) {year_filter}
        GROUP BY fs.urn, fs.name, fs.establishment_type, fs.establishment_type_group,
                 fs.phase_of_education, fs.statutory_low_age, fs.statutory_high_age,
                 fs.street, fs.locality, fs.town, fs.county, fs.postcode,
                 fs.latitude, fs.longitude, fs.local_authority_name, fs.number_of_pupils,
                 fs.establishment_status
        ORDER BY accident_count DESC
    """
    
    try:
        with engine.connect() as conn:
            # Get total count
            count_result = conn.execute(text(count_sql), params)
            total = count_result.scalar() or 0
            
            # Get data
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            
            data = [
                SchoolWithAccidents(
                    urn=row[0],
                    name=row[1],
                    establishment_type=row[2],
                    establishment_type_group=row[3],
                    phase_of_education=row[4],
                    statutory_low_age=int(row[5]) if row[5] else None,
                    statutory_high_age=int(row[6]) if row[6] else None,
                    street=row[7],
                    locality=row[8],
                    town=row[9],
                    county=row[10],
                    postcode=row[11],
                    latitude=row[12],
                    longitude=row[13],
                    local_authority_name=row[14],
                    number_of_pupils=int(row[15]) if row[15] else None,
                    establishment_status=row[16],
                    accident_count=row[17] or 0,
                    fatal_count=row[18] or 0,
                    serious_count=row[19] or 0,
                    slight_count=row[20] or 0
                )
                for row in rows
            ]
            
            return SchoolsResponse(
                total=total,
                page=page,
                page_size=page_size,
                data=data
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/phases", response_model=List[str])
async def get_school_phases():
    """Get list of school phases for filtering."""
    engine = get_db_connection()
    
    sql = """
        SELECT DISTINCT phase_of_education 
        FROM schools 
        WHERE phase_of_education IS NOT NULL 
        ORDER BY phase_of_education
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/local-authorities", response_model=List[str])
async def get_local_authorities():
    """Get list of local authorities for filtering."""
    engine = get_db_connection()
    
    sql = """
        SELECT DISTINCT local_authority_name 
        FROM schools 
        WHERE local_authority_name IS NOT NULL 
        ORDER BY local_authority_name
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/counties", response_model=List[str])
async def get_counties():
    """Get list of counties for filtering."""
    engine = get_db_connection()
    
    sql = """
        SELECT DISTINCT county 
        FROM schools 
        WHERE county IS NOT NULL AND county != ''
        ORDER BY county
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_schools_summary(
    radius: int = Query(500, description="Radius in meters"),
    year: Optional[int] = Query(None, description="Year filter")
):
    """
    Get summary statistics for school safety.
    Uses sampling for fast approximate results with geometry-based spatial query.
    """
    engine = get_db_connection()
    
    # Convert radius from meters to degrees
    radius_degrees = meters_to_degrees(radius)
    
    # Fast count of schools
    schools_count_sql = """
        SELECT COUNT(*) FROM schools WHERE latitude IS NOT NULL
    """
    
    # Sample-based estimate for accident counts (much faster)
    # Using geometry-based ST_DWithin for better performance
    year_filter = f"AND a.accident_year = {year}" if year else ""
    
    sample_sql = f"""
        WITH sampled_schools AS (
            SELECT urn, geom
            FROM schools 
            WHERE latitude IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 500
        ),
        school_accidents AS (
            SELECT 
                ss.urn,
                COUNT(a.accident_id) as accident_count,
                SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as fatal_count,
                SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as serious_count
            FROM sampled_schools ss
            LEFT JOIN accidents a ON ST_DWithin(ss.geom, a.geom, :radius_deg) {year_filter}
            GROUP BY ss.urn
        )
        SELECT 
            COUNT(*) as sample_size,
            AVG(accident_count) as avg_accidents,
            AVG(fatal_count) as avg_fatal,
            AVG(serious_count) as avg_serious,
            SUM(CASE WHEN accident_count >= 10 THEN 1 ELSE 0 END) as high_risk_sample,
            SUM(CASE WHEN accident_count >= 5 AND accident_count < 10 THEN 1 ELSE 0 END) as medium_risk_sample,
            SUM(CASE WHEN accident_count < 5 THEN 1 ELSE 0 END) as low_risk_sample
        FROM school_accidents
    """
    
    try:
        with engine.connect() as conn:
            # Get total schools count
            total_result = conn.execute(text(schools_count_sql))
            total_schools = total_result.scalar() or 0
            
            # Get sample statistics
            sample_result = conn.execute(text(sample_sql), {'radius_deg': radius_degrees})
            row = sample_result.fetchone()
            
            if row and row[0] > 0:
                sample_size = row[0]
                scale_factor = total_schools / sample_size
                
                return {
                    "total_schools": total_schools,
                    "total_accidents": int((row[1] or 0) * total_schools),
                    "total_fatal": int((row[2] or 0) * total_schools),
                    "total_serious": int((row[3] or 0) * total_schools),
                    "high_risk_count": int((row[4] or 0) * scale_factor),
                    "medium_risk_count": int((row[5] or 0) * scale_factor),
                    "low_risk_count": int((row[6] or 0) * scale_factor),
                    "radius_meters": radius,
                    "year": year,
                    "note": "Estimates based on sampling"
                }
            else:
                return {
                    "total_schools": total_schools,
                    "total_accidents": 0,
                    "total_fatal": 0,
                    "total_serious": 0,
                    "high_risk_count": 0,
                    "medium_risk_count": 0,
                    "low_risk_count": total_schools,
                    "radius_meters": radius,
                    "year": year
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{urn}", response_model=SchoolDetail)
async def get_school_detail(
    urn: int,
    radius: int = Query(500, description="Radius in meters"),
    year: Optional[int] = Query(None, description="Year filter"),
    limit: int = Query(100, description="Max accidents to return")
):
    """Get detailed school info with nearby accidents."""
    engine = get_db_connection()
    
    # Convert radius to degrees for fast geometry pre-filter
    # Use slightly larger degree buffer for initial filter, then exact distance check
    radius_degrees = meters_to_degrees(radius * 1.2)  # 20% buffer for safety
    year_filter = f"AND a.accident_year = {year}" if year else ""
    
    # Get school info
    school_sql = """
        SELECT 
            urn, name, establishment_type, establishment_type_group,
            phase_of_education, statutory_low_age, statutory_high_age,
            street, locality, town, county, postcode,
            latitude, longitude, local_authority_name, number_of_pupils,
            establishment_status
        FROM schools
        WHERE urn = :urn
    """
    
    # Get nearby accidents - use geometry pre-filter then compute exact distance
    accidents_sql = f"""
        WITH school_geom AS (
            SELECT geom FROM schools WHERE urn = :urn
        ),
        nearby AS (
            SELECT 
                a.accident_id,
                a.accident_date,
                a.severity,
                a.latitude,
                a.longitude,
                a.number_of_casualties,
                ST_Distance(s.geom::geography, a.geom::geography) as distance_meters
            FROM school_geom s
            JOIN accidents a ON ST_DWithin(s.geom, a.geom, :radius_deg) {year_filter}
        )
        SELECT * FROM nearby
        WHERE distance_meters <= :radius
        ORDER BY distance_meters
        LIMIT :limit
    """
    
    try:
        with engine.connect() as conn:
            # Get school
            school_result = conn.execute(text(school_sql), {'urn': urn})
            school_row = school_result.fetchone()
            
            if not school_row:
                raise HTTPException(status_code=404, detail="School not found")
            
            # Get accidents
            acc_result = conn.execute(text(accidents_sql), {
                'urn': urn,
                'radius_deg': radius_degrees,
                'radius': radius,
                'limit': limit
            })
            accident_rows = acc_result.fetchall()
            
            accidents = [
                NearbyAccident(
                    accident_id=row[0],
                    accident_date=row[1],
                    severity=row[2],
                    latitude=row[3],
                    longitude=row[4],
                    number_of_casualties=row[5],
                    distance_meters=row[6]
                )
                for row in accident_rows
            ]
            
            # Count by severity
            fatal_count = sum(1 for a in accidents if a.severity == 1)
            serious_count = sum(1 for a in accidents if a.severity == 2)
            slight_count = sum(1 for a in accidents if a.severity == 3)
            
            return SchoolDetail(
                urn=school_row[0],
                name=school_row[1],
                establishment_type=school_row[2],
                establishment_type_group=school_row[3],
                phase_of_education=school_row[4],
                statutory_low_age=int(school_row[5]) if school_row[5] else None,
                statutory_high_age=int(school_row[6]) if school_row[6] else None,
                street=school_row[7],
                locality=school_row[8],
                town=school_row[9],
                county=school_row[10],
                postcode=school_row[11],
                latitude=school_row[12],
                longitude=school_row[13],
                local_authority_name=school_row[14],
                number_of_pupils=int(school_row[15]) if school_row[15] else None,
                establishment_status=school_row[16],
                accident_count=len(accidents),
                fatal_count=fatal_count,
                serious_count=serious_count,
                slight_count=slight_count,
                accidents=accidents
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
