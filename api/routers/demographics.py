"""
Demographics API Endpoints

Casualty demographics analysis including age, gender, and casualty class.
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import time

from ..auth import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)

# Connection pool
_engine = None

def get_db_engine():
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


# Cache
_cache = {}
_cache_ttl = 300  # 5 minutes

def get_cached(key: str):
    if key in _cache:
        value, timestamp = _cache[key]
        if time.time() - timestamp < _cache_ttl:
            return value
    return None

def set_cached(key: str, value: Any):
    _cache[key] = (value, time.time())


# Lookup mappings
SEX_LOOKUP = {
    1: "Male",
    2: "Female",
    -1: "Unknown",
    9: "Unknown"
}

AGE_BAND_LOOKUP = {
    1: "0-5",
    2: "6-10",
    3: "11-15",
    4: "16-20",
    5: "21-25",
    6: "26-35",
    7: "36-45",
    8: "46-55",
    9: "56-65",
    10: "66-75",
    11: "Over 75"
}

CASUALTY_CLASS_LOOKUP = {
    1: "Driver/Rider",
    2: "Passenger",
    3: "Pedestrian"
}

CASUALTY_TYPE_LOOKUP = {
    0: "Pedestrian",
    1: "Cyclist",
    2: "Motorcycle 50cc and under",
    3: "Motorcycle 125cc and under",
    4: "Motorcycle over 125cc",
    5: "Motorcycle over 500cc",
    8: "Taxi/Private hire car occupant",
    9: "Car occupant",
    10: "Minibus occupant",
    11: "Bus or coach occupant",
    16: "Horse rider",
    17: "Agricultural vehicle occupant",
    18: "Tram occupant",
    19: "Van occupant",
    20: "Goods vehicle occupant (3.5-7.5t)",
    21: "Goods vehicle occupant (over 7.5t)",
    22: "Mobility scooter rider",
    23: "Electric motorcycle rider",
    90: "Other vehicle occupant",
    97: "Motorcycle - unknown cc",
    98: "Goods vehicle - unknown weight",
    99: "Unknown vehicle type"
}


@router.get("/summary")
async def get_demographics_summary(
    year: Optional[int] = Query(None, description="Filter by year")
) -> Dict[str, Any]:
    """
    Get summary of casualty demographics.
    """
    cache_key = f"demo_summary_{year or 'all'}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    engine = get_db_engine()
    year_filter = "WHERE c.accident_year = :year" if year else ""
    
    try:
        with engine.connect() as conn:
            # Gender breakdown
            gender_result = conn.execute(
                text(f"""
                    SELECT sex, COUNT(*) as count, 
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious
                    FROM casualties c
                    {year_filter}
                    GROUP BY sex
                    ORDER BY count DESC
                """),
                {"year": year} if year else {}
            )
            gender = []
            total_casualties = 0
            for row in gender_result.fetchall():
                count = row[1]
                total_casualties += count
                gender.append({
                    "code": row[0],
                    "name": SEX_LOOKUP.get(row[0], "Unknown"),
                    "count": count,
                    "fatal": row[2],
                    "serious": row[3]
                })
            
            # Add percentages
            for g in gender:
                g["percentage"] = round(g["count"] / total_casualties * 100, 1) if total_casualties > 0 else 0
            
            # Age breakdown
            age_result = conn.execute(
                text(f"""
                    SELECT age_band, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious
                    FROM casualties c
                    {year_filter}
                    {"AND" if year_filter else "WHERE"} age_band > 0
                    GROUP BY age_band
                    ORDER BY age_band
                """),
                {"year": year} if year else {}
            )
            age_groups = []
            for row in age_result.fetchall():
                count = row[1]
                age_groups.append({
                    "code": row[0],
                    "range": AGE_BAND_LOOKUP.get(row[0], "Unknown"),
                    "count": count,
                    "fatal": row[2],
                    "serious": row[3],
                    "percentage": round(count / total_casualties * 100, 1) if total_casualties > 0 else 0
                })
            
            # Casualty class breakdown
            class_result = conn.execute(
                text(f"""
                    SELECT casualty_class, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious
                    FROM casualties c
                    {year_filter}
                    GROUP BY casualty_class
                    ORDER BY count DESC
                """),
                {"year": year} if year else {}
            )
            casualty_class = []
            for row in class_result.fetchall():
                count = row[1]
                casualty_class.append({
                    "code": row[0],
                    "name": CASUALTY_CLASS_LOOKUP.get(row[0], "Unknown"),
                    "count": count,
                    "fatal": row[2],
                    "serious": row[3],
                    "percentage": round(count / total_casualties * 100, 1) if total_casualties > 0 else 0
                })
            
            result = {
                "year": year,
                "total_casualties": total_casualties,
                "gender": gender,
                "age_groups": age_groups,
                "casualty_class": casualty_class
            }
            
            set_cached(cache_key, result)
            return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/by-gender")
async def get_casualties_by_gender(
    year: Optional[int] = Query(None, description="Filter by year"),
    severity: Optional[int] = Query(None, ge=1, le=3, description="Filter by severity")
) -> List[Dict[str, Any]]:
    """
    Get casualties broken down by gender.
    """
    engine = get_db_engine()
    
    conditions = []
    params = {}
    if year:
        conditions.append("c.accident_year = :year")
        params["year"] = year
    if severity:
        conditions.append("c.severity = :severity")
        params["severity"] = severity
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT sex, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                           SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                           AVG(CASE WHEN age > 0 THEN age END) as avg_age
                    FROM casualties c
                    {where_clause}
                    GROUP BY sex
                    ORDER BY count DESC
                """),
                params
            )
            
            return [
                {
                    "code": row[0],
                    "name": SEX_LOOKUP.get(row[0], "Unknown"),
                    "count": row[1],
                    "fatal": row[2],
                    "serious": row[3],
                    "slight": row[4],
                    "avg_age": round(float(row[5]), 1) if row[5] else None
                }
                for row in result.fetchall()
            ]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/by-age")
async def get_casualties_by_age(
    year: Optional[int] = Query(None, description="Filter by year"),
    gender: Optional[int] = Query(None, ge=1, le=2, description="1=Male, 2=Female"),
    severity: Optional[int] = Query(None, ge=1, le=3, description="Filter by severity")
) -> List[Dict[str, Any]]:
    """
    Get casualties broken down by age band.
    """
    engine = get_db_engine()
    
    conditions = ["age_band > 0"]
    params = {}
    if year:
        conditions.append("c.accident_year = :year")
        params["year"] = year
    if gender:
        conditions.append("c.sex = :gender")
        params["gender"] = gender
    if severity:
        conditions.append("c.severity = :severity")
        params["severity"] = severity
    
    where_clause = "WHERE " + " AND ".join(conditions)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT age_band, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                           SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                           SUM(CASE WHEN sex = 1 THEN 1 ELSE 0 END) as male,
                           SUM(CASE WHEN sex = 2 THEN 1 ELSE 0 END) as female
                    FROM casualties c
                    {where_clause}
                    GROUP BY age_band
                    ORDER BY age_band
                """),
                params
            )
            
            return [
                {
                    "code": row[0],
                    "range": AGE_BAND_LOOKUP.get(row[0], "Unknown"),
                    "count": row[1],
                    "fatal": row[2],
                    "serious": row[3],
                    "slight": row[4],
                    "male": row[5],
                    "female": row[6],
                    "ksi_rate": round((row[2] + row[3]) / row[1] * 100, 1) if row[1] > 0 else 0
                }
                for row in result.fetchall()
            ]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/by-casualty-type")
async def get_casualties_by_type(
    year: Optional[int] = Query(None, description="Filter by year"),
    gender: Optional[int] = Query(None, ge=1, le=2, description="1=Male, 2=Female")
) -> List[Dict[str, Any]]:
    """
    Get casualties broken down by casualty type (pedestrian, cyclist, etc).
    """
    engine = get_db_engine()
    
    conditions = ["casualty_type IS NOT NULL"]
    params = {}
    if year:
        conditions.append("c.accident_year = :year")
        params["year"] = year
    if gender:
        conditions.append("c.sex = :gender")
        params["gender"] = gender
    
    where_clause = "WHERE " + " AND ".join(conditions)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT casualty_type, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                           SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight
                    FROM casualties c
                    {where_clause}
                    GROUP BY casualty_type
                    ORDER BY count DESC
                    LIMIT 15
                """),
                params
            )
            
            return [
                {
                    "code": row[0],
                    "name": CASUALTY_TYPE_LOOKUP.get(row[0], f"Type {row[0]}"),
                    "count": row[1],
                    "fatal": row[2],
                    "serious": row[3],
                    "slight": row[4],
                    "ksi_rate": round((row[2] + row[3]) / row[1] * 100, 1) if row[1] > 0 else 0
                }
                for row in result.fetchall()
            ]
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/children")
async def get_child_casualties(
    year: Optional[int] = Query(None, description="Filter by year")
) -> Dict[str, Any]:
    """
    Get statistics specifically for child casualties (under 16).
    """
    engine = get_db_engine()
    year_filter = "AND c.accident_year = :year" if year else ""
    
    try:
        with engine.connect() as conn:
            # Overall child stats
            overall = conn.execute(
                text(f"""
                    SELECT COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious,
                           SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as slight,
                           SUM(CASE WHEN sex = 1 THEN 1 ELSE 0 END) as male,
                           SUM(CASE WHEN sex = 2 THEN 1 ELSE 0 END) as female,
                           AVG(age) as avg_age
                    FROM casualties c
                    WHERE age > 0 AND age < 16
                    {year_filter}
                """),
                {"year": year} if year else {}
            )
            row = overall.fetchone()
            
            # By age band (children only)
            by_age = conn.execute(
                text(f"""
                    SELECT age_band, COUNT(*) as count,
                           SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal,
                           SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious
                    FROM casualties c
                    WHERE age_band IN (1, 2, 3)
                    {year_filter}
                    GROUP BY age_band
                    ORDER BY age_band
                """),
                {"year": year} if year else {}
            )
            age_breakdown = [
                {
                    "range": AGE_BAND_LOOKUP.get(r[0], "Unknown"),
                    "count": r[1],
                    "fatal": r[2],
                    "serious": r[3]
                }
                for r in by_age.fetchall()
            ]
            
            # By casualty class
            by_class = conn.execute(
                text(f"""
                    SELECT casualty_class, COUNT(*) as count
                    FROM casualties c
                    WHERE age > 0 AND age < 16
                    {year_filter}
                    GROUP BY casualty_class
                    ORDER BY count DESC
                """),
                {"year": year} if year else {}
            )
            class_breakdown = [
                {
                    "class": CASUALTY_CLASS_LOOKUP.get(r[0], "Unknown"),
                    "count": r[1]
                }
                for r in by_class.fetchall()
            ]
            
            return {
                "year": year,
                "total_children": row[0] if row else 0,
                "fatal": row[1] if row else 0,
                "serious": row[2] if row else 0,
                "slight": row[3] if row else 0,
                "male": row[4] if row else 0,
                "female": row[5] if row else 0,
                "avg_age": round(float(row[6]), 1) if row and row[6] else None,
                "by_age": age_breakdown,
                "by_class": class_breakdown
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/trends")
async def get_demographic_trends(
    start_year: int = Query(2019, description="Start year"),
    end_year: int = Query(2023, description="End year")
) -> Dict[str, Any]:
    """
    Get demographic trends over time.
    """
    engine = get_db_engine()
    
    try:
        with engine.connect() as conn:
            # Gender trends
            gender_result = conn.execute(
                text("""
                    SELECT accident_year, sex, COUNT(*) as count
                    FROM casualties
                    WHERE accident_year BETWEEN :start_year AND :end_year
                    AND sex IN (1, 2)
                    GROUP BY accident_year, sex
                    ORDER BY accident_year, sex
                """),
                {"start_year": start_year, "end_year": end_year}
            )
            
            gender_trends = {}
            for row in gender_result.fetchall():
                year = row[0]
                if year not in gender_trends:
                    gender_trends[year] = {"year": year, "male": 0, "female": 0}
                if row[1] == 1:
                    gender_trends[year]["male"] = row[2]
                else:
                    gender_trends[year]["female"] = row[2]
            
            # Age group trends
            age_result = conn.execute(
                text("""
                    SELECT accident_year,
                           SUM(CASE WHEN age < 16 THEN 1 ELSE 0 END) as children,
                           SUM(CASE WHEN age >= 16 AND age < 25 THEN 1 ELSE 0 END) as young_adults,
                           SUM(CASE WHEN age >= 25 AND age < 65 THEN 1 ELSE 0 END) as adults,
                           SUM(CASE WHEN age >= 65 THEN 1 ELSE 0 END) as elderly
                    FROM casualties
                    WHERE accident_year BETWEEN :start_year AND :end_year
                    AND age > 0
                    GROUP BY accident_year
                    ORDER BY accident_year
                """),
                {"start_year": start_year, "end_year": end_year}
            )
            
            age_trends = [
                {
                    "year": row[0],
                    "children_under_16": row[1],
                    "young_adults_16_24": row[2],
                    "adults_25_64": row[3],
                    "elderly_65_plus": row[4]
                }
                for row in age_result.fetchall()
            ]
            
            return {
                "period": f"{start_year}-{end_year}",
                "gender_trends": list(gender_trends.values()),
                "age_trends": age_trends
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
