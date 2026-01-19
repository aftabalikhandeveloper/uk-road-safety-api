-- Risk Scoring Functions and Tables for UK Road Safety Platform
-- Run this with: psql -f scripts/risk_scoring.sql

-- Function to calculate LSOA risk score
CREATE OR REPLACE FUNCTION calculate_lsoa_risk_score(
    fatal_count INT,
    serious_count INT,
    slight_count INT,
    total_accidents INT,
    population INT DEFAULT NULL
) RETURNS DECIMAL AS $$
DECLARE
    weighted_casualties DECIMAL;
    risk_score DECIMAL;
BEGIN
    -- Weighted casualties: Fatal=10, Serious=3, Slight=1
    weighted_casualties := (COALESCE(fatal_count, 0) * 10) + 
                          (COALESCE(serious_count, 0) * 3) + 
                          COALESCE(slight_count, 0);
    
    -- Base risk score
    risk_score := weighted_casualties;
    
    -- Normalize by population if available (per 10,000 people)
    IF population IS NOT NULL AND population > 0 THEN
        risk_score := (weighted_casualties / population) * 10000;
    END IF;
    
    RETURN ROUND(risk_score, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Severity category function
CREATE OR REPLACE FUNCTION get_risk_category(risk_score DECIMAL)
RETURNS VARCHAR AS $$
BEGIN
    RETURN CASE
        WHEN risk_score >= 50 THEN 'Very High'
        WHEN risk_score >= 25 THEN 'High'
        WHEN risk_score >= 10 THEN 'Medium'
        WHEN risk_score >= 5 THEN 'Low'
        ELSE 'Very Low'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- LSOA risk scores table
CREATE TABLE IF NOT EXISTS lsoa_risk_scores (
    lsoa_code VARCHAR(15) PRIMARY KEY,
    total_accidents INT DEFAULT 0,
    fatal_accidents INT DEFAULT 0,
    serious_accidents INT DEFAULT 0,
    slight_accidents INT DEFAULT 0,
    total_casualties INT DEFAULT 0,
    fatal_casualties INT DEFAULT 0,
    serious_casualties INT DEFAULT 0,
    slight_casualties INT DEFAULT 0,
    risk_score DECIMAL(10,2),
    risk_category VARCHAR(20),
    accidents_per_year DECIMAL(10,2),
    last_accident_date DATE,
    year_calculated INT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Function to refresh LSOA risk scores
CREATE OR REPLACE FUNCTION refresh_lsoa_risk_scores()
RETURNS INT AS $$
DECLARE
    rows_affected INT;
BEGIN
    -- Clear existing scores
    TRUNCATE TABLE lsoa_risk_scores;
    
    -- Calculate and insert new scores
    INSERT INTO lsoa_risk_scores (
        lsoa_code,
        total_accidents,
        fatal_accidents,
        serious_accidents,
        slight_accidents,
        total_casualties,
        fatal_casualties,
        serious_casualties,
        slight_casualties,
        risk_score,
        risk_category,
        accidents_per_year,
        last_accident_date,
        year_calculated
    )
    SELECT 
        a.lsoa_code,
        COUNT(*)::INT as total_accidents,
        SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END)::INT as fatal_accidents,
        SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END)::INT as serious_accidents,
        SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END)::INT as slight_accidents,
        COALESCE(SUM(a.number_of_casualties), 0)::INT as total_casualties,
        -- Note: Casualty severity breakdown would require joining casualties table
        0 as fatal_casualties,
        0 as serious_casualties,
        0 as slight_casualties,
        calculate_lsoa_risk_score(
            SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END)::INT,
            SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END)::INT,
            SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END)::INT,
            COUNT(*)::INT,
            NULL  -- No population data
        ) as risk_score,
        get_risk_category(
            calculate_lsoa_risk_score(
                SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END)::INT,
                SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END)::INT,
                SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END)::INT,
                COUNT(*)::INT,
                NULL
            )
        ) as risk_category,
        ROUND(COUNT(*)::DECIMAL / NULLIF(MAX(a.accident_year) - MIN(a.accident_year) + 1, 0), 2) as accidents_per_year,
        MAX(a.accident_date) as last_accident_date,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT as year_calculated
    FROM accidents a
    WHERE a.lsoa_code IS NOT NULL AND a.lsoa_code != ''
    GROUP BY a.lsoa_code;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected;
END;
$$ LANGUAGE plpgsql;

-- Hotspot identification view
CREATE OR REPLACE VIEW v_accident_hotspots AS
WITH accident_clusters AS (
    SELECT 
        lsoa_code,
        COUNT(*) as accident_count,
        SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal_count,
        SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious_count,
        SUM(number_of_casualties) as total_casualties,
        AVG(latitude) as center_lat,
        AVG(longitude) as center_lon
    FROM accidents
    WHERE lsoa_code IS NOT NULL
      AND accident_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
    GROUP BY lsoa_code
    HAVING COUNT(*) >= 10  -- Minimum accidents to be a hotspot
)
SELECT 
    ac.*,
    calculate_lsoa_risk_score(ac.fatal_count::INT, ac.serious_count::INT, 
        (ac.accident_count - ac.fatal_count - ac.serious_count)::INT, 
        ac.accident_count::INT, NULL) as risk_score,
    lb.lsoa_name,
    lb.geom
FROM accident_clusters ac
LEFT JOIN lsoa_boundaries lb ON ac.lsoa_code = lb.lsoa_code
ORDER BY risk_score DESC;

-- School proximity risk view
CREATE OR REPLACE VIEW v_school_proximity_accidents AS
SELECT 
    s.urn as school_urn,
    s.name as school_name,
    s.phase_of_education,
    s.postcode,
    COUNT(DISTINCT a.accident_id) as accidents_within_500m,
    SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as fatal_accidents,
    SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as serious_accidents,
    SUM(a.number_of_casualties) as total_casualties,
    calculate_lsoa_risk_score(
        SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END)::INT,
        SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END)::INT,
        SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END)::INT,
        COUNT(DISTINCT a.accident_id)::INT,
        NULL
    ) as proximity_risk_score
FROM schools s
LEFT JOIN accidents a ON ST_DWithin(s.geom::geography, a.geom::geography, 500)
WHERE s.geom IS NOT NULL
GROUP BY s.urn, s.name, s.phase_of_education, s.postcode
HAVING COUNT(DISTINCT a.accident_id) > 0
ORDER BY proximity_risk_score DESC;

-- Time-based risk patterns view
CREATE OR REPLACE VIEW v_temporal_risk_patterns AS
SELECT 
    EXTRACT(HOUR FROM accident_time) as hour_of_day,
    day_of_week,
    CASE 
        WHEN EXTRACT(MONTH FROM accident_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM accident_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM accident_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END as season,
    COUNT(*) as accident_count,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal_count,
    SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious_count,
    SUM(number_of_casualties) as total_casualties,
    ROUND(100.0 * SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as fatal_rate_pct
FROM accidents
WHERE accident_time IS NOT NULL
GROUP BY 
    EXTRACT(HOUR FROM accident_time),
    day_of_week,
    CASE 
        WHEN EXTRACT(MONTH FROM accident_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM accident_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM accident_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END
ORDER BY accident_count DESC;

-- Road condition risk analysis
CREATE OR REPLACE VIEW v_road_condition_risk AS
SELECT 
    road_surface_conditions,
    weather_conditions,
    light_conditions,
    COUNT(*) as accident_count,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as fatal_count,
    SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as serious_count,
    ROUND(100.0 * SUM(CASE WHEN severity <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as ksi_rate_pct,
    calculate_lsoa_risk_score(
        SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END)::INT,
        SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END)::INT,
        SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END)::INT,
        COUNT(*)::INT,
        NULL
    ) as condition_risk_score
FROM accidents
GROUP BY road_surface_conditions, weather_conditions, light_conditions
HAVING COUNT(*) >= 100
ORDER BY condition_risk_score DESC;
