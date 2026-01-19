-- ============================================
-- UK Road Safety Database Schema
-- Version: 1.0
-- ============================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For text search

-- ============================================
-- LOOKUP TABLES
-- ============================================

-- Severity lookup
CREATE TABLE lookup_severity (
    code INT PRIMARY KEY,
    description VARCHAR(50) NOT NULL
);

INSERT INTO lookup_severity (code, description) VALUES
    (1, 'Fatal'),
    (2, 'Serious'),
    (3, 'Slight');

-- Vehicle type lookup
CREATE TABLE lookup_vehicle_type (
    code INT PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO lookup_vehicle_type (code, description) VALUES
    (1, 'Pedal cycle'),
    (2, 'Motorcycle 50cc and under'),
    (3, 'Motorcycle 125cc and under'),
    (4, 'Motorcycle over 125cc and up to 500cc'),
    (5, 'Motorcycle over 500cc'),
    (8, 'Taxi/Private hire car'),
    (9, 'Car'),
    (10, 'Minibus (8-16 passengers)'),
    (11, 'Bus or coach (17+ passengers)'),
    (16, 'Ridden horse'),
    (17, 'Agricultural vehicle'),
    (18, 'Tram'),
    (19, 'Van/Goods 3.5t mgw or under'),
    (20, 'Goods over 3.5t and under 7.5t'),
    (21, 'Goods 7.5t mgw and over'),
    (22, 'Mobility scooter'),
    (23, 'Electric motorcycle'),
    (90, 'Other vehicle'),
    (97, 'Motorcycle - unknown cc'),
    (98, 'Goods vehicle - unknown weight'),
    (-1, 'Data missing');

-- Road type lookup
CREATE TABLE lookup_road_type (
    code INT PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO lookup_road_type (code, description) VALUES
    (1, 'Roundabout'),
    (2, 'One way street'),
    (3, 'Dual carriageway'),
    (6, 'Single carriageway'),
    (7, 'Slip road'),
    (9, 'Unknown'),
    (12, 'One way street/Slip road'),
    (-1, 'Data missing');

-- Weather conditions lookup
CREATE TABLE lookup_weather (
    code INT PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO lookup_weather (code, description) VALUES
    (1, 'Fine no high winds'),
    (2, 'Raining no high winds'),
    (3, 'Snowing no high winds'),
    (4, 'Fine + high winds'),
    (5, 'Raining + high winds'),
    (6, 'Snowing + high winds'),
    (7, 'Fog or mist'),
    (8, 'Other'),
    (9, 'Unknown'),
    (-1, 'Data missing');

-- Light conditions lookup
CREATE TABLE lookup_light_conditions (
    code INT PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO lookup_light_conditions (code, description) VALUES
    (1, 'Daylight'),
    (4, 'Darkness - lights lit'),
    (5, 'Darkness - lights unlit'),
    (6, 'Darkness - no lighting'),
    (7, 'Darkness - lighting unknown'),
    (-1, 'Data missing');

-- Road surface lookup
CREATE TABLE lookup_road_surface (
    code INT PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO lookup_road_surface (code, description) VALUES
    (1, 'Dry'),
    (2, 'Wet or damp'),
    (3, 'Snow'),
    (4, 'Frost or ice'),
    (5, 'Flood over 3cm deep'),
    (6, 'Oil or diesel'),
    (7, 'Mud'),
    (9, 'Unknown'),
    (-1, 'Data missing');

-- Police force lookup
CREATE TABLE lookup_police_force (
    code INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

INSERT INTO lookup_police_force (code, name) VALUES
    (1, 'Metropolitan Police'),
    (3, 'Cumbria'),
    (4, 'Lancashire'),
    (5, 'Merseyside'),
    (6, 'Greater Manchester'),
    (7, 'Cheshire'),
    (10, 'Northumbria'),
    (11, 'Durham'),
    (12, 'North Yorkshire'),
    (13, 'West Yorkshire'),
    (14, 'South Yorkshire'),
    (16, 'Humberside'),
    (17, 'Cleveland'),
    (20, 'West Midlands'),
    (21, 'Staffordshire'),
    (22, 'West Mercia'),
    (23, 'Warwickshire'),
    (30, 'Derbyshire'),
    (31, 'Nottinghamshire'),
    (32, 'Lincolnshire'),
    (33, 'Leicestershire'),
    (34, 'Northamptonshire'),
    (35, 'Cambridgeshire'),
    (36, 'Norfolk'),
    (37, 'Suffolk'),
    (40, 'Bedfordshire'),
    (41, 'Hertfordshire'),
    (42, 'Essex'),
    (43, 'Thames Valley'),
    (44, 'Hampshire'),
    (45, 'Surrey'),
    (46, 'Kent'),
    (47, 'Sussex'),
    (48, 'City of London'),
    (50, 'Devon and Cornwall'),
    (52, 'Avon and Somerset'),
    (53, 'Gloucestershire'),
    (54, 'Wiltshire'),
    (55, 'Dorset'),
    (60, 'North Wales'),
    (61, 'Gwent'),
    (62, 'South Wales'),
    (63, 'Dyfed-Powys'),
    (91, 'Northern'),
    (92, 'Grampian'),
    (93, 'Tayside'),
    (94, 'Fife'),
    (95, 'Lothian and Borders'),
    (96, 'Central'),
    (97, 'Strathclyde'),
    (98, 'Dumfries and Galloway'),
    (99, 'Police Scotland');

-- Day of week lookup
CREATE TABLE lookup_day_of_week (
    code INT PRIMARY KEY,
    name VARCHAR(20) NOT NULL
);

INSERT INTO lookup_day_of_week (code, name) VALUES
    (1, 'Sunday'),
    (2, 'Monday'),
    (3, 'Tuesday'),
    (4, 'Wednesday'),
    (5, 'Thursday'),
    (6, 'Friday'),
    (7, 'Saturday');

-- ============================================
-- CORE TABLES
-- ============================================

-- Main accidents table (will be partitioned by year)
CREATE TABLE accidents (
    accident_id VARCHAR(20) PRIMARY KEY,
    accident_year INT NOT NULL,
    accident_date DATE NOT NULL,
    accident_time TIME,
    day_of_week INT,
    
    -- Location
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    location_easting INT,
    location_northing INT,
    geom GEOMETRY(Point, 4326),
    lsoa_code VARCHAR(15),
    police_force INT,
    local_authority_district VARCHAR(10),
    local_authority_highway VARCHAR(10),
    
    -- Accident details
    severity INT,
    number_of_vehicles INT,
    number_of_casualties INT,
    
    -- Road characteristics
    first_road_class INT,
    first_road_number INT,
    road_type INT,
    speed_limit INT,
    junction_detail INT,
    junction_control INT,
    second_road_class INT,
    second_road_number INT,
    
    -- Conditions
    pedestrian_crossing_human INT,
    pedestrian_crossing_physical INT,
    light_conditions INT,
    weather_conditions INT,
    road_surface_conditions INT,
    special_conditions_at_site INT,
    carriageway_hazards INT,
    
    -- Other
    urban_or_rural INT,
    police_attended INT,
    trunk_road_flag INT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes on accidents
CREATE INDEX idx_accidents_geom ON accidents USING GIST (geom);
CREATE INDEX idx_accidents_lsoa ON accidents (lsoa_code);
CREATE INDEX idx_accidents_date ON accidents (accident_date);
CREATE INDEX idx_accidents_year ON accidents (accident_year);
CREATE INDEX idx_accidents_severity ON accidents (severity);
CREATE INDEX idx_accidents_police_force ON accidents (police_force);
CREATE INDEX idx_accidents_time ON accidents (accident_time);

-- Casualties table
CREATE TABLE casualties (
    casualty_id SERIAL PRIMARY KEY,
    accident_id VARCHAR(20) REFERENCES accidents(accident_id) ON DELETE CASCADE,
    accident_year INT,
    vehicle_reference INT,
    casualty_reference INT,
    casualty_class INT,
    sex INT,
    age INT,
    age_band INT,
    severity INT,
    pedestrian_location INT,
    pedestrian_movement INT,
    car_passenger INT,
    bus_or_coach_passenger INT,
    pedestrian_road_maintenance_worker INT,
    casualty_type INT,
    casualty_home_area_type INT,
    casualty_imd_decile INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_casualties_accident ON casualties (accident_id);
CREATE INDEX idx_casualties_severity ON casualties (severity);
CREATE INDEX idx_casualties_type ON casualties (casualty_type);

-- Vehicles table
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    accident_id VARCHAR(20) REFERENCES accidents(accident_id) ON DELETE CASCADE,
    accident_year INT,
    vehicle_reference INT,
    vehicle_type INT,
    towing_and_articulation INT,
    vehicle_manoeuvre INT,
    vehicle_direction_from INT,
    vehicle_direction_to INT,
    vehicle_location_restricted_lane INT,
    junction_location INT,
    skidding_and_overturning INT,
    hit_object_in_carriageway INT,
    vehicle_leaving_carriageway INT,
    hit_object_off_carriageway INT,
    first_point_of_impact INT,
    vehicle_left_hand_drive INT,
    journey_purpose INT,
    sex_of_driver INT,
    age_of_driver INT,
    age_band_of_driver INT,
    engine_capacity_cc INT,
    propulsion_code INT,
    age_of_vehicle INT,
    generic_make_model VARCHAR(100),
    driver_imd_decile INT,
    driver_home_area_type INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_vehicles_accident ON vehicles (accident_id);
CREATE INDEX idx_vehicles_type ON vehicles (vehicle_type);

-- ============================================
-- GEOGRAPHIC TABLES
-- ============================================

-- LSOA boundaries
CREATE TABLE lsoa_boundaries (
    lsoa_code VARCHAR(15) PRIMARY KEY,
    lsoa_name VARCHAR(100),
    lsoa_name_welsh VARCHAR(100),
    local_authority_code VARCHAR(10),
    local_authority_name VARCHAR(100),
    region_code VARCHAR(10),
    region_name VARCHAR(50),
    area_hectares DECIMAL(12, 2),
    population INT,
    geom GEOMETRY(MultiPolygon, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_lsoa_geom ON lsoa_boundaries USING GIST (geom);
CREATE INDEX idx_lsoa_la ON lsoa_boundaries (local_authority_code);

-- Police force boundaries
CREATE TABLE police_force_boundaries (
    police_force_code INT PRIMARY KEY,
    police_force_name VARCHAR(100),
    geom GEOMETRY(MultiPolygon, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_police_boundaries_geom ON police_force_boundaries USING GIST (geom);

-- Local authority boundaries
CREATE TABLE local_authority_boundaries (
    la_code VARCHAR(10) PRIMARY KEY,
    la_name VARCHAR(100),
    region_code VARCHAR(10),
    region_name VARCHAR(50),
    geom GEOMETRY(MultiPolygon, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_la_boundaries_geom ON local_authority_boundaries USING GIST (geom);

-- ============================================
-- ENRICHMENT TABLES
-- ============================================

-- Traffic count points
CREATE TABLE traffic_count_points (
    count_point_id INT PRIMARY KEY,
    road_name VARCHAR(100),
    road_category VARCHAR(10),
    road_type VARCHAR(50),
    start_junction_road_name VARCHAR(100),
    end_junction_road_name VARCHAR(100),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    local_authority_code VARCHAR(10),
    local_authority_name VARCHAR(100),
    region_name VARCHAR(50),
    link_length_km DECIMAL(6, 3),
    link_length_miles DECIMAL(6, 3),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_traffic_points_geom ON traffic_count_points USING GIST (geom);

-- Annual Average Daily Flow
CREATE TABLE traffic_aadf (
    id SERIAL PRIMARY KEY,
    count_point_id INT REFERENCES traffic_count_points(count_point_id),
    year INT NOT NULL,
    all_motor_vehicles INT,
    pedal_cycles INT,
    two_wheeled_motor_vehicles INT,
    cars_and_taxis INT,
    buses_and_coaches INT,
    lgvs INT,
    all_hgvs INT,
    estimation_method VARCHAR(20),
    estimation_method_detailed VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(count_point_id, year)
);

CREATE INDEX idx_aadf_point_year ON traffic_aadf (count_point_id, year);

-- Schools
CREATE TABLE schools (
    urn INT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    establishment_type VARCHAR(100),
    establishment_type_group VARCHAR(100),
    phase_of_education VARCHAR(50),
    statutory_low_age INT,
    statutory_high_age INT,
    street VARCHAR(200),
    locality VARCHAR(100),
    town VARCHAR(100),
    county VARCHAR(100),
    postcode VARCHAR(10),
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    local_authority_code VARCHAR(10),
    local_authority_name VARCHAR(100),
    number_of_pupils INT,
    establishment_status VARCHAR(50),
    open_date DATE,
    close_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_schools_geom ON schools USING GIST (geom);
CREATE INDEX idx_schools_phase ON schools (phase_of_education);
CREATE INDEX idx_schools_la ON schools (local_authority_code);

-- Weather observations (historical)
CREATE TABLE weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20),
    station_name VARCHAR(100),
    observation_time TIMESTAMP NOT NULL,
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    temperature_c DECIMAL(4, 1),
    feels_like_c DECIMAL(4, 1),
    wind_speed_mph DECIMAL(5, 1),
    wind_gust_mph DECIMAL(5, 1),
    wind_direction VARCHAR(10),
    humidity_pct INT,
    pressure_hpa DECIMAL(6, 1),
    visibility_m INT,
    weather_type VARCHAR(50),
    precipitation_mm DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_weather_geom ON weather_observations USING GIST (geom);
CREATE INDEX idx_weather_time ON weather_observations (observation_time);

-- Speed cameras
CREATE TABLE speed_cameras (
    camera_id SERIAL PRIMARY KEY,
    camera_type VARCHAR(50),
    road_name VARCHAR(100),
    location_description VARCHAR(200),
    speed_limit INT,
    longitude DECIMAL(10, 6),
    latitude DECIMAL(10, 6),
    geom GEOMETRY(Point, 4326),
    direction VARCHAR(50),
    operational_since DATE,
    data_source VARCHAR(100),
    source_region VARCHAR(100),
    last_verified DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_cameras_geom ON speed_cameras USING GIST (geom);

-- ============================================
-- ANALYTICS TABLES
-- ============================================

-- Pre-computed LSOA statistics
CREATE TABLE lsoa_statistics (
    id SERIAL PRIMARY KEY,
    lsoa_code VARCHAR(15) NOT NULL,
    year INT NOT NULL,
    total_accidents INT DEFAULT 0,
    fatal_accidents INT DEFAULT 0,
    serious_accidents INT DEFAULT 0,
    slight_accidents INT DEFAULT 0,
    total_casualties INT DEFAULT 0,
    fatal_casualties INT DEFAULT 0,
    serious_casualties INT DEFAULT 0,
    pedestrian_casualties INT DEFAULT 0,
    cyclist_casualties INT DEFAULT 0,
    motorcycle_casualties INT DEFAULT 0,
    child_casualties INT DEFAULT 0,
    accidents_per_1000_pop DECIMAL(6, 2),
    risk_score DECIMAL(5, 2),
    risk_category VARCHAR(20),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lsoa_code, year)
);

CREATE INDEX idx_lsoa_stats_code_year ON lsoa_statistics (lsoa_code, year);
CREATE INDEX idx_lsoa_stats_risk ON lsoa_statistics (risk_score DESC);

-- Hourly accident patterns (aggregated)
CREATE TABLE hourly_patterns (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    hour INT NOT NULL,
    day_of_week INT,
    total_accidents INT,
    fatal_accidents INT,
    serious_accidents INT,
    avg_casualties DECIMAL(4, 2),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(year, hour, day_of_week)
);

-- Police force statistics
CREATE TABLE police_force_statistics (
    id SERIAL PRIMARY KEY,
    police_force_code INT NOT NULL,
    year INT NOT NULL,
    total_accidents INT DEFAULT 0,
    fatal_accidents INT DEFAULT 0,
    serious_accidents INT DEFAULT 0,
    slight_accidents INT DEFAULT 0,
    total_casualties INT DEFAULT 0,
    ksi_rate DECIMAL(5, 2),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(police_force_code, year)
);

-- ============================================
-- METADATA TABLES
-- ============================================

-- Data source tracking
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50),
    source_url TEXT,
    api_endpoint TEXT,
    update_frequency VARCHAR(50),
    last_checked TIMESTAMP,
    last_updated TIMESTAMP,
    latest_data_date DATE,
    record_count INT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ETL job history
CREATE TABLE etl_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50),
    source_name VARCHAR(100),
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'RUNNING',
    records_processed INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT,
    details JSONB
);

CREATE INDEX idx_etl_jobs_status ON etl_jobs (status);
CREATE INDEX idx_etl_jobs_started ON etl_jobs (started_at DESC);

-- API usage tracking
CREATE TABLE api_usage (
    id SERIAL PRIMARY KEY,
    api_key VARCHAR(100),
    endpoint VARCHAR(200),
    method VARCHAR(10),
    status_code INT,
    response_time_ms INT,
    request_time TIMESTAMP DEFAULT NOW(),
    ip_address VARCHAR(45),
    user_agent TEXT
);

CREATE INDEX idx_api_usage_time ON api_usage (request_time DESC);
CREATE INDEX idx_api_usage_key ON api_usage (api_key);

-- ============================================
-- VIEWS
-- ============================================

-- Accidents with decoded values
CREATE OR REPLACE VIEW v_accidents_decoded AS
SELECT 
    a.*,
    ls.description as severity_desc,
    lrt.description as road_type_desc,
    lw.description as weather_desc,
    llc.description as light_conditions_desc,
    lrs.description as road_surface_desc,
    lpf.name as police_force_name,
    ldw.name as day_name
FROM accidents a
LEFT JOIN lookup_severity ls ON a.severity = ls.code
LEFT JOIN lookup_road_type lrt ON a.road_type = lrt.code
LEFT JOIN lookup_weather lw ON a.weather_conditions = lw.code
LEFT JOIN lookup_light_conditions llc ON a.light_conditions = llc.code
LEFT JOIN lookup_road_surface lrs ON a.road_surface_conditions = lrs.code
LEFT JOIN lookup_police_force lpf ON a.police_force = lpf.code
LEFT JOIN lookup_day_of_week ldw ON a.day_of_week = ldw.code;

-- Recent accidents summary
CREATE OR REPLACE VIEW v_recent_accidents AS
SELECT 
    accident_id,
    accident_date,
    accident_time,
    severity,
    number_of_casualties,
    latitude,
    longitude,
    lsoa_code,
    police_force
FROM accidents
WHERE accident_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY accident_date DESC, accident_time DESC;

-- LSOA risk summary
CREATE OR REPLACE VIEW v_lsoa_risk_summary AS
SELECT 
    ls.*,
    lb.lsoa_name,
    lb.local_authority_name,
    lb.geom
FROM lsoa_statistics ls
JOIN lsoa_boundaries lb ON ls.lsoa_code = lb.lsoa_code
WHERE ls.year = (SELECT MAX(year) FROM lsoa_statistics);

-- ============================================
-- FUNCTIONS
-- ============================================

-- Function to get accidents within radius of a point
CREATE OR REPLACE FUNCTION get_accidents_within_radius(
    p_lat DECIMAL,
    p_lon DECIMAL,
    p_radius_meters INT DEFAULT 500,
    p_years INT[] DEFAULT NULL
)
RETURNS TABLE (
    accident_id VARCHAR,
    accident_date DATE,
    severity INT,
    distance_meters DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.accident_id,
        a.accident_date,
        a.severity,
        ST_Distance(
            a.geom::geography,
            ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography
        )::DECIMAL as distance_meters
    FROM accidents a
    WHERE ST_DWithin(
        a.geom::geography,
        ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography,
        p_radius_meters
    )
    AND (p_years IS NULL OR a.accident_year = ANY(p_years))
    ORDER BY distance_meters;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate route risk score
CREATE OR REPLACE FUNCTION calculate_route_risk(
    p_route_geom GEOMETRY,
    p_buffer_meters INT DEFAULT 50
)
RETURNS TABLE (
    total_accidents INT,
    fatal_count INT,
    serious_count INT,
    slight_count INT,
    risk_score DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::INT as total_accidents,
        SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END)::INT as fatal_count,
        SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END)::INT as serious_count,
        SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END)::INT as slight_count,
        (
            SUM(CASE WHEN a.severity = 1 THEN 10 
                     WHEN a.severity = 2 THEN 3 
                     ELSE 1 END)::DECIMAL / 
            NULLIF(ST_Length(p_route_geom::geography) / 1000, 0)
        ) as risk_score
    FROM accidents a
    WHERE ST_DWithin(
        a.geom::geography,
        p_route_geom::geography,
        p_buffer_meters
    )
    AND a.accident_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 3;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh LSOA statistics
CREATE OR REPLACE FUNCTION refresh_lsoa_statistics(p_year INT DEFAULT NULL)
RETURNS INT AS $$
DECLARE
    v_year INT;
    v_count INT;
BEGIN
    v_year := COALESCE(p_year, EXTRACT(YEAR FROM CURRENT_DATE)::INT);
    
    -- Delete existing stats for year
    DELETE FROM lsoa_statistics WHERE year = v_year;
    
    -- Insert fresh statistics
    INSERT INTO lsoa_statistics (
        lsoa_code, year, total_accidents, fatal_accidents, 
        serious_accidents, slight_accidents, total_casualties,
        fatal_casualties, serious_casualties, pedestrian_casualties,
        cyclist_casualties, motorcycle_casualties, child_casualties,
        updated_at
    )
    SELECT 
        a.lsoa_code,
        v_year,
        COUNT(DISTINCT a.accident_id),
        COUNT(DISTINCT CASE WHEN a.severity = 1 THEN a.accident_id END),
        COUNT(DISTINCT CASE WHEN a.severity = 2 THEN a.accident_id END),
        COUNT(DISTINCT CASE WHEN a.severity = 3 THEN a.accident_id END),
        COALESCE(SUM(a.number_of_casualties), 0),
        (SELECT COUNT(*) FROM casualties c WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND c.severity = 1),
        (SELECT COUNT(*) FROM casualties c WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND c.severity = 2),
        (SELECT COUNT(*) FROM casualties c WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND c.casualty_class = 3),
        (SELECT COUNT(*) FROM casualties c JOIN vehicles v ON c.accident_id = v.accident_id AND c.vehicle_reference = v.vehicle_reference WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND v.vehicle_type = 1),
        (SELECT COUNT(*) FROM casualties c JOIN vehicles v ON c.accident_id = v.accident_id AND c.vehicle_reference = v.vehicle_reference WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND v.vehicle_type IN (2,3,4,5,97)),
        (SELECT COUNT(*) FROM casualties c WHERE c.accident_id = ANY(ARRAY_AGG(a.accident_id)) AND c.age < 16),
        NOW()
    FROM accidents a
    WHERE a.accident_year = v_year
    AND a.lsoa_code IS NOT NULL
    GROUP BY a.lsoa_code;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Update risk scores
    UPDATE lsoa_statistics ls
    SET risk_score = (
        (fatal_accidents * 10 + serious_accidents * 3 + slight_accidents) / 
        NULLIF((SELECT area_hectares FROM lsoa_boundaries WHERE lsoa_code = ls.lsoa_code), 0) * 100
    ),
    risk_category = CASE 
        WHEN (fatal_accidents * 10 + serious_accidents * 3 + slight_accidents) > 50 THEN 'Very High'
        WHEN (fatal_accidents * 10 + serious_accidents * 3 + slight_accidents) > 20 THEN 'High'
        WHEN (fatal_accidents * 10 + serious_accidents * 3 + slight_accidents) > 10 THEN 'Medium'
        ELSE 'Low'
    END
    WHERE year = v_year;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- INITIAL DATA SOURCE RECORDS
-- ============================================

INSERT INTO data_sources (source_name, source_type, source_url, update_frequency, notes) VALUES
('STATS19 Accidents', 'CSV', 'https://data.dft.gov.uk/road-accidents-safety-data/', 'Annual (September)', 'Main accident data from DfT'),
('STATS19 Casualties', 'CSV', 'https://data.dft.gov.uk/road-accidents-safety-data/', 'Annual (September)', 'Casualty details'),
('STATS19 Vehicles', 'CSV', 'https://data.dft.gov.uk/road-accidents-safety-data/', 'Annual (September)', 'Vehicle details'),
('ONS LSOA Boundaries', 'GeoJSON API', 'https://geoportal.statistics.gov.uk/', 'Decennial', 'Lower Super Output Area boundaries'),
('DfT Traffic Counts', 'REST API', 'https://roadtraffic.dft.gov.uk/api', 'Annual (June)', 'Traffic count points and AADF'),
('GIAS Schools', 'CSV', 'https://get-information-schools.service.gov.uk/', 'Daily', 'School locations'),
('Met Office DataHub', 'REST API', 'https://datahub.metoffice.gov.uk/', 'Hourly', 'Weather observations'),
('National Highways WebTRIS', 'REST API', 'https://webtris.highwaysengland.co.uk/api', 'Real-time', 'Traffic data for strategic roads');

COMMIT;
