"""
Configuration settings for UK Road Safety ETL Pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / 'data' / 'raw'))
LOG_DIR = Path(os.getenv('LOG_DIR', BASE_DIR / 'logs'))

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:pass@localhost:5432/roadsafety'
)

# Parse database URL for individual components
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'roadsafety'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'pass'),
}

# ETL Settings
ETL_BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', 10000))
ETL_LOG_LEVEL = os.getenv('ETL_LOG_LEVEL', 'INFO')

# External API Keys
MET_OFFICE_API_KEY = os.getenv('MET_OFFICE_API_KEY', '')

# Data Source URLs
STATS19_BASE_URL = "https://data.dft.gov.uk/road-accidents-safety-data"
STATS19_FILES = {
    'collisions': 'dft-road-casualty-statistics-collision-{year}.csv',
    'casualties': 'dft-road-casualty-statistics-casualty-{year}.csv', 
    'vehicles': 'dft-road-casualty-statistics-vehicle-{year}.csv',
}

# Alternative: Full dataset files (all years)
STATS19_FULL_FILES = {
    'collisions': 'dft-road-casualty-statistics-collision-1979-latest-published-year.csv',
    'casualties': 'dft-road-casualty-statistics-casualty-1979-latest-published-year.csv',
    'vehicles': 'dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv',
}

# ONS Geography Portal
ONS_GEOPORTAL_BASE = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
ONS_LSOA_ENDPOINT = f"{ONS_GEOPORTAL_BASE}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"

# DfT Traffic API
DFT_TRAFFIC_API_BASE = "https://roadtraffic.dft.gov.uk/api"

# National Highways WebTRIS API  
WEBTRIS_API_BASE = "https://webtris.highwaysengland.co.uk/api"

# Met Office DataHub
METOFFICE_DATAHUB_BASE = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0"

# Schools Data (GIAS)
GIAS_DOWNLOAD_URL = "https://get-information-schools.service.gov.uk/Downloads"

# Column mappings for STATS19 data
COLLISION_COLUMN_MAPPING = {
    # Old format (pre-2024)
    'accident_index': 'accident_id',
    'accident_year': 'accident_year',
    'accident_reference': 'accident_reference',
    'accident_severity': 'severity',
    # New format (2024+)
    'collision_index': 'accident_id',
    'collision_year': 'accident_year',
    'collision_ref_no': 'accident_reference',
    'collision_severity': 'severity',
    # Common columns
    'longitude': 'longitude',
    'latitude': 'latitude',
    'location_easting_osgr': 'location_easting',
    'location_northing_osgr': 'location_northing',
    'police_force': 'police_force',
    'number_of_vehicles': 'number_of_vehicles',
    'number_of_casualties': 'number_of_casualties',
    'date': 'accident_date',
    'time': 'accident_time',
    'day_of_week': 'day_of_week',
    'local_authority_district': 'local_authority_district',
    'local_authority_ons_district': 'local_authority_ons_district',
    'local_authority_highway': 'local_authority_highway',
    'first_road_class': 'first_road_class',
    'first_road_number': 'first_road_number',
    'road_type': 'road_type',
    'speed_limit': 'speed_limit',
    'junction_detail': 'junction_detail',
    'junction_control': 'junction_control',
    'second_road_class': 'second_road_class',
    'second_road_number': 'second_road_number',
    'pedestrian_crossing_human_control': 'pedestrian_crossing_human',
    'pedestrian_crossing_physical_facilities': 'pedestrian_crossing_physical',
    'light_conditions': 'light_conditions',
    'weather_conditions': 'weather_conditions',
    'road_surface_conditions': 'road_surface_conditions',
    'special_conditions_at_site': 'special_conditions_at_site',
    'carriageway_hazards': 'carriageway_hazards',
    'urban_or_rural_area': 'urban_or_rural',
    'did_police_officer_attend_scene_of_accident': 'police_attended',
    'trunk_road_flag': 'trunk_road_flag',
    'lsoa_of_accident_location': 'lsoa_code',
}

CASUALTY_COLUMN_MAPPING = {
    # Old format (pre-2024)
    'accident_index': 'accident_id',
    'accident_year': 'accident_year',
    # New format (2024+)
    'collision_index': 'accident_id',
    'collision_year': 'accident_year',
    # Common columns
    'vehicle_reference': 'vehicle_reference',
    'casualty_reference': 'casualty_reference',
    'casualty_class': 'casualty_class',
    'sex_of_casualty': 'sex',
    'age_of_casualty': 'age',
    'age_band_of_casualty': 'age_band',
    'casualty_severity': 'severity',
    'pedestrian_location': 'pedestrian_location',
    'pedestrian_movement': 'pedestrian_movement',
    'car_passenger': 'car_passenger',
    'bus_or_coach_passenger': 'bus_or_coach_passenger',
    'pedestrian_road_maintenance_worker': 'pedestrian_road_maintenance_worker',
    'casualty_type': 'casualty_type',
    'casualty_home_area_type': 'casualty_home_area_type',
    'casualty_imd_decile': 'casualty_imd_decile',
}

VEHICLE_COLUMN_MAPPING = {
    # Old format (pre-2024)
    'accident_index': 'accident_id',
    'accident_year': 'accident_year',
    # New format (2024+)
    'collision_index': 'accident_id',
    'collision_year': 'accident_year',
    # Common columns
    'vehicle_reference': 'vehicle_reference',
    'vehicle_type': 'vehicle_type',
    'towing_and_articulation': 'towing_and_articulation',
    'vehicle_manoeuvre': 'vehicle_manoeuvre',
    'vehicle_direction_from': 'vehicle_direction_from',
    'vehicle_direction_to': 'vehicle_direction_to',
    'vehicle_location_restricted_lane': 'vehicle_location_restricted_lane',
    'junction_location': 'junction_location',
    'skidding_and_overturning': 'skidding_and_overturning',
    'hit_object_in_carriageway': 'hit_object_in_carriageway',
    'vehicle_leaving_carriageway': 'vehicle_leaving_carriageway',
    'hit_object_off_carriageway': 'hit_object_off_carriageway',
    'first_point_of_impact': 'first_point_of_impact',
    'vehicle_left_hand_drive': 'vehicle_left_hand_drive',
    'journey_purpose_of_driver': 'journey_purpose',
    'sex_of_driver': 'sex_of_driver',
    'age_of_driver': 'age_of_driver',
    'age_band_of_driver': 'age_band_of_driver',
    'engine_capacity_cc': 'engine_capacity_cc',
    'propulsion_code': 'propulsion_code',
    'age_of_vehicle': 'age_of_vehicle',
    'generic_make_model': 'generic_make_model',
    'driver_imd_decile': 'driver_imd_decile',
    'driver_home_area_type': 'driver_home_area_type',
}

# Coordinate Reference Systems
CRS_WGS84 = "EPSG:4326"
CRS_OSGB36 = "EPSG:27700"

# Data validation
VALID_YEARS = range(1979, 2030)
VALID_SEVERITY = [1, 2, 3]
UK_BOUNDS = {
    'min_lat': 49.8,
    'max_lat': 60.9,
    'min_lon': -8.2,
    'max_lon': 1.8,
}
