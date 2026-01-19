"""
Met Office Weather Data ETL

Integrates weather data from Met Office DataHub API.
Note: Requires a Met Office DataHub API key (free tier available).
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

from ..config import DATA_DIR
from ..loaders.postgres import PostgresLoader


class WeatherExtractor:
    """
    Extract weather data from Met Office DataHub API.
    
    API Documentation: https://datahub.metoffice.gov.uk/docs/
    """
    
    BASE_URL = "https://data.hub.api.metoffice.gov.uk"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('METOFFICE_API_KEY')
        self.data_dir = Path(DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.api_key:
            logger.warning("No Met Office API key configured. Set METOFFICE_API_KEY environment variable.")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make authenticated request to Met Office API."""
        if not self.api_key:
            raise ValueError("Met Office API key not configured")
        
        url = f"{self.BASE_URL}/{endpoint}"
        headers = {
            "apikey": self.api_key,
            "Accept": "application/json"
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def get_site_list(self) -> pd.DataFrame:
        """Get list of Met Office weather observation sites."""
        try:
            data = self._make_request("sitespecific/v0/point/hourly/sites")
            sites = data.get('features', [])
            
            records = []
            for site in sites:
                props = site.get('properties', {})
                coords = site.get('geometry', {}).get('coordinates', [None, None])
                records.append({
                    'site_id': props.get('id'),
                    'name': props.get('name'),
                    'longitude': coords[0] if coords else None,
                    'latitude': coords[1] if coords else None,
                    'elevation': props.get('elevation'),
                    'region': props.get('region'),
                    'unit_type': props.get('unitType')
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} weather observation sites")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get site list: {e}")
            raise
    
    def get_hourly_observations(
        self, 
        latitude: float, 
        longitude: float,
        hours_back: int = 24
    ) -> pd.DataFrame:
        """
        Get hourly weather observations for a location.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            hours_back: Number of hours of historical data
            
        Returns:
            DataFrame with weather observations
        """
        try:
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "excludeParameterMetadata": "true"
            }
            
            data = self._make_request("sitespecific/v0/point/hourly", params)
            
            # Parse time series data
            features = data.get('features', [])
            if not features:
                return pd.DataFrame()
            
            time_series = features[0].get('properties', {}).get('timeSeries', [])
            
            records = []
            for obs in time_series:
                records.append({
                    'time': obs.get('time'),
                    'temperature_c': obs.get('screenTemperature'),
                    'feels_like_c': obs.get('feelsLikeTemperature'),
                    'humidity_pct': obs.get('screenRelativeHumidity'),
                    'wind_speed_ms': obs.get('windSpeed10m'),
                    'wind_gust_ms': obs.get('windGustSpeed10m'),
                    'wind_direction': obs.get('windDirectionFrom10m'),
                    'visibility_m': obs.get('visibility'),
                    'precipitation_rate': obs.get('precipitationRate'),
                    'pressure_pa': obs.get('mslp'),
                    'uv_index': obs.get('uvIndex'),
                    'weather_code': obs.get('significantWeatherCode')
                })
            
            df = pd.DataFrame(records)
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            
            logger.info(f"Retrieved {len(df)} hourly observations for ({latitude}, {longitude})")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get observations: {e}")
            raise


class WeatherTransformer:
    """Transform weather data for database loading."""
    
    # Weather code mapping (Met Office significant weather codes)
    WEATHER_CODE_MAP = {
        0: 'Clear night',
        1: 'Sunny day',
        2: 'Partly cloudy (night)',
        3: 'Partly cloudy (day)',
        5: 'Mist',
        6: 'Fog',
        7: 'Cloudy',
        8: 'Overcast',
        9: 'Light rain shower (night)',
        10: 'Light rain shower (day)',
        11: 'Drizzle',
        12: 'Light rain',
        13: 'Heavy rain shower (night)',
        14: 'Heavy rain shower (day)',
        15: 'Heavy rain',
        16: 'Sleet shower (night)',
        17: 'Sleet shower (day)',
        18: 'Sleet',
        19: 'Hail shower (night)',
        20: 'Hail shower (day)',
        21: 'Hail',
        22: 'Light snow shower (night)',
        23: 'Light snow shower (day)',
        24: 'Light snow',
        25: 'Heavy snow shower (night)',
        26: 'Heavy snow shower (day)',
        27: 'Heavy snow',
        28: 'Thunder shower (night)',
        29: 'Thunder shower (day)',
        30: 'Thunder'
    }
    
    def transform_observations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform weather observations for loading."""
        if df.empty:
            return df
        
        # Add weather description
        if 'weather_code' in df.columns:
            df['weather_description'] = df['weather_code'].map(self.WEATHER_CODE_MAP)
        
        # Categorize weather for road safety relevance
        df['is_rain'] = df['weather_code'].isin([9, 10, 11, 12, 13, 14, 15])
        df['is_snow_ice'] = df['weather_code'].isin([16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27])
        df['is_fog_mist'] = df['weather_code'].isin([5, 6])
        df['is_adverse'] = df['is_rain'] | df['is_snow_ice'] | df['is_fog_mist']
        
        return df


class WeatherLoader:
    """Load weather data to database."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load_sites(self, df: pd.DataFrame, replace: bool = False) -> int:
        """Load weather observation sites."""
        if df.empty:
            return 0
        
        if_exists = 'replace' if replace else 'append'
        return self.db.load_dataframe(df, 'weather_sites', if_exists=if_exists)
    
    def load_observations(self, df: pd.DataFrame) -> int:
        """Load weather observations."""
        if df.empty:
            return 0
        
        return self.db.load_dataframe(df, 'weather_observations', if_exists='append')


def create_weather_tables(connection_string: str = None):
    """Create weather-related tables in database."""
    from sqlalchemy import create_engine, text
    
    connection_string = connection_string or os.getenv('DATABASE_URL', 
        'postgresql://postgres:pass@localhost:5432/roadsafety')
    engine = create_engine(connection_string)
    
    sql = """
    -- Weather observation sites
    CREATE TABLE IF NOT EXISTS weather_sites (
        site_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(100),
        longitude DECIMAL(10, 6),
        latitude DECIMAL(10, 6),
        elevation DECIMAL(8, 2),
        region VARCHAR(50),
        unit_type VARCHAR(20),
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Weather observations
    CREATE TABLE IF NOT EXISTS weather_observations (
        id SERIAL PRIMARY KEY,
        site_id VARCHAR(20) REFERENCES weather_sites(site_id),
        observation_time TIMESTAMP NOT NULL,
        temperature_c DECIMAL(5, 2),
        feels_like_c DECIMAL(5, 2),
        humidity_pct DECIMAL(5, 2),
        wind_speed_ms DECIMAL(6, 2),
        wind_gust_ms DECIMAL(6, 2),
        wind_direction INT,
        visibility_m INT,
        precipitation_rate DECIMAL(6, 3),
        pressure_pa INT,
        uv_index INT,
        weather_code INT,
        weather_description VARCHAR(50),
        is_rain BOOLEAN,
        is_snow_ice BOOLEAN,
        is_fog_mist BOOLEAN,
        is_adverse BOOLEAN,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(site_id, observation_time)
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_obs_time ON weather_observations(observation_time);
    CREATE INDEX IF NOT EXISTS idx_weather_obs_adverse ON weather_observations(is_adverse);
    """
    
    with engine.connect() as conn:
        for statement in sql.split(';'):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    
    logger.info("Weather tables created")


def run_weather_etl(connection_string: str = None) -> Dict[str, int]:
    """
    Run weather ETL pipeline.
    
    Note: Requires METOFFICE_API_KEY environment variable.
    """
    logger.info("Starting Weather ETL")
    
    stats = {'sites_loaded': 0, 'observations_loaded': 0}
    
    api_key = os.getenv('METOFFICE_API_KEY')
    if not api_key:
        logger.warning("METOFFICE_API_KEY not set. Skipping weather ETL.")
        logger.info("To enable weather data:")
        logger.info("1. Register at https://datahub.metoffice.gov.uk/")
        logger.info("2. Create an API key for the Site Specific API")
        logger.info("3. Set METOFFICE_API_KEY in your .env file")
        return stats
    
    try:
        # Create tables
        create_weather_tables(connection_string)
        
        # Extract
        extractor = WeatherExtractor(api_key)
        sites_df = extractor.get_site_list()
        
        # Load sites
        loader = WeatherLoader(connection_string)
        stats['sites_loaded'] = loader.load_sites(sites_df, replace=True)
        
        logger.info(f"Weather ETL complete: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Weather ETL failed: {e}")
        raise


if __name__ == '__main__':
    import click
    
    @click.command()
    @click.option('--create-tables', is_flag=True, help='Create weather tables')
    def main(create_tables):
        """Weather Data ETL Pipeline"""
        if create_tables:
            create_weather_tables()
            click.echo("Weather tables created")
        else:
            stats = run_weather_etl()
            click.echo(f"Complete: {stats}")
    
    main()
