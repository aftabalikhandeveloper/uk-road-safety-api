"""
WebTRIS Real-time Traffic ETL

Integrates real-time traffic data from Highways England WebTRIS API.
This provides traffic flow data for strategic road network (motorways/major A-roads).
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


class WebTRISExtractor:
    """
    Extract real-time traffic data from WebTRIS API.
    
    API Documentation: https://webtris.highwaysengland.co.uk/api/swagger/ui/index
    No API key required - public access.
    """
    
    BASE_URL = "https://webtris.highwaysengland.co.uk/api/v1"
    
    def __init__(self):
        self.data_dir = Path(DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
    
    def get_sites(self) -> pd.DataFrame:
        """
        Get all WebTRIS monitoring sites.
        
        Returns:
            DataFrame with site metadata
        """
        logger.info("Fetching WebTRIS sites...")
        
        try:
            url = f"{self.BASE_URL}/sites"
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            sites = data.get('sites', [])
            
            records = []
            for site in sites:
                records.append({
                    'site_id': site.get('Id'),
                    'name': site.get('Name'),
                    'description': site.get('Description'),
                    'longitude': site.get('Longitude'),
                    'latitude': site.get('Latitude'),
                    'status': site.get('Status')
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} WebTRIS sites")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get WebTRIS sites: {e}")
            raise
    
    def get_site_quality(self, site_id: str) -> Dict:
        """Get data quality metrics for a site."""
        try:
            url = f"{self.BASE_URL}/quality/overall"
            params = {"sites": site_id}
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get quality for site {site_id}: {e}")
            return {}
    
    def get_daily_report(
        self,
        site_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get daily traffic report for a site.
        
        Args:
            site_id: WebTRIS site ID
            start_date: Start date for report
            end_date: End date for report
            
        Returns:
            DataFrame with daily traffic data
        """
        try:
            start_str = start_date.strftime('%d%m%Y')
            end_str = end_date.strftime('%d%m%Y')
            
            url = f"{self.BASE_URL}/reports/{site_id}/daily/{start_str}/{end_str}"
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            records = data.get('Rows', [])
            if not records:
                return pd.DataFrame()
            
            df = pd.DataFrame(records)
            df['site_id'] = site_id
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get daily report for site {site_id}: {e}")
            return pd.DataFrame()
    
    def get_hourly_report(
        self,
        site_id: str,
        report_date: datetime
    ) -> pd.DataFrame:
        """
        Get hourly traffic report for a site on a specific day.
        
        Args:
            site_id: WebTRIS site ID
            report_date: Date for report
            
        Returns:
            DataFrame with hourly traffic data
        """
        try:
            date_str = report_date.strftime('%d%m%Y')
            
            url = f"{self.BASE_URL}/reports/{site_id}/hourly/{date_str}/{date_str}"
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            records = data.get('Rows', [])
            if not records:
                return pd.DataFrame()
            
            df = pd.DataFrame(records)
            df['site_id'] = site_id
            return df
            
        except Exception as e:
            logger.warning(f"Failed to get hourly report for site {site_id}: {e}")
            return pd.DataFrame()


class WebTRISTransformer:
    """Transform WebTRIS data for database loading."""
    
    def transform_sites(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform site data."""
        if df.empty:
            return df
        
        # Filter to active sites only
        if 'status' in df.columns:
            df = df[df['status'] == 'Active']
            logger.info(f"Filtered to {len(df)} active sites")
        
        # Convert coordinates
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        
        # Remove sites without coordinates
        df = df.dropna(subset=['longitude', 'latitude'])
        
        return df
    
    def transform_daily_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform daily traffic report."""
        if df.empty:
            return df
        
        # Rename columns
        column_map = {
            'Total Volume': 'total_volume',
            'Avg mph': 'avg_speed_mph',
            'Date': 'report_date',
            'Time Period Ending': 'time_period'
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        
        # Parse date
        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
        
        return df


class WebTRISLoader:
    """Load WebTRIS data to database."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load_sites(self, df: pd.DataFrame, replace: bool = False) -> int:
        """Load WebTRIS sites."""
        if df.empty:
            return 0
        
        if_exists = 'replace' if replace else 'append'
        return self.db.load_dataframe(df, 'webtris_sites', if_exists=if_exists)
    
    def load_daily_reports(self, df: pd.DataFrame) -> int:
        """Load daily traffic reports."""
        if df.empty:
            return 0
        
        return self.db.load_dataframe(df, 'webtris_daily_reports', if_exists='append')


def create_webtris_tables(connection_string: str = None):
    """Create WebTRIS-related tables in database."""
    from sqlalchemy import create_engine, text
    
    connection_string = connection_string or os.getenv('DATABASE_URL', 
        'postgresql://postgres:pass@localhost:5432/roadsafety')
    engine = create_engine(connection_string)
    
    sql = """
    -- WebTRIS monitoring sites
    CREATE TABLE IF NOT EXISTS webtris_sites (
        site_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(200),
        description TEXT,
        longitude DECIMAL(10, 6),
        latitude DECIMAL(10, 6),
        status VARCHAR(20),
        geom GEOMETRY(Point, 4326),
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_webtris_sites_geom ON webtris_sites USING GIST (geom);
    
    -- WebTRIS daily traffic reports
    CREATE TABLE IF NOT EXISTS webtris_daily_reports (
        id SERIAL PRIMARY KEY,
        site_id VARCHAR(20) REFERENCES webtris_sites(site_id),
        report_date DATE NOT NULL,
        time_period VARCHAR(20),
        total_volume INT,
        avg_speed_mph DECIMAL(5, 2),
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(site_id, report_date, time_period)
    );
    
    CREATE INDEX IF NOT EXISTS idx_webtris_reports_date ON webtris_daily_reports(report_date);
    CREATE INDEX IF NOT EXISTS idx_webtris_reports_site ON webtris_daily_reports(site_id);
    """
    
    with engine.connect() as conn:
        for statement in sql.split(';'):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    
    logger.info("WebTRIS tables created")


def run_webtris_etl(
    load_sites: bool = True,
    connection_string: str = None
) -> Dict[str, int]:
    """
    Run WebTRIS ETL pipeline.
    
    No API key required - public access.
    """
    logger.info("Starting WebTRIS ETL")
    
    stats = {'sites_loaded': 0}
    
    try:
        # Create tables
        create_webtris_tables(connection_string)
        
        extractor = WebTRISExtractor()
        transformer = WebTRISTransformer()
        loader = WebTRISLoader(connection_string)
        
        if load_sites:
            # Get and load sites
            sites_df = extractor.get_sites()
            sites_df = transformer.transform_sites(sites_df)
            
            # Add geometry column for PostGIS
            if not sites_df.empty:
                from shapely.geometry import Point
                import geopandas as gpd
                
                geometry = [
                    Point(row['longitude'], row['latitude']) 
                    for _, row in sites_df.iterrows()
                ]
                gdf = gpd.GeoDataFrame(sites_df, geometry=geometry, crs='EPSG:4326')
                gdf = gdf.rename_geometry('geom')
                
                # Load to database - truncate first then append
                loader.db.execute_sql("TRUNCATE TABLE webtris_sites CASCADE")
                stats['sites_loaded'] = loader.db.load_geodataframe(
                    gdf, 'webtris_sites', if_exists='append'
                )
        
        logger.info(f"WebTRIS ETL complete: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"WebTRIS ETL failed: {e}")
        raise


if __name__ == '__main__':
    import click
    
    @click.command()
    @click.option('--create-tables', is_flag=True, help='Create WebTRIS tables only')
    @click.option('--sites/--no-sites', default=True, help='Load site data')
    def main(create_tables, sites):
        """WebTRIS Traffic Data ETL Pipeline"""
        if create_tables:
            create_webtris_tables()
            click.echo("WebTRIS tables created")
        else:
            stats = run_webtris_etl(load_sites=sites)
            click.echo(f"Complete: {stats}")
    
    main()
