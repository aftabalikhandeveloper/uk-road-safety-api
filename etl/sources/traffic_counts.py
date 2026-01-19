"""
DfT Traffic Counts ETL

Downloads traffic count data from the Department for Transport API.
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path
from typing import Optional, List, Dict
from shapely.geometry import Point
from loguru import logger
from tqdm import tqdm
import click

from ..config import DATA_DIR, DFT_TRAFFIC_API_BASE, CRS_WGS84
from ..loaders.postgres import PostgresLoader


class TrafficCountsExtractor:
    """Extract traffic count data from DfT API."""
    
    def __init__(self):
        self.base_url = DFT_TRAFFIC_API_BASE
        self.session = requests.Session()
    
    def get_count_points(self, page_size: int = 5000) -> pd.DataFrame:
        """
        Get all traffic count point locations.
        
        Returns:
            DataFrame with count point metadata
        """
        logger.info("Fetching traffic count points from API...")
        all_data = []
        page = 1
        
        while True:
            url = f"{self.base_url}/count-points"
            params = {'page': page, 'page_size': page_size}
            
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                rows = data.get('rows', [])
                if not rows:
                    break
                
                all_data.extend(rows)
                logger.debug(f"Page {page}: {len(all_data)} total records")
                page += 1
                
            except requests.RequestException as e:
                logger.error(f"API request failed: {e}")
                break
        
        logger.info(f"Retrieved {len(all_data)} count points")
        return pd.DataFrame(all_data)
    
    def get_aadf_for_point(self, count_point_id: int) -> pd.DataFrame:
        """Get Annual Average Daily Flow for a specific count point."""
        url = f"{self.base_url}/count-points/{count_point_id}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return pd.DataFrame(response.json())
        except requests.RequestException as e:
            logger.error(f"Failed to get AADF for point {count_point_id}: {e}")
            return pd.DataFrame()
    
    def download_bulk_csv(self, data_type: str = 'aadf') -> Path:
        """
        Download bulk CSV data from DfT.
        
        Args:
            data_type: 'aadf', 'count_points', or 'raw_counts'
            
        Returns:
            Path to downloaded file
        """
        urls = {
            'aadf': 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/dft_traffic_counts_aadf.zip',
            'count_points': 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/dft_traffic_counts_count_point.zip',
            'raw_counts': 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/dft_traffic_counts_raw_counts.zip'
        }
        
        url = urls.get(data_type)
        if not url:
            raise ValueError(f"Unknown data type: {data_type}")
        
        filepath = DATA_DIR / f"traffic_{data_type}.zip"
        
        logger.info(f"Downloading {data_type} data...")
        response = self.session.get(url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded to {filepath}")
        return filepath


class TrafficCountsTransformer:
    """Transform traffic count data for database loading."""
    
    def transform_count_points(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Transform count point data to GeoDataFrame."""
        logger.info(f"Transforming {len(df)} count points")
        
        # Lowercase column names first
        df.columns = df.columns.str.lower().str.strip()
        
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Drop local_authority_id since we have local_authority_code
        if 'local_authority_id' in df.columns and 'local_authority_code' in df.columns:
            df = df.drop(columns=['local_authority_id'])
        
        # Rename columns to match database schema
        column_mapping = {
            'id': 'count_point_id',
            'count_point_id': 'count_point_id',
            'name': 'road_name',
            'road_name': 'road_name',
            'road_category': 'road_category',
            'road_type': 'road_type',
            'start_junction_road_name': 'start_junction_road_name',
            'end_junction_road_name': 'end_junction_road_name',
            'easting': 'easting',
            'northing': 'northing',
            'longitude': 'longitude',
            'latitude': 'latitude',
            'local_authority_id': 'local_authority_code',
            'local_authority_code': 'local_authority_code',
            'local_authority_name': 'local_authority_name',
            'region_id': 'region_id',
            'region_name': 'region_name',
            'link_length_km': 'link_length_km',
            'link_length_miles': 'link_length_miles'
        }
        
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Filter valid coordinates
        df = df.dropna(subset=['longitude', 'latitude'])
        df = df[(df['latitude'] > 49) & (df['latitude'] < 61)]
        df = df[(df['longitude'] > -9) & (df['longitude'] < 2)]
        
        # Create geometry
        geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=CRS_WGS84)
        gdf = gdf.rename_geometry('geom')
        
        logger.info(f"Transformed {len(gdf)} count points with geometry")
        return gdf
    
    def transform_aadf(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform AADF data."""
        logger.info(f"Transforming {len(df)} AADF records")
        
        column_mapping = {
            'count_point_id': 'count_point_id',
            'year': 'year',
            'all_motor_vehicles': 'all_motor_vehicles',
            'pedal_cycles': 'pedal_cycles',
            'two_wheeled_motor_vehicles': 'two_wheeled_motor_vehicles',
            'cars_and_taxis': 'cars_and_taxis',
            'buses_and_coaches': 'buses_and_coaches',
            'lgvs': 'lgvs',
            'all_hgvs': 'all_hgvs',
            'estimation_method': 'estimation_method',
            'estimation_method_detailed': 'estimation_method_detailed'
        }
        
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Convert numeric columns
        numeric_cols = ['all_motor_vehicles', 'pedal_cycles', 'cars_and_taxis', 
                       'buses_and_coaches', 'lgvs', 'all_hgvs']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df


class TrafficCountsLoader:
    """Load traffic count data to database."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load_count_points(self, gdf: gpd.GeoDataFrame, replace: bool = False) -> int:
        """Load count points to database."""
        if_exists = 'replace' if replace else 'append'
        return self.db.load_geodataframe(gdf, 'traffic_count_points', if_exists=if_exists)
    
    def load_aadf(self, df: pd.DataFrame, replace: bool = False) -> int:
        """Load AADF data to database."""
        if_exists = 'replace' if replace else 'append'
        return self.db.load_dataframe(df, 'traffic_aadf', if_exists=if_exists)


def run_traffic_counts_etl(
    refresh_points: bool = True,
    refresh_aadf: bool = True,
    connection_string: str = None
) -> Dict[str, int]:
    """
    Run traffic counts ETL pipeline.
    
    Args:
        refresh_points: Whether to refresh count point data
        refresh_aadf: Whether to refresh AADF data
        connection_string: Database connection URL
        
    Returns:
        Processing statistics
    """
    logger.info("Starting Traffic Counts ETL")
    
    extractor = TrafficCountsExtractor()
    transformer = TrafficCountsTransformer()
    loader = TrafficCountsLoader(connection_string)
    
    stats = {'count_points': 0, 'aadf_records': 0}
    
    try:
        if refresh_points:
            # Try API first, fall back to bulk download
            df = extractor.get_count_points()
            
            if df.empty:
                logger.info("API returned no data, trying bulk CSV download...")
                import zipfile
                import io
                
                # Download count points CSV
                url = 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/count_points.zip'
                response = extractor.session.get(url, timeout=120)
                response.raise_for_status()
                
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)
                
                logger.info(f"Loaded {len(df)} count points from bulk CSV")
            
            if not df.empty:
                gdf = transformer.transform_count_points(df)
                stats['count_points'] = loader.load_count_points(gdf, replace=True)
        
        if refresh_aadf:
            # Download bulk AADF data
            logger.info("Downloading AADF bulk data...")
            import zipfile
            import io
            
            url = 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/dft_traffic_counts_aadf.zip'
            response = extractor.session.get(url, timeout=300)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_name) as f:
                    aadf_df = pd.read_csv(f)
            
            logger.info(f"Loaded {len(aadf_df)} AADF records from bulk CSV")
            
            # Transform and load AADF
            if not aadf_df.empty:
                aadf_df.columns = aadf_df.columns.str.lower().str.replace(' ', '_')
                stats['aadf_records'] = loader.load_aadf(aadf_df, replace=True)
        
    except Exception as e:
        logger.error(f"Traffic counts ETL failed: {e}")
        raise
    
    logger.info(f"Traffic counts ETL complete: {stats}")
    return stats


@click.command()
@click.option('--points/--no-points', default=True, help='Refresh count points')
@click.option('--aadf/--no-aadf', default=True, help='Refresh AADF data')
def main(points, aadf):
    """Traffic Counts ETL Pipeline"""
    stats = run_traffic_counts_etl(refresh_points=points, refresh_aadf=aadf)
    click.echo(f"Complete: {stats}")


if __name__ == '__main__':
    main()
