"""
STATS19 Road Accident Data ETL

Downloads, transforms, and loads UK road accident data from the 
Department for Transport.
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from shapely.geometry import Point
from loguru import logger
from tqdm import tqdm
import click

from ..config import (
    DATA_DIR, 
    STATS19_BASE_URL,
    STATS19_FILES,
    STATS19_FULL_FILES,
    COLLISION_COLUMN_MAPPING,
    CASUALTY_COLUMN_MAPPING,
    VEHICLE_COLUMN_MAPPING,
    CRS_WGS84,
    UK_BOUNDS
)
from ..loaders.postgres import PostgresLoader


class Stats19Extractor:
    """
    Extract STATS19 data from Department for Transport.
    
    Data is published annually in September with provisional updates.
    Full historical data (1979-present) available in single files.
    """
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = Path(data_dir or DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RoadSafetyETL/1.0 (Educational/Research)'
        })
    
    def _download_file(self, url: str, filepath: Path, chunk_size: int = 8192) -> bool:
        """Download a file with progress bar."""
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=filepath.name) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        size = f.write(chunk)
                        pbar.update(size)
            
            logger.info(f"Downloaded: {filepath}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            return False
    
    def download_year(self, year: int) -> Dict[str, Path]:
        """
        Download all STATS19 files for a specific year.
        
        Args:
            year: Year to download (e.g., 2023)
            
        Returns:
            Dictionary mapping data type to filepath
        """
        downloaded = {}
        
        for data_type, filename_template in STATS19_FILES.items():
            filename = filename_template.format(year=year)
            url = f"{STATS19_BASE_URL}/{filename}"
            filepath = self.data_dir / filename
            
            if filepath.exists():
                logger.info(f"File already exists: {filepath}")
                downloaded[data_type] = filepath
                continue
            
            logger.info(f"Downloading {data_type} data for {year}...")
            if self._download_file(url, filepath):
                downloaded[data_type] = filepath
            else:
                downloaded[data_type] = None
        
        return downloaded
    
    def download_full_dataset(self) -> Dict[str, Path]:
        """
        Download the complete historical dataset (1979-present).
        
        Returns:
            Dictionary mapping data type to filepath
        """
        downloaded = {}
        
        for data_type, filename in STATS19_FULL_FILES.items():
            url = f"{STATS19_BASE_URL}/{filename}"
            filepath = self.data_dir / filename
            
            if filepath.exists():
                logger.info(f"File already exists: {filepath}")
                downloaded[data_type] = filepath
                continue
            
            logger.info(f"Downloading full {data_type} dataset...")
            if self._download_file(url, filepath):
                downloaded[data_type] = filepath
            else:
                downloaded[data_type] = None
        
        return downloaded
    
    def download_years_range(self, start_year: int, end_year: int = None) -> Dict[int, Dict[str, Path]]:
        """
        Download STATS19 data for a range of years.
        
        Args:
            start_year: First year to download
            end_year: Last year to download (default: current year)
            
        Returns:
            Dictionary mapping year to file paths
        """
        end_year = end_year or datetime.now().year
        all_files = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Processing year {year}...")
            try:
                files = self.download_year(year)
                if any(f is not None for f in files.values()):
                    all_files[year] = files
            except Exception as e:
                logger.warning(f"Could not download {year}: {e}")
        
        return all_files
    
    def get_available_years(self) -> List[int]:
        """Check which years have been downloaded."""
        years = set()
        for filepath in self.data_dir.glob("*.csv"):
            # Extract year from filename
            for year in range(2000, 2030):
                if str(year) in filepath.name:
                    years.add(year)
                    break
        return sorted(years)


class Stats19Transformer:
    """
    Transform STATS19 data for loading into PostgreSQL.
    
    Handles column mapping, data type conversion, coordinate transformation,
    and data validation.
    """
    
    def __init__(self):
        self.collision_mapping = COLLISION_COLUMN_MAPPING
        self.casualty_mapping = CASUALTY_COLUMN_MAPPING
        self.vehicle_mapping = VEHICLE_COLUMN_MAPPING
    
    def _validate_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and filter invalid coordinates."""
        initial_count = len(df)
        
        # Remove rows with missing coordinates
        df = df.dropna(subset=['longitude', 'latitude'])
        
        # Filter to UK bounds
        df = df[
            (df['latitude'] >= UK_BOUNDS['min_lat']) &
            (df['latitude'] <= UK_BOUNDS['max_lat']) &
            (df['longitude'] >= UK_BOUNDS['min_lon']) &
            (df['longitude'] <= UK_BOUNDS['max_lon'])
        ]
        
        removed = initial_count - len(df)
        if removed > 0:
            logger.warning(f"Removed {removed} rows with invalid coordinates")
        
        return df
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats."""
        formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except (ValueError, TypeError):
                continue
        return None
    
    def _parse_time(self, time_str: str) -> Optional[str]:
        """Parse time string to HH:MM format."""
        try:
            if pd.isna(time_str):
                return None
            time_str = str(time_str).strip()
            if ':' in time_str:
                parts = time_str.split(':')
                return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
            return None
        except (ValueError, TypeError):
            return None
    
    def transform_collisions(self, filepath: Path, year_filter: int = None) -> gpd.GeoDataFrame:
        """
        Transform collision/accident data.
        
        Args:
            filepath: Path to CSV file
            year_filter: Optional year to filter (useful for full dataset)
            
        Returns:
            GeoDataFrame with transformed collision data
        """
        logger.info(f"Transforming collisions from {filepath}")
        
        # Read CSV
        df = pd.read_csv(filepath, low_memory=False)
        initial_count = len(df)
        logger.info(f"Read {initial_count} collision records")
        
        # Filter by year if specified
        if year_filter:
            year_col = [c for c in df.columns if 'year' in c.lower()][0]
            df = df[df[year_col] == year_filter]
            logger.info(f"Filtered to {len(df)} records for year {year_filter}")
        
        # Rename columns
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Apply column mapping (only existing columns)
        rename_dict = {k: v for k, v in self.collision_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        # Parse date and time
        if 'accident_date' in df.columns:
            df['accident_date'] = pd.to_datetime(df['accident_date'], format='%d/%m/%Y', errors='coerce')
        
        if 'accident_time' in df.columns:
            df['accident_time'] = df['accident_time'].apply(self._parse_time)
        
        # Validate coordinates
        df = self._validate_coordinates(df)
        
        # Convert easting/northing to integers (they come as floats in the CSV)
        if 'location_easting' in df.columns:
            df['location_easting'] = pd.to_numeric(df['location_easting'], errors='coerce').fillna(0).astype(int)
        if 'location_northing' in df.columns:
            df['location_northing'] = pd.to_numeric(df['location_northing'], errors='coerce').fillna(0).astype(int)
        
        # Create geometry
        geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=CRS_WGS84)
        
        # Rename geometry column to 'geom' for PostGIS
        gdf = gdf.rename_geometry('geom')
        
        # Select and order columns for database
        db_columns = [
            'accident_id', 'accident_year', 'accident_date', 'accident_time',
            'day_of_week', 'longitude', 'latitude', 'location_easting', 
            'location_northing', 'geom', 'lsoa_code', 'police_force',
            'local_authority_district', 'local_authority_highway',
            'severity', 'number_of_vehicles', 'number_of_casualties',
            'first_road_class', 'first_road_number', 'road_type', 'speed_limit',
            'junction_detail', 'junction_control', 'second_road_class',
            'second_road_number', 'pedestrian_crossing_human',
            'pedestrian_crossing_physical', 'light_conditions',
            'weather_conditions', 'road_surface_conditions',
            'special_conditions_at_site', 'carriageway_hazards',
            'urban_or_rural', 'police_attended', 'trunk_road_flag'
        ]
        
        # Keep only columns that exist
        available_cols = [c for c in db_columns if c in gdf.columns]
        gdf = gdf[available_cols]
        
        logger.info(f"Transformed {len(gdf)} collision records")
        return gdf
    
    def transform_casualties(self, filepath: Path, year_filter: int = None) -> pd.DataFrame:
        """
        Transform casualty data.
        
        Args:
            filepath: Path to CSV file
            year_filter: Optional year to filter
            
        Returns:
            DataFrame with transformed casualty data
        """
        logger.info(f"Transforming casualties from {filepath}")
        
        df = pd.read_csv(filepath, low_memory=False)
        initial_count = len(df)
        logger.info(f"Read {initial_count} casualty records")
        
        # Filter by year if specified
        if year_filter:
            year_col = [c for c in df.columns if 'year' in c.lower()][0]
            df = df[df[year_col] == year_filter]
            logger.info(f"Filtered to {len(df)} records for year {year_filter}")
        
        # Rename columns
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        rename_dict = {k: v for k, v in self.casualty_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        logger.info(f"Transformed {len(df)} casualty records")
        return df
    
    def transform_vehicles(self, filepath: Path, year_filter: int = None) -> pd.DataFrame:
        """
        Transform vehicle data.
        
        Args:
            filepath: Path to CSV file
            year_filter: Optional year to filter
            
        Returns:
            DataFrame with transformed vehicle data
        """
        logger.info(f"Transforming vehicles from {filepath}")
        
        df = pd.read_csv(filepath, low_memory=False)
        initial_count = len(df)
        logger.info(f"Read {initial_count} vehicle records")
        
        # Filter by year if specified
        if year_filter:
            year_col = [c for c in df.columns if 'year' in c.lower()][0]
            df = df[df[year_col] == year_filter]
            logger.info(f"Filtered to {len(df)} records for year {year_filter}")
        
        # Rename columns
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        rename_dict = {k: v for k, v in self.vehicle_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        logger.info(f"Transformed {len(df)} vehicle records")
        return df


class Stats19Loader:
    """Load transformed STATS19 data into PostgreSQL."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load_collisions(self, gdf: gpd.GeoDataFrame) -> int:
        """Load collision data to accidents table."""
        return self.db.load_geodataframe(gdf, 'accidents', if_exists='append')
    
    def load_casualties(self, df: pd.DataFrame) -> int:
        """Load casualty data."""
        return self.db.load_dataframe(df, 'casualties', if_exists='append')
    
    def load_vehicles(self, df: pd.DataFrame) -> int:
        """Load vehicle data."""
        return self.db.load_dataframe(df, 'vehicles', if_exists='append')
    
    def clear_year(self, year: int):
        """Remove all data for a specific year."""
        tables = ['vehicles', 'casualties', 'accidents']
        for table in tables:
            sql = f"DELETE FROM {table} WHERE accident_year = {year}"
            self.db.execute_sql(sql)
            logger.info(f"Cleared {table} data for {year}")


def run_stats19_etl(
    years: List[int] = None,
    full_refresh: bool = False,
    download_only: bool = False,
    connection_string: str = None
) -> Dict[str, int]:
    """
    Run the complete STATS19 ETL pipeline.
    
    Args:
        years: List of years to process (None = latest available)
        full_refresh: If True, clear existing data before loading
        download_only: If True, only download files without loading
        connection_string: Database connection URL
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info("Starting STATS19 ETL pipeline")
    
    extractor = Stats19Extractor()
    transformer = Stats19Transformer()
    loader = Stats19Loader(connection_string)
    
    stats = {
        'collisions_loaded': 0,
        'casualties_loaded': 0,
        'vehicles_loaded': 0,
        'years_processed': 0
    }
    
    # Determine years to process
    if years is None:
        # Download latest year
        current_year = datetime.now().year
        years = [current_year - 1]  # Previous year (most likely available)
    
    # Download data
    for year in years:
        logger.info(f"Processing year {year}")
        
        try:
            files = extractor.download_year(year)
            
            if download_only:
                continue
            
            if files.get('collisions') is None:
                logger.warning(f"No collision data found for {year}")
                continue
            
            # Clear existing data if full refresh
            if full_refresh:
                loader.clear_year(year)
            
            # Transform and load collisions
            gdf = transformer.transform_collisions(files['collisions'])
            stats['collisions_loaded'] += loader.load_collisions(gdf)
            
            # Transform and load casualties
            if files.get('casualties'):
                df = transformer.transform_casualties(files['casualties'])
                stats['casualties_loaded'] += loader.load_casualties(df)
            
            # Transform and load vehicles
            if files.get('vehicles'):
                df = transformer.transform_vehicles(files['vehicles'])
                stats['vehicles_loaded'] += loader.load_vehicles(df)
            
            stats['years_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing year {year}: {e}")
            raise
    
    logger.info(f"ETL complete. Stats: {stats}")
    return stats


# CLI interface
@click.command()
@click.option('--years', '-y', multiple=True, type=int, help='Years to process')
@click.option('--start-year', type=int, help='Start year for range')
@click.option('--end-year', type=int, help='End year for range')
@click.option('--full-refresh', is_flag=True, help='Clear existing data before loading')
@click.option('--download-only', is_flag=True, help='Only download files')
@click.option('--latest', is_flag=True, help='Download only latest available year')
def main(years, start_year, end_year, full_refresh, download_only, latest):
    """STATS19 Road Accident Data ETL Pipeline"""
    
    # Configure logging
    logger.add(
        "logs/stats19_etl_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    # Determine years to process
    if years:
        year_list = list(years)
    elif start_year:
        end = end_year or datetime.now().year
        year_list = list(range(start_year, end + 1))
    elif latest:
        year_list = None  # Will use default (latest)
    else:
        # Default: last 3 years
        current = datetime.now().year
        year_list = list(range(current - 3, current))
    
    try:
        stats = run_stats19_etl(
            years=year_list,
            full_refresh=full_refresh,
            download_only=download_only
        )
        
        click.echo(f"\nETL Complete!")
        click.echo(f"  Years processed: {stats['years_processed']}")
        click.echo(f"  Collisions loaded: {stats['collisions_loaded']:,}")
        click.echo(f"  Casualties loaded: {stats['casualties_loaded']:,}")
        click.echo(f"  Vehicles loaded: {stats['vehicles_loaded']:,}")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
