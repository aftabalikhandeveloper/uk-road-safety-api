"""
Schools Data ETL (GIAS - Get Information About Schools)

Downloads school location data from the Department for Education.
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path
from typing import Optional, Dict
from shapely.geometry import Point
from loguru import logger
import click
import zipfile
import io

from ..config import DATA_DIR, CRS_WGS84
from ..loaders.postgres import PostgresLoader


class SchoolsExtractor:
    """
    Extract school data from GIAS (Get Information About Schools).
    
    The DfE provides a downloadable extract of all schools in England.
    """
    
    # Direct download URL for school data
    # Note: This may require updating - check GIAS website for current URL
    DOWNLOAD_URL = "https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata{date}.csv"
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = Path(data_dir or DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
    
    def download_schools_data(self) -> Path:
        """
        Download schools data from GIAS.
        
        Returns:
            Path to downloaded file
        """
        # Try to get current data - GIAS updates daily
        from datetime import datetime
        
        # The URL format may vary - trying a few options
        urls_to_try = [
            "https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata.csv",
            f"https://get-information-schools.service.gov.uk/Downloads/edubasealldata.csv",
        ]
        
        filepath = self.data_dir / "schools_data.csv"
        
        for url in urls_to_try:
            try:
                logger.info(f"Attempting download from {url}")
                response = self.session.get(url, timeout=120)
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Downloaded schools data to {filepath}")
                return filepath
                
            except requests.RequestException as e:
                logger.warning(f"Failed to download from {url}: {e}")
                continue
        
        raise Exception("Could not download schools data from any source")
    
    def load_local_file(self, filepath: Path) -> pd.DataFrame:
        """Load schools data from a local CSV file."""
        logger.info(f"Loading schools data from {filepath}")
        # Try different encodings - GIAS files often use Windows encoding
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                return pd.read_csv(filepath, encoding=encoding, low_memory=False)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Could not decode file {filepath} with any known encoding")


class SchoolsTransformer:
    """Transform schools data for database loading."""
    
    def transform(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """
        Transform schools data to GeoDataFrame.
        
        Args:
            df: Raw schools DataFrame
            
        Returns:
            GeoDataFrame with school locations
        """
        logger.info(f"Transforming {len(df)} school records")
        
        # Column mapping - GIAS uses various column names
        column_mapping = {
            'URN': 'urn',
            'EstablishmentName': 'name',
            'TypeOfEstablishment (name)': 'establishment_type',
            'EstablishmentTypeGroup (name)': 'establishment_type_group',
            'PhaseOfEducation (name)': 'phase_of_education',
            'StatutoryLowAge': 'statutory_low_age',
            'StatutoryHighAge': 'statutory_high_age',
            'Street': 'street',
            'Locality': 'locality',
            'Town': 'town',
            'County (name)': 'county',
            'Postcode': 'postcode',
            'LA (code)': 'local_authority_code',
            'LA (name)': 'local_authority_name',
            'NumberOfPupils': 'number_of_pupils',
            'EstablishmentStatus (name)': 'establishment_status',
            'OpenDate': 'open_date',
            'CloseDate': 'close_date',
            'Easting': 'easting',
            'Northing': 'northing',
            'UPRN': 'uprn'
        }
        
        # Rename columns
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Filter to open schools only
        if 'establishment_status' in df.columns:
            df = df[df['establishment_status'] == 'Open']
            logger.info(f"Filtered to {len(df)} open schools")
        
        # Handle coordinates
        # GIAS provides Easting/Northing (OSGB36) - need to convert to WGS84
        if 'easting' in df.columns and 'northing' in df.columns:
            # Filter rows with valid coordinates
            df = df.dropna(subset=['easting', 'northing'])
            df = df[(df['easting'] > 0) & (df['northing'] > 0)]
            
            # Create geometry from British National Grid
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
            
            coords = transformer.transform(
                df['easting'].values, 
                df['northing'].values
            )
            df['longitude'] = coords[0]
            df['latitude'] = coords[1]
        
        # Create geometry
        if 'longitude' in df.columns and 'latitude' in df.columns:
            geometry = [
                Point(xy) if pd.notna(xy[0]) and pd.notna(xy[1]) else None
                for xy in zip(df['longitude'], df['latitude'])
            ]
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=CRS_WGS84)
        else:
            logger.warning("No coordinate columns found - creating non-spatial DataFrame")
            gdf = gpd.GeoDataFrame(df)
        
        # Rename geometry column
        if 'geometry' in gdf.columns:
            gdf = gdf.rename_geometry('geom')
        
        # Select columns for database
        keep_cols = [
            'urn', 'name', 'establishment_type', 'establishment_type_group',
            'phase_of_education', 'statutory_low_age', 'statutory_high_age',
            'street', 'locality', 'town', 'county', 'postcode',
            'longitude', 'latitude', 'geom',
            'local_authority_code', 'local_authority_name',
            'number_of_pupils', 'establishment_status', 'open_date', 'close_date'
        ]
        available_cols = [c for c in keep_cols if c in gdf.columns]
        gdf = gdf[available_cols]
        
        # Remove rows without geometry
        if 'geom' in gdf.columns:
            gdf = gdf[gdf['geom'].notna()]
        
        logger.info(f"Transformed {len(gdf)} schools with valid geometry")
        return gdf


class SchoolsLoader:
    """Load schools data to database."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load(self, gdf: gpd.GeoDataFrame, replace: bool = True) -> int:
        """Load schools to database."""
        if_exists = 'replace' if replace else 'append'
        
        if 'geom' in gdf.columns and gdf['geom'].notna().any():
            return self.db.load_geodataframe(
                gdf, 'schools',
                if_exists=if_exists,
                geometry_column='geom'
            )
        else:
            return self.db.load_dataframe(
                gdf.drop(columns=['geom'], errors='ignore'),
                'schools',
                if_exists=if_exists
            )


def run_schools_etl(
    filepath: Path = None,
    download: bool = True,
    connection_string: str = None
) -> Dict[str, int]:
    """
    Run schools ETL pipeline.
    
    Args:
        filepath: Path to local CSV file (optional)
        download: Whether to download fresh data
        connection_string: Database connection URL
        
    Returns:
        Processing statistics
    """
    logger.info("Starting Schools ETL")
    
    extractor = SchoolsExtractor()
    transformer = SchoolsTransformer()
    loader = SchoolsLoader(connection_string)
    
    stats = {'schools_loaded': 0}
    
    try:
        # Get data
        if filepath and Path(filepath).exists():
            df = extractor.load_local_file(Path(filepath))
        elif download:
            csv_path = extractor.download_schools_data()
            df = extractor.load_local_file(csv_path)
        else:
            raise ValueError("No data source specified")
        
        # Transform
        gdf = transformer.transform(df)
        
        # Load
        stats['schools_loaded'] = loader.load(gdf, replace=True)
        
    except Exception as e:
        logger.error(f"Schools ETL failed: {e}")
        raise
    
    logger.info(f"Schools ETL complete: {stats}")
    return stats


@click.command()
@click.option('--file', '-f', type=click.Path(exists=True), help='Local CSV file')
@click.option('--download/--no-download', default=True, help='Download fresh data')
def main(file, download):
    """Schools (GIAS) ETL Pipeline"""
    
    stats = run_schools_etl(filepath=file, download=download)
    click.echo(f"Complete: {stats}")


if __name__ == '__main__':
    main()
