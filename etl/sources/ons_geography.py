"""
ONS Geography ETL

Downloads LSOA and other geographic boundaries from the
ONS Open Geography Portal.
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path
from typing import Optional, List, Dict
from loguru import logger
from tqdm import tqdm
import click
import time

from ..config import DATA_DIR, ONS_LSOA_ENDPOINT, CRS_WGS84
from ..loaders.postgres import PostgresLoader


class ONSGeographyExtractor:
    """
    Extract geographic boundaries from ONS Open Geography Portal.
    
    Uses the ArcGIS REST API to query features.
    """
    
    # Endpoints for different boundary types
    ENDPOINTS = {
        'lsoa_2021': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query',
        'lsoa_2011': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_Layer_Super_Output_Areas_December_2011_Boundaries_EW_BFC/FeatureServer/0/query',
        'msoa_2021': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V7/FeatureServer/0/query',
        'lad_2024': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2024_Boundaries_UK_BFE/FeatureServer/0/query',
        'police_force': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Police_Force_Areas_December_2023_EW_BFC/FeatureServer/0/query'
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RoadSafetyETL/1.0'
        })
    
    def _get_record_count(self, endpoint: str) -> int:
        """Get total record count for an endpoint."""
        params = {
            'where': '1=1',
            'returnCountOnly': 'true',
            'f': 'json'
        }
        response = self.session.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get('count', 0)
    
    def fetch_boundaries(
        self, 
        boundary_type: str,
        batch_size: int = 1000,
        where_clause: str = '1=1'
    ) -> gpd.GeoDataFrame:
        """
        Fetch boundaries from ONS ArcGIS API.
        
        Args:
            boundary_type: Type of boundary ('lsoa_2021', 'lsoa_2011', etc.)
            batch_size: Number of features per request
            where_clause: SQL WHERE clause to filter features
            
        Returns:
            GeoDataFrame with boundaries
        """
        endpoint = self.ENDPOINTS.get(boundary_type)
        if not endpoint:
            raise ValueError(f"Unknown boundary type: {boundary_type}")
        
        # Get total count
        total_count = self._get_record_count(endpoint)
        logger.info(f"Fetching {total_count} {boundary_type} boundaries")
        
        all_features = []
        offset = 0
        
        with tqdm(total=total_count, desc=f"Downloading {boundary_type}") as pbar:
            while offset < total_count:
                params = {
                    'where': where_clause,
                    'outFields': '*',
                    'returnGeometry': 'true',
                    'f': 'geojson',
                    'resultRecordCount': batch_size,
                    'resultOffset': offset
                }
                
                try:
                    response = self.session.get(endpoint, params=params, timeout=120)
                    response.raise_for_status()
                    
                    data = response.json()
                    features = data.get('features', [])
                    
                    if not features:
                        break
                    
                    all_features.extend(features)
                    offset += len(features)
                    pbar.update(len(features))
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
                except requests.RequestException as e:
                    logger.error(f"Request failed at offset {offset}: {e}")
                    time.sleep(5)
                    continue
        
        # Convert to GeoDataFrame
        if all_features:
            geojson = {
                'type': 'FeatureCollection',
                'features': all_features
            }
            gdf = gpd.GeoDataFrame.from_features(geojson, crs=CRS_WGS84)
            logger.info(f"Downloaded {len(gdf)} {boundary_type} boundaries")
            return gdf
        
        return gpd.GeoDataFrame()
    
    def fetch_lsoa_for_region(self, region_code: str) -> gpd.GeoDataFrame:
        """Fetch LSOAs for a specific region."""
        where_clause = f"LSOA21CD LIKE '{region_code}%'"
        return self.fetch_boundaries('lsoa_2021', where_clause=where_clause)


class ONSGeographyTransformer:
    """Transform ONS geographic data for database loading."""
    
    def transform_lsoa(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Transform LSOA boundaries."""
        logger.info(f"Transforming {len(gdf)} LSOA boundaries")
        
        # Column mapping for LSOA 2021
        column_mapping = {
            'LSOA21CD': 'lsoa_code',
            'LSOA21NM': 'lsoa_name',
            'LSOA21NMW': 'lsoa_name_welsh',
            'BNG_E': 'easting',
            'BNG_N': 'northing',
            'LONG': 'longitude',
            'LAT': 'latitude',
            'Shape__Area': 'area_sq_m',
            'Shape__Length': 'perimeter_m',
            'GlobalID': 'global_id'
        }
        
        # Also handle 2011 column names
        column_mapping.update({
            'LSOA11CD': 'lsoa_code',
            'LSOA11NM': 'lsoa_name',
            'LSOA11NMW': 'lsoa_name_welsh'
        })
        
        gdf = gdf.rename(columns={k: v for k, v in column_mapping.items() if k in gdf.columns})
        
        # Convert area to hectares
        if 'area_sq_m' in gdf.columns:
            gdf['area_hectares'] = gdf['area_sq_m'] / 10000
        
        # Rename geometry
        gdf = gdf.rename_geometry('geom')
        
        # Select relevant columns
        keep_cols = ['lsoa_code', 'lsoa_name', 'lsoa_name_welsh', 'area_hectares', 'geom']
        available_cols = [c for c in keep_cols if c in gdf.columns]
        gdf = gdf[available_cols]
        
        return gdf
    
    def transform_local_authority(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Transform Local Authority District boundaries."""
        logger.info(f"Transforming {len(gdf)} LA boundaries")
        
        column_mapping = {
            'LAD24CD': 'la_code',
            'LAD24NM': 'la_name',
            'LAD24NMW': 'la_name_welsh',
            'BNG_E': 'easting',
            'BNG_N': 'northing'
        }
        
        gdf = gdf.rename(columns={k: v for k, v in column_mapping.items() if k in gdf.columns})
        gdf = gdf.rename_geometry('geom')
        
        keep_cols = ['la_code', 'la_name', 'geom']
        available_cols = [c for c in keep_cols if c in gdf.columns]
        
        return gdf[available_cols]
    
    def transform_police_force(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Transform Police Force Area boundaries."""
        logger.info(f"Transforming {len(gdf)} police force boundaries")
        
        column_mapping = {
            'PFA23CD': 'police_force_code',
            'PFA23NM': 'police_force_name'
        }
        
        gdf = gdf.rename(columns={k: v for k, v in column_mapping.items() if k in gdf.columns})
        gdf = gdf.rename_geometry('geom')
        
        return gdf


class ONSGeographyLoader:
    """Load geographic boundaries to database."""
    
    def __init__(self, connection_string: str = None):
        self.db = PostgresLoader(connection_string)
    
    def load_lsoa(self, gdf: gpd.GeoDataFrame, replace: bool = True) -> int:
        """Load LSOA boundaries."""
        if_exists = 'replace' if replace else 'append'
        return self.db.load_geodataframe(
            gdf, 'lsoa_boundaries', 
            if_exists=if_exists,
            geometry_column='geom'
        )
    
    def load_local_authority(self, gdf: gpd.GeoDataFrame, replace: bool = True) -> int:
        """Load LA boundaries."""
        if_exists = 'replace' if replace else 'append'
        return self.db.load_geodataframe(
            gdf, 'local_authority_boundaries',
            if_exists=if_exists,
            geometry_column='geom'
        )
    
    def load_police_force(self, gdf: gpd.GeoDataFrame, replace: bool = True) -> int:
        """Load police force boundaries."""
        if_exists = 'replace' if replace else 'append'
        return self.db.load_geodataframe(
            gdf, 'police_force_boundaries',
            if_exists=if_exists,
            geometry_column='geom'
        )


def run_geography_etl(
    boundary_types: List[str] = None,
    connection_string: str = None
) -> Dict[str, int]:
    """
    Run ONS Geography ETL pipeline.
    
    Args:
        boundary_types: List of types to download ('lsoa', 'la', 'police')
        connection_string: Database connection URL
        
    Returns:
        Processing statistics
    """
    logger.info("Starting ONS Geography ETL")
    
    if boundary_types is None:
        boundary_types = ['lsoa']
    
    extractor = ONSGeographyExtractor()
    transformer = ONSGeographyTransformer()
    loader = ONSGeographyLoader(connection_string)
    
    stats = {}
    
    try:
        if 'lsoa' in boundary_types:
            logger.info("Processing LSOA boundaries...")
            gdf = extractor.fetch_boundaries('lsoa_2021')
            gdf = transformer.transform_lsoa(gdf)
            stats['lsoa'] = loader.load_lsoa(gdf)
        
        if 'la' in boundary_types:
            logger.info("Processing Local Authority boundaries...")
            gdf = extractor.fetch_boundaries('lad_2024')
            gdf = transformer.transform_local_authority(gdf)
            stats['local_authority'] = loader.load_local_authority(gdf)
        
        if 'police' in boundary_types:
            logger.info("Processing Police Force boundaries...")
            gdf = extractor.fetch_boundaries('police_force')
            gdf = transformer.transform_police_force(gdf)
            stats['police_force'] = loader.load_police_force(gdf)
            
    except Exception as e:
        logger.error(f"Geography ETL failed: {e}")
        raise
    
    logger.info(f"Geography ETL complete: {stats}")
    return stats


@click.command()
@click.option('--lsoa/--no-lsoa', default=True, help='Download LSOA boundaries')
@click.option('--la/--no-la', default=False, help='Download Local Authority boundaries')
@click.option('--police/--no-police', default=False, help='Download Police Force boundaries')
def main(lsoa, la, police):
    """ONS Geography ETL Pipeline"""
    
    boundary_types = []
    if lsoa:
        boundary_types.append('lsoa')
    if la:
        boundary_types.append('la')
    if police:
        boundary_types.append('police')
    
    if not boundary_types:
        click.echo("No boundary types selected")
        return
    
    stats = run_geography_etl(boundary_types=boundary_types)
    click.echo(f"Complete: {stats}")


if __name__ == '__main__':
    main()
