"""Quick script to load casualties and vehicles for 2020-2023"""
import sys
from pathlib import Path
from etl.sources.stats19 import Stats19Extractor, Stats19Transformer, Stats19Loader
from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")

def load_casualties_vehicles_only(years):
    """Load only casualties and vehicles for specified years"""
    extractor = Stats19Extractor()
    transformer = Stats19Transformer()
    loader = Stats19Loader()
    
    for year in years:
        logger.info(f"Processing year {year}")
        
        try:
            # Download files if needed
            extractor.download_year(year)
            
            # Transform and load casualties
            logger.info(f"Loading casualties for {year}")
            casualties_file = Path(f'data/raw/dft-road-casualty-statistics-casualty-{year}.csv')
            casualties_df = transformer.transform_casualties(casualties_file)
            casualties_count = loader.load_casualties(casualties_df)
            logger.info(f"Loaded {casualties_count} casualties for {year}")
            
            # Transform and load vehicles
            logger.info(f"Loading vehicles for {year}")
            vehicles_file = Path(f'data/raw/dft-road-casualty-statistics-vehicle-{year}.csv')
            vehicles_df = transformer.transform_vehicles(vehicles_file)
            vehicles_count = loader.load_vehicles(vehicles_df)
            logger.info(f"Loaded {vehicles_count} vehicles for {year}")
            
        except Exception as e:
            logger.error(f"Error processing year {year}: {e}")
            continue
    
    logger.info("Complete!")

if __name__ == '__main__':
    years = [2020, 2021, 2022, 2023]
    load_casualties_vehicles_only(years)
