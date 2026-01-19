#!/usr/bin/env python
"""
UK Road Safety Platform - Main ETL Entry Point

Run the complete ETL pipeline or individual sources.
"""

import click
from loguru import logger
from datetime import datetime
from pathlib import Path
import sys

from .config import LOG_DIR, DATABASE_URL
from .sources import (
    run_stats19_etl,
    run_traffic_counts_etl,
    run_geography_etl,
    run_schools_etl
)
from .loaders.postgres import PostgresLoader


def setup_logging(log_level: str = "INFO"):
    """Configure logging for ETL runs."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
    )
    
    # Add file handler
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
    logger.add(
        log_file,
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
    )
    
    logger.info(f"Logging configured. Log file: {log_file}")


@click.group()
@click.option('--log-level', default='INFO', help='Logging level')
@click.pass_context
def cli(ctx, log_level):
    """UK Road Safety Platform ETL CLI"""
    ctx.ensure_object(dict)
    setup_logging(log_level)


@cli.command()
@click.option('--years', '-y', multiple=True, type=int, help='Specific years to process')
@click.option('--start-year', type=int, default=2020, help='Start year for range')
@click.option('--end-year', type=int, help='End year for range (default: current year)')
@click.option('--full-refresh', is_flag=True, help='Clear existing data before loading')
def stats19(years, start_year, end_year, full_refresh):
    """Load STATS19 road accident data"""
    logger.info("Starting STATS19 ETL")
    
    if years:
        year_list = list(years)
    else:
        end = end_year or datetime.now().year
        year_list = list(range(start_year, end + 1))
    
    click.echo(f"Processing years: {year_list}")
    
    stats = run_stats19_etl(
        years=year_list,
        full_refresh=full_refresh
    )
    
    click.echo(f"\n✅ STATS19 ETL Complete:")
    click.echo(f"   Years processed: {stats['years_processed']}")
    click.echo(f"   Collisions: {stats['collisions_loaded']:,}")
    click.echo(f"   Casualties: {stats['casualties_loaded']:,}")
    click.echo(f"   Vehicles: {stats['vehicles_loaded']:,}")


@cli.command()
@click.option('--lsoa/--no-lsoa', default=True, help='Load LSOA boundaries')
@click.option('--la/--no-la', default=False, help='Load Local Authority boundaries')
@click.option('--police/--no-police', default=False, help='Load Police Force boundaries')
def geography(lsoa, la, police):
    """Load geographic boundaries from ONS"""
    logger.info("Starting Geography ETL")
    
    boundary_types = []
    if lsoa:
        boundary_types.append('lsoa')
    if la:
        boundary_types.append('la')
    if police:
        boundary_types.append('police')
    
    if not boundary_types:
        click.echo("No boundary types selected. Use --lsoa, --la, or --police")
        return
    
    stats = run_geography_etl(boundary_types=boundary_types)
    
    click.echo(f"\n✅ Geography ETL Complete:")
    for boundary_type, count in stats.items():
        click.echo(f"   {boundary_type}: {count:,}")


@cli.command()
def traffic():
    """Load traffic count data from DfT"""
    logger.info("Starting Traffic Counts ETL")
    
    stats = run_traffic_counts_etl()
    
    click.echo(f"\n✅ Traffic Counts ETL Complete:")
    click.echo(f"   Count points: {stats.get('count_points', 0):,}")
    click.echo(f"   AADF records: {stats.get('aadf_records', 0):,}")


@cli.command()
@click.option('--file', '-f', type=click.Path(exists=True), help='Local CSV file')
def schools(file):
    """Load schools data from GIAS"""
    logger.info("Starting Schools ETL")
    
    stats = run_schools_etl(filepath=file)
    
    click.echo(f"\n✅ Schools ETL Complete:")
    click.echo(f"   Schools loaded: {stats['schools_loaded']:,}")


@cli.command()
@click.option('--stats19-years', default='2020-2024', help='Year range for STATS19')
@click.option('--skip-geography', is_flag=True, help='Skip geographic boundaries')
@click.option('--skip-traffic', is_flag=True, help='Skip traffic counts')
@click.option('--skip-schools', is_flag=True, help='Skip schools data')
def full(stats19_years, skip_geography, skip_traffic, skip_schools):
    """Run full ETL pipeline (all sources)"""
    logger.info("Starting Full ETL Pipeline")
    
    click.echo("=" * 60)
    click.echo("UK Road Safety Platform - Full ETL Pipeline")
    click.echo("=" * 60)
    
    results = {}
    
    # Parse year range
    if '-' in stats19_years:
        start, end = stats19_years.split('-')
        years = list(range(int(start), int(end) + 1))
    else:
        years = [int(stats19_years)]
    
    # 1. STATS19 (always run)
    click.echo("\n📊 [1/4] Loading STATS19 accident data...")
    try:
        results['stats19'] = run_stats19_etl(years=years)
        click.echo(f"   ✅ Loaded {results['stats19']['collisions_loaded']:,} accidents")
    except Exception as e:
        click.echo(f"   ❌ Failed: {e}")
        results['stats19'] = {'error': str(e)}
    
    # 2. Geography
    if not skip_geography:
        click.echo("\n🗺️  [2/4] Loading geographic boundaries...")
        try:
            results['geography'] = run_geography_etl(boundary_types=['lsoa'])
            click.echo(f"   ✅ Loaded {results['geography'].get('lsoa', 0):,} LSOA boundaries")
        except Exception as e:
            click.echo(f"   ❌ Failed: {e}")
            results['geography'] = {'error': str(e)}
    else:
        click.echo("\n🗺️  [2/4] Skipping geographic boundaries")
    
    # 3. Traffic
    if not skip_traffic:
        click.echo("\n🚗 [3/4] Loading traffic count data...")
        try:
            results['traffic'] = run_traffic_counts_etl()
            click.echo(f"   ✅ Loaded {results['traffic'].get('count_points', 0):,} count points")
        except Exception as e:
            click.echo(f"   ❌ Failed: {e}")
            results['traffic'] = {'error': str(e)}
    else:
        click.echo("\n🚗 [3/4] Skipping traffic counts")
    
    # 4. Schools
    if not skip_schools:
        click.echo("\n🏫 [4/4] Loading schools data...")
        try:
            results['schools'] = run_schools_etl()
            click.echo(f"   ✅ Loaded {results['schools']['schools_loaded']:,} schools")
        except Exception as e:
            click.echo(f"   ❌ Failed: {e}")
            results['schools'] = {'error': str(e)}
    else:
        click.echo("\n🏫 [4/4] Skipping schools")
    
    # Summary
    click.echo("\n" + "=" * 60)
    click.echo("ETL Pipeline Complete!")
    click.echo("=" * 60)
    
    for source, stats in results.items():
        if 'error' in stats:
            click.echo(f"❌ {source}: FAILED - {stats['error']}")
        else:
            click.echo(f"✅ {source}: SUCCESS")


@cli.command()
def test_connection():
    """Test database connection"""
    click.echo(f"Testing connection to: {DATABASE_URL[:50]}...")
    
    loader = PostgresLoader()
    if loader.test_connection():
        click.echo("✅ Database connection successful!")
        
        # Show table counts
        tables = ['accidents', 'casualties', 'vehicles', 'lsoa_boundaries', 'schools']
        click.echo("\nTable row counts:")
        for table in tables:
            try:
                count = loader.get_row_count(table)
                click.echo(f"  {table}: {count:,}")
            except:
                click.echo(f"  {table}: (table not found)")
    else:
        click.echo("❌ Database connection failed!")
        raise SystemExit(1)


@cli.command()
@click.option('--year', type=int, help='Year to refresh (default: all)')
def refresh_statistics(year):
    """Refresh pre-computed statistics tables"""
    logger.info("Refreshing statistics")
    
    loader = PostgresLoader()
    
    # Refresh LSOA statistics
    if year:
        sql = f"SELECT refresh_lsoa_statistics({year})"
    else:
        sql = "SELECT refresh_lsoa_statistics()"
    
    try:
        result = loader.execute_sql(sql)
        click.echo(f"✅ Refreshed LSOA statistics")
    except Exception as e:
        click.echo(f"❌ Failed to refresh statistics: {e}")


if __name__ == '__main__':
    cli()
