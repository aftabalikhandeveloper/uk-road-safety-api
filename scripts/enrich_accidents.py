"""
Spatial enrichment script for accidents data.

Computes nearest school and traffic point for each accident.
Uses batch processing for efficiency.
"""

import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from loguru import logger
from tqdm import tqdm

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:pass@localhost:5432/roadsafety')


def enrich_accidents_batch(batch_size: int = 10000, year: int = None):
    """
    Enrich accidents with nearest school and traffic point data.
    
    Args:
        batch_size: Number of records to process at once
        year: Optional year filter
    """
    engine = create_engine(DATABASE_URL)
    
    # Get count of accidents to enrich
    year_filter = f"AND a.accident_year = {year}" if year else ""
    
    with engine.connect() as conn:
        count_sql = f"""
            SELECT COUNT(*) FROM accidents a
            WHERE NOT EXISTS (
                SELECT 1 FROM accident_enrichment e 
                WHERE e.accident_id = a.accident_id
            )
            {year_filter}
        """
        total = conn.execute(text(count_sql)).scalar()
        logger.info(f"Found {total} accidents to enrich")
        
        if total == 0:
            logger.info("No accidents to enrich")
            return
        
        # Process in batches
        offset = 0
        with tqdm(total=total, desc="Enriching accidents") as pbar:
            while offset < total:
                enrich_sql = f"""
                    INSERT INTO accident_enrichment (
                        accident_id,
                        nearest_school_urn,
                        nearest_school_name,
                        nearest_school_distance_m,
                        nearest_school_phase,
                        nearest_traffic_point_id,
                        nearest_traffic_point_distance_m,
                        lsoa_name,
                        local_authority_name
                    )
                    SELECT 
                        a.accident_id,
                        -- Nearest school within 1km
                        (SELECT s.urn FROM schools s 
                         WHERE s.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, s.geom::geography, 1000)
                         ORDER BY ST_Distance(a.geom::geography, s.geom::geography) LIMIT 1),
                        (SELECT s.name FROM schools s 
                         WHERE s.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, s.geom::geography, 1000)
                         ORDER BY ST_Distance(a.geom::geography, s.geom::geography) LIMIT 1),
                        (SELECT ROUND(ST_Distance(a.geom::geography, s.geom::geography)::numeric, 0)::INT
                         FROM schools s 
                         WHERE s.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, s.geom::geography, 1000)
                         ORDER BY ST_Distance(a.geom::geography, s.geom::geography) LIMIT 1),
                        (SELECT s.phase_of_education FROM schools s 
                         WHERE s.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, s.geom::geography, 1000)
                         ORDER BY ST_Distance(a.geom::geography, s.geom::geography) LIMIT 1),
                        -- Nearest traffic count point within 2km
                        (SELECT t.count_point_id FROM traffic_count_points t
                         WHERE t.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, t.geom::geography, 2000)
                         ORDER BY ST_Distance(a.geom::geography, t.geom::geography) LIMIT 1),
                        (SELECT ROUND(ST_Distance(a.geom::geography, t.geom::geography)::numeric, 0)::INT
                         FROM traffic_count_points t
                         WHERE t.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, t.geom::geography, 2000)
                         ORDER BY ST_Distance(a.geom::geography, t.geom::geography) LIMIT 1),
                        -- LSOA info
                        lb.lsoa_name,
                        -- Get local authority from schools table for area
                        (SELECT s.local_authority_name FROM schools s 
                         WHERE s.geom IS NOT NULL AND a.geom IS NOT NULL
                         AND ST_DWithin(a.geom::geography, s.geom::geography, 5000)
                         ORDER BY ST_Distance(a.geom::geography, s.geom::geography) LIMIT 1)
                    FROM accidents a
                    LEFT JOIN lsoa_boundaries lb ON a.lsoa_code = lb.lsoa_code
                    WHERE NOT EXISTS (
                        SELECT 1 FROM accident_enrichment e 
                        WHERE e.accident_id = a.accident_id
                    )
                    {year_filter}
                    ORDER BY a.accident_id
                    LIMIT {batch_size}
                    ON CONFLICT (accident_id) DO NOTHING
                """
                
                result = conn.execute(text(enrich_sql))
                conn.commit()
                rows_inserted = result.rowcount
                pbar.update(rows_inserted)
                offset += rows_inserted
                
                if rows_inserted == 0:
                    break
                    
                logger.debug(f"Processed batch: {offset}/{total}")
    
    logger.info(f"Enrichment complete: {offset} accidents enriched")


def get_enrichment_stats():
    """Get statistics about enrichment coverage."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        stats_sql = """
            SELECT 
                COUNT(*) as total_enriched,
                COUNT(nearest_school_urn) as with_school,
                COUNT(nearest_traffic_point_id) as with_traffic_point,
                ROUND(100.0 * COUNT(nearest_school_urn) / NULLIF(COUNT(*), 0), 1) as school_coverage_pct,
                ROUND(100.0 * COUNT(nearest_traffic_point_id) / NULLIF(COUNT(*), 0), 1) as traffic_coverage_pct,
                ROUND(AVG(nearest_school_distance_m), 0) as avg_school_distance_m,
                ROUND(AVG(nearest_traffic_point_distance_m), 0) as avg_traffic_distance_m
            FROM accident_enrichment
        """
        result = conn.execute(text(stats_sql)).fetchone()
        return dict(result._mapping)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Enrich accidents with spatial data')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size')
    parser.add_argument('--year', type=int, help='Filter by year')
    parser.add_argument('--stats', action='store_true', help='Show enrichment stats')
    args = parser.parse_args()
    
    if args.stats:
        stats = get_enrichment_stats()
        print("\n=== Enrichment Statistics ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    else:
        enrich_accidents_batch(batch_size=args.batch_size, year=args.year)
        stats = get_enrichment_stats()
        print("\n=== Enrichment Statistics ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
