"""
Optimized spatial enrichment using LATERAL joins.
Much faster than the original correlated subquery approach.
"""

import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from loguru import logger
from tqdm import tqdm

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:pass@localhost:5432/roadsafety')


def enrich_nearest_school(batch_size: int = 1000):
    """Enrich accidents with nearest school using efficient LATERAL join."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Get count
        total = conn.execute(text("""
            SELECT COUNT(*) FROM accidents a
            WHERE NOT EXISTS (
                SELECT 1 FROM accident_enrichment e 
                WHERE e.accident_id = a.accident_id
            )
        """)).scalar()
        
        logger.info(f"Found {total} accidents to enrich with school data")
        
        if total == 0:
            return 0
        
        enriched = 0
        with tqdm(total=total, desc="Enriching with schools") as pbar:
            while enriched < total:
                # Use LATERAL join for efficiency
                result = conn.execute(text(f"""
                    INSERT INTO accident_enrichment (
                        accident_id,
                        nearest_school_urn,
                        nearest_school_name,
                        nearest_school_distance_m,
                        nearest_school_phase,
                        lsoa_name
                    )
                    SELECT 
                        a.accident_id,
                        ns.urn,
                        ns.name,
                        ns.distance_m,
                        ns.phase_of_education,
                        lb.lsoa_name
                    FROM accidents a
                    LEFT JOIN lsoa_boundaries lb ON a.lsoa_code = lb.lsoa_code
                    LEFT JOIN LATERAL (
                        SELECT 
                            s.urn,
                            s.name,
                            s.phase_of_education,
                            ROUND(ST_Distance(a.geom::geography, s.geom::geography)::numeric, 0)::INT as distance_m
                        FROM schools s
                        WHERE s.geom IS NOT NULL
                          AND a.geom IS NOT NULL
                          AND ST_DWithin(a.geom::geography, s.geom::geography, 1000)
                        ORDER BY ST_Distance(a.geom::geography, s.geom::geography)
                        LIMIT 1
                    ) ns ON true
                    WHERE NOT EXISTS (
                        SELECT 1 FROM accident_enrichment e 
                        WHERE e.accident_id = a.accident_id
                    )
                    ORDER BY a.accident_id
                    LIMIT {batch_size}
                    ON CONFLICT (accident_id) DO NOTHING
                """))
                conn.commit()
                
                rows = result.rowcount
                if rows == 0:
                    break
                    
                enriched += rows
                pbar.update(rows)
        
        return enriched


def update_traffic_points(batch_size: int = 1000):
    """Update enrichment records with nearest traffic point."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Get count of records needing traffic point update
        total = conn.execute(text("""
            SELECT COUNT(*) FROM accident_enrichment e
            WHERE e.nearest_traffic_point_id IS NULL
        """)).scalar()
        
        logger.info(f"Found {total} records to update with traffic point data")
        
        if total == 0:
            return 0
        
        updated = 0
        with tqdm(total=total, desc="Adding traffic points") as pbar:
            while updated < total:
                result = conn.execute(text(f"""
                    WITH to_update AS (
                        SELECT e.accident_id
                        FROM accident_enrichment e
                        WHERE e.nearest_traffic_point_id IS NULL
                        LIMIT {batch_size}
                    ),
                    nearest AS (
                        SELECT 
                            a.accident_id,
                            nt.count_point_id,
                            nt.distance_m
                        FROM accidents a
                        JOIN to_update tu ON a.accident_id = tu.accident_id
                        LEFT JOIN LATERAL (
                            SELECT 
                                t.count_point_id,
                                ROUND(ST_Distance(a.geom::geography, t.geom::geography)::numeric, 0)::INT as distance_m
                            FROM traffic_count_points t
                            WHERE t.geom IS NOT NULL
                              AND a.geom IS NOT NULL
                              AND ST_DWithin(a.geom::geography, t.geom::geography, 2000)
                            ORDER BY ST_Distance(a.geom::geography, t.geom::geography)
                            LIMIT 1
                        ) nt ON true
                    )
                    UPDATE accident_enrichment e
                    SET 
                        nearest_traffic_point_id = n.count_point_id,
                        nearest_traffic_point_distance_m = n.distance_m
                    FROM nearest n
                    WHERE e.accident_id = n.accident_id
                """))
                conn.commit()
                
                rows = result.rowcount
                if rows == 0:
                    break
                    
                updated += rows
                pbar.update(rows)
        
        return updated


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


def run_full_enrichment(batch_size: int = 1000):
    """Run full enrichment process."""
    logger.info("Starting optimized enrichment process")
    
    # Step 1: Enrich with nearest school
    school_count = enrich_nearest_school(batch_size)
    logger.info(f"Enriched {school_count} accidents with school data")
    
    # Step 2: Add traffic point data
    traffic_count = update_traffic_points(batch_size)
    logger.info(f"Updated {traffic_count} records with traffic point data")
    
    # Get stats
    stats = get_enrichment_stats()
    logger.info(f"Enrichment complete: {stats}")
    
    return stats


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Optimized spatial enrichment')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size')
    parser.add_argument('--stats', action='store_true', help='Show enrichment stats only')
    args = parser.parse_args()
    
    if args.stats:
        stats = get_enrichment_stats()
        print("\n=== Enrichment Statistics ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    else:
        stats = run_full_enrichment(batch_size=args.batch_size)
        print("\n=== Final Enrichment Statistics ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
