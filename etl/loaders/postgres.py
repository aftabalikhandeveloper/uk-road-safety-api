"""
PostgreSQL/PostGIS data loader utilities
"""

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from typing import Optional, List, Dict, Any
from loguru import logger
from datetime import datetime

from ..config import DATABASE_URL, ETL_BATCH_SIZE


class PostgresLoader:
    """
    Utility class for loading data into PostgreSQL/PostGIS database.
    """
    
    def __init__(self, connection_string: str = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection URL
        """
        self.connection_string = connection_string or DATABASE_URL
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def execute_sql(self, sql: str, params: Dict = None) -> Any:
        """Execute raw SQL query."""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result
    
    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'append',
        index: bool = False,
        batch_size: int = None
    ) -> int:
        """
        Load a pandas DataFrame to a database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index
            batch_size: Number of rows per batch
            
        Returns:
            Number of rows loaded
        """
        batch_size = batch_size or ETL_BATCH_SIZE
        total_rows = len(df)
        
        logger.info(f"Loading {total_rows} rows to {table_name}")
        
        try:
            # Load in batches for large datasets
            if total_rows > batch_size:
                rows_loaded = 0
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:i + batch_size]
                    batch.to_sql(
                        table_name, 
                        self.engine, 
                        if_exists='append' if i > 0 or if_exists == 'append' else if_exists,
                        index=index,
                        method='multi'
                    )
                    rows_loaded += len(batch)
                    logger.debug(f"Loaded batch {i // batch_size + 1}: {rows_loaded}/{total_rows} rows")
            else:
                df.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists=if_exists,
                    index=index,
                    method='multi'
                )
            
            logger.info(f"Successfully loaded {total_rows} rows to {table_name}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {e}")
            raise
    
    def load_geodataframe(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        if_exists: str = 'append',
        index: bool = False,
        geometry_column: str = 'geom',
        srid: int = 4326
    ) -> int:
        """
        Load a GeoDataFrame to a PostGIS table.
        
        Args:
            gdf: GeoDataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists
            index: Whether to write DataFrame index
            geometry_column: Name of geometry column
            srid: Spatial reference ID
            
        Returns:
            Number of rows loaded
        """
        total_rows = len(gdf)
        logger.info(f"Loading {total_rows} rows with geometry to {table_name}")
        
        try:
            # Ensure geometry column name
            if gdf.geometry.name != geometry_column:
                gdf = gdf.rename_geometry(geometry_column)
            
            # Set CRS if not set
            if gdf.crs is None:
                gdf = gdf.set_crs(f"EPSG:{srid}")
            
            # Load to PostGIS
            gdf.to_postgis(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index,
                dtype={geometry_column: Geometry('POINT', srid=srid)}
            )
            
            logger.info(f"Successfully loaded {total_rows} rows to {table_name}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Failed to load geodata to {table_name}: {e}")
            raise
    
    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_key: str,
        update_columns: List[str] = None
    ) -> Dict[str, int]:
        """
        Upsert (insert or update) data using conflict resolution.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            primary_key: Primary key column name
            update_columns: Columns to update on conflict (None = all)
            
        Returns:
            Dictionary with 'inserted' and 'updated' counts
        """
        if update_columns is None:
            update_columns = [c for c in df.columns if c != primary_key]
        
        # Create temp table
        temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Load to temp table
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Build upsert SQL
            columns = ', '.join(df.columns)
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_sql = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({primary_key})
                DO UPDATE SET {update_set}
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(upsert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
            
            logger.info(f"Upserted {len(df)} rows to {table_name}")
            return {'processed': len(df)}
            
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            # Cleanup temp table
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
            raise
    
    def truncate_table(self, table_name: str, cascade: bool = False):
        """Truncate a table."""
        cascade_sql = " CASCADE" if cascade else ""
        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}{cascade_sql}"))
            conn.commit()
        logger.info(f"Truncated table {table_name}")
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            )
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), {'table_name': table_name})
            return result.scalar()
    
    def log_etl_job(
        self,
        job_name: str,
        job_type: str,
        source_name: str,
        status: str = 'RUNNING',
        records_processed: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        error_message: str = None
    ) -> int:
        """
        Log an ETL job execution.
        
        Returns:
            Job ID
        """
        sql = """
            INSERT INTO etl_jobs 
            (job_name, job_type, source_name, started_at, status, 
             records_processed, records_inserted, records_updated, error_message)
            VALUES 
            (:job_name, :job_type, :source_name, NOW(), :status,
             :records_processed, :records_inserted, :records_updated, :error_message)
            RETURNING job_id
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), {
                'job_name': job_name,
                'job_type': job_type,
                'source_name': source_name,
                'status': status,
                'records_processed': records_processed,
                'records_inserted': records_inserted,
                'records_updated': records_updated,
                'error_message': error_message
            })
            conn.commit()
            return result.scalar()
    
    def update_etl_job(
        self,
        job_id: int,
        status: str,
        records_processed: int = None,
        records_inserted: int = None,
        records_updated: int = None,
        records_failed: int = None,
        error_message: str = None
    ):
        """Update an ETL job record."""
        updates = ["status = :status", "completed_at = NOW()"]
        params = {'job_id': job_id, 'status': status}
        
        if records_processed is not None:
            updates.append("records_processed = :records_processed")
            params['records_processed'] = records_processed
        if records_inserted is not None:
            updates.append("records_inserted = :records_inserted")
            params['records_inserted'] = records_inserted
        if records_updated is not None:
            updates.append("records_updated = :records_updated")
            params['records_updated'] = records_updated
        if records_failed is not None:
            updates.append("records_failed = :records_failed")
            params['records_failed'] = records_failed
        if error_message is not None:
            updates.append("error_message = :error_message")
            params['error_message'] = error_message
        
        sql = f"UPDATE etl_jobs SET {', '.join(updates)} WHERE job_id = :job_id"
        
        with self.engine.connect() as conn:
            conn.execute(text(sql), params)
            conn.commit()


def get_loader() -> PostgresLoader:
    """Factory function to get a PostgresLoader instance."""
    return PostgresLoader()
