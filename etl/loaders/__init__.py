"""Data loaders for PostgreSQL/PostGIS"""

from .postgres import PostgresLoader, get_loader

__all__ = ['PostgresLoader', 'get_loader']
