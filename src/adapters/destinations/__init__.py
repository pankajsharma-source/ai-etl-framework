"""Destination adapters for loading data"""

from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.json_loader import JSONLoader
from src.adapters.destinations.postgres_loader import PostgreSQLLoader
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.adapters.destinations.parquet_loader import ParquetLoader

__all__ = [
    'CSVLoader',
    'JSONLoader',
    'PostgreSQLLoader',
    'SQLiteLoader',
    'ParquetLoader',
]
