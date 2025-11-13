"""
SQLite destination adapter for loading data into SQLite database
"""
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List

from src.adapters.base import DestinationAdapter
from src.common.models import Field, FieldType, Record, Schema
from src.common.exceptions import ConnectionError, SchemaError, WriteError


class SQLiteLoader(DestinationAdapter):
    """Destination adapter for SQLite database"""

    def __init__(
        self,
        db_path: str,
        table: str,
        create_if_missing: bool = True,
        **kwargs
    ):
        """
        Initialize SQLite loader

        Args:
            db_path: Path to SQLite database file
            table: Table name to load data into
            create_if_missing: Create table if it doesn't exist
            **kwargs: Additional configuration
        """
        config = {
            'db_path': db_path,
            'table': table,
            'create_if_missing': create_if_missing,
            **kwargs
        }
        super().__init__(config)

        self.db_path = Path(db_path)
        self.table = table
        self.create_if_missing = create_if_missing

        self._conn: sqlite3.Connection = None
        self._cursor: sqlite3.Cursor = None
        self._batch: List[Dict] = []

    def connect(self) -> None:
        """Establish connection to SQLite database"""
        try:
            # Create parent directory if it doesn't exist
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect to database
            self._conn = sqlite3.connect(str(self.db_path))
            self._cursor = self._conn.cursor()
            self._connected = True

            self.logger.info(f"Connected to SQLite database: {self.db_path}")

        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQLite database: {e}")

    def create_schema(self, schema: Schema) -> None:
        """
        Create table based on schema

        Args:
            schema: Schema definition

        Raises:
            SchemaError: If schema creation fails
        """
        if not self._connected:
            raise SchemaError("Not connected. Call connect() first.")

        try:
            # Check if table exists
            self._cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.table,)
            )
            exists = self._cursor.fetchone() is not None

            if exists and not self.create_if_missing:
                self.logger.info(f"Table '{self.table}' already exists")
                return

            if exists:
                self.logger.warning(f"Table '{self.table}' exists, will append data")
                return

            # Create table
            columns = []
            for field in schema.fields:
                sql_type = self._map_field_type_to_sql(field.type)
                nullable = "" if field.nullable else "NOT NULL"
                columns.append(f"{field.name} {sql_type} {nullable}")

            # Add primary key if specified
            if schema.primary_key:
                pk_cols = ", ".join(schema.primary_key)
                columns.append(f"PRIMARY KEY ({pk_cols})")

            create_sql = f"CREATE TABLE {self.table} ({', '.join(columns)})"
            self._cursor.execute(create_sql)
            self._conn.commit()

            self.logger.info(f"Created table '{self.table}' with {len(schema.fields)} columns")

        except Exception as e:
            raise SchemaError(f"Failed to create schema: {e}")

    def _map_field_type_to_sql(self, field_type: FieldType) -> str:
        """Map FieldType to SQLite type"""
        mapping = {
            FieldType.STRING: "TEXT",
            FieldType.INTEGER: "INTEGER",
            FieldType.FLOAT: "REAL",
            FieldType.BOOLEAN: "INTEGER",  # SQLite doesn't have boolean
            FieldType.DATE: "TEXT",
            FieldType.DATETIME: "TEXT",
            FieldType.TIMESTAMP: "TEXT",
            FieldType.JSON: "TEXT",
            FieldType.ARRAY: "TEXT",
        }
        return mapping.get(field_type, "TEXT")

    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to SQLite table

        Args:
            records: Iterator of records to write

        Returns:
            int: Number of records written

        Raises:
            WriteError: If writing fails
        """
        if not self._connected:
            raise WriteError("Not connected. Call connect() first.")

        count = 0
        batch_size = self.config.get('batch_size', 1000)

        try:
            for record in records:
                self._batch.append(record.data)
                count += 1

                # Write batch when it reaches batch_size
                if len(self._batch) >= batch_size:
                    self._flush_batch()

            # Write remaining records
            if self._batch:
                self._flush_batch()

            self.logger.info(f"Wrote {count} records to table '{self.table}'")
            return count

        except Exception as e:
            raise WriteError(f"Failed to write records: {e}")

    def _flush_batch(self) -> None:
        """Flush current batch to database"""
        if not self._batch:
            return

        try:
            # Get column names from first record
            columns = list(self._batch[0].keys())
            placeholders = ", ".join(["?" for _ in columns])
            column_names = ", ".join(columns)

            insert_sql = f"INSERT INTO {self.table} ({column_names}) VALUES ({placeholders})"

            # Prepare data - convert unsupported types to JSON strings
            import json
            data = []
            for record in self._batch:
                row = []
                for col in columns:
                    value = record.get(col)
                    # Convert lists and dicts to JSON strings for SQLite
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    row.append(value)
                data.append(row)

            # Execute batch insert
            self._cursor.executemany(insert_sql, data)

            # Clear batch
            self._batch = []

            self.logger.debug(f"Flushed batch of {len(data)} records")

        except Exception as e:
            raise WriteError(f"Failed to flush batch: {e}")

    def begin_transaction(self) -> None:
        """Begin transaction"""
        super().begin_transaction()
        if self._conn:
            self._conn.execute("BEGIN")

    def commit(self) -> None:
        """Commit transaction"""
        if self._conn:
            # Flush any remaining batch
            if self._batch:
                self._flush_batch()

            self._conn.commit()
            self.logger.debug("Transaction committed")

        super().commit()

    def rollback(self) -> None:
        """Rollback transaction"""
        if self._conn:
            self._conn.rollback()
            self._batch = []  # Clear batch
            self.logger.debug("Transaction rolled back")

        super().rollback()

    def close(self) -> None:
        """Close database connection"""
        if self._cursor:
            self._cursor.close()

        if self._conn:
            # Final commit before closing
            if self._batch:
                try:
                    self._flush_batch()
                    self._conn.commit()
                except Exception as e:
                    self.logger.error(f"Error during final flush: {e}")

            self._conn.close()

        self._connected = False
        self.logger.info("SQLite connection closed")
