"""
PostgreSQL destination adapter for loading data into PostgreSQL database
"""
from typing import Any, Dict, Iterator, List, Optional

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.extensions
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from src.adapters.base import DestinationAdapter
from src.common.models import Field, FieldType, Record, Schema
from src.common.exceptions import ConnectionError, SchemaError, WriteError


class PostgreSQLLoader(DestinationAdapter):
    """Destination adapter for PostgreSQL database"""

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        table: str,
        port: int = 5432,
        schema: str = "public",
        create_if_missing: bool = True,
        drop_if_exists: bool = False,
        **kwargs
    ):
        """
        Initialize PostgreSQL loader

        Args:
            host: Database host
            database: Database name
            user: Username
            password: Password
            table: Table name to load data into
            port: Port (default: 5432)
            schema: Schema name (default: 'public')
            create_if_missing: Create table if it doesn't exist
            drop_if_exists: Drop and recreate table if it exists
            **kwargs: Additional psycopg2 connection parameters
        """
        if not HAS_PSYCOPG2:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. "
                "Install it with: pip install psycopg2-binary"
            )

        config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'table': table,
            'port': port,
            'schema': schema,
            'create_if_missing': create_if_missing,
            'drop_if_exists': drop_if_exists,
            **kwargs
        }
        super().__init__(config)

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.port = port
        self.schema_name = schema
        self.create_if_missing = create_if_missing
        self.drop_if_exists = drop_if_exists

        self._conn: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor] = None
        self._batch: List[Dict] = []

    def connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self._conn.autocommit = False  # We'll manage transactions manually

            self._cursor = self._conn.cursor()
            self._connected = True

            self.logger.info(
                f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}"
            )

        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

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
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
                """,
                (self.schema_name, self.table)
            )
            exists = self._cursor.fetchone()[0]

            # Drop if requested
            if exists and self.drop_if_exists:
                drop_sql = f"DROP TABLE {self.schema_name}.{self.table}"
                self._cursor.execute(drop_sql)
                self._conn.commit()
                self.logger.info(f"Dropped table '{self.schema_name}.{self.table}'")
                exists = False

            if exists and not self.create_if_missing:
                self.logger.info(f"Table '{self.schema_name}.{self.table}' already exists")
                return

            if exists:
                self.logger.warning(f"Table '{self.schema_name}.{self.table}' exists, will append data")
                return

            # Create schema if it doesn't exist
            self._cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"
            )

            # Create table
            columns = []
            for field in schema.fields:
                pg_type = self._map_field_type_to_postgres(field.type)
                nullable = "NULL" if field.nullable else "NOT NULL"
                columns.append(f"{field.name} {pg_type} {nullable}")

            # Add primary key if specified
            if schema.primary_key:
                pk_cols = ", ".join(schema.primary_key)
                columns.append(f"PRIMARY KEY ({pk_cols})")

            create_sql = f"""
                CREATE TABLE {self.schema_name}.{self.table} (
                    {', '.join(columns)}
                )
            """
            self._cursor.execute(create_sql)
            self._conn.commit()

            self.logger.info(
                f"Created table '{self.schema_name}.{self.table}' "
                f"with {len(schema.fields)} columns"
            )

        except psycopg2.Error as e:
            self._conn.rollback()
            raise SchemaError(f"Failed to create schema: {e}")

    def _map_field_type_to_postgres(self, field_type: FieldType) -> str:
        """Map FieldType to PostgreSQL type"""
        mapping = {
            FieldType.STRING: "TEXT",
            FieldType.INTEGER: "INTEGER",
            FieldType.FLOAT: "DOUBLE PRECISION",
            FieldType.BOOLEAN: "BOOLEAN",
            FieldType.DATE: "DATE",
            FieldType.DATETIME: "TIMESTAMP",
            FieldType.TIMESTAMP: "TIMESTAMP",
            FieldType.JSON: "JSONB",
            FieldType.ARRAY: "TEXT[]",
        }
        return mapping.get(field_type, "TEXT")

    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to PostgreSQL table

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

            self.logger.info(f"Wrote {count} records to table '{self.schema_name}.{self.table}'")
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
            column_names = ", ".join(columns)
            placeholders = ", ".join(["%s" for _ in columns])

            insert_sql = f"""
                INSERT INTO {self.schema_name}.{self.table} ({column_names})
                VALUES ({placeholders})
            """

            # Prepare data
            data = [
                [record.get(col) for col in columns]
                for record in self._batch
            ]

            # Execute batch insert using execute_batch for better performance
            psycopg2.extras.execute_batch(
                self._cursor,
                insert_sql,
                data,
                page_size=len(data)
            )

            # Clear batch
            self._batch = []

            self.logger.debug(f"Flushed batch of {len(data)} records")

        except psycopg2.Error as e:
            raise WriteError(f"Failed to flush batch: {e}")

    def begin_transaction(self) -> None:
        """Begin transaction"""
        super().begin_transaction()
        # PostgreSQL connection is already in transaction mode (autocommit=False)

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
                    self._conn.rollback()

            self._conn.close()

        self._connected = False
        self.logger.info("PostgreSQL connection closed")
