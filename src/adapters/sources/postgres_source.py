"""
PostgreSQL source adapter for reading from PostgreSQL database
"""
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from src.adapters.base import SourceAdapter
from src.common.models import Field, FieldType, Record, RecordMetadata, Schema
from src.common.exceptions import ConnectionError, ReadError


class PostgreSQLSource(SourceAdapter):
    """Source adapter for PostgreSQL database"""

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
        table: Optional[str] = None,
        query: Optional[str] = None,
        schema: str = "public",
        **kwargs
    ):
        """
        Initialize PostgreSQL source

        Args:
            host: Database host
            database: Database name
            user: Username
            password: Password
            port: Port (default: 5432)
            table: Table name to read from (if not using custom query)
            query: Custom SQL query (overrides table)
            schema: Schema name (default: 'public')
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
            'port': port,
            'table': table,
            'query': query,
            'schema': schema,
            **kwargs
        }
        super().__init__(config)

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.table = table
        self.query = query
        self.schema_name = schema

        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be specified")

        self._conn: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor] = None
        self._records_count = 0

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

            # Use DictCursor to get results as dictionaries
            self._cursor = self._conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )

            self._connected = True
            self.logger.info(
                f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}"
            )

        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def read(self, batch_size: int = 1000) -> Iterator[Record]:
        """
        Read records from PostgreSQL

        Args:
            batch_size: Number of rows to fetch at once

        Yields:
            Record: Individual records
        """
        if not self._connected:
            raise ReadError("Not connected. Call connect() first.")

        try:
            # Determine query to execute
            if self.query:
                sql = self.query
            else:
                sql = f"SELECT * FROM {self.schema_name}.{self.table}"

            # Execute query
            self.logger.info(f"Executing query: {sql[:100]}...")
            self._cursor.execute(sql)

            # Fetch and yield records in batches
            while True:
                rows = self._cursor.fetchmany(batch_size)
                if not rows:
                    break

                for row in rows:
                    # Convert RealDictRow to regular dict
                    data = dict(row)

                    # Create metadata
                    metadata = RecordMetadata(
                        source_type="postgres",
                        source_id=f"{self.database}.{self.schema_name}.{self.table or 'query'}",
                        record_id=f"row_{self._records_count}",
                        stage="extract"
                    )

                    # Create record
                    record = Record(
                        data=data,
                        metadata=metadata,
                        extracted_at=datetime.now()
                    )

                    yield record
                    self._records_count += 1

            self.logger.info(f"Read {self._records_count} records from PostgreSQL")

        except psycopg2.Error as e:
            raise ReadError(f"Error reading from PostgreSQL: {e}")

    def get_schema(self) -> Schema:
        """
        Get schema from PostgreSQL table

        Returns:
            Schema: Table schema
        """
        if not self._connected:
            self.connect()

        if not self.table:
            raise ReadError("Schema inference only supported for table-based queries")

        try:
            # Query information_schema to get column information
            query = """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """

            self._cursor.execute(query, (self.schema_name, self.table))
            columns = self._cursor.fetchall()

            if not columns:
                raise ReadError(f"Table not found: {self.schema_name}.{self.table}")

            # Convert to Field objects
            fields = []
            for col in columns:
                field_type = self._map_postgres_type(col['data_type'])
                nullable = col['is_nullable'] == 'YES'

                field = Field(
                    name=col['column_name'],
                    type=field_type,
                    nullable=nullable,
                    inferred=False,  # From actual schema, not inferred
                    confidence=1.0
                )
                fields.append(field)

            # Get primary key information
            pk_query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                    AND i.indisprimary
            """
            table_name = f"{self.schema_name}.{self.table}"
            self._cursor.execute(pk_query, (table_name,))
            pk_cols = [row['attname'] for row in self._cursor.fetchall()]

            schema = Schema(
                name=self.table,
                fields=fields,
                primary_key=pk_cols if pk_cols else None,
                inferred=False,
                created_at=datetime.now()
            )

            self.logger.info(f"Retrieved schema with {len(fields)} fields")
            return schema

        except psycopg2.Error as e:
            raise ReadError(f"Error getting schema: {e}")

    def _map_postgres_type(self, pg_type: str) -> FieldType:
        """Map PostgreSQL data type to FieldType"""
        pg_type = pg_type.lower()

        if pg_type in ('integer', 'bigint', 'smallint', 'serial', 'bigserial'):
            return FieldType.INTEGER
        elif pg_type in ('numeric', 'decimal', 'real', 'double precision'):
            return FieldType.FLOAT
        elif pg_type == 'boolean':
            return FieldType.BOOLEAN
        elif pg_type == 'date':
            return FieldType.DATE
        elif pg_type in ('timestamp', 'timestamp without time zone', 'timestamp with time zone'):
            return FieldType.DATETIME
        elif pg_type in ('json', 'jsonb'):
            return FieldType.JSON
        elif pg_type in ('array', 'ARRAY'):
            return FieldType.ARRAY
        else:
            return FieldType.STRING  # Default to string

    def supports_incremental(self) -> bool:
        """
        PostgreSQL supports incremental via timestamp or ID columns
        """
        return True

    def get_state(self) -> Dict[str, Any]:
        """Get current state for incremental processing"""
        return {
            'database': self.database,
            'table': self.table,
            'schema': self.schema_name,
            'records_processed': self._records_count,
            'last_read': datetime.now().isoformat()
        }

    def close(self) -> None:
        """Close database connection"""
        if self._cursor:
            self._cursor.close()

        if self._conn:
            self._conn.close()

        self._connected = False
        self.logger.info("PostgreSQL connection closed")
