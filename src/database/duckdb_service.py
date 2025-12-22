"""
DuckDB Service for Interactive Dashboard Analytics

Provides fast OLAP-style queries on Parquet/CSV files in the gold layer.
Supports filtering, aggregation, drill-down, and schema discovery.
"""

import duckdb
import pandas as pd
import os
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Base data directory
DATA_BASE_PATH = os.getenv('DATA_BASE_PATH', '/app/data')


@dataclass
class Filter:
    """Represents a filter condition for queries."""
    column: str
    operator: str  # 'eq', 'neq', 'in', 'not_in', 'between', 'gt', 'gte', 'lt', 'lte', 'contains', 'is_null', 'is_not_null'
    value: Any = None  # Value(s) for the filter


@dataclass
class AggregationSpec:
    """Specification for data aggregation."""
    group_by: List[str] = field(default_factory=list)
    metrics: List[Dict[str, str]] = field(default_factory=list)  # [{"column": "amount", "agg": "sum", "alias": "total_amount"}]
    order_by: Optional[str] = None
    order_desc: bool = True
    limit: Optional[int] = None


class DuckDBService:
    """
    DuckDB-based analytics service for interactive dashboards.

    Provides fast queries on Parquet/CSV files with support for:
    - Schema discovery (column types, distinct values, min/max)
    - Filtered aggregations
    - Drill-down queries
    - Cross-filtering
    """

    def __init__(self, data_root: str = None):
        """Initialize the DuckDB service."""
        self.data_root = data_root or DATA_BASE_PATH
        self._connections: Dict[str, duckdb.DuckDBPyConnection] = {}

    def _get_data_file_path(self, org_slug: str, source_slug: str) -> Optional[str]:
        """
        Get the path to the gold layer data file for a source.

        Looks for Parquet first, then CSV.
        """
        base_path = f"{self.data_root}/{org_slug}/gold/bi/{source_slug}"

        # Check for Parquet first (preferred for performance)
        parquet_path = f"{base_path}/{source_slug}.parquet"
        if Path(parquet_path).exists():
            return parquet_path

        # Fall back to CSV
        csv_path = f"{base_path}/{source_slug}.csv"
        if Path(csv_path).exists():
            return csv_path

        return None

    def _get_connection(self, org_slug: str, source_slug: str) -> Tuple[duckdb.DuckDBPyConnection, str]:
        """
        Get or create a DuckDB connection with the data file registered as a view.

        Returns:
            Tuple of (connection, view_name)
        """
        cache_key = f"{org_slug}/{source_slug}"

        if cache_key not in self._connections:
            file_path = self._get_data_file_path(org_slug, source_slug)
            if not file_path:
                raise FileNotFoundError(f"No data file found for {org_slug}/{source_slug}")

            # Create new in-memory connection
            conn = duckdb.connect(':memory:')

            # Register the file as a view
            view_name = f"data_{source_slug.replace('-', '_')}"

            if file_path.endswith('.parquet'):
                conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{file_path}')")
            else:
                conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_csv_auto('{file_path}')")

            self._connections[cache_key] = (conn, view_name)
            logger.info(f"Created DuckDB connection for {cache_key}")

        return self._connections[cache_key]

    def invalidate_cache(self, org_slug: str, source_slug: str) -> None:
        """Invalidate cached connection for a source (e.g., after data refresh)."""
        cache_key = f"{org_slug}/{source_slug}"
        if cache_key in self._connections:
            conn, _ = self._connections[cache_key]
            conn.close()
            del self._connections[cache_key]
            logger.info(f"Invalidated DuckDB cache for {cache_key}")

    def get_schema(self, org_slug: str, source_slug: str, sample_size: int = 10000) -> Dict[str, Any]:
        """
        Get schema metadata for a data source.

        Returns column information including:
        - dtype: 'numeric', 'categorical', 'datetime', 'boolean'
        - For categorical: distinct values (up to 100)
        - For numeric: min, max, mean
        - For datetime: min, max

        Args:
            org_slug: Organization slug
            source_slug: Data source slug
            sample_size: Number of rows to sample for analysis

        Returns:
            Schema metadata dictionary
        """
        conn, view_name = self._get_connection(org_slug, source_slug)

        # Get total row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]

        # Get column info
        columns_info = conn.execute(f"DESCRIBE {view_name}").fetchall()

        columns = []
        suggested_dimensions = []
        suggested_metrics = []

        for col_name, col_type, null_ok, key, default, extra in columns_info:
            col_info = {
                'name': col_name,
                'dtype': 'unknown',
                'nullable': null_ok == 'YES'
            }

            # Determine logical dtype based on DuckDB type
            type_lower = col_type.lower()

            if any(t in type_lower for t in ['int', 'bigint', 'smallint', 'tinyint', 'float', 'double', 'decimal', 'numeric', 'real']):
                col_info['dtype'] = 'numeric'

                # Get stats
                stats = conn.execute(f"""
                    SELECT
                        MIN("{col_name}") as min_val,
                        MAX("{col_name}") as max_val,
                        AVG("{col_name}")::DOUBLE as mean_val,
                        COUNT(DISTINCT "{col_name}") as distinct_count
                    FROM {view_name}
                """).fetchone()

                col_info['min'] = float(stats[0]) if stats[0] is not None else None
                col_info['max'] = float(stats[1]) if stats[1] is not None else None
                col_info['mean'] = float(stats[2]) if stats[2] is not None else None
                col_info['distinct_count'] = stats[3]

                # Suggest as metric
                if not self._is_likely_id_column(col_name, stats[3], row_count):
                    suggested_metrics.append(col_name)

            elif any(t in type_lower for t in ['date', 'timestamp', 'time']):
                col_info['dtype'] = 'datetime'

                # Get date range
                stats = conn.execute(f"""
                    SELECT
                        MIN("{col_name}")::VARCHAR as min_val,
                        MAX("{col_name}")::VARCHAR as max_val
                    FROM {view_name}
                """).fetchone()

                col_info['min'] = stats[0]
                col_info['max'] = stats[1]

                suggested_dimensions.append(col_name)

            elif 'bool' in type_lower:
                col_info['dtype'] = 'boolean'
                col_info['distinct_values'] = [True, False]

            else:
                # Treat as categorical (VARCHAR, etc.)
                col_info['dtype'] = 'categorical'

                # Get distinct count and values
                distinct_count = conn.execute(f"""
                    SELECT COUNT(DISTINCT "{col_name}") FROM {view_name}
                """).fetchone()[0]

                col_info['distinct_count'] = distinct_count

                # Only fetch distinct values if cardinality is reasonable
                if distinct_count <= 100:
                    distinct_vals = conn.execute(f"""
                        SELECT DISTINCT "{col_name}"
                        FROM {view_name}
                        WHERE "{col_name}" IS NOT NULL
                        ORDER BY "{col_name}"
                        LIMIT 100
                    """).fetchall()
                    col_info['distinct_values'] = [v[0] for v in distinct_vals]
                else:
                    # Get sample values
                    sample_vals = conn.execute(f"""
                        SELECT DISTINCT "{col_name}"
                        FROM {view_name}
                        WHERE "{col_name}" IS NOT NULL
                        LIMIT 20
                    """).fetchall()
                    col_info['sample_values'] = [v[0] for v in sample_vals]
                    col_info['high_cardinality'] = True

                # Suggest as dimension if not an ID column
                if distinct_count <= 50 and not self._is_likely_id_column(col_name, distinct_count, row_count):
                    suggested_dimensions.append(col_name)

            columns.append(col_info)

        return {
            'columns': columns,
            'row_count': row_count,
            'suggested_dimensions': suggested_dimensions[:5],  # Top 5 dimensions
            'suggested_metrics': suggested_metrics[:5]  # Top 5 metrics
        }

    def _is_likely_id_column(self, col_name: str, distinct_count: int, row_count: int) -> bool:
        """Check if a column is likely an ID column."""
        name_lower = col_name.lower()
        # Check name patterns
        if any(pattern in name_lower for pattern in ['_id', 'id_', 'key', 'uuid', 'guid']):
            return True
        if name_lower in ['id', 'pk', 'index']:
            return True
        # Check if distinct count equals row count (unique per row)
        if distinct_count == row_count and row_count > 100:
            return True
        return False

    def _build_where_clause(self, filters: List[Filter]) -> Tuple[str, List[Any]]:
        """
        Build SQL WHERE clause from filters.

        Returns:
            Tuple of (where_clause_string, parameters_list)
        """
        if not filters:
            return "", []

        conditions = []
        params = []

        for f in filters:
            col = f'"{f.column}"'

            if f.operator == 'eq':
                conditions.append(f"{col} = ?")
                params.append(f.value)

            elif f.operator == 'neq':
                conditions.append(f"{col} != ?")
                params.append(f.value)

            elif f.operator == 'in':
                if not f.value:
                    continue
                placeholders = ', '.join(['?'] * len(f.value))
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(f.value)

            elif f.operator == 'not_in':
                if not f.value:
                    continue
                placeholders = ', '.join(['?'] * len(f.value))
                conditions.append(f"{col} NOT IN ({placeholders})")
                params.extend(f.value)

            elif f.operator == 'between':
                if len(f.value) == 2:
                    conditions.append(f"{col} BETWEEN ? AND ?")
                    params.extend(f.value)

            elif f.operator == 'gt':
                conditions.append(f"{col} > ?")
                params.append(f.value)

            elif f.operator == 'gte':
                conditions.append(f"{col} >= ?")
                params.append(f.value)

            elif f.operator == 'lt':
                conditions.append(f"{col} < ?")
                params.append(f.value)

            elif f.operator == 'lte':
                conditions.append(f"{col} <= ?")
                params.append(f.value)

            elif f.operator == 'contains':
                conditions.append(f"{col} ILIKE ?")
                params.append(f"%{f.value}%")

            elif f.operator == 'is_null':
                conditions.append(f"{col} IS NULL")

            elif f.operator == 'is_not_null':
                conditions.append(f"{col} IS NOT NULL")

        where_clause = " AND ".join(conditions)
        return f"WHERE {where_clause}" if where_clause else "", params

    def query_with_filters(
        self,
        org_slug: str,
        source_slug: str,
        filters: List[Filter] = None,
        aggregation: AggregationSpec = None
    ) -> Dict[str, Any]:
        """
        Execute a filtered aggregation query.

        Args:
            org_slug: Organization slug
            source_slug: Data source slug
            filters: List of filter conditions
            aggregation: Aggregation specification

        Returns:
            Query results with data and metadata
        """
        import time
        start_time = time.time()

        conn, view_name = self._get_connection(org_slug, source_slug)
        filters = filters or []
        aggregation = aggregation or AggregationSpec()

        # Build WHERE clause
        where_clause, params = self._build_where_clause(filters)

        # Build SELECT clause
        if aggregation.group_by or aggregation.metrics:
            # Aggregation query
            select_parts = []

            # Group by columns
            for col in aggregation.group_by:
                select_parts.append(f'"{col}"')

            # Metric aggregations
            for metric in aggregation.metrics:
                col = metric['column']
                agg = metric.get('agg', 'sum').upper()
                alias = metric.get('alias', f"{col}_{agg.lower()}")

                if agg in ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT']:
                    select_parts.append(f'{agg}("{col}") AS "{alias}"')
                elif agg == 'COUNT_DISTINCT':
                    select_parts.append(f'COUNT(DISTINCT "{col}") AS "{alias}"')

            select_clause = ", ".join(select_parts) if select_parts else "*"

            # Build GROUP BY clause
            group_by_clause = ""
            if aggregation.group_by:
                group_cols = ", ".join([f'"{col}"' for col in aggregation.group_by])
                group_by_clause = f"GROUP BY {group_cols}"

            # Build ORDER BY clause
            order_clause = ""
            if aggregation.order_by:
                direction = "DESC" if aggregation.order_desc else "ASC"
                order_clause = f'ORDER BY "{aggregation.order_by}" {direction}'
            elif aggregation.metrics:
                # Default: order by first metric descending
                first_metric = aggregation.metrics[0]
                alias = first_metric.get('alias', f"{first_metric['column']}_{first_metric.get('agg', 'sum').lower()}")
                order_clause = f'ORDER BY "{alias}" DESC'

            # Build LIMIT clause
            limit_clause = ""
            if aggregation.limit:
                limit_clause = f"LIMIT {aggregation.limit}"

            query = f"""
                SELECT {select_clause}
                FROM {view_name}
                {where_clause}
                {group_by_clause}
                {order_clause}
                {limit_clause}
            """
        else:
            # Simple filtered query (no aggregation)
            query = f"""
                SELECT *
                FROM {view_name}
                {where_clause}
                LIMIT 1000
            """

        # Execute query
        logger.debug(f"Executing query: {query}")
        logger.debug(f"Parameters: {params}")

        try:
            result = conn.execute(query, params).fetchdf()

            query_time_ms = int((time.time() - start_time) * 1000)

            return {
                'data': result.to_dict(orient='records'),
                'columns': list(result.columns),
                'row_count': len(result),
                'query_time_ms': query_time_ms
            }
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    def get_drill_down_data(
        self,
        org_slug: str,
        source_slug: str,
        dimension: str = None,
        dimension_value: Any = None,
        filters: List[Filter] = None,
        columns: List[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get raw records for drill-down view.

        Args:
            org_slug: Organization slug
            source_slug: Data source slug
            dimension: Dimension column to filter on (optional)
            dimension_value: Value to filter for (optional, if None returns all records)
            filters: Additional filter conditions
            columns: Specific columns to return (None = all)
            limit: Maximum rows to return
            offset: Offset for pagination

        Returns:
            Paginated records with total count
        """
        import time
        start_time = time.time()

        conn, view_name = self._get_connection(org_slug, source_slug)

        # Combine filters with dimension filter (only if dimension_value is provided)
        all_filters = list(filters) if filters else []
        if dimension and dimension_value is not None:
            all_filters.append(Filter(column=dimension, operator='eq', value=dimension_value))

        # Build WHERE clause
        where_clause, params = self._build_where_clause(all_filters)

        # Build SELECT clause
        if columns:
            select_clause = ", ".join([f'"{col}"' for col in columns])
        else:
            select_clause = "*"

        # Get total count
        count_query = f"SELECT COUNT(*) FROM {view_name} {where_clause}"
        total_count = conn.execute(count_query, params).fetchone()[0]

        # Get paginated records
        query = f"""
            SELECT {select_clause}
            FROM {view_name}
            {where_clause}
            LIMIT {limit}
            OFFSET {offset}
        """

        result = conn.execute(query, params).fetchdf()
        query_time_ms = int((time.time() - start_time) * 1000)

        return {
            'records': result.to_dict(orient='records'),
            'columns': list(result.columns),
            'total_count': total_count,
            'limit': limit,
            'offset': offset,
            'query_time_ms': query_time_ms
        }

    def get_filter_values(
        self,
        org_slug: str,
        source_slug: str,
        column: str,
        search: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get distinct values for a column (for filter dropdowns).

        Args:
            org_slug: Organization slug
            source_slug: Data source slug
            column: Column to get values for
            search: Optional search term to filter values
            limit: Maximum values to return

        Returns:
            Distinct values with count info
        """
        conn, view_name = self._get_connection(org_slug, source_slug)

        # Get total distinct count
        total_query = f'SELECT COUNT(DISTINCT "{column}") FROM {view_name}'
        total_count = conn.execute(total_query).fetchone()[0]

        # Build query with optional search
        if search:
            query = f"""
                SELECT DISTINCT "{column}"
                FROM {view_name}
                WHERE "{column}" IS NOT NULL
                  AND "{column}"::VARCHAR ILIKE ?
                ORDER BY "{column}"
                LIMIT {limit}
            """
            result = conn.execute(query, [f"%{search}%"]).fetchall()
        else:
            query = f"""
                SELECT DISTINCT "{column}"
                FROM {view_name}
                WHERE "{column}" IS NOT NULL
                ORDER BY "{column}"
                LIMIT {limit}
            """
            result = conn.execute(query).fetchall()

        values = [row[0] for row in result]

        return {
            'column': column,
            'values': values,
            'total_count': total_count,
            'truncated': total_count > limit
        }

    def close(self):
        """Close all connections."""
        for key, (conn, _) in self._connections.items():
            try:
                conn.close()
            except:
                pass
        self._connections.clear()
        logger.info("Closed all DuckDB connections")


# Global service instance
_duckdb_service: Optional[DuckDBService] = None


def get_duckdb_service() -> DuckDBService:
    """Get the global DuckDB service instance."""
    global _duckdb_service
    if _duckdb_service is None:
        _duckdb_service = DuckDBService()
    return _duckdb_service
