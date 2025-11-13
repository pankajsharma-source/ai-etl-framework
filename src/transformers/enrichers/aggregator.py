"""
Aggregator transformer for grouping and aggregating records
"""
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
from collections import defaultdict

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class Aggregator(Transformer):
    """Transformer that groups records and applies aggregation functions"""

    # Built-in aggregation functions
    AGG_FUNCTIONS = {
        'sum': lambda values: sum(v for v in values if v is not None and isinstance(v, (int, float))),
        'avg': lambda values: sum(v for v in values if v is not None and isinstance(v, (int, float))) / len([v for v in values if v is not None and isinstance(v, (int, float))]) if len([v for v in values if v is not None and isinstance(v, (int, float))]) > 0 else None,
        'min': lambda values: min(v for v in values if v is not None and isinstance(v, (int, float))) if any(v is not None and isinstance(v, (int, float)) for v in values) else None,
        'max': lambda values: max(v for v in values if v is not None and isinstance(v, (int, float))) if any(v is not None and isinstance(v, (int, float)) for v in values) else None,
        'count': lambda values: len(values),
        'count_distinct': lambda values: len(set(str(v) for v in values if v is not None)),
        'first': lambda values: values[0] if values else None,
        'last': lambda values: values[-1] if values else None,
        'concat': lambda values: ', '.join(str(v) for v in values if v is not None),
        'list': lambda values: [v for v in values if v is not None],
    }

    def __init__(
        self,
        group_by: List[str],
        aggregations: Dict[str, Dict[str, str]],
        keep_group_fields: bool = True,
        **kwargs
    ):
        """
        Initialize aggregator

        Args:
            group_by: List of field names to group by
            aggregations: Dictionary of aggregations to perform:
                {
                    'output_field': {
                        'field': 'source_field',
                        'function': 'sum|avg|min|max|count|first|last|concat|list'
                    }
                }
                Example:
                {
                    'total_salary': {'field': 'salary', 'function': 'sum'},
                    'avg_age': {'field': 'age', 'function': 'avg'},
                    'employee_count': {'field': 'id', 'function': 'count'}
                }
            keep_group_fields: Include group_by fields in output (default: True)
            **kwargs: Additional configuration
        """
        super().__init__({
            'group_by': group_by,
            'aggregations': aggregations,
            'keep_group_fields': keep_group_fields,
            **kwargs
        })

        self.group_by = group_by
        self.aggregations = aggregations
        self.keep_group_fields = keep_group_fields

        # Validate parameters
        if not group_by:
            raise ValueError("group_by cannot be empty. Provide at least one field to group by.")

        if not aggregations:
            raise ValueError("aggregations cannot be empty. Provide at least one aggregation.")

        # Validate aggregation functions
        for output_field, agg_spec in aggregations.items():
            if 'field' not in agg_spec or 'function' not in agg_spec:
                raise ValueError(
                    f"Aggregation '{output_field}' must have 'field' and 'function' keys. "
                    f"Got: {agg_spec}"
                )

            func = agg_spec['function']
            if func not in self.AGG_FUNCTIONS:
                raise ValueError(
                    f"Unknown aggregation function: {func}. "
                    f"Valid functions: {list(self.AGG_FUNCTIONS.keys())}"
                )

    def transform(self, record: Record) -> Optional[Record]:
        """
        Single record transform - not used for aggregation
        Aggregation requires batch processing via transform_batch()

        Args:
            record: Input record

        Returns:
            Record: Unchanged record (actual aggregation happens in transform_batch)
        """
        # Single record transform is a no-op for aggregator
        # All logic is in transform_batch() since we need to group records
        return record

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform a batch of records by grouping and aggregating

        Args:
            records: Input records

        Returns:
            List[Record]: Aggregated records (one per group)
        """
        if not records:
            return []

        try:
            # Group records
            groups = self._group_records(records)

            # Create schema for aggregated data
            aggregated_schema = self._create_aggregated_schema(records[0].schema)

            # Aggregate each group
            result = []
            for group_key, group_records in groups.items():
                aggregated = self._aggregate_group(group_key, group_records)
                # Set the new schema
                aggregated.schema = aggregated_schema
                result.append(aggregated)
                self.stats.records_processed += 1

            # Update filtered count
            self.stats.records_filtered = len(records) - len(result)

            self.logger.info(
                f"Aggregated {len(records)} records into {len(result)} groups"
            )

            return result

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in Aggregator: {e}")

    def _group_records(self, records: List[Record]) -> Dict[tuple, List[Record]]:
        """
        Group records by group_by fields

        Args:
            records: Input records

        Returns:
            Dict mapping group key (tuple of values) to list of records
        """
        groups = defaultdict(list)

        for record in records:
            # Build group key from group_by fields
            try:
                group_key = tuple(
                    record.data.get(field) for field in self.group_by
                )
                groups[group_key].append(record)
            except Exception as e:
                self.logger.warning(f"Skipping record due to grouping error: {e}")
                self.stats.errors += 1

        return groups

    def _aggregate_group(
        self,
        group_key: tuple,
        group_records: List[Record]
    ) -> Record:
        """
        Aggregate a group of records

        Args:
            group_key: Tuple of group_by field values
            group_records: Records in this group

        Returns:
            Record: Aggregated record
        """
        # Start with group fields if requested
        aggregated_data = {}

        if self.keep_group_fields:
            for i, field in enumerate(self.group_by):
                aggregated_data[field] = group_key[i]

        # Apply each aggregation
        for output_field, agg_spec in self.aggregations.items():
            source_field = agg_spec['field']
            func_name = agg_spec['function']

            # Extract values from all records in group
            values = [
                record.data.get(source_field)
                for record in group_records
            ]

            # Apply aggregation function
            agg_func = self.AGG_FUNCTIONS[func_name]
            try:
                aggregated_value = agg_func(values)
                aggregated_data[output_field] = aggregated_value
            except Exception as e:
                self.logger.warning(
                    f"Error aggregating {output_field} with {func_name}: {e}"
                )
                aggregated_data[output_field] = None

        # Create new record with aggregated data
        # Use the first record as a template
        template = group_records[0]

        # Copy metadata from template
        from src.common.models import RecordMetadata
        aggregated_metadata = RecordMetadata(
            source_type=template.metadata.source_type,
            source_id=template.metadata.source_id,
            pipeline_id=template.metadata.pipeline_id,
            stage="transform",
            custom={
                'group_size': len(group_records),
                'transformation_type': 'aggregation'
            }
        )

        aggregated_record = Record(
            data=aggregated_data,
            metadata=aggregated_metadata,
            schema=template.schema,
            extracted_at=template.extracted_at,
            transformed_at=datetime.now()
        )

        return aggregated_record

    def _create_aggregated_schema(self, source_schema: Optional[Any]) -> Any:
        """
        Create schema for aggregated data

        Args:
            source_schema: Original schema (can be None)

        Returns:
            Schema for aggregated data
        """
        from src.common.models import Schema, Field, FieldType

        fields = []

        # Add group_by fields
        if self.keep_group_fields:
            for field_name in self.group_by:
                # Try to get type from source schema
                field_type = FieldType.STRING  # Default
                if source_schema:
                    source_field = source_schema.get_field(field_name)
                    if source_field:
                        field_type = source_field.type

                fields.append(Field(
                    name=field_name,
                    type=field_type,
                    nullable=True
                ))

        # Add aggregation output fields
        for output_field, agg_spec in self.aggregations.items():
            func_name = agg_spec['function']

            # Determine output type based on function
            if func_name in ['sum', 'avg', 'min', 'max']:
                field_type = FieldType.FLOAT
            elif func_name in ['count', 'count_distinct']:
                field_type = FieldType.INTEGER
            elif func_name in ['list']:
                field_type = FieldType.JSON
            else:  # first, last, concat
                field_type = FieldType.STRING

            fields.append(Field(
                name=output_field,
                type=field_type,
                nullable=True
            ))

        # Create new schema
        schema_name = f"aggregated_{self.table if hasattr(self, 'table') else 'data'}"
        return Schema(
            name=schema_name,
            fields=fields,
            inferred=True
        )

    def add_custom_function(
        self,
        name: str,
        func: Callable[[List[Any]], Any]
    ) -> None:
        """
        Add a custom aggregation function

        Args:
            name: Name of the function
            func: Function that takes a list of values and returns aggregated value

        Example:
            aggregator.add_custom_function(
                'median',
                lambda values: sorted(values)[len(values)//2] if values else None
            )
        """
        self.AGG_FUNCTIONS[name] = func
        self.logger.info(f"Added custom aggregation function: {name}")
