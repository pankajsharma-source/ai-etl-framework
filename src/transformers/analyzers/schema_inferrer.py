"""
Advanced schema inference transformer using ML techniques
"""
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict, Counter

from src.transformers.base_transformer import Transformer
from src.common.models import Record, Schema, Field, FieldType
from src.common.exceptions import TransformError


class SchemaInferrer(Transformer):
    """
    Transformer that infers detailed schema information from data using ML techniques

    Analyzes data samples to:
    - Infer accurate data types
    - Detect patterns (emails, URLs, dates, etc.)
    - Suggest constraints (min/max, nullable, enum values)
    - Calculate confidence scores
    - Handle mixed types
    """

    # Regex patterns for common data formats
    PATTERNS = {
        'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
        'url': re.compile(r'^https?://[^\s]+$'),
        'ipv4': re.compile(r'^(\d{1,3}\.){3}\d{1,3}$'),
        'phone_us': re.compile(r'^\+?1?\s*\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$'),
        'date_iso': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        'datetime_iso': re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'),
        'uuid': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I),
        'credit_card': re.compile(r'^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$'),
        'ssn': re.compile(r'^\d{3}-\d{2}-\d{4}$'),
    }

    def __init__(
        self,
        sample_size: int = 1000,
        confidence_threshold: float = 0.8,
        detect_patterns: bool = True,
        infer_constraints: bool = True,
        suggest_enums: bool = True,
        enum_threshold: int = 10,  # Max distinct values for enum suggestion
        **kwargs
    ):
        """
        Initialize schema inferrer

        Args:
            sample_size: Number of records to sample for inference
            confidence_threshold: Minimum confidence to accept inference (0-1)
            detect_patterns: Enable pattern detection (email, URL, etc.)
            infer_constraints: Infer min/max, nullable constraints
            suggest_enums: Suggest enum values for low-cardinality fields
            enum_threshold: Max distinct values to suggest as enum
            **kwargs: Additional configuration
        """
        super().__init__({
            'sample_size': sample_size,
            'confidence_threshold': confidence_threshold,
            'detect_patterns': detect_patterns,
            'infer_constraints': infer_constraints,
            'suggest_enums': suggest_enums,
            'enum_threshold': enum_threshold,
            **kwargs
        })

        self.sample_size = sample_size
        self.confidence_threshold = confidence_threshold
        self.detect_patterns = detect_patterns
        self.infer_constraints = infer_constraints
        self.suggest_enums = suggest_enums
        self.enum_threshold = enum_threshold

        # Track field statistics across samples
        self.field_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'types': Counter(),
            'values': [],
            'nulls': 0,
            'total': 0,
            'patterns': Counter(),
            'numeric_values': [],
        })

    def transform(self, record: Record) -> Optional[Record]:
        """
        Single record transform - collects statistics for inference

        Args:
            record: Input record

        Returns:
            Record: Unchanged record (inference happens in finalize)
        """
        # Collect statistics from this record
        for field_name, value in record.data.items():
            stats = self.field_stats[field_name]
            stats['total'] += 1

            if value is None or value == "":
                stats['nulls'] += 1
            else:
                # Track type
                value_type = type(value).__name__
                stats['types'][value_type] += 1

                # Store sample values (limited)
                if len(stats['values']) < self.sample_size:
                    stats['values'].append(value)

                # Track numeric values for min/max
                if isinstance(value, (int, float)):
                    stats['numeric_values'].append(value)

                # Detect patterns
                if self.detect_patterns and isinstance(value, str):
                    pattern = self._detect_pattern(value)
                    if pattern:
                        stats['patterns'][pattern] += 1

        return record

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform batch - infers schema and updates records

        Args:
            records: Input records

        Returns:
            List[Record]: Records with inferred schema attached
        """
        if not records:
            return []

        try:
            # Collect statistics from all records
            for record in records:
                self.transform(record)
                self.stats.records_processed += 1

            # Infer schema from collected statistics
            inferred_schema = self._infer_schema()

            # Attach schema to all records
            for record in records:
                record.schema = inferred_schema
                record.metadata.custom = record.metadata.custom or {}
                record.metadata.custom['schema_inferred'] = True

            self.logger.info(
                f"Inferred schema with {len(inferred_schema.fields)} fields "
                f"from {len(records)} records"
            )

            return records

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in SchemaInferrer: {e}")

    def _infer_schema(self) -> Schema:
        """
        Infer schema from collected field statistics

        Returns:
            Schema: Inferred schema
        """
        fields = []

        for field_name, stats in self.field_stats.items():
            field = self._infer_field(field_name, stats)
            fields.append(field)

        schema = Schema(
            name="inferred_schema",
            fields=fields,
            inferred=True,
            created_at=datetime.now(),
            sample_size=sum(s['total'] for s in self.field_stats.values()) // len(self.field_stats) if self.field_stats else 0,
            null_counts={
                name: stats['nulls']
                for name, stats in self.field_stats.items()
            }
        )

        return schema

    def _infer_field(self, field_name: str, stats: Dict[str, Any]) -> Field:
        """
        Infer field definition from statistics

        Args:
            field_name: Name of the field
            stats: Statistics dictionary

        Returns:
            Field: Inferred field definition
        """
        total = stats['total']
        nulls = stats['nulls']
        non_nulls = total - nulls

        # Determine if nullable
        nullable = nulls > 0

        # Infer type from most common type
        field_type, type_confidence = self._infer_type(stats)

        # Detect pattern if string type
        pattern = None
        pattern_confidence = 0.0
        if field_type == FieldType.STRING and self.detect_patterns:
            pattern, pattern_confidence = self._get_dominant_pattern(stats)

        # Infer constraints
        min_value = None
        max_value = None
        enum_values = None

        if self.infer_constraints:
            if stats['numeric_values']:
                min_value = min(stats['numeric_values'])
                max_value = max(stats['numeric_values'])

            # Suggest enum if low cardinality
            if self.suggest_enums and stats['values']:
                unique_values = set(str(v) for v in stats['values'] if v is not None)
                if len(unique_values) <= self.enum_threshold:
                    enum_values = sorted(list(unique_values))

        # Overall confidence (average of type and pattern confidence)
        confidence = type_confidence
        if pattern_confidence > 0:
            confidence = (type_confidence + pattern_confidence) / 2

        field = Field(
            name=field_name,
            type=field_type,
            nullable=nullable,
            description=self._generate_description(field_name, field_type, pattern),
            min_value=min_value,
            max_value=max_value,
            pattern=pattern,
            enum_values=enum_values,
            inferred=True,
            confidence=confidence
        )

        return field

    def _infer_type(self, stats: Dict[str, Any]) -> tuple[FieldType, float]:
        """
        Infer field type from type distribution

        Args:
            stats: Field statistics

        Returns:
            Tuple of (FieldType, confidence_score)
        """
        type_counts = stats['types']
        total_typed = sum(type_counts.values())

        if total_typed == 0:
            return FieldType.STRING, 0.0

        # Get most common type
        most_common_type, count = type_counts.most_common(1)[0]
        confidence = count / total_typed

        # Map Python type to FieldType
        type_mapping = {
            'int': FieldType.INTEGER,
            'float': FieldType.FLOAT,
            'bool': FieldType.BOOLEAN,
            'str': FieldType.STRING,
            'list': FieldType.ARRAY,
            'dict': FieldType.JSON,
        }

        field_type = type_mapping.get(most_common_type, FieldType.STRING)

        # Special handling for mixed numeric types
        if 'int' in type_counts and 'float' in type_counts:
            # If both int and float present, use FLOAT
            field_type = FieldType.FLOAT
            confidence = (type_counts['int'] + type_counts['float']) / total_typed

        return field_type, confidence

    def _detect_pattern(self, value: str) -> Optional[str]:
        """
        Detect pattern in string value

        Args:
            value: String value to check

        Returns:
            Pattern name if detected, None otherwise
        """
        for pattern_name, pattern_regex in self.PATTERNS.items():
            if pattern_regex.match(value):
                return pattern_name
        return None

    def _get_dominant_pattern(self, stats: Dict[str, Any]) -> tuple[Optional[str], float]:
        """
        Get dominant pattern from pattern counts

        Args:
            stats: Field statistics

        Returns:
            Tuple of (pattern_name, confidence)
        """
        pattern_counts = stats['patterns']
        total_values = len(stats['values'])

        if not pattern_counts or total_values == 0:
            return None, 0.0

        # Get most common pattern
        pattern, count = pattern_counts.most_common(1)[0]
        confidence = count / total_values

        # Only return pattern if above threshold
        if confidence >= self.confidence_threshold:
            return pattern, confidence

        return None, 0.0

    def _generate_description(
        self,
        field_name: str,
        field_type: FieldType,
        pattern: Optional[str]
    ) -> str:
        """
        Generate human-readable field description

        Args:
            field_name: Field name
            field_type: Inferred type
            pattern: Detected pattern (if any)

        Returns:
            Description string
        """
        desc_parts = []

        # Add pattern info if detected
        if pattern:
            pattern_names = {
                'email': 'Email address',
                'url': 'URL',
                'ipv4': 'IPv4 address',
                'phone_us': 'US phone number',
                'date_iso': 'ISO date',
                'datetime_iso': 'ISO datetime',
                'uuid': 'UUID',
                'credit_card': 'Credit card number',
                'ssn': 'Social Security Number',
            }
            desc_parts.append(pattern_names.get(pattern, pattern))
        else:
            # Generic description based on type
            desc_parts.append(field_type.value.capitalize())

        # Add field name context
        desc_parts.append(f"field: {field_name}")

        return " - ".join(desc_parts)

    def get_inferred_schema(self) -> Optional[Schema]:
        """
        Get the inferred schema (after processing records)

        Returns:
            Schema if records have been processed, None otherwise
        """
        if not self.field_stats:
            return None

        return self._infer_schema()

    def reset_stats(self) -> None:
        """Reset statistics and field stats"""
        super().reset_stats()
        self.field_stats.clear()
