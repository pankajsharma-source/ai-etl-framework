"""
Null value remover transformer
"""
from datetime import datetime
from typing import Optional

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class NullRemover(Transformer):
    """Transformer that handles null/missing values"""

    def __init__(self, strategy: str = "drop", fill_value: any = None, **kwargs):
        """
        Initialize null remover

        Args:
            strategy: How to handle nulls:
                - 'drop': Remove records with any null values
                - 'drop_all': Remove only records where all values are null
                - 'remove_fields': Keep record but remove fields with null values
                - 'fill': Fill nulls with a specific value
            fill_value: Value to fill nulls with (when strategy='fill')
            **kwargs: Additional configuration
        """
        super().__init__({
            'strategy': strategy,
            'fill_value': fill_value,
            **kwargs
        })

        self.strategy = strategy
        self.fill_value = fill_value

        if strategy not in ['drop', 'drop_all', 'remove_fields', 'fill']:
            raise ValueError(
                f"Invalid strategy: {strategy}. "
                f"Must be one of: 'drop', 'drop_all', 'remove_fields', 'fill'"
            )

    def transform(self, record: Record) -> Optional[Record]:
        """
        Transform a single record by handling null values

        Args:
            record: Input record

        Returns:
            Optional[Record]: Transformed record, or None if record should be filtered
        """
        try:
            if self.strategy == "drop":
                # Drop if any value is null
                if self._has_null_values(record.data):
                    self.stats.records_filtered += 1
                    return None

            elif self.strategy == "drop_all":
                # Drop only if all values are null
                if self._all_null_values(record.data):
                    self.stats.records_filtered += 1
                    return None

            elif self.strategy == "remove_fields":
                # Remove fields with null values
                record.data = {
                    k: v for k, v in record.data.items()
                    if v is not None and v != ""
                }
                self.stats.records_modified += 1

            elif self.strategy == "fill":
                # Fill null values
                record.data = {
                    k: (v if v is not None and v != "" else self.fill_value)
                    for k, v in record.data.items()
                }
                self.stats.records_modified += 1

            # Update timestamp
            record.transformed_at = datetime.now()
            record.metadata.stage = "transform"

            return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in NullRemover: {e}")

    def _has_null_values(self, data: dict) -> bool:
        """Check if dictionary has any null values"""
        return any(v is None or v == "" for v in data.values())

    def _all_null_values(self, data: dict) -> bool:
        """Check if all values in dictionary are null"""
        return all(v is None or v == "" for v in data.values())
