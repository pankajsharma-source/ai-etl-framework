"""
Column Remover Transformer

Removes specified columns from records.
Useful for cleaning up metadata columns or removing unwanted fields.
"""
from typing import Optional, List, Set
import re

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class ColumnRemover(Transformer):
    """
    Transformer that removes specified columns from records

    Supports:
    1. Exact column name matching
    2. Prefix matching (e.g., "_meta_")
    3. Regex pattern matching

    Records with removed columns pass through to the next stage.
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        prefix: Optional[str] = None,
        pattern: Optional[str] = None,
        keep_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize column remover

        Args:
            columns: List of exact column names to remove
            prefix: Remove all columns starting with this prefix
            pattern: Regex pattern - remove columns matching this pattern
            keep_columns: Columns to keep (overrides removal - useful with prefix/pattern)
            **kwargs: Additional configuration

        Example:
            # Remove specific columns
            ColumnRemover(columns=["_meta_is_anomaly", "_meta_anomaly_method"])

            # Remove all columns starting with "_temp_"
            ColumnRemover(prefix="_temp_")

            # Remove all metadata except quality scores
            ColumnRemover(prefix="_meta_", keep_columns=["_meta_quality_score"])

            # Remove columns matching pattern
            ColumnRemover(pattern=r"^temp_.*_id$")
        """
        super().__init__({
            'columns': columns or [],
            'prefix': prefix,
            'pattern': pattern,
            'keep_columns': keep_columns or [],
            **kwargs
        })

        self.columns_to_remove = set(columns or [])
        self.prefix = prefix
        self.pattern = re.compile(pattern) if pattern else None
        self.keep_columns = set(keep_columns or [])

        # Statistics
        self._columns_removed: Set[str] = set()
        self._total_removed = 0

    def setup(self):
        """Log configuration"""
        if self.columns_to_remove:
            self.logger.info(f"Will remove columns: {', '.join(sorted(self.columns_to_remove))}")
        if self.prefix:
            self.logger.info(f"Will remove columns with prefix: '{self.prefix}'")
            if self.keep_columns:
                self.logger.info(f"  Except: {', '.join(sorted(self.keep_columns))}")
        if self.pattern:
            self.logger.info(f"Will remove columns matching pattern: {self.pattern.pattern}")

    def _should_remove_column(self, column_name: str) -> bool:
        """
        Check if a column should be removed

        Args:
            column_name: Name of the column

        Returns:
            True if column should be removed, False otherwise
        """
        # Check keep list first (override)
        if column_name in self.keep_columns:
            return False

        # Check exact match
        if column_name in self.columns_to_remove:
            return True

        # Check prefix
        if self.prefix and column_name.startswith(self.prefix):
            return True

        # Check pattern
        if self.pattern and self.pattern.match(column_name):
            return True

        return False

    def transform(self, record: Record) -> Optional[Record]:
        """
        Remove specified columns from record

        Args:
            record: Input record

        Returns:
            Record with specified columns removed
        """
        try:
            columns_to_drop = []

            # Find columns to remove
            for column in record.data.keys():
                if self._should_remove_column(column):
                    columns_to_drop.append(column)
                    self._columns_removed.add(column)

            # Remove columns
            if columns_to_drop:
                for column in columns_to_drop:
                    del record.data[column]
                    self._total_removed += 1

                self.stats.records_modified += 1

            return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in ColumnRemover: {e}")

    def cleanup(self):
        """Log summary of removed columns"""
        if self._columns_removed:
            self.logger.info(
                f"Removed {len(self._columns_removed)} unique columns: "
                f"{', '.join(sorted(self._columns_removed))}"
            )
            self.logger.info(f"Total column removals: {self._total_removed}")
        else:
            self.logger.info("No columns removed")

    def get_stats(self) -> dict:
        """Get removal statistics"""
        base_stats = super().get_stats()
        base_stats.update({
            'columns_removed': sorted(self._columns_removed),
            'unique_columns_removed': len(self._columns_removed),
            'total_removals': self._total_removed
        })
        return base_stats
