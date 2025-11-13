"""
Base transformer interface
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.common.models import Record
from src.common.exceptions import TransformError
from src.common.logging import get_logger


@dataclass
class TransformerStats:
    """Statistics for transformer execution"""
    records_processed: int = 0
    records_filtered: int = 0
    records_modified: int = 0
    errors: int = 0


class Transformer(ABC):
    """Abstract base class for all transformers"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize transformer

        Args:
            config: Transformer-specific configuration
        """
        self.config = config or {}
        self.stats = TransformerStats()
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def transform(self, record: Record) -> Optional[Record]:
        """
        Transform a single record

        Args:
            record: Input record

        Returns:
            Optional[Record]: Transformed record, or None if record should be filtered

        Raises:
            TransformError: If transformation fails
        """
        pass

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform a batch of records (default implementation)

        Override this for batch-optimized transformations

        Args:
            records: Input records

        Returns:
            List[Record]: Transformed records
        """
        result = []
        for record in records:
            try:
                transformed = self.transform(record)
                if transformed is not None:
                    result.append(transformed)
                    self.stats.records_processed += 1
                else:
                    self.stats.records_filtered += 1
            except TransformError as e:
                self.stats.errors += 1
                self.logger.error(f"Transform error: {e}")
                # Re-raise or handle based on configuration
                if self.config.get('error_handling') == 'fail':
                    raise
                # Otherwise skip the record

        return result

    def get_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics

        Returns:
            Dict: Statistics (records processed, filtered, errors, etc.)
        """
        return {
            'records_processed': self.stats.records_processed,
            'records_filtered': self.stats.records_filtered,
            'records_modified': self.stats.records_modified,
            'errors': self.stats.errors
        }

    def reset_stats(self) -> None:
        """Reset statistics"""
        self.stats = TransformerStats()
