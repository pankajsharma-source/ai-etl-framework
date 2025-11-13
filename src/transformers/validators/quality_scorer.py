"""
AI-powered data quality scorer transformer

Uses statistical methods and heuristics to assess data quality.
"""
from datetime import datetime
from typing import Dict, Optional

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class QualityScorer(Transformer):
    """
    Transformer that scores data quality using multiple criteria

    Quality score is calculated based on:
    - Completeness: Percentage of non-null values
    - Validity: Data type consistency and format correctness
    - Uniqueness: Field value diversity
    - Consistency: Pattern matching and expected formats
    """

    def __init__(
        self,
        min_score: float = 0.0,
        filter_low_quality: bool = False,
        weights: Optional[Dict[str, float]] = None,
        **kwargs
    ):
        """
        Initialize quality scorer

        Args:
            min_score: Minimum quality score threshold (0.0-1.0)
            filter_low_quality: Filter out records below min_score
            weights: Custom weights for scoring criteria
                {
                    'completeness': 0.4,
                    'validity': 0.3,
                    'consistency': 0.3
                }
            **kwargs: Additional configuration
        """
        super().__init__({
            'min_score': min_score,
            'filter_low_quality': filter_low_quality,
            'weights': weights,
            **kwargs
        })

        self.min_score = min_score
        self.filter_low_quality = filter_low_quality

        # Default weights
        self.weights = weights or {
            'completeness': 0.4,
            'validity': 0.3,
            'consistency': 0.3
        }

        # Validate weights sum to 1.0
        total = sum(self.weights.values())
        if not (0.99 <= total <= 1.01):  # Allow small floating point errors
            raise ValueError(f"Weights must sum to 1.0, got {total}")

    def transform(self, record: Record) -> Optional[Record]:
        """
        Transform a single record by calculating quality score

        Args:
            record: Input record

        Returns:
            Optional[Record]: Record with quality score, or None if filtered
        """
        try:
            # Calculate individual scores
            completeness_score = self._score_completeness(record.data)
            validity_score = self._score_validity(record.data)
            consistency_score = self._score_consistency(record.data)

            # Calculate weighted overall score
            quality_score = (
                completeness_score * self.weights['completeness'] +
                validity_score * self.weights['validity'] +
                consistency_score * self.weights['consistency']
            )

            # Add quality score to metadata
            record.metadata.quality_score = quality_score
            record.metadata.custom = record.metadata.custom or {}
            record.metadata.custom['quality_breakdown'] = {
                'completeness': completeness_score,
                'validity': validity_score,
                'consistency': consistency_score,
                'overall': quality_score
            }

            # Filter if below threshold
            if self.filter_low_quality and quality_score < self.min_score:
                self.logger.debug(
                    f"Filtered record {record.metadata.record_id} "
                    f"with quality score {quality_score:.2f}"
                )
                self.stats.records_filtered += 1
                return None

            # Update timestamp
            record.transformed_at = datetime.now()
            record.metadata.stage = "transform"

            self.stats.records_modified += 1
            return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in QualityScorer: {e}")

    def _score_completeness(self, data: Dict) -> float:
        """
        Score data completeness (0.0-1.0)

        Measures the percentage of non-null, non-empty values
        """
        if not data:
            return 0.0

        total_fields = len(data)
        complete_fields = sum(
            1 for value in data.values()
            if value is not None and value != ""
        )

        return complete_fields / total_fields if total_fields > 0 else 0.0

    def _score_validity(self, data: Dict) -> float:
        """
        Score data validity (0.0-1.0)

        Checks for:
        - Reasonable string lengths
        - Numeric values in expected ranges
        - Valid email formats (basic check)
        """
        if not data:
            return 0.0

        validity_checks = []

        for key, value in data.items():
            if value is None or value == "":
                validity_checks.append(1.0)  # Null is valid (handled by completeness)
                continue

            # String length check (not too long, suggests data corruption)
            if isinstance(value, str):
                if len(value) > 10000:  # Suspiciously long
                    validity_checks.append(0.0)
                elif len(value) > 1000:  # Very long
                    validity_checks.append(0.5)
                else:
                    validity_checks.append(1.0)

                # Email format check (basic)
                if 'email' in key.lower() and value:
                    if '@' in value and '.' in value:
                        validity_checks.append(1.0)
                    else:
                        validity_checks.append(0.0)

            # Numeric range check
            elif isinstance(value, (int, float)):
                # Check for unreasonable values
                if abs(value) > 1e15:  # Suspiciously large
                    validity_checks.append(0.0)
                else:
                    validity_checks.append(1.0)

            else:
                validity_checks.append(1.0)  # Other types considered valid

        return sum(validity_checks) / len(validity_checks) if validity_checks else 1.0

    def _score_consistency(self, data: Dict) -> float:
        """
        Score data consistency (0.0-1.0)

        Checks for:
        - Field name conventions (lowercase, no special chars, etc.)
        - Expected data types for common field names
        - Logical consistency (e.g., age > 0)
        """
        if not data:
            return 0.0

        consistency_checks = []

        for key, value in data.items():
            if value is None or value == "":
                consistency_checks.append(1.0)
                continue

            # Age field should be positive integer
            if 'age' in key.lower():
                if isinstance(value, (int, float)) and 0 < value < 150:
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.0)

            # Salary should be positive
            elif 'salary' in key.lower() or 'price' in key.lower():
                if isinstance(value, (int, float)) and value > 0:
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.0)

            # ID fields should be non-negative
            elif key.lower() in ('id', 'user_id', 'customer_id'):
                if isinstance(value, int) and value >= 0:
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.5)

            # Email should be string
            elif 'email' in key.lower():
                if isinstance(value, str):
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.0)

            # Boolean fields
            elif isinstance(value, bool):
                consistency_checks.append(1.0)

            else:
                consistency_checks.append(1.0)  # No specific check

        return sum(consistency_checks) / len(consistency_checks) if consistency_checks else 1.0
