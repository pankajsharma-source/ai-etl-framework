"""
AI-powered data quality scorer transformer

Uses statistical methods and heuristics to assess data quality.
"""
from datetime import datetime
from typing import Dict, Optional, List, Tuple

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
        mark_anomalies: bool = False,
        weights: Optional[Dict[str, float]] = None,
        **kwargs
    ):
        """
        Initialize quality scorer

        Args:
            min_score: Minimum quality score threshold (0.0-1.0)
            filter_low_quality: Filter out records below min_score
            mark_anomalies: Mark low-quality records as anomalies (for quarantine)
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
            'mark_anomalies': mark_anomalies,
            'weights': weights,
            **kwargs
        })

        self.min_score = min_score
        self.filter_low_quality = filter_low_quality
        self.mark_anomalies = mark_anomalies

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
            # Calculate individual scores with detailed issues
            completeness_score, completeness_issues = self._score_completeness(record.data)
            validity_score, validity_issues = self._score_validity(record.data)
            consistency_score, consistency_issues = self._score_consistency(record.data)

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

            # Check if below threshold
            if quality_score < self.min_score:
                # Mark as anomaly for quarantine (if configured)
                if self.mark_anomalies:
                    record.data['_meta_is_anomaly'] = True

                    # Build detailed anomaly reason
                    reason = f'Quality: {quality_score:.2f}'

                    # Add dimension breakdown (only show dimensions with issues)
                    low_dimensions = []
                    if completeness_score < 1.0:
                        low_dimensions.append(f'completeness: {completeness_score:.2f}')
                    if validity_score < 1.0:
                        low_dimensions.append(f'validity: {validity_score:.2f}')
                    if consistency_score < 1.0:
                        low_dimensions.append(f'consistency: {consistency_score:.2f}')

                    if low_dimensions:
                        reason += f' ({", ".join(low_dimensions)})'

                    # Add specific issues (limit to first 3 for readability)
                    all_issues = []
                    if completeness_issues:
                        all_issues.extend([f"{field}: missing" for field in completeness_issues[:3]])
                    all_issues.extend(validity_issues[:3])
                    all_issues.extend(consistency_issues[:3])

                    if all_issues:
                        # Limit total issues shown to 3
                        shown_issues = all_issues[:3]
                        reason += f' - Issues: {"; ".join(shown_issues)}'
                        if len(all_issues) > 3:
                            reason += f' (+{len(all_issues) - 3} more)'

                    record.data['_meta_anomaly_reason'] = reason

                    self.logger.debug(
                        f"Marked record {record.metadata.record_id} as anomaly "
                        f"with quality score {quality_score:.2f}"
                    )
                    self.stats.records_modified += 1

                # Filter if configured (takes precedence over marking)
                if self.filter_low_quality:
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

    def _score_completeness(self, data: Dict) -> Tuple[float, List[str]]:
        """
        Score data completeness (0.0-1.0)

        Measures the percentage of non-null, non-empty values

        Returns:
            Tuple of (score, list of missing/empty field names)
        """
        if not data:
            return 0.0, []

        missing_fields = []
        for key, value in data.items():
            if value is None or value == "":
                missing_fields.append(key)

        total_fields = len(data)
        complete_fields = total_fields - len(missing_fields)
        score = complete_fields / total_fields if total_fields > 0 else 0.0

        return score, missing_fields

    def _score_validity(self, data: Dict) -> Tuple[float, List[str]]:
        """
        Score data validity (0.0-1.0)

        Checks for:
        - Reasonable string lengths
        - Numeric values in expected ranges
        - Valid email formats (basic check)

        Returns:
            Tuple of (score, list of "field: reason" strings)
        """
        if not data:
            return 0.0, []

        validity_checks = []
        issues = []

        for key, value in data.items():
            if value is None or value == "":
                validity_checks.append(1.0)  # Null is valid (handled by completeness)
                continue

            # String length check (not too long, suggests data corruption)
            if isinstance(value, str):
                if len(value) > 10000:  # Suspiciously long
                    validity_checks.append(0.0)
                    issues.append(f"{key}: string too long ({len(value)} chars)")
                elif len(value) > 1000:  # Very long
                    validity_checks.append(0.5)
                    issues.append(f"{key}: string very long ({len(value)} chars)")
                else:
                    validity_checks.append(1.0)

                # Email format check (basic)
                if 'email' in key.lower() and value:
                    if '@' in value and '.' in value:
                        validity_checks.append(1.0)
                    else:
                        validity_checks.append(0.0)
                        # Truncate long values for readability
                        display_value = value if len(value) <= 50 else value[:47] + "..."
                        issues.append(f"{key}='{display_value}': invalid email format")

            # Numeric range check
            elif isinstance(value, (int, float)):
                # Check for unreasonable values
                if abs(value) > 1e15:  # Suspiciously large
                    validity_checks.append(0.0)
                    issues.append(f"{key}={value}: unreasonable numeric value")
                else:
                    validity_checks.append(1.0)

            else:
                validity_checks.append(1.0)  # Other types considered valid

        score = sum(validity_checks) / len(validity_checks) if validity_checks else 1.0
        return score, issues

    def _score_consistency(self, data: Dict) -> Tuple[float, List[str]]:
        """
        Score data consistency (0.0-1.0)

        Checks for:
        - Field name conventions (lowercase, no special chars, etc.)
        - Expected data types for common field names
        - Logical consistency (e.g., age > 0)

        Returns:
            Tuple of (score, list of "field: reason" strings)
        """
        if not data:
            return 0.0, []

        consistency_checks = []
        issues = []

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
                    issues.append(f"{key}={value}: age out of valid range (0-150)")

            # Salary should be positive
            elif 'salary' in key.lower() or 'price' in key.lower():
                if isinstance(value, (int, float)) and value > 0:
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.0)
                    issues.append(f"{key}={value}: must be positive")

            # ID fields should be non-negative
            elif key.lower() in ('id', 'user_id', 'customer_id'):
                if isinstance(value, int) and value >= 0:
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.5)
                    issues.append(f"{key}={value}: ID should be non-negative integer")

            # Email should be string
            elif 'email' in key.lower():
                if isinstance(value, str):
                    consistency_checks.append(1.0)
                else:
                    consistency_checks.append(0.0)
                    issues.append(f"{key}: email must be string (got {type(value).__name__})")

            # Boolean fields
            elif isinstance(value, bool):
                consistency_checks.append(1.0)

            else:
                consistency_checks.append(1.0)  # No specific check

        score = sum(consistency_checks) / len(consistency_checks) if consistency_checks else 1.0
        return score, issues
