"""
Anomaly detection transformer using statistical and ML methods
"""
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class AnomalyDetector(Transformer):
    """
    Transformer that detects anomalies in data using multiple methods

    Methods supported:
    - Statistical: Z-score, IQR (Interquartile Range)
    - ML-based: Isolation Forest
    - Pattern-based: Unexpected formats, missing required fields
    - Rule-based: Custom business rules
    """

    def __init__(
        self,
        method: str = "statistical",
        threshold: float = 3.0,
        numeric_fields: Optional[List[str]] = None,
        filter_anomalies: bool = False,
        contamination: float = 0.1,
        **kwargs
    ):
        """
        Initialize anomaly detector

        Args:
            method: Detection method:
                - 'statistical': Z-score based detection
                - 'iqr': Interquartile range method
                - 'isolation_forest': ML-based isolation forest
                - 'combined': Use multiple methods
            threshold: Threshold for anomaly detection
                - For z-score: Number of standard deviations (default: 3.0)
                - For IQR: Multiplier for IQR (default: 1.5)
            numeric_fields: List of numeric fields to analyze (None = auto-detect)
            filter_anomalies: If True, filter out anomalies. If False, flag them.
            contamination: Expected proportion of anomalies (for isolation_forest)
            **kwargs: Additional configuration
        """
        super().__init__({
            'method': method,
            'threshold': threshold,
            'numeric_fields': numeric_fields,
            'filter_anomalies': filter_anomalies,
            'contamination': contamination,
            **kwargs
        })

        self.method = method
        self.threshold = threshold
        self.numeric_fields = numeric_fields
        self.filter_anomalies = filter_anomalies
        self.contamination = contamination

        # Validate method
        valid_methods = ['statistical', 'iqr', 'isolation_forest', 'combined']
        if method not in valid_methods:
            raise ValueError(
                f"Invalid method: {method}. "
                f"Must be one of: {valid_methods}"
            )

        # Statistics for statistical methods
        self.field_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'values': [],
            'mean': None,
            'std': None,
            'q1': None,
            'q3': None,
            'iqr': None,
        })

        # Isolation forest model
        self.isolation_forest = None
        self.training_complete = False

    def transform(self, record: Record) -> Optional[Record]:
        """
        Single record transform - not ideal for anomaly detection
        Use transform_batch() for better results

        Args:
            record: Input record

        Returns:
            Optional[Record]: Record with anomaly flag, or None if filtered
        """
        # For single record, just flag as non-anomalous
        # Real detection happens in batch mode
        record.metadata.custom = record.metadata.custom or {}
        record.metadata.custom['is_anomaly'] = False
        record.metadata.custom['anomaly_score'] = 0.0
        return record

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform batch - detects anomalies across all records

        Args:
            records: Input records

        Returns:
            List[Record]: Records with anomaly flags (or filtered)
        """
        if not records:
            return []

        try:
            # Detect anomalies based on method
            if self.method == 'statistical':
                anomaly_indices = self._detect_statistical(records)
            elif self.method == 'iqr':
                anomaly_indices = self._detect_iqr(records)
            elif self.method == 'isolation_forest':
                anomaly_indices = self._detect_isolation_forest(records)
            elif self.method == 'combined':
                anomaly_indices = self._detect_combined(records)
            else:
                anomaly_indices = set()

            # Mark or filter anomalies
            result = []
            for i, record in enumerate(records):
                is_anomaly = i in anomaly_indices

                # Add anomaly metadata
                record.metadata.custom = record.metadata.custom or {}
                record.metadata.custom['is_anomaly'] = is_anomaly
                record.metadata.custom['anomaly_method'] = self.method

                if is_anomaly:
                    record.metadata.custom['anomaly_reasons'] = self._get_anomaly_reasons(
                        record, i, anomaly_indices
                    )

                    if self.filter_anomalies:
                        # Filter out anomaly
                        self.stats.records_filtered += 1
                        continue
                    else:
                        # Keep but mark
                        self.stats.records_modified += 1

                result.append(record)
                self.stats.records_processed += 1

            self.logger.info(
                f"Detected {len(anomaly_indices)} anomalies out of {len(records)} records "
                f"using {self.method} method"
            )

            return result

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in AnomalyDetector: {e}")

    def _detect_statistical(self, records: List[Record]) -> Set[int]:
        """
        Detect anomalies using Z-score method

        Args:
            records: Input records

        Returns:
            Set of indices of anomalous records
        """
        # Extract numeric values
        numeric_data = self._extract_numeric_data(records)

        if not numeric_data:
            self.logger.warning("No numeric fields found for statistical detection")
            return set()

        anomalies = set()

        # Calculate statistics for each field
        for field_name, values in numeric_data.items():
            if len(values) < 3:
                continue

            # Remove None values before calculating statistics
            clean_values = [v for v in values if v is not None]

            if not clean_values or len(clean_values) < 3:
                continue

            # Calculate mean and std on clean values
            mean = np.mean(clean_values)
            std = np.std(clean_values)

            if std == 0:
                continue

            # Find outliers using Z-score
            for i, value in enumerate(values):
                if value is not None:
                    z_score = abs((value - mean) / std)
                    if z_score > self.threshold:
                        anomalies.add(i)

        return anomalies

    def _detect_iqr(self, records: List[Record]) -> Set[int]:
        """
        Detect anomalies using IQR method

        Args:
            records: Input records

        Returns:
            Set of indices of anomalous records
        """
        # Extract numeric values
        numeric_data = self._extract_numeric_data(records)

        if not numeric_data:
            self.logger.warning("No numeric fields found for IQR detection")
            return set()

        anomalies = set()

        # Calculate IQR for each field
        for field_name, values in numeric_data.items():
            if len(values) < 4:
                continue

            # Remove None values
            clean_values = [v for v in values if v is not None]

            if not clean_values:
                continue

            # Calculate quartiles
            q1 = np.percentile(clean_values, 25)
            q3 = np.percentile(clean_values, 75)
            iqr = q3 - q1

            if iqr == 0:
                continue

            # Calculate bounds
            lower_bound = q1 - (self.threshold * iqr)
            upper_bound = q3 + (self.threshold * iqr)

            # Find outliers
            for i, value in enumerate(values):
                if value is not None:
                    if value < lower_bound or value > upper_bound:
                        anomalies.add(i)

        return anomalies

    def _detect_isolation_forest(self, records: List[Record]) -> Set[int]:
        """
        Detect anomalies using Isolation Forest

        Args:
            records: Input records

        Returns:
            Set of indices of anomalous records
        """
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            raise TransformError(
                "scikit-learn is required for isolation_forest method. "
                "Install it with: pip install scikit-learn"
            )

        # Extract numeric features
        numeric_data = self._extract_numeric_data(records)

        if not numeric_data:
            self.logger.warning("No numeric fields found for Isolation Forest")
            return set()

        # Prepare feature matrix
        field_names = sorted(numeric_data.keys())
        X = []

        for i in range(len(records)):
            row = []
            for field_name in field_names:
                value = numeric_data[field_name][i]
                # Replace None with mean
                if value is None:
                    clean_values = [v for v in numeric_data[field_name] if v is not None]
                    value = np.mean(clean_values) if clean_values else 0
                row.append(value)
            X.append(row)

        X = np.array(X)

        if len(X) < 2:
            return set()

        # Train Isolation Forest
        self.logger.info(f"Training Isolation Forest on {len(X)} samples...")

        clf = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )

        predictions = clf.fit_predict(X)

        # -1 means anomaly, 1 means normal
        anomalies = {i for i, pred in enumerate(predictions) if pred == -1}

        return anomalies

    def _detect_combined(self, records: List[Record]) -> Set[int]:
        """
        Detect anomalies using combined methods

        Args:
            records: Input records

        Returns:
            Set of indices of anomalous records
        """
        # Run all methods
        statistical_anomalies = self._detect_statistical(records)
        iqr_anomalies = self._detect_iqr(records)

        # Try isolation forest if sklearn available
        try:
            if_anomalies = self._detect_isolation_forest(records)
        except TransformError:
            if_anomalies = set()

        # Combine: record is anomaly if detected by at least 2 methods
        all_anomalies = [statistical_anomalies, iqr_anomalies, if_anomalies]
        combined = set()

        for i in range(len(records)):
            detection_count = sum(1 for method_anomalies in all_anomalies if i in method_anomalies)
            if detection_count >= 2:
                combined.add(i)

        return combined

    def _extract_numeric_data(self, records: List[Record]) -> Dict[str, List[Optional[float]]]:
        """
        Extract numeric data from records

        Args:
            records: Input records

        Returns:
            Dict mapping field names to lists of values
        """
        numeric_data = defaultdict(list)

        # Determine which fields to analyze
        if self.numeric_fields:
            fields_to_analyze = set(self.numeric_fields)
        else:
            # Auto-detect numeric fields from first record
            fields_to_analyze = set()
            if records:
                for key, value in records[0].data.items():
                    if isinstance(value, (int, float)):
                        fields_to_analyze.add(key)

        # Extract values
        for record in records:
            for field_name in fields_to_analyze:
                value = record.data.get(field_name)

                # Convert to float if numeric
                if isinstance(value, (int, float)):
                    numeric_data[field_name].append(float(value))
                else:
                    numeric_data[field_name].append(None)

        return dict(numeric_data)

    def _get_anomaly_reasons(
        self,
        record: Record,
        index: int,
        anomaly_indices: Set[int]
    ) -> List[str]:
        """
        Get human-readable reasons why record is anomalous

        Args:
            record: The record
            index: Index in batch
            anomaly_indices: Set of anomaly indices

        Returns:
            List of reason strings
        """
        reasons = []

        # Extract numeric values
        for key, value in record.data.items():
            if isinstance(value, (int, float)):
                field_stats = self.field_stats.get(key)

                if field_stats and field_stats.get('mean') is not None:
                    mean = field_stats['mean']
                    std = field_stats['std']

                    if std and std > 0:
                        z_score = abs((value - mean) / std)
                        if z_score > self.threshold:
                            reasons.append(
                                f"{key}={value} is {z_score:.2f} standard deviations from mean ({mean:.2f})"
                            )

        if not reasons:
            reasons.append(f"Anomalous based on {self.method} method")

        return reasons

    def reset_stats(self) -> None:
        """Reset statistics and field stats"""
        super().reset_stats()
        self.field_stats.clear()
        self.training_complete = False
