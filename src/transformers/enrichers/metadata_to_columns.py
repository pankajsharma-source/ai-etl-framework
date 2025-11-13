"""
Metadata to Columns Transformer

Converts record metadata into data columns for visibility in output files.
Useful for exposing anomaly flags, quality scores, and other metadata in CSV/JSON outputs.
"""
from datetime import datetime
from typing import Optional, List

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class MetadataToColumnsTransformer(Transformer):
    """
    Transformer that moves metadata fields into data columns

    Adds columns for:
    - is_anomaly: Boolean flag indicating if record is anomalous
    - anomaly_method: Detection method used (if anomaly)
    - anomaly_reasons: Human-readable reasons (if anomaly)
    - quality_score: Overall quality score (0.0-1.0)
    - completeness_score: Completeness metric
    - validity_score: Validity metric
    - consistency_score: Consistency metric
    """

    def __init__(
        self,
        include_anomaly_info: bool = True,
        include_quality_info: bool = True,
        include_quality_breakdown: bool = True,
        prefix: str = "_meta_",
        **kwargs
    ):
        """
        Initialize metadata to columns transformer

        Args:
            include_anomaly_info: Include anomaly detection columns
            include_quality_info: Include quality score column
            include_quality_breakdown: Include detailed quality breakdown
            prefix: Prefix for metadata columns (default: "_meta_")
            **kwargs: Additional configuration
        """
        super().__init__({
            'include_anomaly_info': include_anomaly_info,
            'include_quality_info': include_quality_info,
            'include_quality_breakdown': include_quality_breakdown,
            'prefix': prefix,
            **kwargs
        })

        self.include_anomaly_info = include_anomaly_info
        self.include_quality_info = include_quality_info
        self.include_quality_breakdown = include_quality_breakdown
        self.prefix = prefix

    def transform(self, record: Record) -> Optional[Record]:
        """
        Transform a single record by adding metadata as columns

        Args:
            record: Input record

        Returns:
            Record: Record with metadata columns added
        """
        try:
            # Get custom metadata (set by other transformers)
            custom = record.metadata.custom or {}

            # Add anomaly detection columns
            if self.include_anomaly_info:
                is_anomaly = custom.get('is_anomaly', False)
                record.data[f'{self.prefix}is_anomaly'] = is_anomaly

                if is_anomaly:
                    # Add anomaly method
                    anomaly_method = custom.get('anomaly_method', 'unknown')
                    record.data[f'{self.prefix}anomaly_method'] = anomaly_method

                    # Add anomaly reasons (join list into string)
                    anomaly_reasons = custom.get('anomaly_reasons', [])
                    if anomaly_reasons:
                        reasons_str = "; ".join(anomaly_reasons)
                        record.data[f'{self.prefix}anomaly_reasons'] = reasons_str
                    else:
                        record.data[f'{self.prefix}anomaly_reasons'] = ""
                else:
                    record.data[f'{self.prefix}anomaly_method'] = ""
                    record.data[f'{self.prefix}anomaly_reasons'] = ""

            # Add quality score
            if self.include_quality_info:
                quality_score = record.metadata.quality_score
                if quality_score is not None:
                    record.data[f'{self.prefix}quality_score'] = round(quality_score, 4)
                else:
                    record.data[f'{self.prefix}quality_score'] = None

            # Add quality breakdown
            if self.include_quality_breakdown:
                quality_breakdown = custom.get('quality_breakdown', {})
                if quality_breakdown:
                    record.data[f'{self.prefix}completeness'] = round(
                        quality_breakdown.get('completeness', 0.0), 4
                    )
                    record.data[f'{self.prefix}validity'] = round(
                        quality_breakdown.get('validity', 0.0), 4
                    )
                    record.data[f'{self.prefix}consistency'] = round(
                        quality_breakdown.get('consistency', 0.0), 4
                    )
                else:
                    record.data[f'{self.prefix}completeness'] = None
                    record.data[f'{self.prefix}validity'] = None
                    record.data[f'{self.prefix}consistency'] = None

            # Update timestamp
            record.transformed_at = datetime.now()
            record.metadata.stage = "transform"

            self.stats.records_modified += 1
            return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in MetadataToColumnsTransformer: {e}")
