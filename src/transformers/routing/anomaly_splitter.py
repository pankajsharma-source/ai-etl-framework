"""
Anomaly Splitter Transformer

Splits records into clean and anomalous, writing anomalies to a quarantine file.
Clean records pass through to the next stage unchanged.
"""
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
import pandas as pd

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class AnomalySplitter(Transformer):
    """
    Transformer that splits records into clean vs anomalous

    Anomalous records (where _meta_is_anomaly=True) are written to a quarantine file.
    Clean records pass through to the next stage for further processing.

    This enables:
    1. Clean data pipeline for automated systems (RAG, dashboards)
    2. Separate review queue for manual inspection
    3. Full audit trail of data quality issues
    """

    def __init__(
        self,
        output_path: str,
        write_on_completion: bool = True,
        **kwargs
    ):
        """
        Initialize anomaly splitter

        Args:
            output_path: Path to write anomalies CSV
            write_on_completion: If True, write anomalies at end. If False, write incrementally.
            **kwargs: Additional configuration
        """
        super().__init__({
            'output_path': output_path,
            'write_on_completion': write_on_completion,
            **kwargs
        })

        self.output_path = Path(output_path)
        self.write_on_completion = write_on_completion

        # Storage for anomalous records
        self._anomalies: List[Dict] = []
        self._clean_count = 0
        self._anomaly_count = 0

    def setup(self):
        """Initialize (create output directory)"""
        # Auto-create output directory
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Anomaly quarantine file: {self.output_path}")

    def transform(self, record: Record) -> Optional[Record]:
        """
        Check if record is anomaly and route accordingly

        Args:
            record: Input record

        Returns:
            Record if clean (passes through), None if filtered
        """
        try:
            # Check if record is anomalous
            is_anomaly = record.data.get('_meta_is_anomaly', False)

            if is_anomaly:
                # Store anomalous record
                self._anomalies.append(record.data.copy())
                self._anomaly_count += 1

                # Filter out (don't pass to next stage)
                self.stats.records_filtered += 1
                return None
            else:
                # Clean record - pass through
                self._clean_count += 1
                return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in AnomalySplitter: {e}")

    def cleanup(self):
        """Write anomalies to file at end of processing"""
        if self._anomalies:
            try:
                # Convert to DataFrame
                df = pd.DataFrame(self._anomalies)

                # Write to CSV
                df.to_csv(self.output_path, index=False)

                self.logger.info(
                    f"Wrote {len(self._anomalies)} anomalous records to {self.output_path}"
                )
                self.logger.info(
                    f"Split complete: {self._clean_count} clean, {self._anomaly_count} anomalies"
                )

            except Exception as e:
                self.logger.error(f"Failed to write anomalies file: {e}")
                raise TransformError(f"Failed to write anomalies: {e}")
        else:
            self.logger.info("No anomalies detected - quarantine file not created")

    def get_stats(self) -> dict:
        """Get splitting statistics"""
        base_stats = super().get_stats()
        base_stats.update({
            'clean_count': self._clean_count,
            'anomaly_count': self._anomaly_count,
            'quarantine_file': str(self.output_path)
        })
        return base_stats
