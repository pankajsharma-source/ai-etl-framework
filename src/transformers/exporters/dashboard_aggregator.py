"""
Dashboard Aggregator Transformer

Generates aggregated Parquet files optimized for dashboard/BI tools.
Records pass through unchanged - this transformer only creates summary files.
"""
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
import pandas as pd

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class DashboardAggregator(Transformer):
    """
    Transformer that generates aggregated Parquet files for dashboards

    Creates multiple aggregation views:
    1. Overall summary (total claims, total billed, averages)
    2. By provider (claims per provider, total/avg billed)
    3. By diagnosis (claims per diagnosis code)
    4. By date (time series of daily metrics)

    Output format: Parquet (columnar, compressed, fast for BI tools)

    Records pass through unchanged to allow downstream processing.
    """

    def __init__(
        self,
        output_dir: str,
        date_column: str = "Service Start Date",
        **kwargs
    ):
        """
        Initialize dashboard aggregator

        Args:
            output_dir: Directory to write Parquet files
            date_column: Column to use for date-based aggregations
            **kwargs: Additional configuration
        """
        super().__init__({
            'output_dir': output_dir,
            'date_column': date_column,
            **kwargs
        })

        self.output_dir = Path(output_dir)
        self.date_column = date_column

        # Storage for all records (for aggregation)
        self._records: List[Dict] = []

    def setup(self):
        """Initialize (create output directory)"""
        # Auto-create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Dashboard Parquet output: {self.output_dir}")

    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """
        Auto-detect column by pattern matching (case-insensitive)

        Args:
            df: DataFrame to search
            patterns: List of keywords to match

        Returns:
            Column name if found, None otherwise
        """
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None

    def transform(self, record: Record) -> Optional[Record]:
        """
        Store record for aggregation and pass through

        Args:
            record: Input record

        Returns:
            Record (unchanged - passes through)
        """
        try:
            # Store record data for aggregation
            self._records.append(record.data.copy())

            # Pass through unchanged
            return record

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in DashboardAggregator: {e}")

    def cleanup(self):
        """Generate and write all aggregation files"""
        if not self._records:
            self.logger.warning("No records to aggregate")
            return

        try:
            # Convert to DataFrame for aggregation
            df = pd.DataFrame(self._records)

            self.logger.info(f"Generating dashboard aggregations from {len(df)} records")

            # Generate each aggregation
            self._generate_summary(df)
            self._generate_by_provider(df)
            self._generate_by_diagnosis(df)
            self._generate_by_date(df)

            self.logger.info(f"✅ Dashboard Parquet files written to {self.output_dir}")

        except Exception as e:
            self.logger.error(f"Failed to generate dashboard aggregations: {e}")
            raise TransformError(f"Failed to generate aggregations: {e}")

    def _generate_summary(self, df: pd.DataFrame):
        """Generate overall summary metrics"""
        try:
            # Auto-detect amount column
            amount_col = self._find_column(df, ['billed', 'amount', 'charged', 'cost', 'price', 'payment'])

            # Overall metrics
            summary = {
                'run_date': datetime.now().strftime('%Y-%m-%d'),
                'total_claims': len(df),
            }

            # Add amount metrics if found
            if amount_col:
                summary['total_billed'] = df[amount_col].sum()
                summary['avg_billed'] = df[amount_col].mean()
                summary['min_billed'] = df[amount_col].min()
                summary['max_billed'] = df[amount_col].max()

            # Quality metrics (if available)
            if '_meta_quality_score' in df.columns:
                summary['avg_quality_score'] = df['_meta_quality_score'].mean()

            # Anomaly metrics (if available)
            if '_meta_is_anomaly' in df.columns:
                summary['anomaly_count'] = df['_meta_is_anomaly'].sum()
                summary['anomaly_rate'] = df['_meta_is_anomaly'].mean()

            # Convert to DataFrame and write
            summary_df = pd.DataFrame([summary])
            output_path = self.output_dir / 'claims_summary.parquet'
            summary_df.to_parquet(output_path, index=False, compression='snappy')

            self.logger.info(f"  ✓ claims_summary.parquet (1 row)")

        except Exception as e:
            self.logger.error(f"Failed to generate summary: {e}")

    def _generate_by_provider(self, df: pd.DataFrame):
        """Generate provider-level aggregations"""
        try:
            # Auto-detect provider column
            provider_col = self._find_column(df, ['prov', 'provider', 'doctor', 'physician', 'practitioner'])
            if not provider_col:
                self.logger.warning("No provider column found, skipping provider aggregation")
                return

            # Auto-detect amount and claim ID columns
            amount_col = self._find_column(df, ['billed', 'amount', 'charged', 'cost', 'price'])
            claim_col = self._find_column(df, ['claim', 'id', 'number']) or provider_col  # Fallback to row count

            # Build aggregation dict
            agg_dict = {claim_col: 'count'}
            if amount_col and amount_col in df.columns:
                agg_dict[amount_col] = ['sum', 'mean', 'min', 'max']

            # Aggregate by provider
            provider_agg = df.groupby(provider_col).agg(agg_dict).reset_index()

            # Flatten column names
            provider_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                                   for col in provider_agg.columns]

            # Rename for clarity (dynamic based on detected columns)
            rename_map = {
                f'{claim_col}_count': 'total_claims'
            }
            if amount_col:
                rename_map.update({
                    f'{amount_col}_sum': 'total_billed',
                    f'{amount_col}_mean': 'avg_billed',
                    f'{amount_col}_min': 'min_billed',
                    f'{amount_col}_max': 'max_billed'
                })
            provider_agg.rename(columns=rename_map, inplace=True)

            # Add quality metrics if available
            if '_meta_quality_score' in df.columns:
                quality_by_provider = df.groupby(provider_col)['_meta_quality_score'].mean()
                provider_agg = provider_agg.merge(
                    quality_by_provider.rename('avg_quality_score'),
                    left_on=provider_col,
                    right_index=True,
                    how='left'
                )

            # Sort by total_billed descending
            if 'total_billed' in provider_agg.columns:
                provider_agg = provider_agg.sort_values('total_billed', ascending=False)

            # Write to Parquet
            output_path = self.output_dir / 'claims_by_provider.parquet'
            provider_agg.to_parquet(output_path, index=False, compression='snappy')

            self.logger.info(f"  ✓ claims_by_provider.parquet ({len(provider_agg)} providers)")

        except Exception as e:
            self.logger.error(f"Failed to generate provider aggregation: {e}")

    def _generate_by_diagnosis(self, df: pd.DataFrame):
        """Generate diagnosis-level aggregations"""
        try:
            # Auto-detect diagnosis column
            diagnosis_col = self._find_column(df, ['dx', 'diagnosis', 'condition', 'icd', 'disease'])
            if not diagnosis_col:
                self.logger.warning("No diagnosis column found, skipping diagnosis aggregation")
                return

            # Auto-detect amount and claim ID columns
            amount_col = self._find_column(df, ['billed', 'amount', 'charged', 'cost', 'price'])
            claim_col = self._find_column(df, ['claim', 'id', 'number']) or diagnosis_col  # Fallback

            # Build aggregation dict
            agg_dict = {claim_col: 'count'}
            if amount_col and amount_col in df.columns:
                agg_dict[amount_col] = ['sum', 'mean']

            # Aggregate by diagnosis
            diagnosis_agg = df.groupby(diagnosis_col).agg(agg_dict).reset_index()

            # Flatten column names
            diagnosis_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                                    for col in diagnosis_agg.columns]

            # Rename for clarity (dynamic)
            rename_map = {
                f'{claim_col}_count': 'total_claims'
            }
            if amount_col:
                rename_map.update({
                    f'{amount_col}_sum': 'total_billed',
                    f'{amount_col}_mean': 'avg_billed'
                })
            diagnosis_agg.rename(columns=rename_map, inplace=True)

            # Sort by total_claims descending
            if 'total_claims' in diagnosis_agg.columns:
                diagnosis_agg = diagnosis_agg.sort_values('total_claims', ascending=False)

            # Write to Parquet
            output_path = self.output_dir / 'claims_by_diagnosis.parquet'
            diagnosis_agg.to_parquet(output_path, index=False, compression='snappy')

            self.logger.info(f"  ✓ claims_by_diagnosis.parquet ({len(diagnosis_agg)} diagnoses)")

        except Exception as e:
            self.logger.error(f"Failed to generate diagnosis aggregation: {e}")

    def _generate_by_date(self, df: pd.DataFrame):
        """Generate time-series aggregations"""
        try:
            # Auto-detect date column
            date_col = self._find_column(df, ['service', 'date', 'admission', 'claim', 'received'])
            if not date_col:
                self.logger.warning("No date column found, skipping date aggregation")
                return

            # Auto-detect amount and claim ID columns
            amount_col = self._find_column(df, ['billed', 'amount', 'charged', 'cost', 'price'])
            claim_col = self._find_column(df, ['claim', 'id', 'number']) or date_col  # Fallback

            # Convert date column to datetime
            df_copy = df.copy()
            df_copy['date'] = pd.to_datetime(df_copy[date_col], errors='coerce')

            # Drop rows with invalid dates
            df_copy = df_copy.dropna(subset=['date'])

            if len(df_copy) == 0:
                self.logger.warning("No valid dates found for time-series aggregation")
                return

            # Extract just the date (no time)
            df_copy['date'] = df_copy['date'].dt.date

            # Build aggregation dict
            agg_dict = {claim_col: 'count'}
            if amount_col and amount_col in df_copy.columns:
                agg_dict[amount_col] = ['sum', 'mean']

            # Aggregate by date
            date_agg = df_copy.groupby('date').agg(agg_dict).reset_index()

            # Flatten column names
            date_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col
                               for col in date_agg.columns]

            # Rename for clarity (dynamic)
            rename_map = {
                f'{claim_col}_count': 'total_claims'
            }
            if amount_col:
                rename_map.update({
                    f'{amount_col}_sum': 'total_billed',
                    f'{amount_col}_mean': 'avg_billed'
                })
            date_agg.rename(columns=rename_map, inplace=True)

            # Sort by date
            date_agg = date_agg.sort_values('date')

            # Write to Parquet
            output_path = self.output_dir / 'claims_by_date.parquet'
            date_agg.to_parquet(output_path, index=False, compression='snappy')

            self.logger.info(f"  ✓ claims_by_date.parquet ({len(date_agg)} dates)")

        except Exception as e:
            self.logger.error(f"Failed to generate date aggregation: {e}")

    def get_stats(self) -> dict:
        """Get aggregation statistics"""
        base_stats = super().get_stats()
        base_stats.update({
            'records_aggregated': len(self._records),
            'output_dir': str(self.output_dir),
            'files_generated': [
                'claims_summary.parquet',
                'claims_by_provider.parquet',
                'claims_by_diagnosis.parquet',
                'claims_by_date.parquet'
            ]
        })
        return base_stats
