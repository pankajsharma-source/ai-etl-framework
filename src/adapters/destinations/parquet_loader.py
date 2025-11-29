"""
Parquet destination adapter for writing data to Parquet files
"""
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
import tempfile
import shutil

from src.adapters.base import DestinationAdapter
from src.common.models import Field, FieldType, Record, Schema
from src.common.exceptions import ConnectionError, SchemaError, WriteError


class ParquetLoader(DestinationAdapter):
    """Destination adapter for Parquet files"""

    def __init__(
        self,
        file_path: str,
        compression: str = "snappy",  # 'snappy', 'gzip', 'brotli', 'zstd', 'lz4', None
        mode: str = "overwrite",  # 'overwrite' or 'append'
        partition_cols: Optional[List[str]] = None,
        row_group_size: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize Parquet loader

        Args:
            file_path: Path to output Parquet file
            compression: Compression codec (default: 'snappy')
            mode: Write mode - 'overwrite' (default) or 'append'
            partition_cols: Columns to partition by (for directory partitioning)
            row_group_size: Number of rows per row group
            **kwargs: Additional pandas to_parquet parameters
        """
        config = {
            'file_path': file_path,
            'compression': compression,
            'mode': mode,
            'partition_cols': partition_cols,
            'row_group_size': row_group_size,
            **kwargs
        }
        super().__init__(config)

        self.file_path = Path(file_path)
        self.compression = compression
        self.write_mode = mode
        self.partition_cols = partition_cols
        self.row_group_size = row_group_size
        self.pandas_kwargs = kwargs

        self._batch: List[Dict] = []
        self._temp_file: Optional[Path] = None
        self._schema: Optional[Schema] = None

    def connect(self) -> None:
        """Establish connection (validate/create output directory)"""
        try:
            # Create parent directory if it doesn't exist
            self.file_path.parent.mkdir(parents=True, exist_ok=True)

            # If appending, check if file exists
            if self.write_mode == 'append' and self.file_path.exists():
                if not self.file_path.is_file():
                    raise ConnectionError(f"Path exists but is not a file: {self.file_path}")
                self.logger.info(f"Appending to existing Parquet: {self.file_path}")
            else:
                self.logger.info(f"Will write Parquet to: {self.file_path}")

            # Create temporary file for transaction support
            temp_dir = self.file_path.parent
            self._temp_file = Path(tempfile.mktemp(
                suffix='.parquet.tmp',
                dir=str(temp_dir)
            ))

            self._connected = True
            self.logger.info(f"Parquet loader connected")

        except Exception as e:
            raise ConnectionError(f"Failed to connect Parquet loader: {e}")

    def create_schema(self, schema: Schema) -> None:
        """
        Store schema for Parquet schema generation

        Args:
            schema: Schema definition

        Raises:
            SchemaError: If schema creation fails
        """
        if not self._connected:
            raise SchemaError("Not connected. Call connect() first.")

        try:
            self._schema = schema
            self.logger.info(f"Schema set with {len(schema.fields)} columns")

        except Exception as e:
            raise SchemaError(f"Failed to create schema: {e}")

    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to Parquet file

        Args:
            records: Iterator of records to write

        Returns:
            int: Number of records written

        Raises:
            WriteError: If writing fails
        """
        if not self._connected:
            raise WriteError("Not connected. Call connect() first.")

        count = 0
        batch_size = self.config.get('batch_size', 1000)

        try:
            for record in records:
                self._batch.append(record.data)
                count += 1

                # Write batch when it reaches batch_size
                if len(self._batch) >= batch_size:
                    self._flush_batch()

            # Write remaining records
            if self._batch:
                self._flush_batch()

            self.logger.info(f"Wrote {count} records to Parquet batch")
            return count

        except Exception as e:
            raise WriteError(f"Failed to write records: {e}")

    def _flush_batch(self) -> None:
        """Flush current batch to Parquet file"""
        if not self._batch:
            return

        try:
            # Convert batch to DataFrame
            df = pd.DataFrame(self._batch)

            # Determine column order from schema if available
            if self._schema:
                schema_columns = [field.name for field in self._schema.fields]
                df_columns = list(df.columns)
                extra_columns = [c for c in df_columns if c not in schema_columns]

                if not extra_columns:
                    # Exact match - use schema order
                    df = df.reindex(columns=schema_columns)
                else:
                    # Extra columns exist (e.g., metadata) - preserve all columns
                    # Put schema columns first, then extra columns
                    ordered_columns = [c for c in schema_columns if c in df_columns] + extra_columns
                    df = df[ordered_columns]

            # Apply type conversions based on schema
            if self._schema:
                df = self._apply_schema_types(df)

            # Determine output file
            if self._transaction_active:
                # During transaction, write to temp file
                output_file = self._temp_file
            else:
                # Not in transaction, write directly
                output_file = self.file_path

            # Prepare to_parquet kwargs
            parquet_kwargs = {
                'compression': self.compression,
                'index': False,
                **self.pandas_kwargs
            }

            # Add row_group_size if specified
            if self.row_group_size:
                parquet_kwargs['row_group_size'] = self.row_group_size

            # Handle append mode
            if self.write_mode == 'append' and output_file.exists():
                # For append, read existing and concatenate
                existing_df = pd.read_parquet(output_file)
                df = pd.concat([existing_df, df], ignore_index=True)

            # Write to Parquet
            if self.partition_cols:
                # Partitioned write (creates directory structure)
                df.to_parquet(
                    output_file.parent,
                    partition_cols=self.partition_cols,
                    **parquet_kwargs
                )
            else:
                # Single file write
                df.to_parquet(output_file, **parquet_kwargs)

            self._batch = []

            self.logger.debug(f"Flushed batch of {len(df)} records to {output_file}")

        except Exception as e:
            raise WriteError(f"Failed to flush batch: {e}")

    def _apply_schema_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply schema type conversions to DataFrame

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with correct types
        """
        if not self._schema:
            return df

        for field in self._schema.fields:
            if field.name not in df.columns:
                continue

            try:
                # Map FieldType to pandas dtype
                if field.type == FieldType.INTEGER:
                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce').astype('Int64')
                elif field.type == FieldType.FLOAT:
                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce')
                elif field.type == FieldType.BOOLEAN:
                    df[field.name] = df[field.name].astype('boolean')
                elif field.type in [FieldType.DATE, FieldType.DATETIME, FieldType.TIMESTAMP]:
                    df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
                elif field.type == FieldType.STRING:
                    df[field.name] = df[field.name].astype('string')
                # JSON and ARRAY types remain as objects

            except Exception as e:
                self.logger.warning(f"Failed to convert column {field.name} to {field.type}: {e}")

        return df

    def begin_transaction(self) -> None:
        """Begin transaction"""
        super().begin_transaction()

    def commit(self) -> None:
        """Commit transaction - finalize the file"""
        if self._transaction_active:
            try:
                # Flush any remaining batch
                if self._batch:
                    self._flush_batch()

                # Move temp file to final location
                if self._temp_file and self._temp_file.exists():
                    # If appending and target exists, merge
                    if self.write_mode == 'append' and self.file_path.exists():
                        # Read both and concatenate
                        existing_df = pd.read_parquet(self.file_path)
                        temp_df = pd.read_parquet(self._temp_file)
                        merged_df = pd.concat([existing_df, temp_df], ignore_index=True)

                        # Write merged data
                        parquet_kwargs = {
                            'compression': self.compression,
                            'index': False,
                            **self.pandas_kwargs
                        }
                        if self.row_group_size:
                            parquet_kwargs['row_group_size'] = self.row_group_size

                        merged_df.to_parquet(self.file_path, **parquet_kwargs)

                        # Remove temp file
                        self._temp_file.unlink()
                    else:
                        # Move temp file to final location
                        shutil.move(str(self._temp_file), str(self.file_path))

                    self.logger.info(f"Transaction committed, Parquet written to {self.file_path}")

            except Exception as e:
                self.logger.error(f"Error during commit: {e}")
                self.rollback()
                raise WriteError(f"Failed to commit: {e}")

        super().commit()

    def rollback(self) -> None:
        """Rollback transaction - discard temp file"""
        if self._transaction_active:
            # Delete temp file
            if self._temp_file and self._temp_file.exists():
                try:
                    self._temp_file.unlink()
                    self.logger.debug("Temp file deleted (rollback)")
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp file: {e}")

            # Clear batch
            self._batch = []

            self.logger.debug("Transaction rolled back")

        super().rollback()

    def close(self) -> None:
        """Close and cleanup"""
        # If not in transaction, flush any remaining batch
        if not self._transaction_active and self._batch:
            try:
                self._flush_batch()
            except Exception as e:
                self.logger.error(f"Error during final flush: {e}")

        # Clean up temp file if exists
        if self._temp_file and self._temp_file.exists():
            try:
                self._temp_file.unlink()
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file: {e}")

        self._connected = False
        self.logger.info("Parquet loader closed")
