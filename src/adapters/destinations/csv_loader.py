"""
CSV destination adapter for writing data to CSV files
"""
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
import tempfile
import shutil

from src.adapters.base import DestinationAdapter
from src.common.models import Field, FieldType, Record, Schema
from src.common.exceptions import ConnectionError, SchemaError, WriteError


class CSVLoader(DestinationAdapter):
    """Destination adapter for CSV files"""

    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        mode: str = "overwrite",  # 'overwrite' or 'append'
        compression: Optional[str] = None,  # None, 'gzip', 'bz2', 'zip', 'xz'
        include_index: bool = False,
        **kwargs
    ):
        """
        Initialize CSV loader

        Args:
            file_path: Path to output CSV file
            delimiter: CSV delimiter (default: ',')
            encoding: File encoding (default: 'utf-8')
            mode: Write mode - 'overwrite' (default) or 'append'
            compression: Compression format (None, 'gzip', 'bz2', 'zip', 'xz')
            include_index: Include pandas index in output (default: False)
            **kwargs: Additional pandas to_csv parameters
        """
        config = {
            'file_path': file_path,
            'delimiter': delimiter,
            'encoding': encoding,
            'mode': mode,
            'compression': compression,
            'include_index': include_index,
            **kwargs
        }
        super().__init__(config)

        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.write_mode = mode
        self.compression = compression
        self.include_index = include_index
        self.pandas_kwargs = kwargs

        self._batch: List[Dict] = []
        self._temp_file: Optional[Path] = None
        self._schema: Optional[Schema] = None
        self._header_written = False

    def connect(self) -> None:
        """Establish connection (validate/create output directory)"""
        try:
            # Create parent directory if it doesn't exist
            self.file_path.parent.mkdir(parents=True, exist_ok=True)

            # If appending, check if file exists
            if self.write_mode == 'append' and self.file_path.exists():
                if not self.file_path.is_file():
                    raise ConnectionError(f"Path exists but is not a file: {self.file_path}")
                self._header_written = True  # Don't write header again
                self.logger.info(f"Appending to existing CSV: {self.file_path}")
            else:
                self.logger.info(f"Will write CSV to: {self.file_path}")

            # Create temporary file for transaction support
            temp_dir = self.file_path.parent
            self._temp_file = Path(tempfile.mktemp(
                suffix='.csv.tmp',
                dir=str(temp_dir)
            ))

            self._connected = True
            self.logger.info(f"CSV loader connected")

        except Exception as e:
            raise ConnectionError(f"Failed to connect CSV loader: {e}")

    def create_schema(self, schema: Schema) -> None:
        """
        Store schema for CSV header generation

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
        Write records to CSV file

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

            self.logger.info(f"Wrote {count} records to CSV batch")
            return count

        except Exception as e:
            raise WriteError(f"Failed to write records: {e}")

    def _flush_batch(self) -> None:
        """Flush current batch to CSV file"""
        if not self._batch:
            return

        try:
            # Convert batch to DataFrame
            df = pd.DataFrame(self._batch)

            # Determine column order from schema if available
            # NOTE: Only use schema for column ordering if all columns match
            # Don't drop extra columns (e.g., metadata columns added by transformers)
            if self._schema:
                schema_columns = [field.name for field in self._schema.fields]
                # Only reorder if no extra columns exist
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

            # Determine write mode
            if self._transaction_active:
                # During transaction, write to temp file
                output_file = self._temp_file
                write_header = not self._header_written
                file_mode = 'a' if self._header_written else 'w'
            else:
                # Not in transaction, write directly
                output_file = self.file_path
                if self.write_mode == 'append' and self.file_path.exists():
                    write_header = False
                    file_mode = 'a'
                else:
                    write_header = True
                    file_mode = 'w'

            # Write to CSV
            df.to_csv(
                output_file,
                sep=self.delimiter,
                encoding=self.encoding,
                mode=file_mode,
                header=write_header,
                index=self.include_index,
                compression=self.compression,
                **self.pandas_kwargs
            )

            self._header_written = True
            self._batch = []

            self.logger.debug(f"Flushed batch of {len(df)} records to {output_file}")

        except Exception as e:
            raise WriteError(f"Failed to flush batch: {e}")

    def begin_transaction(self) -> None:
        """Begin transaction"""
        super().begin_transaction()
        self._header_written = False  # Reset for transaction

    def commit(self) -> None:
        """Commit transaction - finalize the file"""
        if self._transaction_active:
            try:
                # Flush any remaining batch
                if self._batch:
                    self._flush_batch()

                # Move temp file to final location
                if self._temp_file and self._temp_file.exists():
                    # If appending and target exists, append temp to target
                    if self.write_mode == 'append' and self.file_path.exists():
                        # Read temp file and append to target
                        temp_df = pd.read_csv(
                            self._temp_file,
                            sep=self.delimiter,
                            encoding=self.encoding
                        )
                        temp_df.to_csv(
                            self.file_path,
                            sep=self.delimiter,
                            encoding=self.encoding,
                            mode='a',
                            header=False,
                            index=self.include_index,
                            compression=self.compression
                        )
                        # Remove temp file
                        self._temp_file.unlink()
                    else:
                        # Move temp file to final location
                        shutil.move(str(self._temp_file), str(self.file_path))

                    self.logger.info(f"Transaction committed, CSV written to {self.file_path}")

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
            self._header_written = False

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
        self.logger.info("CSV loader closed")
