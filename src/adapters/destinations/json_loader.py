"""
JSON destination adapter for writing data to JSON/JSONL files
"""
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
import tempfile
import shutil
import gzip
import bz2

from src.adapters.base import DestinationAdapter
from src.common.models import Field, FieldType, Record, Schema
from src.common.exceptions import ConnectionError, SchemaError, WriteError


class JSONLoader(DestinationAdapter):
    """Destination adapter for JSON and JSONL files"""

    def __init__(
        self,
        file_path: str,
        mode: str = "array",  # 'array' for JSON array, 'lines' for JSONL
        encoding: str = "utf-8",
        pretty: bool = False,  # Pretty print JSON (only for 'array' mode)
        indent: int = 2,  # Indentation for pretty printing
        compression: Optional[str] = None,  # None, 'gzip', 'bz2'
        export_schema: bool = False,  # Export schema to .schema.json file
        **kwargs
    ):
        """
        Initialize JSON loader

        Args:
            file_path: Path to output JSON file
            mode: Output format:
                - 'array': JSON array of objects (default)
                - 'lines': JSONL (one JSON object per line)
            encoding: File encoding (default: 'utf-8')
            pretty: Pretty print JSON (array mode only, default: False)
            indent: Indentation level for pretty printing (default: 2)
            compression: Compression format (None, 'gzip', 'bz2')
            export_schema: Export schema to separate .schema.json file
            **kwargs: Additional configuration
        """
        config = {
            'file_path': file_path,
            'mode': mode,
            'encoding': encoding,
            'pretty': pretty,
            'indent': indent,
            'compression': compression,
            'export_schema': export_schema,
            **kwargs
        }
        super().__init__(config)

        self.file_path = Path(file_path)
        self.mode = mode
        self.encoding = encoding
        self.pretty = pretty
        self.indent = indent
        self.compression = compression
        self.export_schema = export_schema

        # Validate mode
        if mode not in ['array', 'lines']:
            raise ValueError(f"Invalid mode: {mode}. Must be 'array' or 'lines'")

        self._batch: List[Dict] = []
        self._schema: Optional[Schema] = None
        self._file_handle = None

    def connect(self) -> None:
        """Establish connection (validate/create output directory)"""
        try:
            # Create parent directory if it doesn't exist
            self.file_path.parent.mkdir(parents=True, exist_ok=True)

            self._connected = True
            self.logger.info(f"JSON loader connected, will write to: {self.file_path}")

        except Exception as e:
            raise ConnectionError(f"Failed to connect JSON loader: {e}")

    def create_schema(self, schema: Schema) -> None:
        """
        Store schema for optional export

        Args:
            schema: Schema definition

        Raises:
            SchemaError: If schema creation fails
        """
        if not self._connected:
            raise SchemaError("Not connected. Call connect() first.")

        try:
            self._schema = schema
            self.logger.info(f"Schema set with {len(schema.fields)} fields")

        except Exception as e:
            raise SchemaError(f"Failed to create schema: {e}")

    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to JSON buffer

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

        try:
            # For array mode, buffer all records
            # For lines mode, can write incrementally
            for record in records:
                self._batch.append(record.data)
                count += 1

                # For JSONL mode, write incrementally to save memory
                if self.mode == 'lines':
                    batch_size = self.config.get('batch_size', 1000)
                    if len(self._batch) >= batch_size:
                        self._flush_jsonl_batch()

            self.logger.info(f"Buffered {count} records for JSON output")
            return count

        except Exception as e:
            raise WriteError(f"Failed to write records: {e}")

    def _flush_jsonl_batch(self) -> None:
        """Flush batch for JSONL mode (incremental writing)"""
        if not self._batch:
            return

        try:
            # Open file in append mode
            file_handle = self._get_file_handle('a')

            for record in self._batch:
                json_line = json.dumps(record, ensure_ascii=False)
                file_handle.write(json_line + '\n')

            file_handle.close()
            self._batch = []

            self.logger.debug(f"Flushed JSONL batch")

        except Exception as e:
            raise WriteError(f"Failed to flush JSONL batch: {e}")

    def _get_file_handle(self, mode: str):
        """
        Get appropriate file handle based on compression

        Args:
            mode: File open mode ('w', 'a', etc.)

        Returns:
            File handle
        """
        if self.compression == 'gzip':
            return gzip.open(self.file_path, mode + 't', encoding=self.encoding)
        elif self.compression == 'bz2':
            return bz2.open(self.file_path, mode + 't', encoding=self.encoding)
        else:
            return open(self.file_path, mode, encoding=self.encoding)

    def _write_array_mode(self) -> None:
        """Write all records as JSON array"""
        try:
            file_handle = self._get_file_handle('w')

            if self.pretty:
                json.dump(
                    self._batch,
                    file_handle,
                    ensure_ascii=False,
                    indent=self.indent
                )
            else:
                json.dump(self._batch, file_handle, ensure_ascii=False)

            file_handle.close()

            self.logger.info(f"Wrote {len(self._batch)} records to JSON array")

        except Exception as e:
            raise WriteError(f"Failed to write JSON array: {e}")

    def _write_lines_mode(self) -> None:
        """Write remaining records as JSONL"""
        if not self._batch:
            return

        try:
            # If there are remaining records, flush them
            self._flush_jsonl_batch()

        except Exception as e:
            raise WriteError(f"Failed to write JSONL: {e}")

    def _export_schema_file(self) -> None:
        """Export schema to separate .schema.json file"""
        if not self._schema or not self.export_schema:
            return

        try:
            schema_path = self.file_path.parent / f"{self.file_path.stem}.schema.json"

            schema_dict = {
                'name': self._schema.name,
                'fields': [
                    {
                        'name': field.name,
                        'type': field.type.value,
                        'nullable': field.nullable,
                        'description': field.description,
                        'pattern': field.pattern,
                        'min_value': field.min_value,
                        'max_value': field.max_value,
                        'enum_values': field.enum_values
                    }
                    for field in self._schema.fields
                ],
                'inferred': self._schema.inferred,
                'primary_key': self._schema.primary_key
            }

            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(schema_dict, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Schema exported to {schema_path}")

        except Exception as e:
            self.logger.warning(f"Failed to export schema: {e}")

    def begin_transaction(self) -> None:
        """Begin transaction"""
        super().begin_transaction()

    def commit(self) -> None:
        """Commit transaction - write buffered records to file"""
        if self._transaction_active:
            try:
                # Write based on mode
                if self.mode == 'array':
                    self._write_array_mode()
                elif self.mode == 'lines':
                    self._write_lines_mode()

                # Export schema if requested
                if self.export_schema and self._schema:
                    self._export_schema_file()

                # Clear batch
                self._batch = []

                self.logger.info(f"Transaction committed, JSON written to {self.file_path}")

            except Exception as e:
                self.logger.error(f"Error during commit: {e}")
                self.rollback()
                raise WriteError(f"Failed to commit: {e}")

        super().commit()

    def rollback(self) -> None:
        """Rollback transaction - discard buffered records"""
        if self._transaction_active:
            # Clear batch
            self._batch = []

            # Delete output file if it exists (for JSONL partial writes)
            if self.file_path.exists():
                try:
                    self.file_path.unlink()
                    self.logger.debug("Output file deleted (rollback)")
                except Exception as e:
                    self.logger.warning(f"Failed to delete output file: {e}")

            self.logger.debug("Transaction rolled back")

        super().rollback()

    def close(self) -> None:
        """Close and cleanup"""
        # If not in transaction and there's data, this is an error
        # (should have been committed)
        if not self._transaction_active and self._batch:
            self.logger.warning(
                f"{len(self._batch)} records in buffer were not written "
                "(commit was not called)"
            )

        self._connected = False
        self.logger.info("JSON loader closed")
