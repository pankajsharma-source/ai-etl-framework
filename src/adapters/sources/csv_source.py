"""
CSV source adapter for reading CSV files
"""
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import pandas as pd

from src.adapters.base import SourceAdapter
from src.common.models import Field, FieldType, Record, RecordMetadata, Schema
from src.common.exceptions import ConnectionError, ReadError


class CSVSource(SourceAdapter):
    """Source adapter for CSV files"""

    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        has_header: bool = True,
        **kwargs
    ):
        """
        Initialize CSV source

        Args:
            file_path: Path to CSV file
            delimiter: CSV delimiter (default: ',')
            encoding: File encoding (default: 'utf-8')
            has_header: Whether CSV has header row (default: True)
            **kwargs: Additional pandas read_csv parameters
        """
        config = {
            'file_path': file_path,
            'delimiter': delimiter,
            'encoding': encoding,
            'has_header': has_header,
            **kwargs
        }
        super().__init__(config)

        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.has_header = has_header
        self.pandas_kwargs = kwargs

        self._df: Optional[pd.DataFrame] = None
        self._current_row = 0
        self._file_hash: Optional[str] = None

    def connect(self) -> None:
        """Establish connection (validate file exists)"""
        if not self.file_path.exists():
            raise ConnectionError(f"CSV file not found: {self.file_path}")

        if not self.file_path.is_file():
            raise ConnectionError(f"Path is not a file: {self.file_path}")

        self._connected = True
        self.logger.info(f"Connected to CSV file: {self.file_path}")

        # Calculate file hash for state tracking
        self._file_hash = self._calculate_file_hash()

    def _calculate_file_hash(self) -> str:
        """Calculate SHA256 hash of file for change detection"""
        sha256 = hashlib.sha256()
        with open(self.file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def read(self, batch_size: int = 100) -> Iterator[Record]:
        """
        Read records from CSV file

        Args:
            batch_size: Number of rows to read at once

        Yields:
            Record: Individual records
        """
        if not self._connected:
            raise ReadError("Not connected. Call connect() first.")

        try:
            # Read CSV in chunks for memory efficiency
            chunk_iter = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                header=0 if self.has_header else None,
                chunksize=batch_size,
                **self.pandas_kwargs
            )

            row_num = 0
            for chunk in chunk_iter:
                for idx, row in chunk.iterrows():
                    # Convert row to dictionary
                    data = row.to_dict()

                    # Create metadata
                    metadata = RecordMetadata(
                        source_type="csv",
                        source_id=str(self.file_path),
                        record_id=f"row_{row_num}",
                        stage="extract"
                    )

                    # Create record
                    record = Record(
                        data=data,
                        metadata=metadata,
                        extracted_at=datetime.now()
                    )

                    yield record
                    row_num += 1

            self.logger.info(f"Read {row_num} records from CSV")

        except Exception as e:
            raise ReadError(f"Error reading CSV file: {e}")

    def get_schema(self) -> Schema:
        """
        Infer schema from CSV file

        Returns:
            Schema: Inferred schema
        """
        if not self._connected:
            self.connect()

        try:
            # Read small sample to infer schema
            sample_df = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                header=0 if self.has_header else None,
                nrows=100,
                **self.pandas_kwargs
            )

            fields = []
            for col_name, dtype in sample_df.dtypes.items():
                # Map pandas dtype to our FieldType
                field_type = self._map_pandas_dtype(dtype)

                # Check for nulls
                null_count = sample_df[col_name].isnull().sum()
                nullable = null_count > 0

                field = Field(
                    name=str(col_name),
                    type=field_type,
                    nullable=nullable,
                    inferred=True,
                    confidence=0.9  # High confidence for pandas inference
                )
                fields.append(field)

            schema = Schema(
                name=self.file_path.stem,
                fields=fields,
                inferred=True,
                created_at=datetime.now(),
                sample_size=len(sample_df)
            )

            self.logger.info(f"Inferred schema with {len(fields)} fields")
            return schema

        except Exception as e:
            raise ReadError(f"Error inferring schema: {e}")

    def _map_pandas_dtype(self, dtype) -> FieldType:
        """Map pandas dtype to FieldType"""
        dtype_str = str(dtype)

        if 'int' in dtype_str:
            return FieldType.INTEGER
        elif 'float' in dtype_str:
            return FieldType.FLOAT
        elif 'bool' in dtype_str:
            return FieldType.BOOLEAN
        elif 'datetime' in dtype_str:
            return FieldType.DATETIME
        elif 'object' in dtype_str:
            return FieldType.STRING
        else:
            return FieldType.STRING  # Default to string

    def supports_incremental(self) -> bool:
        """CSV files support incremental via file hash comparison"""
        return True

    def get_state(self) -> Dict[str, Any]:
        """Get current state for incremental processing"""
        return {
            'file_path': str(self.file_path),
            'file_hash': self._file_hash,
            'last_modified': self.file_path.stat().st_mtime,
            'rows_processed': self._current_row
        }

    def close(self) -> None:
        """Close and cleanup"""
        self._df = None
        self._connected = False
        self.logger.info("CSV source closed")
