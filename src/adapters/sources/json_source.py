"""
JSON source adapter for reading JSON and JSONL files
"""
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from src.adapters.base import SourceAdapter
from src.common.models import Field, FieldType, Record, RecordMetadata, Schema
from src.common.exceptions import ConnectionError, ReadError


class JSONSource(SourceAdapter):
    """Source adapter for JSON and JSONL files"""

    def __init__(
        self,
        file_path: str,
        encoding: str = "utf-8",
        mode: str = "auto",  # 'auto', 'array', 'lines'
        json_path: Optional[str] = None,  # Path to nested array (e.g., "data.records")
        **kwargs
    ):
        """
        Initialize JSON source

        Args:
            file_path: Path to JSON/JSONL file
            encoding: File encoding (default: 'utf-8')
            mode: JSON format mode:
                - 'auto': Auto-detect (default)
                - 'array': JSON array of objects
                - 'lines': JSONL (one JSON object per line)
            json_path: Dot-notation path to nested array (e.g., "data.records")
            **kwargs: Additional configuration
        """
        config = {
            'file_path': file_path,
            'encoding': encoding,
            'mode': mode,
            'json_path': json_path,
            **kwargs
        }
        super().__init__(config)

        self.file_path = Path(file_path)
        self.encoding = encoding
        self.mode = mode
        self.json_path = json_path

        self._file_hash: Optional[str] = None
        self._records_count = 0

    def connect(self) -> None:
        """Establish connection (validate file exists)"""
        if not self.file_path.exists():
            raise ConnectionError(f"JSON file not found: {self.file_path}")

        if not self.file_path.is_file():
            raise ConnectionError(f"Path is not a file: {self.file_path}")

        self._connected = True
        self.logger.info(f"Connected to JSON file: {self.file_path}")

        # Calculate file hash for state tracking
        self._file_hash = self._calculate_file_hash()

        # Auto-detect mode if set to 'auto'
        if self.mode == 'auto':
            self.mode = self._detect_mode()
            self.logger.info(f"Auto-detected mode: {self.mode}")

    def _calculate_file_hash(self) -> str:
        """Calculate SHA256 hash of file for change detection"""
        sha256 = hashlib.sha256()
        with open(self.file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _detect_mode(self) -> str:
        """Auto-detect JSON format"""
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                first_line = f.readline().strip()

                # Check if it's JSONL (object on first line)
                if first_line.startswith('{') and first_line.endswith('}'):
                    try:
                        json.loads(first_line)
                        return 'lines'
                    except json.JSONDecodeError:
                        pass

                # Check if it's JSON array
                if first_line.startswith('['):
                    return 'array'

                # Default to array
                return 'array'

        except Exception as e:
            self.logger.warning(f"Error detecting mode: {e}, defaulting to 'array'")
            return 'array'

    def read(self, batch_size: int = 100) -> Iterator[Record]:
        """
        Read records from JSON file

        Args:
            batch_size: Not used for JSON, kept for interface compatibility

        Yields:
            Record: Individual records
        """
        if not self._connected:
            raise ReadError("Not connected. Call connect() first.")

        try:
            if self.mode == 'lines':
                yield from self._read_jsonl()
            elif self.mode == 'array':
                yield from self._read_json_array()
            else:
                raise ReadError(f"Unsupported mode: {self.mode}")

            self.logger.info(f"Read {self._records_count} records from JSON")

        except Exception as e:
            raise ReadError(f"Error reading JSON file: {e}")

    def _read_jsonl(self) -> Iterator[Record]:
        """Read JSONL file (one JSON object per line)"""
        with open(self.file_path, 'r', encoding=self.encoding) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)

                    # Create metadata
                    metadata = RecordMetadata(
                        source_type="jsonl",
                        source_id=str(self.file_path),
                        record_id=f"line_{line_num}",
                        stage="extract"
                    )

                    # Create record
                    record = Record(
                        data=data if isinstance(data, dict) else {"value": data},
                        metadata=metadata,
                        extracted_at=datetime.now()
                    )

                    yield record
                    self._records_count += 1

                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue

    def _read_json_array(self) -> Iterator[Record]:
        """Read JSON file containing an array of objects"""
        with open(self.file_path, 'r', encoding=self.encoding) as f:
            data = json.load(f)

            # Navigate to nested path if specified
            if self.json_path:
                data = self._get_nested_value(data, self.json_path)

            # Ensure data is a list
            if not isinstance(data, list):
                raise ReadError(
                    f"JSON data is not an array. Found: {type(data).__name__}. "
                    f"Use json_path parameter if data is nested."
                )

            # Iterate through array
            for idx, item in enumerate(data):
                # Create metadata
                metadata = RecordMetadata(
                    source_type="json",
                    source_id=str(self.file_path),
                    record_id=f"item_{idx}",
                    stage="extract"
                )

                # Create record
                record = Record(
                    data=item if isinstance(item, dict) else {"value": item},
                    metadata=metadata,
                    extracted_at=datetime.now()
                )

                yield record
                self._records_count += 1

    def _get_nested_value(self, data: Any, path: str) -> Any:
        """
        Navigate to nested value using dot notation

        Args:
            data: Source data
            path: Dot-separated path (e.g., "data.records")

        Returns:
            Nested value
        """
        keys = path.split('.')
        value = data

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                raise ReadError(f"Invalid json_path: '{path}' not found")

        return value

    def get_schema(self) -> Schema:
        """
        Infer schema from JSON file

        Returns:
            Schema: Inferred schema
        """
        if not self._connected:
            self.connect()

        try:
            # Read sample to infer schema
            sample = []
            for i, record in enumerate(self.read()):
                sample.append(record.data)
                if i >= 99:  # Sample first 100 records
                    break

            if not sample:
                raise ReadError("No records found to infer schema")

            # Get all unique keys across samples
            all_keys = set()
            for item in sample:
                all_keys.update(item.keys())

            # Infer type for each field
            fields = []
            for key in sorted(all_keys):
                field_type, nullable = self._infer_field_type(sample, key)

                field = Field(
                    name=key,
                    type=field_type,
                    nullable=nullable,
                    inferred=True,
                    confidence=0.85
                )
                fields.append(field)

            schema = Schema(
                name=self.file_path.stem,
                fields=fields,
                inferred=True,
                created_at=datetime.now(),
                sample_size=len(sample)
            )

            self.logger.info(f"Inferred schema with {len(fields)} fields")
            return schema

        except Exception as e:
            raise ReadError(f"Error inferring schema: {e}")

    def _infer_field_type(self, sample: List[Dict], key: str) -> tuple:
        """
        Infer field type from sample data

        Returns:
            (FieldType, nullable)
        """
        values = [item.get(key) for item in sample if key in item]
        non_null_values = [v for v in values if v is not None]

        # Check if nullable
        nullable = len(non_null_values) < len(values)

        if not non_null_values:
            return FieldType.STRING, True

        # Infer type from non-null values
        types = set(type(v).__name__ for v in non_null_values)

        if 'dict' in types:
            return FieldType.JSON, nullable
        elif 'list' in types:
            return FieldType.ARRAY, nullable
        elif 'bool' in types:
            return FieldType.BOOLEAN, nullable
        elif 'int' in types:
            return FieldType.INTEGER, nullable
        elif 'float' in types:
            return FieldType.FLOAT, nullable
        else:
            return FieldType.STRING, nullable

    def supports_incremental(self) -> bool:
        """JSON files support incremental via file hash comparison"""
        return True

    def get_state(self) -> Dict[str, Any]:
        """Get current state for incremental processing"""
        return {
            'file_path': str(self.file_path),
            'file_hash': self._file_hash,
            'last_modified': self.file_path.stat().st_mtime,
            'records_processed': self._records_count
        }

    def close(self) -> None:
        """Close and cleanup"""
        self._connected = False
        self.logger.info("JSON source closed")
