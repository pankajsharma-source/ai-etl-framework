"""
File-based intermediate storage implementation
"""
import json
import pickle
from pathlib import Path
from typing import List, Tuple, Optional
from dataclasses import asdict
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.base import IntermediateStorage
from src.common.models import Record, Schema
from src.common.exceptions import StorageError
from src.common.logging import get_logger


class FileStorage(IntermediateStorage):
    """
    File-based intermediate storage using Parquet format

    Stores data on local filesystem for testing and local development
    """

    def __init__(self, base_path: str = "./.state/intermediate"):
        """
        Initialize file storage

        Args:
            base_path: Base directory for storing data
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("FileStorage")

    def save_records(
        self,
        key: str,
        records: List[Record],
        schema: Optional[Schema] = None,
        metadata: Optional[dict] = None
    ) -> None:
        """Save records to Parquet file"""
        try:
            file_path = self._get_file_path(key)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert records to Arrow table
            table = self._records_to_arrow_table(records)

            # Write to Parquet
            pq.write_table(table, file_path, compression='snappy')

            # Save metadata separately
            # Serialize schema properly (handle enums)
            schema_dict = None
            if schema:
                schema_dict = asdict(schema)
                # Convert FieldType enums to their values
                for field in schema_dict['fields']:
                    if 'type' in field:
                        field['type'] = field['type'].value if hasattr(field['type'], 'value') else str(field['type'])

            metadata_dict = {
                'record_count': len(records),
                'schema': schema_dict,
                'custom_metadata': metadata or {}
            }

            metadata_path = file_path.with_suffix('.meta.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata_dict, f, indent=2, default=str)

            self.logger.info(f"Saved {len(records)} records to {file_path}")

        except Exception as e:
            raise StorageError(f"Failed to save records to {key}: {e}")

    def load_records(self, key: str) -> Tuple[List[Record], Optional[Schema]]:
        """Load records from Parquet file"""
        try:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                raise KeyError(f"Key not found: {key}")

            # Read from Parquet
            table = pq.read_table(file_path)

            # Convert to records
            records = self._arrow_table_to_records(table)

            # Load metadata
            schema = None
            metadata_path = file_path.with_suffix('.meta.json')
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    if metadata.get('schema'):
                        schema = self._dict_to_schema(metadata['schema'])

            self.logger.info(f"Loaded {len(records)} records from {file_path}")

            return records, schema

        except KeyError:
            raise
        except Exception as e:
            raise StorageError(f"Failed to load records from {key}: {e}")

    def exists(self, key: str) -> bool:
        """Check if file exists"""
        file_path = self._get_file_path(key)
        return file_path.exists()

    def delete(self, key: str) -> None:
        """Delete file and metadata"""
        try:
            file_path = self._get_file_path(key)
            metadata_path = file_path.with_suffix('.meta.json')

            if file_path.exists():
                file_path.unlink()

            if metadata_path.exists():
                metadata_path.unlink()

            self.logger.info(f"Deleted {key}")

        except Exception as e:
            raise StorageError(f"Failed to delete {key}: {e}")

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all Parquet files, optionally filtered by prefix"""
        try:
            pattern = f"{prefix}*" if prefix else "*"
            files = self.base_path.rglob(f"{pattern}.parquet")

            # Convert file paths back to keys
            keys = []
            for file_path in files:
                relative_path = file_path.relative_to(self.base_path)
                key = str(relative_path.with_suffix(''))
                keys.append(key)

            return keys

        except Exception as e:
            raise StorageError(f"Failed to list keys: {e}")

    def _get_file_path(self, key: str) -> Path:
        """Convert key to file path"""
        # Replace slashes with path separators
        return self.base_path / f"{key}.parquet"

    def _dict_to_schema(self, schema_dict: dict) -> Schema:
        """Convert dict back to Schema object"""
        from src.common.models import Field, FieldType
        from datetime import datetime

        # Reconstruct fields
        fields = []
        for field_dict in schema_dict['fields']:
            # Convert FieldType string back to enum
            field_type = FieldType(field_dict['type'])
            field = Field(
                name=field_dict['name'],
                type=field_type,
                nullable=field_dict.get('nullable', True),
                description=field_dict.get('description'),
                min_value=field_dict.get('min_value'),
                max_value=field_dict.get('max_value'),
                pattern=field_dict.get('pattern'),
                enum_values=field_dict.get('enum_values'),
                inferred=field_dict.get('inferred', False),
                confidence=field_dict.get('confidence')
            )
            fields.append(field)

        # Reconstruct schema
        schema = Schema(
            name=schema_dict['name'],
            fields=fields,
            primary_key=schema_dict.get('primary_key'),
            version=schema_dict.get('version', '1.0'),
            inferred=schema_dict.get('inferred', False),
            created_at=datetime.fromisoformat(schema_dict['created_at']) if schema_dict.get('created_at') else None,
            sample_size=schema_dict.get('sample_size'),
            null_counts=schema_dict.get('null_counts')
        )

        return schema

    def _records_to_arrow_table(self, records: List[Record]) -> pa.Table:
        """Convert Record objects to Arrow Table"""
        if not records:
            return pa.table({})

        # Extract data from records
        data_dicts = [record.data for record in records]

        # Convert to pandas DataFrame first (easier)
        import pandas as pd
        df = pd.DataFrame(data_dicts)

        # Convert to Arrow table
        return pa.Table.from_pandas(df)

    def _arrow_table_to_records(self, table: pa.Table) -> List[Record]:
        """Convert Arrow Table to Record objects"""
        from datetime import datetime
        from src.common.models import RecordMetadata

        # Convert to pandas DataFrame
        df = table.to_pandas()

        # Convert to records
        records = []
        for idx, row in df.iterrows():
            data = row.to_dict()
            metadata = RecordMetadata(
                source_type="FileStorage",
                source_id="intermediate",
                record_id=str(idx)
            )
            record = Record(
                data=data,
                metadata=metadata,
                extracted_at=datetime.now()
            )
            records.append(record)

        return records
