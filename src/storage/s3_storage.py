"""
S3-based intermediate storage implementation
"""
import json
import io
from typing import List, Tuple, Optional
from dataclasses import asdict
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError

from src.storage.base import IntermediateStorage
from src.common.models import Record, Schema
from src.common.exceptions import StorageError
from src.common.logging import get_logger


class S3Storage(IntermediateStorage):
    """
    S3-based intermediate storage using Parquet format

    Stores data in S3 for production cloud deployments
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "intermediate",
        region: Optional[str] = None
    ):
        """
        Initialize S3 storage

        Args:
            bucket: S3 bucket name
            prefix: Key prefix (folder) for all data
            region: AWS region (optional, uses default if not specified)
        """
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.logger = get_logger("S3Storage")

        # Initialize S3 client
        if region:
            self.s3_client = boto3.client('s3', region_name=region)
        else:
            self.s3_client = boto3.client('s3')

    def save_records(
        self,
        key: str,
        records: List[Record],
        schema: Optional[Schema] = None,
        metadata: Optional[dict] = None
    ) -> None:
        """Save records to S3 as Parquet"""
        try:
            s3_key = self._get_s3_key(key)

            # Convert records to Arrow table
            table = self._records_to_arrow_table(records)

            # Write to in-memory buffer
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )

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

            metadata_key = f"{s3_key}.meta.json"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=metadata_key,
                Body=json.dumps(metadata_dict, indent=2, default=str).encode('utf-8'),
                ContentType='application/json'
            )

            self.logger.info(
                f"Saved {len(records)} records to s3://{self.bucket}/{s3_key}"
            )

        except Exception as e:
            raise StorageError(f"Failed to save records to {key}: {e}")

    def load_records(self, key: str) -> Tuple[List[Record], Optional[Schema]]:
        """Load records from S3 Parquet"""
        try:
            s3_key = self._get_s3_key(key)

            # Check if exists
            if not self.exists(key):
                raise KeyError(f"Key not found: {key}")

            # Download from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=s3_key
            )

            # Read Parquet from buffer
            buffer = io.BytesIO(response['Body'].read())
            table = pq.read_table(buffer)

            # Convert to records
            records = self._arrow_table_to_records(table)

            # Load metadata
            schema = None
            metadata_key = f"{s3_key}.meta.json"
            try:
                metadata_response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=metadata_key
                )
                metadata = json.loads(metadata_response['Body'].read().decode('utf-8'))
                if metadata.get('schema'):
                    schema = self._dict_to_schema(metadata['schema'])
            except ClientError:
                # Metadata not found - not critical
                pass

            self.logger.info(
                f"Loaded {len(records)} records from s3://{self.bucket}/{s3_key}"
            )

            return records, schema

        except KeyError:
            raise
        except Exception as e:
            raise StorageError(f"Failed to load records from {key}: {e}")

    def exists(self, key: str) -> bool:
        """Check if S3 object exists"""
        try:
            s3_key = self._get_s3_key(key)
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            return False

    def delete(self, key: str) -> None:
        """Delete S3 object and metadata"""
        try:
            s3_key = self._get_s3_key(key)
            metadata_key = f"{s3_key}.meta.json"

            # Delete both files
            objects_to_delete = [
                {'Key': s3_key},
                {'Key': metadata_key}
            ]

            self.s3_client.delete_objects(
                Bucket=self.bucket,
                Delete={'Objects': objects_to_delete}
            )

            self.logger.info(f"Deleted s3://{self.bucket}/{s3_key}")

        except Exception as e:
            raise StorageError(f"Failed to delete {key}: {e}")

    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all S3 objects, optionally filtered by prefix"""
        try:
            # Combine storage prefix with optional filter prefix
            full_prefix = f"{self.prefix}/"
            if prefix:
                full_prefix += prefix

            # List objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=full_prefix)

            keys = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_key = obj['Key']
                        # Skip metadata files
                        if s3_key.endswith('.meta.json'):
                            continue
                        # Convert S3 key back to storage key
                        key = s3_key.replace(f"{self.prefix}/", "").replace(".parquet", "")
                        keys.append(key)

            return keys

        except Exception as e:
            raise StorageError(f"Failed to list keys: {e}")

    def get_presigned_upload_url(self, key: str, expiration: int = 3600) -> str:
        """
        Generate presigned URL for direct file upload

        Args:
            key: Storage key
            expiration: URL expiration time in seconds

        Returns:
            Presigned URL for PUT operation
        """
        try:
            s3_key = self._get_s3_key(key)
            url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': self.bucket, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            raise StorageError(f"Failed to generate presigned URL: {e}")

    def get_presigned_download_url(self, key: str, expiration: int = 3600) -> str:
        """
        Generate presigned URL for direct file download

        Args:
            key: Storage key
            expiration: URL expiration time in seconds

        Returns:
            Presigned URL for GET operation
        """
        try:
            s3_key = self._get_s3_key(key)
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            raise StorageError(f"Failed to generate presigned URL: {e}")

    def _get_s3_key(self, key: str) -> str:
        """Convert storage key to S3 key with prefix"""
        return f"{self.prefix}/{key}.parquet"

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
                source_type="S3Storage",
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
