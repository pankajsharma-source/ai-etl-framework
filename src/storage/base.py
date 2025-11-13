"""
Base interface for intermediate storage
"""
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
from pathlib import Path

from src.common.models import Record, Schema


class IntermediateStorage(ABC):
    """
    Abstract base class for intermediate data storage

    Used by StagedPipeline to persist data between pipeline stages
    """

    @abstractmethod
    def save_records(
        self,
        key: str,
        records: List[Record],
        schema: Optional[Schema] = None,
        metadata: Optional[dict] = None
    ) -> None:
        """
        Save records to intermediate storage

        Args:
            key: Storage key (e.g., 'pipeline_123/extracted')
            records: List of records to save
            schema: Optional schema information
            metadata: Optional metadata (record counts, timestamps, etc.)

        Raises:
            StorageError: If save operation fails
        """
        pass

    @abstractmethod
    def load_records(self, key: str) -> Tuple[List[Record], Optional[Schema]]:
        """
        Load records from intermediate storage

        Args:
            key: Storage key (e.g., 'pipeline_123/extracted')

        Returns:
            Tuple of (records, schema)

        Raises:
            StorageError: If load operation fails
            KeyError: If key doesn't exist
        """
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in storage

        Args:
            key: Storage key to check

        Returns:
            bool: True if key exists
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Delete data at the specified key

        Args:
            key: Storage key to delete

        Raises:
            StorageError: If delete operation fails
        """
        pass

    @abstractmethod
    def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """
        List all keys, optionally filtered by prefix

        Args:
            prefix: Optional prefix to filter keys

        Returns:
            List of storage keys
        """
        pass

    def cleanup(self, pipeline_id: str) -> None:
        """
        Clean up all data for a pipeline

        Args:
            pipeline_id: Pipeline ID to clean up
        """
        # Default implementation: delete all keys with pipeline_id prefix
        keys = self.list_keys(prefix=pipeline_id)
        for key in keys:
            try:
                self.delete(key)
            except Exception as e:
                # Log but don't fail cleanup
                print(f"Warning: Failed to delete {key}: {e}")
