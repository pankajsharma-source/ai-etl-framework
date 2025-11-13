"""
Base adapter interfaces for sources and destinations
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional

from src.common.models import Record, Schema
from src.common.exceptions import ConnectionError, ReadError, WriteError, SchemaError
from src.common.logging import get_logger


class SourceAdapter(ABC):
    """Abstract base class for all source adapters"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize adapter with configuration

        Args:
            config: Adapter-specific configuration
        """
        self.config = config
        self._connected = False
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to data source

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def read(self, batch_size: int = 100) -> Iterator[Record]:
        """
        Read records from source

        Args:
            batch_size: Number of records to read in each batch

        Yields:
            Record: Individual records

        Raises:
            ReadError: If reading fails
        """
        pass

    @abstractmethod
    def get_schema(self) -> Schema:
        """
        Get or infer schema from source

        Returns:
            Schema: Data schema

        Note:
            May use ML-based inference if schema not explicitly defined
        """
        pass

    @abstractmethod
    def supports_incremental(self) -> bool:
        """
        Check if source supports incremental reading

        Returns:
            bool: True if incremental supported
        """
        pass

    def get_state(self) -> Dict[str, Any]:
        """
        Get current state for incremental processing

        Returns:
            Dict: State information (e.g., last processed timestamp, offset)
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """
        Set state for resuming incremental processing

        Args:
            state: Previous state to resume from
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection and cleanup resources"""
        pass

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


class DestinationAdapter(ABC):
    """Abstract base class for all destination adapters"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize adapter with configuration

        Args:
            config: Adapter-specific configuration
        """
        self.config = config
        self._connected = False
        self._transaction_active = False
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to destination

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def create_schema(self, schema: Schema) -> None:
        """
        Create schema at destination (e.g., create table)

        Args:
            schema: Schema to create

        Raises:
            SchemaError: If schema creation fails
        """
        pass

    @abstractmethod
    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to destination

        Args:
            records: Records to write

        Returns:
            int: Number of records written

        Raises:
            WriteError: If writing fails
        """
        pass

    def begin_transaction(self) -> None:
        """Begin a transaction (if supported)"""
        self._transaction_active = True
        self.logger.debug("Transaction started")

    def commit(self) -> None:
        """Commit transaction"""
        self._transaction_active = False
        self.logger.debug("Transaction committed")

    def rollback(self) -> None:
        """Rollback transaction"""
        self._transaction_active = False
        self.logger.debug("Transaction rolled back")

    @abstractmethod
    def close(self) -> None:
        """Close connection and cleanup resources"""
        pass

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type and self._transaction_active:
            self.rollback()
        self.close()
