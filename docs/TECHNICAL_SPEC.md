# AI-ETL Framework - Technical Specification

## Table of Contents
1. [Data Models](#data-models)
2. [Interface Specifications](#interface-specifications)
3. [Configuration Schema](#configuration-schema)
4. [State Management](#state-management)
5. [Error Handling](#error-handling)
6. [Performance Specifications](#performance-specifications)
7. [Security Specifications](#security-specifications)
8. [API Specifications](#api-specifications)

---

## Data Models

### Core Data Structures

#### Record
The fundamental unit of data flowing through the pipeline.

```python
from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime

@dataclass
class Record:
    """Standardized record format for pipeline data flow"""

    # Primary data payload
    data: Dict[str, Any]

    # Metadata about the record
    metadata: RecordMetadata

    # Optional schema reference
    schema: Optional['Schema'] = None

    # Processing timestamps
    extracted_at: Optional[datetime] = None
    transformed_at: Optional[datetime] = None
    loaded_at: Optional[datetime] = None

@dataclass
class RecordMetadata:
    """Metadata associated with a record"""

    # Source information
    source_type: str  # 'csv', 'postgres', 'api', etc.
    source_id: str    # File path, table name, endpoint, etc.

    # Record identification
    record_id: Optional[str] = None

    # Quality metrics
    quality_score: Optional[float] = None  # 0.0-1.0

    # Lineage tracking
    pipeline_id: str = ""
    stage: str = ""  # 'extract', 'transform', 'load'

    # Custom metadata
    custom: Dict[str, Any] = None
```

#### Schema

```python
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from enum import Enum

class FieldType(Enum):
    """Supported field types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    JSON = "json"
    ARRAY = "array"

@dataclass
class Field:
    """Schema field definition"""
    name: str
    type: FieldType
    nullable: bool = True
    description: Optional[str] = None

    # Constraints
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    pattern: Optional[str] = None  # Regex for string validation
    enum_values: Optional[List[Any]] = None

    # Metadata
    inferred: bool = False  # True if inferred by ML
    confidence: Optional[float] = None  # Confidence in inference

@dataclass
class Schema:
    """Complete schema definition"""
    name: str
    fields: List[Field]

    # Keys
    primary_key: Optional[List[str]] = None

    # Metadata
    version: str = "1.0"
    inferred: bool = False
    created_at: Optional[datetime] = None

    # Statistics (for ML-inferred schemas)
    sample_size: Optional[int] = None
    null_counts: Optional[Dict[str, int]] = None

    def get_field(self, name: str) -> Optional[Field]:
        """Get field by name"""
        for field in self.fields:
            if field.name == name:
                return field
        return None
```

#### Pipeline Result

```python
@dataclass
class PipelineResult:
    """Result of pipeline execution"""

    # Success/failure
    success: bool

    # Statistics
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0

    # Timing
    start_time: datetime = None
    end_time: datetime = None
    duration_seconds: float = 0.0

    # Stage timings
    extract_duration: float = 0.0
    transform_duration: float = 0.0
    load_duration: float = 0.0

    # Errors
    errors: List['PipelineError'] = None

    # Quality metrics
    avg_quality_score: Optional[float] = None

    # State
    final_state: Optional[Dict] = None

@dataclass
class PipelineError:
    """Error information"""
    stage: str  # 'extract', 'transform', 'load'
    error_type: str
    message: str
    record_id: Optional[str] = None
    timestamp: datetime = None
    stacktrace: Optional[str] = None
    retryable: bool = False
```

---

## Interface Specifications

### Source Adapter Interface

```python
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Optional

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
```

### Implemented Destination Adapters

#### Database Loaders

**SQLiteLoader** - Write to SQLite database
- Auto table creation with schema inference
- Batch insertion with configurable batch sizes
- Transaction support (begin, commit, rollback)
- JSON field support for nested data

**PostgreSQLLoader** - Write to PostgreSQL database
- Schema and table creation
- execute_batch optimization for performance
- Full ACID transaction support
- Type mapping from generic to PostgreSQL types

#### File-Based Loaders (Phase 2.5)

**CSVLoader** (`src/adapters/destinations/csv_loader.py`) - Write to CSV files
- **Format**: CSV with configurable delimiter and encoding
- **Modes**: Overwrite or append
- **Compression**: gzip, bz2, zip, xz
- **Batch Processing**: Accumulate records and flush in batches using pandas
- **Transaction Support**: Write to temp file, move on commit
- **Features**:
  - Pandas-based efficient DataFrame writing
  - Automatic header handling
  - Configurable batch size for memory management
  - Atomic writes via temp file pattern

**JSONLoader** (`src/adapters/destinations/json_loader.py`) - Write to JSON/JSONL files
- **Modes**:
  - `array`: Single JSON array `[{...}, {...}]`
  - `lines`: Line-delimited JSON (JSONL) `{...}\n{...}\n`
- **Compression**: gzip, bz2
- **Pretty Printing**: Optional indentation for readability
- **Schema Export**: Optionally export inferred schema to `.schema.json`
- **Transaction Support**: Buffer in memory, write on commit
- **Features**:
  - Memory-efficient batch buffering
  - Automatic schema serialization
  - Support for nested/complex data structures
  - UTF-8 encoding with proper JSON escaping

### Destination Adapter Interface

```python
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

    def commit(self) -> None:
        """Commit transaction"""
        self._transaction_active = False

    def rollback(self) -> None:
        """Rollback transaction"""
        self._transaction_active = False

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
```

### Transformer Interface

```python
class Transformer(ABC):
    """Abstract base class for all transformers"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize transformer

        Args:
            config: Transformer-specific configuration
        """
        self.config = config or {}
        self.stats = TransformerStats()

    @abstractmethod
    def transform(self, record: Record) -> Optional[Record]:
        """
        Transform a single record

        Args:
            record: Input record

        Returns:
            Optional[Record]: Transformed record, or None if record should be filtered

        Raises:
            TransformError: If transformation fails
        """
        pass

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform a batch of records (default implementation)

        Override this for batch-optimized transformations

        Args:
            records: Input records

        Returns:
            List[Record]: Transformed records
        """
        result = []
        for record in records:
            transformed = self.transform(record)
            if transformed is not None:
                result.append(transformed)
        return result

    def get_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics

        Returns:
            Dict: Statistics (records processed, filtered, errors, etc.)
        """
        return {
            'records_processed': self.stats.records_processed,
            'records_filtered': self.stats.records_filtered,
            'records_modified': self.stats.records_modified,
            'errors': self.stats.errors
        }

    def reset_stats(self) -> None:
        """Reset statistics"""
        self.stats = TransformerStats()

@dataclass
class TransformerStats:
    """Statistics for transformer execution"""
    records_processed: int = 0
    records_filtered: int = 0
    records_modified: int = 0
    errors: int = 0
```

---

## Configuration Schema

### Pipeline Configuration (YAML)

```yaml
# Pipeline metadata
pipeline:
  name: "my-etl-pipeline"
  description: "Extract CSV, clean data, load to PostgreSQL"
  version: "1.0"

  # Execution settings
  execution:
    batch_size: 1000
    max_workers: 4
    timeout_seconds: 3600
    retry_policy:
      max_retries: 3
      backoff_multiplier: 2
      max_backoff_seconds: 60

  # Source configuration
  source:
    type: "csv"
    config:
      path: "/path/to/data.csv"
      encoding: "utf-8"
      delimiter: ","
      has_header: true

    # State for incremental processing
    state_file: ".state/csv_source.json"

  # Transformations (applied in order)
  transforms:
    - type: "null_remover"
      config:
        strategy: "drop"  # 'drop' or 'impute'

    - type: "quality_scorer"
      config:
        min_score: 0.5

    - type: "deduplicator"
      config:
        key_fields: ["id", "email"]
        strategy: "keep_first"

  # Destination configuration
  destination:
    type: "postgres"
    config:
      host: "localhost"
      port: 5432
      database: "mydb"
      table: "clean_data"
      username: "${DB_USER}"  # Environment variable
      password: "${DB_PASSWORD}"

    # Schema handling
    schema_mode: "create_if_missing"  # 'create_if_missing', 'fail_if_missing', 'drop_and_create'

  # Monitoring and logging
  monitoring:
    log_level: "INFO"
    metrics_enabled: true

  # Error handling
  error_handling:
    on_extract_error: "fail"  # 'fail', 'skip', 'retry'
    on_transform_error: "skip"
    on_load_error: "fail"
    dead_letter_queue: ".errors/dlq.jsonl"
```

### Environment Configuration (.env)

```bash
# Environment
ENV=local  # local, dev, staging, prod

# Database Connections
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_dev
DB_USER=etl_user
DB_PASSWORD=secret

# AWS (for S3, etc.)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET=my-data-bucket

# Storage
LOCAL_DATA_PATH=./data
LOCAL_STATE_PATH=./.state

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json  # json or text

# AI/ML
OPENAI_API_KEY=
ML_MODEL_PATH=./models

# Performance
DEFAULT_BATCH_SIZE=1000
MAX_WORKERS=4
```

---

## State Management

### State File Format

State is stored in JSON format for incremental processing.

```json
{
  "pipeline_id": "csv-to-postgres-pipeline",
  "adapter_type": "csv",
  "adapter_id": "/path/to/data.csv",
  "last_run": "2025-01-08T10:30:00Z",
  "last_modified": "2025-01-07T15:20:00Z",
  "records_processed": 10000,
  "file_hash": "sha256:abc123...",
  "checkpoint": {
    "offset": 10000,
    "last_record_id": "rec-10000",
    "custom_state": {}
  },
  "metadata": {
    "version": "1.0",
    "created_at": "2025-01-01T00:00:00Z"
  }
}
```

### State Manager Interface

```python
class StateManager:
    """Manages pipeline state for incremental processing"""

    def __init__(self, state_dir: str = ".state"):
        self.state_dir = state_dir

    def get_state(self, pipeline_id: str, adapter_id: str) -> Optional[Dict]:
        """Retrieve state for a specific adapter"""
        pass

    def save_state(self, pipeline_id: str, adapter_id: str, state: Dict) -> None:
        """Save state for an adapter"""
        pass

    def clear_state(self, pipeline_id: str, adapter_id: str) -> None:
        """Clear state (force full reprocessing)"""
        pass
```

---

## Error Handling

### Exception Hierarchy

```python
class ETLError(Exception):
    """Base exception for all ETL errors"""
    pass

class ConnectionError(ETLError):
    """Connection to source/destination failed"""
    pass

class SchemaError(ETLError):
    """Schema-related errors"""
    pass

class ReadError(ETLError):
    """Error reading from source"""
    pass

class WriteError(ETLError):
    """Error writing to destination"""
    pass

class TransformError(ETLError):
    """Error during transformation"""
    pass

class ConfigurationError(ETLError):
    """Invalid configuration"""
    pass

class StateError(ETLError):
    """State management error"""
    pass
```

### Retry Policy

```python
@dataclass
class RetryPolicy:
    """Configuration for retry behavior"""
    max_retries: int = 3
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    retryable_exceptions: List[Type[Exception]] = None

    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if error should be retried"""
        if attempt >= self.max_retries:
            return False

        if self.retryable_exceptions:
            return type(error) in self.retryable_exceptions

        # Default: retry on transient errors
        return isinstance(error, (ConnectionError, ReadError, WriteError))

    def get_backoff_seconds(self, attempt: int) -> float:
        """Calculate backoff duration"""
        backoff = min(
            self.backoff_multiplier ** attempt,
            self.max_backoff_seconds
        )
        return backoff
```

---

## Performance Specifications

### Throughput Targets

| Scale | Records/Second | Latency (p95) | Resource Usage |
|-------|---------------|---------------|----------------|
| **Small** (< 10K records) | 100-500 | < 1s | Single core, 512MB RAM |
| **Medium** (10K-1M) | 500-2000 | < 5s | 4 cores, 2GB RAM |
| **Large** (1M-100M) | 2000-10000 | < 30s | 16 cores, 8GB RAM |
| **Very Large** (> 100M) | 10000+ | Variable | Distributed (Spark) |

### Batch Size Guidelines

```python
# Recommended batch sizes by operation
BATCH_SIZES = {
    'csv_read': 10000,
    'database_read': 1000,
    'transform': 1000,
    'database_write': 1000,
    'file_write': 10000,
    's3_write': 100,
}
```

### Memory Management

- **Streaming**: Use iterators, not lists, for large datasets
- **Batch Processing**: Process in chunks to control memory
- **Cleanup**: Explicitly close connections and free resources

---

## Security Specifications

### Credentials Management

1. **Never hardcode credentials** in code or config files
2. **Use environment variables** for local development
3. **Use secrets managers** in production (AWS Secrets Manager, etc.)
4. **Rotate credentials** regularly

### Data Security

1. **Encryption at Rest**: All data stored encrypted
2. **Encryption in Transit**: TLS for all connections
3. **Access Control**: Least privilege principle
4. **Audit Logging**: Log all data access

### Sensitive Data Handling

```python
# Sensitive field detection patterns
SENSITIVE_PATTERNS = {
    'ssn': r'\d{3}-\d{2}-\d{4}',
    'credit_card': r'\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}',
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'phone': r'\+?\d{1,3}?[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
}

# Masking strategies
def mask_sensitive_data(value: str, pattern: str) -> str:
    """Mask sensitive data for logging"""
    # Implementation
    pass
```

---

## API Specifications

### REST API (Future)

Endpoints for pipeline control and monitoring.

#### Start Pipeline

```http
POST /api/v1/pipelines/{pipeline_id}/start
Content-Type: application/json

{
  "force_full_refresh": false,
  "override_config": {}
}

Response 200:
{
  "execution_id": "exec-123",
  "status": "running",
  "started_at": "2025-01-08T10:00:00Z"
}
```

#### Get Pipeline Status

```http
GET /api/v1/pipelines/{pipeline_id}/executions/{execution_id}

Response 200:
{
  "execution_id": "exec-123",
  "status": "running",
  "progress": {
    "records_extracted": 5000,
    "records_transformed": 4500,
    "records_loaded": 4000,
    "percent_complete": 50
  },
  "started_at": "2025-01-08T10:00:00Z",
  "estimated_completion": "2025-01-08T10:10:00Z"
}
```

#### Stop Pipeline

```http
POST /api/v1/pipelines/{pipeline_id}/executions/{execution_id}/stop

Response 200:
{
  "execution_id": "exec-123",
  "status": "stopping"
}
```

#### List Pipelines

```http
GET /api/v1/pipelines

Response 200:
{
  "pipelines": [
    {
      "id": "csv-to-postgres",
      "name": "CSV to PostgreSQL Pipeline",
      "last_run": "2025-01-08T09:00:00Z",
      "status": "success"
    }
  ]
}
```

---

## Appendix

### Field Type Mapping

Mapping between source and destination types:

| Generic Type | PostgreSQL | SQLite | CSV | JSON/JSONL | Parquet |
|--------------|------------|--------|-----|------------|---------|
| STRING | VARCHAR | TEXT | string | string | STRING |
| INTEGER | INTEGER | INTEGER | integer | number | INT64 |
| FLOAT | DOUBLE PRECISION | REAL | float | number | DOUBLE |
| BOOLEAN | BOOLEAN | INTEGER | boolean | boolean | BOOLEAN |
| DATE | DATE | TEXT | date string | string | DATE |
| DATETIME | TIMESTAMP | TEXT | datetime string | string | TIMESTAMP |
| JSON | JSONB | TEXT | json string | object | STRING |

### Configuration Validation

All configurations validated using Pydantic schemas:

```python
from pydantic import BaseModel, validator

class PipelineConfig(BaseModel):
    name: str
    version: str = "1.0"
    source: SourceConfig
    transforms: List[TransformConfig]
    destination: DestinationConfig

    @validator('name')
    def name_must_be_valid(cls, v):
        if not v or len(v) < 3:
            raise ValueError('Name must be at least 3 characters')
        return v
```

---

**Document Version**: 1.1
**Last Updated**: 2025-10-11
**Author**: AI-ETL Framework Team
**Changes**: Added Phase 2.5 file-based destination adapters (CSVLoader, JSONLoader)
