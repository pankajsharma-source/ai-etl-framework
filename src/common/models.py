"""
Data models for the AI-ETL framework
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


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
        for f in self.fields:
            if f.name == name:
                return f
        return None


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
    custom: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Record:
    """Standardized record format for pipeline data flow"""

    # Primary data payload
    data: Dict[str, Any]

    # Metadata about the record
    metadata: RecordMetadata

    # Optional schema reference
    schema: Optional[Schema] = None

    # Processing timestamps
    extracted_at: Optional[datetime] = None
    transformed_at: Optional[datetime] = None
    loaded_at: Optional[datetime] = None


@dataclass
class PipelineError:
    """Error information"""
    stage: str  # 'extract', 'transform', 'load'
    error_type: str
    message: str
    record_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    stacktrace: Optional[str] = None
    retryable: bool = False


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
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    # Stage timings
    extract_duration: float = 0.0
    transform_duration: float = 0.0
    load_duration: float = 0.0

    # Errors
    errors: List[PipelineError] = field(default_factory=list)

    # Quality metrics
    avg_quality_score: Optional[float] = None

    # State
    final_state: Optional[Dict] = None
