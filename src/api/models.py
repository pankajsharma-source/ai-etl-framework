"""
Pydantic models for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from enum import Enum


class ExecutionMode(str, Enum):
    """Pipeline execution mode"""
    UNIFIED = "unified"
    STAGED = "staged"


class StageStatus(str, Enum):
    """Individual stage status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# ============================================================
# Configuration Models
# ============================================================

class SourceConfig(BaseModel):
    """Source configuration"""
    type: Literal["csv", "json", "api", "database"]
    path: Optional[str] = None
    url: Optional[str] = None
    connection_string: Optional[str] = None
    table_name: Optional[str] = None
    query: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None


class TransformerConfig(BaseModel):
    """Transformer configuration"""
    type: Literal[
        "null_remover",
        "dedup",
        "quality_scorer",
        "anomaly_detector",
        "schema_inferrer",
        "aggregator",
        "column_remover",
        "metadata_to_columns",
        "anomaly_splitter",
        "dashboard_aggregator",
        "type_converter",
        "custom"
    ]
    config: Optional[Dict[str, Any]] = None


class DestinationConfig(BaseModel):
    """Destination configuration"""
    type: Literal["sqlite", "postgres", "csv", "json", "parquet", "api"]
    path: Optional[str] = None
    connection_string: Optional[str] = None
    table_name: str = "etl_data"
    url: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    # PostgreSQL specific fields
    host: Optional[str] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = 5432


class StorageConfig(BaseModel):
    """Intermediate storage configuration"""
    type: Literal["file", "s3"] = "file"
    path: Optional[str] = "./.state/intermediate"
    bucket: Optional[str] = None
    prefix: Optional[str] = "intermediate"
    region: Optional[str] = None


class PostProcessingConfig(BaseModel):
    """Post-processing configuration for pipeline outputs"""
    enable_rag_indexing: bool = Field(
        default=False,
        description="Enable RAG indexing after pipeline completion"
    )
    rag_api_url: str = Field(
        default="http://rag-api:8000",
        description="RAG API endpoint URL"
    )
    index_name: str = Field(
        ...,
        description="Target index name for RAG system"
    )
    batch_size: int = Field(
        default=500,
        description="Batch size for indexing operations"
    )


class PipelineConfig(BaseModel):
    """Complete pipeline configuration"""
    name: str = Field(..., description="Pipeline name")
    mode: ExecutionMode = Field(..., description="Execution mode")
    source: SourceConfig
    transformers: List[TransformerConfig] = []
    destination: Optional[DestinationConfig] = None  # Single destination (legacy)
    destinations: Optional[List[DestinationConfig]] = None  # Multiple destinations
    storage: StorageConfig = StorageConfig()
    post_processing: Optional[PostProcessingConfig] = None
    metadata: Optional[Dict[str, Any]] = None


# ============================================================
# Response Models
# ============================================================

class StageResult(BaseModel):
    """Result from a single stage execution"""
    stage: Literal["extract", "transform", "load"]
    status: StageStatus
    records_in: Optional[int] = None
    records_out: Optional[int] = None
    duration_seconds: float
    error: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None


class StageResponse(BaseModel):
    """Response for staged execution endpoint"""
    pipeline_id: str
    stage: Literal["extract", "transform", "load"]
    status: StageStatus
    records: Optional[int] = None
    duration_seconds: float
    message: str
    error: Optional[str] = None


class PipelineResponse(BaseModel):
    """Response for pipeline creation/execution"""
    pipeline_id: str
    mode: ExecutionMode
    status: str
    message: str
    stages: Optional[List[StageResult]] = None
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class PipelineStatus(BaseModel):
    """Pipeline status information"""
    pipeline_id: str
    name: str
    mode: ExecutionMode
    overall_status: str
    extract_status: StageStatus
    transform_status: StageStatus
    load_status: StageStatus
    created_at: datetime
    updated_at: datetime
    extract_records: Optional[int] = None
    transform_records: Optional[int] = None
    load_records: Optional[int] = None
    total_duration: Optional[float] = None
    error: Optional[str] = None
