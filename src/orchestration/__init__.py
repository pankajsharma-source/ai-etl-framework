"""
Pipeline orchestration module

Provides the unified Pipeline class for ETL operations.
"""
from src.orchestration.pipeline import Pipeline, StageResult
from src.orchestration.pipeline_core import (
    apply_transformers,
    resolve_schema,
    load_to_destinations
)

__all__ = [
    'Pipeline',
    'StageResult',
    'apply_transformers',
    'resolve_schema',
    'load_to_destinations'
]
