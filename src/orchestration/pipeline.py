"""
Pipeline orchestrator for AI-ETL framework

This pipeline automatically persists intermediate data between stages,
making it safe for any data size and enabling retry/resume capabilities.
"""
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

from src.adapters.base import SourceAdapter, DestinationAdapter
from src.transformers.base_transformer import Transformer
from src.common.models import PipelineResult, PipelineError, Record, Schema
from src.common.exceptions import PipelineError as PipelineException
from src.common.logging import get_logger
from src.storage.file_storage import FileStorage
from src.orchestration.pipeline_core import (
    apply_transformers,
    resolve_schema,
    load_to_destinations
)


@dataclass
class StageResult:
    """Result from a single stage execution"""
    record_count: int
    duration_seconds: float
    schema: Optional[Schema] = None
    metadata: Optional[Dict[str, Any]] = None


class Pipeline:
    """
    Pipeline orchestrator that connects Extract, Transform, and Load stages

    Automatically persists intermediate data between stages for:
    - Safety with large datasets
    - Retry/resume capability
    - Debugging (with cleanup_cache=False)

    Example:
        pipeline = (Pipeline("my_pipeline")
            .extract(CSVSource("data.csv"))
            .transform(NullRemover())
            .load(SQLiteLoader("output.db", table="data"))
            .run())
    """

    def __init__(
        self,
        pipeline_id: Optional[str] = None,
        cache_dir: str = ".pipeline_cache",
        cleanup_cache: bool = True
    ):
        """
        Initialize pipeline

        Args:
            pipeline_id: Optional pipeline identifier
            cache_dir: Directory for intermediate data storage
            cleanup_cache: If True, delete intermediate data after successful run
        """
        self.pipeline_id = pipeline_id or f"pipeline_{int(time.time())}"
        self.logger = get_logger("Pipeline")
        self.cache_dir = cache_dir
        self.cleanup_cache = cleanup_cache

        # Initialize storage for intermediate data
        self._storage = FileStorage(cache_dir)

        self._source: Optional[SourceAdapter] = None
        self._transformers: List[Transformer] = []
        self._destinations: List[DestinationAdapter] = []
        self._schema: Optional[Schema] = None

        self.result: Optional[PipelineResult] = None

    def extract(self, source: SourceAdapter) -> 'Pipeline':
        """
        Set the data source

        Args:
            source: Source adapter

        Returns:
            self for chaining
        """
        self._source = source
        self.logger.info(f"Source set: {source.__class__.__name__}")
        return self

    def transform(self, transformer: Transformer) -> 'Pipeline':
        """
        Add a transformer to the pipeline

        Args:
            transformer: Transformer to add

        Returns:
            self for chaining
        """
        self._transformers.append(transformer)
        self.logger.info(f"Transformer added: {transformer.__class__.__name__}")
        return self

    def load(self, destination: DestinationAdapter) -> 'Pipeline':
        """
        Add a destination to the pipeline (supports multiple destinations)

        Args:
            destination: Destination adapter

        Returns:
            self for chaining
        """
        self._destinations.append(destination)
        self.logger.info(f"Destination added: {destination.__class__.__name__}")
        return self

    def run(self) -> 'Pipeline':
        """
        Execute the pipeline

        Returns:
            self with result populated

        Raises:
            PipelineException: If pipeline execution fails
        """
        if not self._source:
            raise PipelineException("No source adapter set. Call extract() first.")

        if not self._destinations:
            raise PipelineException("No destination adapter set. Call load() first.")

        self.logger.info(f"Starting pipeline execution: {self.pipeline_id}")

        # Initialize result
        result = PipelineResult(
            success=False,
            start_time=datetime.now()
        )

        try:
            # Stage 1: Extract
            extract_start = time.time()
            self.logger.info("Stage 1: Extract - Starting")

            with self._source as source:
                # Get schema
                self._schema = source.get_schema()
                self.logger.info(f"Schema inferred: {len(self._schema.fields)} fields")

                # Read records
                records = list(source.read())
                result.records_extracted = len(records)

                self.logger.info(f"Extracted {result.records_extracted} records")

            # Persist extracted data to intermediate storage
            extract_key = f"{self.pipeline_id}/extracted"
            self._storage.save_records(
                key=extract_key,
                records=records,
                schema=self._schema,
                metadata={
                    'stage': 'extract',
                    'timestamp': datetime.now().isoformat(),
                    'source_type': self._source.__class__.__name__
                }
            )
            self.logger.info(f"Saved extracted data to {extract_key}")

            result.extract_duration = time.time() - extract_start

            # Stage 2: Transform
            transform_start = time.time()
            self.logger.info(f"Stage 2: Transform - {len(self._transformers)} transformer(s)")

            # Use shared function for transform logic
            records = apply_transformers(records, self._transformers, self.logger)

            result.records_transformed = len(records)

            # Persist transformed data to intermediate storage
            transform_key = f"{self.pipeline_id}/transformed"
            transform_schema = resolve_schema(records, self._schema)
            self._storage.save_records(
                key=transform_key,
                records=records,
                schema=transform_schema,
                metadata={
                    'stage': 'transform',
                    'timestamp': datetime.now().isoformat(),
                    'input_count': result.records_extracted,
                    'output_count': result.records_transformed,
                    'transformers': [t.__class__.__name__ for t in self._transformers]
                }
            )
            self.logger.info(f"Saved transformed data to {transform_key}")

            result.transform_duration = time.time() - transform_start

            # Stage 3: Load
            load_start = time.time()
            self.logger.info(f"Stage 3: Load - Writing to {len(self._destinations)} destination(s)")

            # Determine schema for loading
            load_schema = resolve_schema(records, self._schema)
            if load_schema != self._schema:
                self.logger.info(f"Using transformed schema: {len(load_schema.fields)} fields")
            else:
                self.logger.info("Using source schema")

            # Use shared function for load logic
            total_loaded = load_to_destinations(
                records, load_schema, self._destinations, self.logger
            )

            result.records_loaded = total_loaded
            result.load_duration = time.time() - load_start

            # Success!
            result.success = True
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()

            self.logger.info(
                f"Pipeline completed successfully in {result.duration_seconds:.2f}s"
            )
            self.logger.info(
                f"Summary: {result.records_extracted} extracted, "
                f"{result.records_transformed} transformed, "
                f"{result.records_loaded} loaded"
            )

            # Cleanup intermediate data if configured
            if self.cleanup_cache:
                self._storage.cleanup(self.pipeline_id)
                self.logger.info(f"Cleaned up intermediate data for {self.pipeline_id}")

        except Exception as e:
            result.success = False
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()

            error = PipelineError(
                stage="unknown",
                error_type=type(e).__name__,
                message=str(e),
                timestamp=datetime.now(),
                retryable=False
            )
            result.errors.append(error)

            self.logger.error(f"Pipeline failed: {e}")
            raise PipelineException(f"Pipeline execution failed: {e}") from e

        finally:
            self.result = result

        return self

    def get_stats(self) -> dict:
        """
        Get pipeline statistics

        Returns:
            dict: Pipeline statistics
        """
        if not self.result:
            return {}

        stats = {
            'pipeline_id': self.pipeline_id,
            'success': self.result.success,
            'records_extracted': self.result.records_extracted,
            'records_transformed': self.result.records_transformed,
            'records_loaded': self.result.records_loaded,
            'duration_seconds': self.result.duration_seconds,
            'extract_duration': self.result.extract_duration,
            'transform_duration': self.result.transform_duration,
            'load_duration': self.result.load_duration,
        }

        # Add transformer stats
        for i, transformer in enumerate(self._transformers):
            t_stats = transformer.get_stats()
            stats[f'transformer_{i}'] = t_stats

        return stats

    def get_intermediate_data(self, stage: str = "transformed") -> List[Record]:
        """
        Get intermediate data for debugging

        Only available if cleanup_cache=False was set, or if called
        during pipeline execution before cleanup.

        Args:
            stage: Which stage to get data from ('extracted' or 'transformed')

        Returns:
            List of records from the specified stage

        Raises:
            KeyError: If the data doesn't exist (already cleaned up or not yet created)
        """
        key = f"{self.pipeline_id}/{stage}"
        if not self._storage.exists(key):
            raise KeyError(
                f"Intermediate data for stage '{stage}' not found. "
                f"Set cleanup_cache=False to preserve intermediate data."
            )

        records, _ = self._storage.load_records(key)
        return records

    def get_status(self) -> dict:
        """
        Get pipeline status including intermediate storage info

        Returns:
            dict: Pipeline status
        """
        extracted_exists = self._storage.exists(f"{self.pipeline_id}/extracted")
        transformed_exists = self._storage.exists(f"{self.pipeline_id}/transformed")

        return {
            'pipeline_id': self.pipeline_id,
            'has_extracted_data': extracted_exists,
            'has_transformed_data': transformed_exists,
            'cleanup_cache': self.cleanup_cache,
            'cache_dir': self.cache_dir,
            'run_completed': self.result is not None,
            'run_success': self.result.success if self.result else None
        }

    # ============================================================
    # Staged Execution Methods
    # ============================================================

    def run_extract_only(self, source: SourceAdapter) -> StageResult:
        """
        Run only the extract stage and save to intermediate storage.

        For staged execution mode where each stage runs separately.

        Args:
            source: Source adapter to extract from

        Returns:
            StageResult with record count and duration
        """
        self.logger.info(f"Running extract stage only for {self.pipeline_id}")
        start_time = time.time()

        with source as src:
            # Get schema
            self._schema = src.get_schema()
            self.logger.info(f"Schema inferred: {len(self._schema.fields)} fields")

            # Read records
            records = list(src.read())
            record_count = len(records)

            self.logger.info(f"Extracted {record_count} records")

        # Save to intermediate storage
        extract_key = f"{self.pipeline_id}/extracted"
        self._storage.save_records(
            key=extract_key,
            records=records,
            schema=self._schema,
            metadata={
                'stage': 'extract',
                'timestamp': datetime.now().isoformat(),
                'source_type': source.__class__.__name__
            }
        )
        self.logger.info(f"Saved extracted data to {extract_key}")

        duration = time.time() - start_time
        return StageResult(
            record_count=record_count,
            duration_seconds=duration,
            schema=self._schema
        )

    def run_transform_only(self, transformers: List[Transformer]) -> StageResult:
        """
        Run only the transform stage using data from intermediate storage.

        Args:
            transformers: List of transformers to apply

        Returns:
            StageResult with record count and duration
        """
        self.logger.info(f"Running transform stage only for {self.pipeline_id}")
        start_time = time.time()

        # Load extracted data
        extract_key = f"{self.pipeline_id}/extracted"
        if not self._storage.exists(extract_key):
            raise PipelineException(
                f"No extracted data found for {self.pipeline_id}. "
                f"Run extract stage first."
            )

        records, schema = self._storage.load_records(extract_key)
        self._schema = schema
        self.logger.info(f"Loaded {len(records)} records from extract stage")

        # Apply transformers
        records = apply_transformers(records, transformers, self.logger)
        record_count = len(records)

        # Save to intermediate storage
        transform_key = f"{self.pipeline_id}/transformed"
        transform_schema = resolve_schema(records, self._schema)
        self._storage.save_records(
            key=transform_key,
            records=records,
            schema=transform_schema,
            metadata={
                'stage': 'transform',
                'timestamp': datetime.now().isoformat(),
                'transformers': [t.__class__.__name__ for t in transformers]
            }
        )
        self.logger.info(f"Saved transformed data to {transform_key}")

        duration = time.time() - start_time
        return StageResult(
            record_count=record_count,
            duration_seconds=duration,
            schema=transform_schema
        )

    def run_load_only(self, destinations: List[DestinationAdapter]) -> StageResult:
        """
        Run only the load stage using data from intermediate storage.

        Args:
            destinations: List of destination adapters

        Returns:
            StageResult with record count and duration
        """
        self.logger.info(f"Running load stage only for {self.pipeline_id}")
        start_time = time.time()

        # Load transformed data
        transform_key = f"{self.pipeline_id}/transformed"
        if not self._storage.exists(transform_key):
            raise PipelineException(
                f"No transformed data found for {self.pipeline_id}. "
                f"Run transform stage first."
            )

        records, schema = self._storage.load_records(transform_key)
        self.logger.info(f"Loaded {len(records)} records from transform stage")

        # Load to destinations
        total_loaded = load_to_destinations(records, schema, destinations, self.logger)

        duration = time.time() - start_time
        return StageResult(
            record_count=total_loaded,
            duration_seconds=duration,
            schema=schema
        )

    def cleanup(self) -> None:
        """
        Clean up intermediate data for this pipeline.

        Call this after staged execution completes to free up storage.
        """
        self._storage.cleanup(self.pipeline_id)
        self.logger.info(f"Cleaned up intermediate data for {self.pipeline_id}")

    @property
    def storage(self) -> FileStorage:
        """Access to intermediate storage for staged execution"""
        return self._storage
