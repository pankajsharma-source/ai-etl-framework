"""
Staged Pipeline orchestrator for independent E-T-L execution
"""
import time
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass

from src.adapters.base import SourceAdapter, DestinationAdapter
from src.transformers.base_transformer import Transformer
from src.storage.base import IntermediateStorage
from src.common.models import Record, Schema
from src.common.exceptions import PipelineError as PipelineException
from src.common.logging import get_logger


@dataclass
class StageResult:
    """Result from a single pipeline stage"""
    stage: str
    success: bool
    record_count: int
    duration_seconds: float
    storage_key: Optional[str] = None
    error_message: Optional[str] = None


class StagedPipeline:
    """
    Pipeline orchestrator that allows running E-T-L stages independently

    Unlike the unified Pipeline, this allows:
    - Running Extract separately, saving to intermediate storage
    - Running Transform on previously extracted data
    - Running Load on previously transformed data
    - Pausing between stages for inspection or debugging

    Example:
        storage = FileStorage("./.state/intermediate")
        pipeline = StagedPipeline("pipeline_001", storage)

        # Stage 1: Extract
        result = pipeline.run_extract_only(CSVSource("data.csv"))

        # ... time passes, inspect data, etc ...

        # Stage 2: Transform
        result = pipeline.run_transform_only([NullRemover(), Deduplicator()])

        # Stage 3: Load
        result = pipeline.run_load_only(SQLiteLoader("output.db"))
    """

    def __init__(
        self,
        pipeline_id: str,
        storage: IntermediateStorage,
        config: Optional[dict] = None
    ):
        """
        Initialize staged pipeline

        Args:
            pipeline_id: Unique identifier for this pipeline
            storage: Intermediate storage backend
            config: Optional pipeline configuration
        """
        self.pipeline_id = pipeline_id
        self.storage = storage
        self.config = config or {}
        self.logger = get_logger("StagedPipeline")

        # Track stage completion
        self._extract_complete = False
        self._transform_complete = False
        self._load_complete = False

        # Store schema
        self._schema: Optional[Schema] = None

    def run_extract_only(self, source: SourceAdapter) -> StageResult:
        """
        Run Extract stage only

        Extracts data from source and saves to intermediate storage

        Args:
            source: Source adapter to extract from

        Returns:
            StageResult with extraction details
        """
        self.logger.info(f"Pipeline {self.pipeline_id}: Starting Extract stage")
        start_time = time.time()

        try:
            with source as src:
                # Get schema
                self._schema = src.get_schema()
                self.logger.info(f"Schema inferred: {len(self._schema.fields)} fields")

                # Read records
                records = list(src.read())
                record_count = len(records)
                self.logger.info(f"Extracted {record_count} records")

                # Save to intermediate storage
                storage_key = f"{self.pipeline_id}/extracted"
                self.storage.save_records(
                    key=storage_key,
                    records=records,
                    schema=self._schema,
                    metadata={
                        'stage': 'extract',
                        'timestamp': datetime.now().isoformat(),
                        'source_type': source.__class__.__name__
                    }
                )

                self._extract_complete = True
                duration = time.time() - start_time

                self.logger.info(
                    f"Extract stage completed in {duration:.2f}s"
                )

                return StageResult(
                    stage='extract',
                    success=True,
                    record_count=record_count,
                    duration_seconds=duration,
                    storage_key=storage_key
                )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Extract stage failed: {e}")
            return StageResult(
                stage='extract',
                success=False,
                record_count=0,
                duration_seconds=duration,
                error_message=str(e)
            )

    def run_transform_only(self, transformers: List[Transformer]) -> StageResult:
        """
        Run Transform stage only

        Loads data from intermediate storage, transforms it, and saves back

        Args:
            transformers: List of transformers to apply

        Returns:
            StageResult with transformation details

        Raises:
            PipelineException: If extract stage hasn't been run
        """
        if not self._extract_complete and not self.storage.exists(f"{self.pipeline_id}/extracted"):
            raise PipelineException(
                "Extract stage must be run before Transform. "
                "No extracted data found in storage."
            )

        self.logger.info(
            f"Pipeline {self.pipeline_id}: Starting Transform stage "
            f"with {len(transformers)} transformer(s)"
        )
        start_time = time.time()

        try:
            # Load extracted records
            extract_key = f"{self.pipeline_id}/extracted"
            records, schema = self.storage.load_records(extract_key)
            self._schema = schema or self._schema

            input_count = len(records)
            self.logger.info(f"Loaded {input_count} records from storage")

            # Call setup on transformers that need it
            for transformer in transformers:
                if hasattr(transformer, 'setup'):
                    transformer.setup()

            # Apply transformers
            for transformer in transformers:
                self.logger.info(f"Applying transformer: {transformer.__class__.__name__}")
                records = transformer.transform_batch(records)
                self.logger.info(f"After transform: {len(records)} records remain")

            # Call cleanup on transformers
            for transformer in transformers:
                if hasattr(transformer, 'cleanup'):
                    transformer.cleanup()

            output_count = len(records)

            # Update schema if transformed
            if records and records[0].schema:
                self._schema = records[0].schema

            # Save transformed records
            storage_key = f"{self.pipeline_id}/transformed"
            self.storage.save_records(
                key=storage_key,
                records=records,
                schema=self._schema,
                metadata={
                    'stage': 'transform',
                    'timestamp': datetime.now().isoformat(),
                    'input_count': input_count,
                    'output_count': output_count,
                    'transformers': [t.__class__.__name__ for t in transformers]
                }
            )

            self._transform_complete = True
            duration = time.time() - start_time

            self.logger.info(
                f"Transform stage completed in {duration:.2f}s "
                f"({input_count} → {output_count} records)"
            )

            return StageResult(
                stage='transform',
                success=True,
                record_count=output_count,
                duration_seconds=duration,
                storage_key=storage_key
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Transform stage failed: {e}")
            return StageResult(
                stage='transform',
                success=False,
                record_count=0,
                duration_seconds=duration,
                error_message=str(e)
            )

    def run_load_only(self, destination: DestinationAdapter | List[DestinationAdapter]) -> StageResult:
        """
        Run Load stage only

        Loads transformed data from storage and writes to destination(s)

        Args:
            destination: Destination adapter(s) to write to (single or list)

        Returns:
            StageResult with load details

        Raises:
            PipelineException: If transform stage hasn't been run
        """
        if not self._transform_complete and not self.storage.exists(f"{self.pipeline_id}/transformed"):
            raise PipelineException(
                "Transform stage must be run before Load. "
                "No transformed data found in storage."
            )

        self.logger.info(f"Pipeline {self.pipeline_id}: Starting Load stage")
        start_time = time.time()

        try:
            # Convert single destination to list for uniform processing
            destinations = [destination] if not isinstance(destination, list) else destination
            self.logger.info(f"Writing to {len(destinations)} destination(s)")

            # Load transformed records
            transform_key = f"{self.pipeline_id}/transformed"
            records, schema = self.storage.load_records(transform_key)
            load_schema = schema or self._schema

            record_count = len(records)
            self.logger.info(f"Loaded {record_count} records from storage")

            # Track total records written
            total_written = 0

            # Write to each destination
            for i, dest_adapter in enumerate(destinations):
                dest_name = dest_adapter.__class__.__name__
                self.logger.info(f"Loading to destination {i+1}/{len(destinations)}: {dest_name}")

                with dest_adapter as dest:
                    # Create schema at destination
                    if load_schema:
                        dest.create_schema(load_schema)
                        self.logger.info(f"Schema created: {len(load_schema.fields)} fields")

                    # Write records with transaction
                    dest.begin_transaction()
                    try:
                        written = dest.write(iter(records))
                        dest.commit()

                        if i == 0:
                            total_written = written

                        self.logger.info(f"Loaded {written} records to {dest_name}")

                    except Exception as e:
                        dest.rollback()
                        self.logger.error(f"Failed to load to {dest_name}: {e}")
                        raise

            self._load_complete = True
            duration = time.time() - start_time

            self.logger.info(
                f"Load stage completed in {duration:.2f}s"
            )

            return StageResult(
                stage='load',
                success=True,
                record_count=total_written,
                duration_seconds=duration
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Load stage failed: {e}")
            return StageResult(
                stage='load',
                success=False,
                record_count=0,
                duration_seconds=duration,
                error_message=str(e)
            )

    def get_status(self) -> dict:
        """
        Get pipeline status

        Returns:
            dict: Status of all stages
        """
        return {
            'pipeline_id': self.pipeline_id,
            'extract_complete': self._extract_complete,
            'transform_complete': self._transform_complete,
            'load_complete': self._load_complete,
            'fully_complete': self._extract_complete and self._transform_complete and self._load_complete
        }

    def cleanup(self) -> None:
        """Clean up intermediate storage for this pipeline"""
        self.logger.info(f"Cleaning up pipeline {self.pipeline_id}")
        self.storage.cleanup(self.pipeline_id)
