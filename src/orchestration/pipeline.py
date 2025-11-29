"""
Pipeline orchestrator for AI-ETL framework
"""
import time
from datetime import datetime
from typing import List, Optional

from src.adapters.base import SourceAdapter, DestinationAdapter
from src.transformers.base_transformer import Transformer
from src.common.models import PipelineResult, PipelineError, Record
from src.common.exceptions import PipelineError as PipelineException
from src.common.logging import get_logger


class Pipeline:
    """
    Pipeline orchestrator that connects Extract, Transform, and Load stages

    Example:
        pipeline = (Pipeline()
            .extract(CSVSource("data.csv"))
            .transform(NullRemover())
            .load(SQLiteLoader("output.db", table="data"))
            .run())
    """

    def __init__(self, pipeline_id: Optional[str] = None):
        """
        Initialize pipeline

        Args:
            pipeline_id: Optional pipeline identifier
        """
        self.pipeline_id = pipeline_id or f"pipeline_{int(time.time())}"
        self.logger = get_logger("Pipeline")

        self._source: Optional[SourceAdapter] = None
        self._transformers: List[Transformer] = []
        self._destinations: List[DestinationAdapter] = []

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
                schema = source.get_schema()
                self.logger.info(f"Schema inferred: {len(schema.fields)} fields")

                # Read records
                records = list(source.read())
                result.records_extracted = len(records)

                self.logger.info(f"Extracted {result.records_extracted} records")

            result.extract_duration = time.time() - extract_start

            # Stage 2: Transform
            transform_start = time.time()
            self.logger.info(f"Stage 2: Transform - {len(self._transformers)} transformer(s)")

            # Call setup on transformers that need it
            for transformer in self._transformers:
                if hasattr(transformer, 'setup'):
                    transformer.setup()

            for transformer in self._transformers:
                self.logger.info(f"Applying transformer: {transformer.__class__.__name__}")
                records = transformer.transform_batch(records)
                self.logger.info(f"After transform: {len(records)} records remain")

            # Call cleanup on all transformers (for file writing, etc.)
            for transformer in self._transformers:
                if hasattr(transformer, 'cleanup'):
                    transformer.cleanup()

            result.records_transformed = len(records)
            result.transform_duration = time.time() - transform_start

            # Stage 3: Load
            load_start = time.time()
            self.logger.info(f"Stage 3: Load - Writing to {len(self._destinations)} destination(s)")

            # Use transformed schema if available (e.g., after aggregation)
            # Otherwise use source schema
            if records and records[0].schema:
                load_schema = records[0].schema
                self.logger.info(
                    f"Using transformed schema: {len(load_schema.fields)} fields"
                )
            else:
                load_schema = schema
                self.logger.info("Using source schema")

            # Track total records loaded (should be same for all destinations)
            total_loaded = 0

            # Write to each destination
            for i, destination in enumerate(self._destinations):
                dest_name = destination.__class__.__name__
                self.logger.info(f"Loading to destination {i+1}/{len(self._destinations)}: {dest_name}")

                with destination:
                    # Create schema at destination
                    destination.create_schema(load_schema)

                    # Write records
                    destination.begin_transaction()
                    try:
                        written = destination.write(iter(records))
                        destination.commit()

                        if i == 0:
                            total_loaded = written

                        self.logger.info(f"Loaded {written} records to {dest_name}")

                    except Exception as e:
                        destination.rollback()
                        self.logger.error(f"Failed to load to {dest_name}: {e}")
                        raise

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
