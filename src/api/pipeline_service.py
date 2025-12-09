"""
Pipeline service - manages pipeline execution and state
"""
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime
import traceback

from src.api.models import (
    PipelineConfig,
    PipelineResponse,
    StageResponse,
    PipelineStatus,
    ExecutionMode,
    StageStatus,
    StageResult
)
from src.orchestration.pipeline import Pipeline
from src.orchestration.staged_pipeline import StagedPipeline, StageResult as CoreStageResult
from src.storage.file_storage import FileStorage
from src.storage.s3_storage import S3Storage
from src.adapters.sources.csv_source import CSVSource
from src.adapters.sources.json_source import JSONSource
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.cleaners.column_remover import ColumnRemover
from src.transformers.enrichers.deduplicator import Deduplicator
from src.transformers.enrichers.aggregator import Aggregator
from src.transformers.enrichers.metadata_to_columns import MetadataToColumnsTransformer
from src.transformers.validators.quality_scorer import QualityScorer
from src.transformers.analyzers.anomaly_detector import AnomalyDetector
from src.transformers.analyzers.schema_inferrer import SchemaInferrer
from src.transformers.routing.anomaly_splitter import AnomalySplitter
from src.transformers.exporters.dashboard_aggregator import DashboardAggregator
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.adapters.destinations.postgres_loader import PostgreSQLLoader
from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.json_loader import JSONLoader
from src.adapters.destinations.parquet_loader import ParquetLoader
from src.common.logging import get_logger

logger = get_logger("PipelineService")


class PipelineService:
    """
    Service for managing pipeline executions
    Handles both unified and staged modes
    """

    def __init__(self):
        # In-memory state (replace with DynamoDB for production)
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        self.staged_pipelines: Dict[str, StagedPipeline] = {}

    def get_active_pipelines(self) -> List[str]:
        """Get list of active pipeline IDs"""
        return list(self.pipelines.keys())

    async def run_unified(
        self,
        pipeline_id: str,
        config: PipelineConfig
    ) -> PipelineResponse:
        """
        Execute entire pipeline in unified mode
        """
        try:
            started_at = datetime.utcnow()

            # Debug: Log post_processing config
            print(f"DEBUG: Pipeline config received - post_processing: {config.post_processing}", flush=True)
            if config.post_processing:
                print(f"DEBUG:   enable_rag_indexing: {config.post_processing.enable_rag_indexing}", flush=True)
                print(f"DEBUG:   index_name: {config.post_processing.index_name}", flush=True)

            # Create pipeline state
            self.pipelines[pipeline_id] = {
                "id": pipeline_id,
                "name": config.name,
                "mode": ExecutionMode.UNIFIED,
                "status": "running",
                "created_at": started_at,
                "config": config.model_dump()
            }

            # Build source
            source = self._build_source(config.source)

            # Build transformers
            transformers = [
                self._build_transformer(t) for t in config.transformers
            ]

            # Build destination(s) - support both single and multiple
            destinations = []
            if config.destinations:
                # Multiple destinations
                destinations = [self._build_destination(d) for d in config.destinations]
            elif config.destination:
                # Single destination (legacy)
                destinations = [self._build_destination(config.destination)]
            else:
                raise ValueError("No destination specified")

            # Create and run pipeline
            pipeline = Pipeline(pipeline_id=pipeline_id)
            pipeline.extract(source)
            for transformer in transformers:
                pipeline.transform(transformer)
            for destination in destinations:
                pipeline.load(destination)

            # Execute (run in thread pool for async)
            await asyncio.get_event_loop().run_in_executor(
                None,
                pipeline.run
            )

            # Access result from pipeline
            result = pipeline.result

            # Update state
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()

            self.pipelines[pipeline_id].update({
                "status": "completed",
                "updated_at": completed_at,
                "records": result.records_loaded,
                "extract_records": result.records_extracted,
                "transform_records": result.records_transformed,
                "load_records": result.records_loaded,
                "duration": duration
            })

            # Build response
            stages = [
                StageResult(
                    stage="extract",
                    status=StageStatus.COMPLETED,
                    records_in=0,
                    records_out=result.records_extracted,
                    duration_seconds=result.extract_duration,
                    started_at=started_at,
                    completed_at=completed_at
                ),
                StageResult(
                    stage="transform",
                    status=StageStatus.COMPLETED,
                    records_in=result.records_extracted,
                    records_out=result.records_transformed,
                    duration_seconds=result.transform_duration,
                    started_at=started_at,
                    completed_at=completed_at
                ),
                StageResult(
                    stage="load",
                    status=StageStatus.COMPLETED,
                    records_in=result.records_transformed,
                    records_out=result.records_loaded,
                    duration_seconds=result.load_duration,
                    started_at=started_at,
                    completed_at=completed_at
                )
            ]

            # Trigger RAG indexing if configured
            rag_job_id = None
            if config.post_processing and config.post_processing.enable_rag_indexing:
                # Get output file paths from destination configs, not adapter objects
                output_files = []
                if config.destinations:
                    output_files = [d.path for d in config.destinations if d.path]
                elif config.destination and config.destination.path:
                    output_files = [config.destination.path]

                rag_job_id = await self.trigger_rag_indexing(
                    pipeline_id=pipeline_id,
                    output_files=output_files,
                    config=config.post_processing
                )

            # Build response with optional RAG job ID
            response = PipelineResponse(
                pipeline_id=pipeline_id,
                mode=ExecutionMode.UNIFIED,
                status="completed",
                message=f"Pipeline completed successfully: {result.records_loaded} records loaded",
                stages=stages,
                created_at=started_at
            )

            # Add RAG job ID to metadata if indexing was triggered
            if rag_job_id:
                if not response.metadata:
                    response.metadata = {}
                response.metadata['rag_job_id'] = rag_job_id
                response.metadata['rag_index_name'] = config.post_processing.index_name

            return response

        except Exception as e:
            logger.error(f"Unified pipeline failed: {e}\n{traceback.format_exc()}")

            # Update state
            self.pipelines[pipeline_id].update({
                "status": "failed",
                "error": str(e),
                "updated_at": datetime.utcnow()
            })

            raise

    async def trigger_rag_indexing(
        self,
        pipeline_id: str,
        output_files: List[str],
        config
    ) -> Optional[str]:
        """
        Trigger RAG indexing after successful pipeline execution

        Args:
            pipeline_id: Pipeline identifier
            output_files: List of output file paths from pipeline
            config: PostProcessingConfig with RAG settings

        Returns:
            job_id from RAG API, or None if failed
        """
        try:
            import httpx

            logger.info(f"Triggering RAG indexing for pipeline {pipeline_id}")
            logger.info(f"RAG API URL: {config.rag_api_url}")
            logger.info(f"Index name: {config.index_name}")

            # Prefer rag_input_path (accessible in RAG container), fall back to output_files
            rag_path = None
            if hasattr(config, 'rag_input_path') and config.rag_input_path:
                rag_path = config.rag_input_path
            elif output_files and len(output_files) > 0:
                rag_path = output_files[0]

            if not rag_path:
                logger.error("No RAG input path available for indexing")
                return None

            logger.info(f"RAG input path: {rag_path}")

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{config.rag_api_url}/index/hybrid/local",
                    json={
                        "path": rag_path,
                        "target_index": config.index_name,
                        "batch_size": config.batch_size
                    }
                )

                if response.status_code == 200:
                    result = response.json()
                    job_id = result.get('job_id')
                    logger.info(f"RAG indexing triggered successfully: job_id={job_id}")
                    return job_id
                else:
                    logger.error(f"RAG indexing failed: {response.status_code} - {response.text}")
                    return None

        except Exception as e:
            logger.error(f"Failed to trigger RAG indexing: {e}")
            return None  # Don't fail pipeline if indexing fails

    async def init_staged(
        self,
        pipeline_id: str,
        config: PipelineConfig
    ) -> PipelineResponse:
        """
        Initialize a staged pipeline
        """
        try:
            started_at = datetime.utcnow()

            # Create storage
            storage = self._build_storage(config.storage)

            # Create staged pipeline
            staged_pipeline = StagedPipeline(
                pipeline_id=pipeline_id,
                storage=storage,
                config=config.model_dump()
            )

            # Store references
            self.staged_pipelines[pipeline_id] = staged_pipeline
            self.pipelines[pipeline_id] = {
                "id": pipeline_id,
                "name": config.name,
                "mode": ExecutionMode.STAGED,
                "status": "initialized",
                "extract_status": StageStatus.PENDING,
                "transform_status": StageStatus.PENDING,
                "load_status": StageStatus.PENDING,
                "created_at": started_at,
                "updated_at": started_at,
                "config": config.model_dump()
            }

            return PipelineResponse(
                pipeline_id=pipeline_id,
                mode=ExecutionMode.STAGED,
                status="initialized",
                message="Staged pipeline initialized. Ready for extract stage.",
                created_at=started_at
            )

        except Exception as e:
            logger.error(f"Staged pipeline init failed: {e}")
            raise

    async def run_extract(self, pipeline_id: str) -> StageResponse:
        """Execute extract stage"""
        try:
            # Get staged pipeline
            staged = self._get_staged_pipeline(pipeline_id)
            config = PipelineConfig(**self.pipelines[pipeline_id]["config"])

            # Build source
            source = self._build_source(config.source)

            # Update status
            self.pipelines[pipeline_id]["extract_status"] = StageStatus.RUNNING
            started_at = datetime.utcnow()

            # Execute extract
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                staged.run_extract_only,
                source
            )

            # Update status
            self.pipelines[pipeline_id].update({
                "extract_status": StageStatus.COMPLETED,
                "extract_records": result.record_count,
                "updated_at": datetime.utcnow()
            })

            return StageResponse(
                pipeline_id=pipeline_id,
                stage="extract",
                status=StageStatus.COMPLETED,
                records=result.record_count,
                duration_seconds=result.duration_seconds,
                message=f"Extract completed: {result.record_count} records extracted"
            )

        except Exception as e:
            logger.error(f"Extract failed: {e}")
            self.pipelines[pipeline_id]["extract_status"] = StageStatus.FAILED
            raise

    async def run_transform(self, pipeline_id: str) -> StageResponse:
        """Execute transform stage"""
        try:
            # Get staged pipeline
            staged = self._get_staged_pipeline(pipeline_id)
            config = PipelineConfig(**self.pipelines[pipeline_id]["config"])

            # Build transformers
            transformers = [
                self._build_transformer(t) for t in config.transformers
            ]

            # Update status
            self.pipelines[pipeline_id]["transform_status"] = StageStatus.RUNNING

            # Execute transform
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                staged.run_transform_only,
                transformers
            )

            # Update status
            self.pipelines[pipeline_id].update({
                "transform_status": StageStatus.COMPLETED,
                "transform_records": result.record_count,
                "updated_at": datetime.utcnow()
            })

            return StageResponse(
                pipeline_id=pipeline_id,
                stage="transform",
                status=StageStatus.COMPLETED,
                records=result.record_count,
                duration_seconds=result.duration_seconds,
                message=f"Transform completed: {result.record_count} records transformed"
            )

        except Exception as e:
            logger.error(f"Transform failed: {e}")
            self.pipelines[pipeline_id]["transform_status"] = StageStatus.FAILED
            raise

    async def run_load(self, pipeline_id: str) -> StageResponse:
        """Execute load stage"""
        try:
            # Get staged pipeline
            staged = self._get_staged_pipeline(pipeline_id)
            config = PipelineConfig(**self.pipelines[pipeline_id]["config"])

            # Build destination(s) - support both single and multiple
            destinations = []
            if config.destinations:
                destinations = [self._build_destination(d) for d in config.destinations]
            elif config.destination:
                destinations = [self._build_destination(config.destination)]
            else:
                raise ValueError("No destination specified")

            destination = destinations  # Pass as list to staged pipeline

            # Update status
            self.pipelines[pipeline_id]["load_status"] = StageStatus.RUNNING

            # Execute load
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                staged.run_load_only,
                destination
            )

            # Update status
            self.pipelines[pipeline_id].update({
                "load_status": StageStatus.COMPLETED,
                "load_records": result.record_count,
                "status": "completed",
                "updated_at": datetime.utcnow()
            })

            return StageResponse(
                pipeline_id=pipeline_id,
                stage="load",
                status=StageStatus.COMPLETED,
                records=result.record_count,
                duration_seconds=result.duration_seconds,
                message=f"Load completed: {result.record_count} records loaded"
            )

        except Exception as e:
            logger.error(f"Load failed: {e}")
            self.pipelines[pipeline_id]["load_status"] = StageStatus.FAILED
            raise

    async def get_status(self, pipeline_id: str) -> Optional[PipelineStatus]:
        """Get pipeline status"""
        if pipeline_id not in self.pipelines:
            return None

        state = self.pipelines[pipeline_id]

        return PipelineStatus(
            pipeline_id=pipeline_id,
            name=state["name"],
            mode=state["mode"],
            overall_status=state["status"],
            extract_status=state.get("extract_status", StageStatus.PENDING),
            transform_status=state.get("transform_status", StageStatus.PENDING),
            load_status=state.get("load_status", StageStatus.PENDING),
            created_at=state["created_at"],
            updated_at=state["updated_at"],
            extract_records=state.get("extract_records"),
            transform_records=state.get("transform_records"),
            load_records=state.get("load_records"),
            total_duration=state.get("duration"),
            error=state.get("error")
        )

    async def list_pipelines(
        self,
        limit: int = 50,
        offset: int = 0,
        mode: Optional[ExecutionMode] = None
    ) -> List[PipelineStatus]:
        """List all pipelines"""
        statuses = []

        for pipeline_id in list(self.pipelines.keys())[offset:offset + limit]:
            status = await self.get_status(pipeline_id)
            if status and (not mode or status.mode == mode):
                statuses.append(status)

        return statuses

    async def delete_pipeline(self, pipeline_id: str):
        """Delete pipeline and cleanup storage"""
        if pipeline_id in self.staged_pipelines:
            staged = self.staged_pipelines[pipeline_id]
            staged.cleanup()
            del self.staged_pipelines[pipeline_id]

        if pipeline_id in self.pipelines:
            del self.pipelines[pipeline_id]

    async def preview_data(
        self,
        pipeline_id: str,
        stage: str,
        limit: int = 100
    ) -> Optional[Dict[str, Any]]:
        """Preview data at a specific stage"""
        if pipeline_id not in self.staged_pipelines:
            return None

        staged = self.staged_pipelines[pipeline_id]

        # Load records from storage
        key = f"{pipeline_id}/{stage}"
        if not staged.storage.exists(key):
            return None

        records, schema = staged.storage.load_records(key)

        # Limit records
        records = records[:limit]

        return {
            "records": [r.data for r in records],
            "count": len(records),
            "schema": schema.name if schema else None
        }

    # ============================================================
    # Helper Methods
    # ============================================================

    def _get_staged_pipeline(self, pipeline_id: str) -> StagedPipeline:
        """Get staged pipeline or raise error"""
        if pipeline_id not in self.staged_pipelines:
            raise ValueError(f"Staged pipeline not found: {pipeline_id}")
        return self.staged_pipelines[pipeline_id]

    def _build_source(self, config):
        """Build source adapter from config"""
        if config.type == "csv":
            return CSVSource(file_path=config.path)
        elif config.type == "json":
            return JSONSource(file_path=config.path)
        # Add other source types as needed
        else:
            raise ValueError(f"Unsupported source type: {config.type}")

    def _build_transformer(self, config):
        """Build transformer from config"""
        # Get config dict if provided
        cfg = config.config or {}

        if config.type == "null_remover":
            return NullRemover(
                strategy=cfg.get('strategy', 'drop')
            )
        elif config.type == "dedup":
            return Deduplicator(
                match_mode=cfg.get('match_mode', 'exact')
            )
        elif config.type == "quality_scorer":
            return QualityScorer(
                min_score=cfg.get('min_score', 0.7),
                filter_low_quality=cfg.get('filter_low_quality', False),
                mark_anomalies=cfg.get('mark_anomalies', False)
            )
        elif config.type == "anomaly_detector":
            return AnomalyDetector(
                method=cfg.get('method', 'statistical'),
                threshold=cfg.get('threshold', 3.0)
            )
        elif config.type == "schema_inferrer":
            return SchemaInferrer()
        elif config.type == "aggregator":
            return Aggregator(
                group_by=cfg.get('group_by', []),
                aggregations=cfg.get('aggregations', {})
            )
        elif config.type == "column_remover":
            return ColumnRemover(
                columns=cfg.get('columns', [])
            )
        elif config.type == "metadata_to_columns":
            return MetadataToColumnsTransformer()
        elif config.type == "anomaly_splitter":
            return AnomalySplitter(
                output_path=cfg.get('output_path', '/app/quarantine/anomalies.csv'),
                write_on_completion=cfg.get('write_on_completion', True)
            )
        elif config.type == "dashboard_aggregator":
            return DashboardAggregator(
                output_dir=cfg.get('output_dir', './output/dashboards')
            )
        elif config.type == "type_converter":
            # Custom type converter - would need implementation
            raise ValueError("type_converter not yet implemented")
        elif config.type == "custom":
            # Custom transformer - would need implementation
            raise ValueError("custom transformers not yet implemented")
        else:
            raise ValueError(f"Unsupported transformer type: {config.type}")

    def _build_destination(self, config):
        """Build destination adapter from config"""
        if config.type == "sqlite":
            return SQLiteLoader(
                db_path=config.path or "./output.db",
                table=config.table_name
            )
        elif config.type == "postgres":
            # Parse connection string or use individual params
            if config.connection_string:
                # TODO: Parse connection string into host, database, user, password
                raise ValueError("PostgreSQL connection_string parsing not yet implemented. Please use individual host/database/user/password in config.")
            else:
                return PostgreSQLLoader(
                    host=config.host or "localhost",
                    database=config.database or "etl_db",
                    user=config.user or "postgres",
                    password=config.password or "",
                    table=config.table_name,
                    port=config.port or 5432
                )
        elif config.type == "csv":
            return CSVLoader(
                file_path=config.path or "./output.csv",
                mode="overwrite"
            )
        elif config.type == "json":
            return JSONLoader(
                file_path=config.path or "./output.json",
                mode="array"  # or "lines" for JSONL
            )
        elif config.type == "parquet":
            return ParquetLoader(
                file_path=config.path or "./output.parquet",
                compression="snappy"
            )
        else:
            raise ValueError(f"Unsupported destination type: {config.type}")

    def _build_storage(self, config):
        """Build storage from config"""
        if config.type == "file":
            return FileStorage(base_path=config.path)
        elif config.type == "s3":
            return S3Storage(
                bucket=config.bucket,
                prefix=config.prefix,
                region=config.region
            )
        else:
            raise ValueError(f"Unsupported storage type: {config.type}")
