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
from src.transformers.enrichers.deduplicator import Deduplicator
from src.adapters.destinations.sqlite_loader import SQLiteLoader
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

            # Build destination
            destination = self._build_destination(config.destination)

            # Create and run pipeline
            pipeline = Pipeline(pipeline_id=pipeline_id)
            pipeline.extract(source)
            for transformer in transformers:
                pipeline.transform(transformer)
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

            return PipelineResponse(
                pipeline_id=pipeline_id,
                mode=ExecutionMode.UNIFIED,
                status="completed",
                message=f"Pipeline completed successfully: {result.records_loaded} records loaded",
                stages=stages,
                created_at=started_at
            )

        except Exception as e:
            logger.error(f"Unified pipeline failed: {e}\n{traceback.format_exc()}")

            # Update state
            self.pipelines[pipeline_id].update({
                "status": "failed",
                "error": str(e),
                "updated_at": datetime.utcnow()
            })

            raise

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

            # Build destination
            destination = self._build_destination(config.destination)

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
        if config.type == "null_remover":
            return NullRemover()
        elif config.type == "dedup":
            return Deduplicator()
        # Add other transformer types as needed
        else:
            raise ValueError(f"Unsupported transformer type: {config.type}")

    def _build_destination(self, config):
        """Build destination adapter from config"""
        if config.type == "sqlite":
            return SQLiteLoader(
                db_path=config.path,
                table=config.table_name
            )
        # Add other destination types as needed
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
