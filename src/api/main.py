"""
FastAPI application for AI ETL Framework
Supports both unified and staged execution modes
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from src.api.models import (
    PipelineConfig,
    PipelineResponse,
    StageResponse,
    PipelineStatus,
    ExecutionMode
)
from src.api.pipeline_service import PipelineService
from src.common.logging import get_logger

logger = get_logger("API")

app = FastAPI(
    title="AI ETL Framework API",
    description="Backend API for unified and staged ETL pipeline execution",
    version="1.0.0"
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pipeline service (manages executions)
pipeline_service = PipelineService()


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "AI ETL Framework API",
        "version": "1.0.0",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "active_pipelines": len(pipeline_service.get_active_pipelines()),
        "timestamp": datetime.utcnow().isoformat()
    }


# ============================================================
# Unified Mode Endpoints
# ============================================================

@app.post("/api/pipeline/unified", response_model=PipelineResponse)
async def run_unified_pipeline(
    config: PipelineConfig,
    background_tasks: BackgroundTasks
):
    """
    Execute entire ETL pipeline (Extract → Transform → Load)

    Runs synchronously for small datasets, or async with status polling
    """
    try:
        logger.info(f"Starting unified pipeline: {config.name}")

        # Generate pipeline ID
        pipeline_id = str(uuid.uuid4())

        # Execute pipeline (can run in background for large datasets)
        result = await pipeline_service.run_unified(
            pipeline_id=pipeline_id,
            config=config
        )

        return result

    except Exception as e:
        logger.error(f"Unified pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Staged Mode Endpoints
# ============================================================

@app.post("/api/pipeline/staged/init", response_model=PipelineResponse)
async def init_staged_pipeline(config: PipelineConfig):
    """
    Initialize a staged pipeline (creates storage, returns pipeline_id)
    """
    try:
        logger.info(f"Initializing staged pipeline: {config.name}")

        # Generate pipeline ID
        pipeline_id = str(uuid.uuid4())

        # Initialize staged pipeline
        result = await pipeline_service.init_staged(
            pipeline_id=pipeline_id,
            config=config
        )

        return result

    except Exception as e:
        logger.error(f"Staged pipeline init failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/staged/{pipeline_id}/extract", response_model=StageResponse)
async def run_extract(pipeline_id: str):
    """
    Execute Extract stage only
    """
    try:
        logger.info(f"Running extract stage: {pipeline_id}")

        result = await pipeline_service.run_extract(pipeline_id)

        return result

    except Exception as e:
        logger.error(f"Extract stage failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/staged/{pipeline_id}/transform", response_model=StageResponse)
async def run_transform(pipeline_id: str):
    """
    Execute Transform stage only
    """
    try:
        logger.info(f"Running transform stage: {pipeline_id}")

        result = await pipeline_service.run_transform(pipeline_id)

        return result

    except Exception as e:
        logger.error(f"Transform stage failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/staged/{pipeline_id}/load", response_model=StageResponse)
async def run_load(pipeline_id: str):
    """
    Execute Load stage only
    """
    try:
        logger.info(f"Running load stage: {pipeline_id}")

        result = await pipeline_service.run_load(pipeline_id)

        return result

    except Exception as e:
        logger.error(f"Load stage failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Status and Management Endpoints
# ============================================================

@app.get("/api/pipeline/{pipeline_id}/status", response_model=PipelineStatus)
async def get_pipeline_status(pipeline_id: str):
    """
    Get current status of a pipeline
    """
    try:
        status = await pipeline_service.get_status(pipeline_id)

        if not status:
            raise HTTPException(status_code=404, detail="Pipeline not found")

        return status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pipelines", response_model=List[PipelineStatus])
async def list_pipelines(
    limit: int = 50,
    offset: int = 0,
    mode: Optional[ExecutionMode] = None
):
    """
    List all pipelines with optional filtering
    """
    try:
        pipelines = await pipeline_service.list_pipelines(
            limit=limit,
            offset=offset,
            mode=mode
        )

        return pipelines

    except Exception as e:
        logger.error(f"List pipelines failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/pipeline/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """
    Delete pipeline and cleanup intermediate storage
    """
    try:
        await pipeline_service.delete_pipeline(pipeline_id)

        return {"message": "Pipeline deleted successfully", "pipeline_id": pipeline_id}

    except Exception as e:
        logger.error(f"Delete pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pipeline/{pipeline_id}/data/preview")
async def preview_data(
    pipeline_id: str,
    stage: str = "transformed",
    limit: int = 100
):
    """
    Preview data at a specific stage

    Args:
        pipeline_id: Pipeline identifier
        stage: Stage to preview (extracted, transformed)
        limit: Max records to return
    """
    try:
        data = await pipeline_service.preview_data(
            pipeline_id=pipeline_id,
            stage=stage,
            limit=limit
        )

        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for stage '{stage}'"
            )

        return data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Data preview failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
