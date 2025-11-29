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
from src.database import (
    init_database,
    get_data_sources,
    save_data_source,
    delete_data_source,
    get_pipeline_history,
    save_pipeline_run,
    migrate_from_localstorage
)

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


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        init_database()
        logger.info("Analytics database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize analytics database: {e}")
        # Don't fail startup - allow pipeline operations to continue


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


# ============================================================
# Analytics Interface Storage Endpoints
# ============================================================

@app.get("/api/analytics/data-sources")
async def get_analytics_data_sources(role: str):
    """
    Get all data sources for a specific role

    Args:
        role: User role (ceo, cto, analyst, etc.)
    """
    try:
        sources = get_data_sources(role)
        return {
            "role": role,
            "sources": sources,
            "count": len(sources)
        }
    except Exception as e:
        logger.error(f"Failed to get data sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/data-sources")
async def save_analytics_data_source(data: Dict[str, Any]):
    """
    Save or update a data source configuration

    Body should contain:
        - role: User role
        - source: Data source object
    """
    try:
        role = data.get('role')
        source = data.get('source')

        if not role or not source:
            raise HTTPException(
                status_code=400,
                detail="Both 'role' and 'source' are required"
            )

        save_data_source(role, source)

        return {
            "message": "Data source saved successfully",
            "source_id": source.get('id'),
            "role": role
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/data-sources/{source_id}")
async def delete_analytics_data_source(source_id: str, role: str):
    """
    Delete a data source

    Args:
        source_id: Data source ID
        role: User role (query parameter)
    """
    try:
        success = delete_data_source(role, source_id)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Data source '{source_id}' not found for role '{role}'"
            )

        return {
            "message": "Data source deleted successfully",
            "source_id": source_id,
            "role": role
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/history")
async def get_analytics_history(role: str, limit: int = 50):
    """
    Get pipeline execution history for a specific role

    Args:
        role: User role
        limit: Maximum number of records to return (default: 50)
    """
    try:
        history = get_pipeline_history(role, limit)
        return {
            "role": role,
            "history": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/history")
async def save_analytics_history(data: Dict[str, Any]):
    """
    Save a pipeline run to history

    Body should contain:
        - role: User role
        - run: Pipeline run data
    """
    try:
        role = data.get('role')
        run = data.get('run')

        if not role or not run:
            raise HTTPException(
                status_code=400,
                detail="Both 'role' and 'run' are required"
            )

        run_id = save_pipeline_run(role, run)

        return {
            "message": "Pipeline run saved successfully",
            "run_id": run_id,
            "role": role
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save pipeline run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/migrate")
async def migrate_analytics_data(data: Dict[str, Any]):
    """
    Migrate data from localStorage to PostgreSQL

    Body should contain:
        - role: User role
        - dataSources: Array of data source objects
        - history: Array of pipeline run objects
    """
    try:
        role = data.get('role')
        data_sources = data.get('dataSources', [])
        history = data.get('history', [])

        if not role:
            raise HTTPException(
                status_code=400,
                detail="'role' is required"
            )

        result = migrate_from_localstorage(role, data_sources, history)

        return {
            "message": "Migration completed",
            "role": role,
            "result": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/history")
async def clear_analytics_history(role: str):
    """
    Clear all pipeline history for a specific role

    Args:
        role: User role (query parameter)
    """
    try:
        from src.database.analytics_db import clear_pipeline_history

        deleted_count = clear_pipeline_history(role)

        return {
            "message": "Pipeline history cleared successfully",
            "role": role,
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Failed to clear pipeline history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
