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


@app.delete("/api/analytics/data-sources/{source_id}/cleanup")
async def cleanup_data_source(source_id: str, role: str):
    """
    Delete all insights and visualizations for a data source.
    Does NOT delete the data source configuration itself.

    Args:
        source_id: Data source ID
        role: User role (query parameter)
    """
    try:
        from src.database.analytics_db import cleanup_source_data

        result = cleanup_source_data(role, source_id)

        return {
            "success": True,
            "source_id": source_id,
            "role": role,
            "insights_deleted": result['insights_deleted'],
            "visualizations_deleted": result['visualizations_deleted']
        }
    except Exception as e:
        logger.error(f"Failed to cleanup data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/analytics/data-sources/{source_id}/last-run")
async def update_source_last_run(source_id: str, role: str, last_run: Dict[str, Any]):
    """
    Update a data source's lastRun field (preserves card metadata when history is cleared)

    Args:
        source_id: Data source ID
        role: User role (query parameter)
        last_run: Last run data (timestamp, type, records, duration)
    """
    try:
        from src.database.analytics_db import update_source_last_run as db_update_last_run

        success = db_update_last_run(role, source_id, last_run)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Data source '{source_id}' not found for role '{role}'"
            )

        return {
            "message": "Last run updated successfully",
            "source_id": source_id,
            "role": role
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update source last run: {e}")
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


# ============================================================================
# INSIGHT GENERATION ENDPOINTS
# ============================================================================

@app.get("/api/analytics/insights")
async def get_analytics_insights(role: str):
    """
    Get all AI-generated insights for a specific role

    Args:
        role: User role (query parameter)
    """
    try:
        from src.database.analytics_db import get_source_insights

        insights = get_source_insights(role)

        return {
            "role": role,
            "insights": insights,
            "count": len(insights)
        }
    except Exception as e:
        logger.error(f"Failed to get insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/generate-insights")
async def generate_analytics_insights(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Trigger AI insight generation for a data source.

    ETL insights take precedence over RAG insights:
    - If ETL insights exist, never overwrite
    - If RAG insights exist and this is an ETL run, regenerate
    - If RAG insights exist and this is a RAG run, skip

    Body should contain:
        - source_id: Data source ID
        - role: User role
        - file_path: Path to the processed data file
        - source_name: Data source name
        - source_icon: Data source icon emoji
        - run_type: 'etl', 'rag', or 'etl+rag'
    """
    try:
        from src.database.analytics_db import get_source_insight, save_source_insights

        source_id = data.get('source_id')
        role = data.get('role')
        file_path = data.get('file_path')
        source_name = data.get('source_name')
        source_icon = data.get('source_icon', '📊')
        run_type = data.get('run_type', 'etl')

        if not all([source_id, role, file_path, source_name]):
            raise HTTPException(
                status_code=400,
                detail="source_id, role, file_path, and source_name are required"
            )

        # Check if insights already exist
        existing = get_source_insight(role, source_id)
        is_etl_run = run_type in ['etl', 'etl+rag']

        if existing:
            existing_from_etl = existing.get('generatedFrom') == 'etl'

            if existing_from_etl:
                # ETL insights exist - never overwrite
                return {
                    "status": "skipped",
                    "reason": "ETL insights already exist",
                    "source_id": source_id
                }

            if not is_etl_run:
                # RAG insights exist, and this is another RAG run - skip
                return {
                    "status": "skipped",
                    "reason": "Insights already exist",
                    "source_id": source_id
                }

            # RAG insights exist, but this is an ETL run - will regenerate
            logger.info(f"Regenerating insights for {source_id}: ETL takes precedence over RAG")

        # Generate insights in background to not block the response
        background_tasks.add_task(
            _generate_and_save_insights,
            source_id, role, file_path, source_name, source_icon, is_etl_run
        )

        return {
            "status": "generating",
            "message": "Insight generation started",
            "source_id": source_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger insight generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _generate_and_save_insights(
    source_id: str,
    role: str,
    file_path: str,
    source_name: str,
    source_icon: str,
    is_etl_run: bool
):
    """Background task to generate and save insights."""
    try:
        from src.api.insight_generator import get_insight_generator
        from src.database.analytics_db import save_source_insights

        generator = get_insight_generator()
        result = generator.generate_insights(file_path, source_name)

        # Save to database
        save_source_insights(
            role=role,
            source_id=source_id,
            source_name=source_name,
            source_icon=source_icon,
            data_summary=result['summary'],
            insights=result['insights'],
            records_analyzed=result['records_analyzed'],
            generated_from='etl' if is_etl_run else 'rag'
        )

        logger.info(f"Insights saved for {source_id} (role: {role})")

    except Exception as e:
        logger.error(f"Background insight generation failed for {source_id}: {e}")


# ============================================================================
# VISUALIZATION ENDPOINTS
# ============================================================================

@app.get("/api/analytics/visualizations")
async def get_analytics_visualizations(role: str, source_id: str = None):
    """
    Get visualizations for a specific data source or all sources for a role.

    Args:
        role: User role (required)
        source_id: Data source ID (optional - if not provided, returns all for role)
    """
    try:
        from src.database.analytics_db import get_visualizations, get_all_visualizations_for_role

        if source_id:
            visualizations = get_visualizations(role, source_id)
        else:
            visualizations = get_all_visualizations_for_role(role)

        return {
            "role": role,
            "source_id": source_id,
            "visualizations": visualizations,
            "count": len(visualizations)
        }
    except Exception as e:
        logger.error(f"Failed to get visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/visualizations/sources")
async def get_visualization_sources(role: str):
    """
    Get list of source IDs that have visualizations stored.
    Used to filter the data source dropdown on the Visualizations page.

    Args:
        role: User role (required)
    """
    try:
        from src.database.analytics_db import get_sources_with_visualizations

        source_ids = get_sources_with_visualizations(role)

        return {
            "role": role,
            "source_ids": source_ids,
            "count": len(source_ids)
        }
    except Exception as e:
        logger.error(f"Failed to get sources with visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/visualizations/generate")
async def generate_analytics_visualizations(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Generate visualizations for a data source.
    Called after ETL processing completes.

    Body should contain:
        - source_id: Data source ID
        - role: User role
        - file_path: Path to the processed data file (Parquet or CSV)
    """
    try:
        source_id = data.get('source_id')
        role = data.get('role')
        file_path = data.get('file_path')

        if not all([source_id, role, file_path]):
            raise HTTPException(
                status_code=400,
                detail="source_id, role, and file_path are required"
            )

        # Generate visualizations in background to not block the response
        background_tasks.add_task(
            _generate_and_save_visualizations,
            source_id, role, file_path
        )

        return {
            "status": "generating",
            "message": "Visualization generation started",
            "source_id": source_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger visualization generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _generate_and_save_visualizations(source_id: str, role: str, file_path: str):
    """Background task to generate and save visualizations."""
    try:
        from src.api.visualization_generator import generate_all_charts
        from src.database.analytics_db import save_visualizations

        logger.info(f"Generating visualizations for {source_id} from {file_path}")

        # Generate charts
        charts = generate_all_charts(file_path, max_charts=10)

        if charts:
            # Save to database
            saved_count = save_visualizations(role, source_id, charts)
            logger.info(f"Saved {saved_count} visualizations for {source_id}")
        else:
            logger.warning(f"No visualizations generated for {source_id}")

    except Exception as e:
        logger.error(f"Background visualization generation failed for {source_id}: {e}")


@app.post("/api/analytics/visualizations/custom")
async def generate_custom_visualization(data: Dict[str, Any]):
    """
    Generate a custom visualization based on user prompt.
    Used by AI chat to create on-demand charts.

    Body should contain:
        - source_id: Data source ID
        - role: User role
        - prompt: Natural language description of desired chart
    """
    try:
        from src.api.visualization_generator import generate_custom_chart
        from src.database.analytics_db import get_data_sources

        source_id = data.get('source_id')
        role = data.get('role')
        prompt = data.get('prompt')

        if not all([source_id, role, prompt]):
            raise HTTPException(
                status_code=400,
                detail="source_id, role, and prompt are required"
            )

        # Get the file path for this source
        sources = get_data_sources(role)
        source = next((s for s in sources if s.get('id') == source_id), None)

        if not source:
            raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

        # Get the ETL output path
        file_path = source.get('etlPath') or source.get('filePath')
        if not file_path:
            raise HTTPException(status_code=400, detail="No processed data file available for this source")

        # Generate custom chart
        chart = generate_custom_chart(file_path, prompt)

        if not chart:
            return {
                "status": "error",
                "message": "Could not generate chart from the given prompt",
                "source_id": source_id
            }

        return {
            "status": "success",
            "chart": chart,
            "source_id": source_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate custom visualization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/visualizations")
async def delete_analytics_visualizations(role: str, source_id: str):
    """
    Delete all visualizations for a specific data source.

    Args:
        role: User role (query parameter)
        source_id: Data source ID (query parameter)
    """
    try:
        from src.database.analytics_db import delete_visualizations

        deleted_count = delete_visualizations(role, source_id)

        return {
            "message": "Visualizations deleted successfully",
            "role": role,
            "source_id": source_id,
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Failed to delete visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
