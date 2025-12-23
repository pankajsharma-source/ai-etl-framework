"""
FastAPI application for AI ETL Framework
Supports both unified and staged execution modes
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from src.api.models import (
    PipelineConfig,
    PipelineResponse,
    StageResponse,
    PipelineStatus,
    ExecutionMode,
    DestinationConfig,
    PostProcessingConfig
)
from src.api.pipeline_service import PipelineService
from src.api.path_generator import (
    generate_outputs,
    ensure_directories_exist,
    delete_source_files,
    ensure_bronze_folder,
    list_bronze_files
)
from src.common.logging import get_logger
from src.database import (
    init_database,
    get_data_sources,
    save_data_source,
    delete_data_source,
    get_pipeline_history,
    save_pipeline_run,
    get_organization_by_id
)
# complete_delete_data_source no longer used - individual cleanup functions imported as needed
import httpx
import os

# RAG API URL for index operations
RAG_API_URL = os.environ.get('RAG_API_URL', 'http://rag-api:8000')

logger = get_logger("API")

app = FastAPI(
    title="AI ETL Framework API",
    description="Backend API for unified and staged ETL pipeline execution",
    version="2.0.0"
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
        "version": "2.0.0",
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

    Destinations and RAG index names are auto-generated based on org and data source name.
    """
    try:
        logger.info(f"Starting unified pipeline: {config.name}")

        # Generate pipeline ID
        pipeline_id = str(uuid.uuid4())

        # Get organization slug for path generation
        org = get_organization_by_id(config.org_id)
        if not org:
            raise HTTPException(status_code=404, detail=f"Organization not found: {config.org_id}")

        org_slug = org.get('slug', config.org_id)

        # Determine ETL output type from existing destinations or default to parquet
        etl_output_type = 'parquet'
        if config.destinations:
            for dest in config.destinations:
                if dest.type in ('parquet', 'csv'):
                    etl_output_type = dest.type
                    break

        # Auto-generate org-isolated destinations and RAG index
        outputs = generate_outputs(
            org_slug=org_slug,
            data_source_name=config.name,
            etl_output_type=etl_output_type
        )

        logger.info(f"Auto-generated outputs: {outputs}")

        # Create directories for output files
        ensure_directories_exist(outputs)

        # Build auto-generated destinations (overrides any provided destinations)
        config.destinations = [
            DestinationConfig(type=etl_output_type, path=outputs['etl_path']),
            DestinationConfig(type='csv', path=outputs['rag_path'])
        ]

        # If RAG indexing is enabled, set the auto-generated index name
        if config.post_processing and config.post_processing.enable_rag_indexing:
            config.post_processing.index_name = outputs['rag_index']
            config.post_processing.rag_input_path = outputs['rag_path']
            logger.info(f"RAG indexing enabled with index: {outputs['rag_index']}")

        # Update quarantine path in anomaly_splitter transformer if present
        for transformer in config.transformers:
            if transformer.type == 'anomaly_splitter':
                if not transformer.config:
                    transformer.config = {}
                transformer.config['output_path'] = outputs['quarantine_path']

        # Execute pipeline
        result = await pipeline_service.run_unified(
            pipeline_id=pipeline_id,
            config=config
        )

        return result

    except HTTPException:
        raise
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

    Destinations and RAG index names are auto-generated based on org and data source name.
    """
    try:
        logger.info(f"Initializing staged pipeline: {config.name}")

        # Generate pipeline ID
        pipeline_id = str(uuid.uuid4())

        # Get organization slug for path generation
        org = get_organization_by_id(config.org_id)
        if not org:
            raise HTTPException(status_code=404, detail=f"Organization not found: {config.org_id}")

        org_slug = org.get('slug', config.org_id)

        # Determine ETL output type from existing destinations or default to parquet
        etl_output_type = 'parquet'
        if config.destinations:
            for dest in config.destinations:
                if dest.type in ('parquet', 'csv'):
                    etl_output_type = dest.type
                    break

        # Auto-generate org-isolated destinations and RAG index
        outputs = generate_outputs(
            org_slug=org_slug,
            data_source_name=config.name,
            etl_output_type=etl_output_type
        )

        logger.info(f"Auto-generated outputs for staged pipeline: {outputs}")

        # Create directories for output files
        ensure_directories_exist(outputs)

        # Build auto-generated destinations
        config.destinations = [
            DestinationConfig(type=etl_output_type, path=outputs['etl_path']),
            DestinationConfig(type='csv', path=outputs['rag_path'])
        ]

        # If RAG indexing is enabled, set the auto-generated index name
        if config.post_processing and config.post_processing.enable_rag_indexing:
            config.post_processing.index_name = outputs['rag_index']
            config.post_processing.rag_input_path = outputs['rag_path']

        # Update quarantine path in anomaly_splitter transformer if present
        for transformer in config.transformers:
            if transformer.type == 'anomaly_splitter':
                if not transformer.config:
                    transformer.config = {}
                transformer.config['output_path'] = outputs['quarantine_path']

        # Initialize staged pipeline
        result = await pipeline_service.init_staged(
            pipeline_id=pipeline_id,
            config=config
        )

        return result

    except HTTPException:
        raise
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
async def get_analytics_data_sources(org_id: str):
    """
    Get all data sources for a specific organization

    Args:
        org_id: Organization UUID
    """
    try:
        sources = get_data_sources(org_id)
        return {
            "org_id": org_id,
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
        - org_id: Organization UUID
        - source: Data source object
    """
    try:
        org_id = data.get('org_id')
        source = data.get('source')

        if not org_id or not source:
            raise HTTPException(
                status_code=400,
                detail="Both 'org_id' and 'source' are required"
            )

        save_data_source(org_id, source)

        return {
            "message": "Data source saved successfully",
            "source_id": source.get('id'),
            "org_id": org_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/data-sources/{source_id}")
async def delete_analytics_data_source(source_id: str, org_id: str):
    """
    Delete ONLY the data source configuration.

    This removes the data source from the list but preserves:
    - Pipeline history
    - AI-generated insights
    - Visualizations
    - Output files (Parquet, CSV)
    - Search index

    Use DELETE /api/analytics/data-sources/{source_id}/data to clean all data.

    Args:
        source_id: Data source ID
        org_id: Organization UUID (query parameter)
    """
    try:
        # Check source exists
        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)

        if not source:
            raise HTTPException(
                status_code=404,
                detail=f"Data source '{source_id}' not found for org '{org_id}'"
            )

        source_name = source.get('name', source_id)

        # Only delete the config from database
        success = delete_data_source(org_id, source_id)

        return {
            "success": success,
            "source_id": source_id,
            "source_name": source_name,
            "deleted": {
                "config": success
            },
            "message": "Configuration removed. Data, files, and index remain intact."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete data source config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/data-sources/{source_id}/data")
async def clean_source_data(source_id: str, org_id: str):
    """
    Clean ALL data for a data source but keep the configuration.

    This deletes:
    - Pipeline history
    - AI-generated insights
    - Visualizations
    - Output files (Parquet, CSV)
    - Search index

    The data source configuration is preserved so you can re-run the pipeline.

    Args:
        source_id: Data source ID
        org_id: Organization UUID (query parameter)
    """
    try:
        from src.database.analytics_db import (
            delete_source_history,
            delete_source_insights,
            delete_visualizations
        )

        # Get source info (need name for file/index cleanup)
        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)

        if not source:
            raise HTTPException(
                status_code=404,
                detail=f"Data source '{source_id}' not found for org '{org_id}'"
            )

        source_name = source.get('name', source_id)

        # Get org slug for file paths
        org = get_organization_by_id(org_id)
        org_slug = org.get('slug', org_id) if org else org_id

        result = {
            "success": True,
            "source_id": source_id,
            "source_name": source_name,
            "config_kept": True,
            "deleted": {}
        }

        # 1. Delete pipeline history
        try:
            history_count = delete_source_history(org_id, source_id)
            result["deleted"]["history_records"] = history_count
        except Exception as e:
            logger.error(f"Failed to delete history for source {source_id}: {e}")
            result["deleted"]["history_records"] = {"error": str(e)}

        # 2. Delete insights
        try:
            insights_count = delete_source_insights(org_id, source_id)
            result["deleted"]["insights"] = insights_count
        except Exception as e:
            logger.error(f"Failed to delete insights for source {source_id}: {e}")
            result["deleted"]["insights"] = {"error": str(e)}

        # 3. Delete visualizations
        try:
            viz_count = delete_visualizations(org_id, source_id)
            result["deleted"]["visualizations"] = viz_count
        except Exception as e:
            logger.error(f"Failed to delete visualizations for source {source_id}: {e}")
            result["deleted"]["visualizations"] = {"error": str(e)}

        # 4. Delete output files
        try:
            file_result = delete_source_files(org_slug, source_name)
            result["deleted"]["files"] = file_result
            logger.info(f"Deleted files for source {source_id}: {file_result}")
        except Exception as e:
            logger.error(f"Failed to delete files for source {source_id}: {e}")
            result["deleted"]["files"] = {"error": str(e)}

        # 5. Delete search index
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                from src.api.path_generator import slugify
                index_slug = slugify(source_name)
                response = await client.delete(
                    f"{RAG_API_URL}/indexes/{index_slug}",
                    params={"org_id": org_id}
                )
                if response.status_code == 200:
                    result["deleted"]["search_index"] = response.json()
                    logger.info(f"Deleted search index for source {source_id}")
                else:
                    result["deleted"]["search_index"] = {
                        "success": False,
                        "error": f"RAG API returned {response.status_code}"
                    }
        except Exception as e:
            logger.error(f"Failed to delete search index for source {source_id}: {e}")
            result["deleted"]["search_index"] = {"error": str(e)}

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to clean source data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/data-sources/{source_id}/cleanup")
async def cleanup_data_source(source_id: str, org_id: str):
    """
    Delete all insights and visualizations for a data source.
    Does NOT delete the data source configuration itself.

    Args:
        source_id: Data source ID
        org_id: Organization UUID (query parameter)
    """
    try:
        from src.database.analytics_db import cleanup_source_data

        result = cleanup_source_data(org_id, source_id)

        return {
            "success": True,
            "source_id": source_id,
            "org_id": org_id,
            "insights_deleted": result['insights_deleted'],
            "visualizations_deleted": result['visualizations_deleted']
        }
    except Exception as e:
        logger.error(f"Failed to cleanup data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/analytics/data-sources/{source_id}/last-run")
async def update_source_last_run(source_id: str, org_id: str, last_run: Dict[str, Any]):
    """
    Update a data source's lastRun field (preserves card metadata when history is cleared)

    Args:
        source_id: Data source ID
        org_id: Organization UUID (query parameter)
        last_run: Last run data (timestamp, type, records, duration)
    """
    try:
        from src.database.analytics_db import update_source_last_run as db_update_last_run

        success = db_update_last_run(org_id, source_id, last_run)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Data source '{source_id}' not found for org '{org_id}'"
            )

        return {
            "message": "Last run updated successfully",
            "source_id": source_id,
            "org_id": org_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update source last run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/history")
async def get_analytics_history(org_id: str, limit: int = 50):
    """
    Get pipeline execution history for a specific organization

    Args:
        org_id: Organization UUID
        limit: Maximum number of records to return (default: 50)
    """
    try:
        history = get_pipeline_history(org_id, limit)
        return {
            "org_id": org_id,
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
        - org_id: Organization UUID
        - run: Pipeline run data
    """
    try:
        org_id = data.get('org_id')
        run = data.get('run')

        if not org_id or not run:
            raise HTTPException(
                status_code=400,
                detail="Both 'org_id' and 'run' are required"
            )

        run_id = save_pipeline_run(org_id, run)

        return {
            "message": "Pipeline run saved successfully",
            "run_id": run_id,
            "org_id": org_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save pipeline run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/analytics/history")
async def clear_analytics_history(org_id: str):
    """
    Clear all pipeline history for a specific organization

    Args:
        org_id: Organization UUID (query parameter)
    """
    try:
        from src.database.analytics_db import clear_pipeline_history

        deleted_count = clear_pipeline_history(org_id)

        return {
            "message": "Pipeline history cleared successfully",
            "org_id": org_id,
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Failed to clear pipeline history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INSIGHT GENERATION ENDPOINTS
# ============================================================================

@app.get("/api/analytics/insights")
async def get_analytics_insights(org_id: str):
    """
    Get all AI-generated insights for a specific organization

    Args:
        org_id: Organization UUID (query parameter)
    """
    try:
        from src.database.analytics_db import get_source_insights

        insights = get_source_insights(org_id)

        return {
            "org_id": org_id,
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
        - org_id: Organization UUID
        - file_path: Path to the processed data file
        - source_name: Data source name
        - source_icon: Data source icon emoji
        - run_type: 'etl', 'rag', or 'etl+rag'
    """
    try:
        from src.database.analytics_db import get_source_insight, save_source_insights

        source_id = data.get('source_id')
        org_id = data.get('org_id')
        file_path = data.get('file_path')
        source_name = data.get('source_name')
        source_icon = data.get('source_icon', '📊')
        run_type = data.get('run_type', 'etl')

        if not all([source_id, org_id, file_path, source_name]):
            raise HTTPException(
                status_code=400,
                detail="source_id, org_id, file_path, and source_name are required"
            )

        # Check if insights already exist
        existing = get_source_insight(org_id, source_id)
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
            source_id, org_id, file_path, source_name, source_icon, is_etl_run
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
    org_id: str,
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
            org_id=org_id,
            source_id=source_id,
            source_name=source_name,
            source_icon=source_icon,
            data_summary=result['summary'],
            insights=result['insights'],
            records_analyzed=result['records_analyzed'],
            generated_from='etl' if is_etl_run else 'rag'
        )

        logger.info(f"Insights saved for {source_id} (org: {org_id})")

    except Exception as e:
        logger.error(f"Background insight generation failed for {source_id}: {e}")


# ============================================================================
# INTERACTIVE DASHBOARD ENDPOINTS (DuckDB-powered)
# ============================================================================

@app.get("/api/analytics/dashboard/schema/{source_id}")
async def get_dashboard_schema(source_id: str, org_id: str):
    """
    Get schema metadata for a data source for filter panel construction.

    Returns column information including types, distinct values, and min/max ranges.
    Used by the frontend to dynamically build filter controls.

    Args:
        source_id: Data source ID
        org_id: Organization UUID (query parameter)
    """
    try:
        from src.database.duckdb_service import get_duckdb_service, Filter
        from src.database.analytics_db import get_data_sources, get_organization_by_id

        # Get org slug for file path
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")

        org_slug = org.get('slug', org_id)

        # Get source info for the slug
        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)
        if not source:
            raise HTTPException(status_code=404, detail="Data source not found")

        # Slugify source name
        from src.api.path_generator import slugify
        source_slug = slugify(source['name'])

        # Get schema from DuckDB
        duckdb_service = get_duckdb_service()
        schema = duckdb_service.get_schema(org_slug, source_slug)

        return {
            "org_id": org_id,
            "source_id": source_id,
            "source_name": source['name'],
            **schema
        }

    except FileNotFoundError as e:
        logger.warning(f"Data file not found for dashboard schema: {e}")
        raise HTTPException(status_code=404, detail="Data file not found. Run ETL pipeline first.")
    except Exception as e:
        logger.error(f"Failed to get dashboard schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/dashboard/query")
async def query_dashboard_data(data: Dict[str, Any]):
    """
    Execute filtered aggregation query for chart data.

    Used by the interactive dashboard to refresh charts with current filters.

    Body:
        {
            "org_id": "uuid",
            "source_id": "source-123",
            "filters": [
                {"column": "region", "operator": "in", "value": ["North", "East"]},
                {"column": "order_date", "operator": "between", "value": ["2024-01-01", "2024-06-30"]}
            ],
            "aggregation": {
                "group_by": ["product_category"],
                "metrics": [{"column": "amount", "agg": "sum", "alias": "total_amount"}],
                "order_by": "total_amount",
                "order_desc": true,
                "limit": 20
            }
        }
    """
    try:
        from src.database.duckdb_service import get_duckdb_service, Filter, AggregationSpec
        from src.database.analytics_db import get_data_sources, get_organization_by_id

        org_id = data.get('org_id')
        source_id = data.get('source_id')

        if not org_id or not source_id:
            raise HTTPException(status_code=400, detail="org_id and source_id are required")

        # Get org slug
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        org_slug = org.get('slug', org_id)

        # Get source slug
        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)
        if not source:
            raise HTTPException(status_code=404, detail="Data source not found")

        from src.api.path_generator import slugify
        source_slug = slugify(source['name'])

        # Parse filters
        filters = []
        for f in data.get('filters', []):
            filters.append(Filter(
                column=f['column'],
                operator=f['operator'],
                value=f.get('value')
            ))

        # Parse aggregation
        agg_data = data.get('aggregation', {})
        aggregation = AggregationSpec(
            group_by=agg_data.get('group_by', []),
            metrics=agg_data.get('metrics', []),
            order_by=agg_data.get('order_by'),
            order_desc=agg_data.get('order_desc', True),
            limit=agg_data.get('limit')
        )

        # Execute query
        duckdb_service = get_duckdb_service()
        result = duckdb_service.query_with_filters(
            org_slug=org_slug,
            source_slug=source_slug,
            filters=filters,
            aggregation=aggregation
        )

        return result

    except FileNotFoundError as e:
        logger.warning(f"Data file not found for dashboard query: {e}")
        raise HTTPException(status_code=404, detail="Data file not found. Run ETL pipeline first.")
    except Exception as e:
        logger.error(f"Failed to execute dashboard query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analytics/dashboard/drill-down")
async def get_drill_down_records(data: Dict[str, Any]):
    """
    Get raw records for drill-down view.

    Used when user clicks on a chart element to see the underlying data.

    Body:
        {
            "org_id": "uuid",
            "source_id": "source-123",
            "dimension": "product_category",
            "dimension_value": "Electronics",
            "filters": [...],
            "columns": ["order_id", "customer", "amount"],  // optional
            "limit": 100,
            "offset": 0
        }
    """
    try:
        from src.database.duckdb_service import get_duckdb_service, Filter
        from src.database.analytics_db import get_data_sources, get_organization_by_id

        org_id = data.get('org_id')
        source_id = data.get('source_id')
        dimension = data.get('dimension')
        dimension_value = data.get('dimension_value')

        if not all([org_id, source_id, dimension]):
            raise HTTPException(
                status_code=400,
                detail="org_id, source_id, and dimension are required"
            )

        # Get org and source slugs
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        org_slug = org.get('slug', org_id)

        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)
        if not source:
            raise HTTPException(status_code=404, detail="Data source not found")

        from src.api.path_generator import slugify
        source_slug = slugify(source['name'])

        # Parse filters
        filters = []
        for f in data.get('filters', []):
            filters.append(Filter(
                column=f['column'],
                operator=f['operator'],
                value=f.get('value')
            ))

        # Execute drill-down query
        duckdb_service = get_duckdb_service()
        result = duckdb_service.get_drill_down_data(
            org_slug=org_slug,
            source_slug=source_slug,
            dimension=dimension,
            dimension_value=dimension_value,
            filters=filters,
            columns=data.get('columns'),
            limit=data.get('limit', 100),
            offset=data.get('offset', 0)
        )

        return result

    except FileNotFoundError as e:
        logger.warning(f"Data file not found for drill-down: {e}")
        raise HTTPException(status_code=404, detail="Data file not found. Run ETL pipeline first.")
    except Exception as e:
        logger.error(f"Failed to get drill-down data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/dashboard/filter-values/{source_id}")
async def get_filter_values(
    source_id: str,
    org_id: str,
    column: str,
    search: str = None,
    limit: int = 100
):
    """
    Get distinct values for a column (for filter dropdowns).

    Supports search for high-cardinality columns.

    Args:
        source_id: Data source ID
        org_id: Organization UUID
        column: Column to get values for
        search: Optional search term to filter values
        limit: Maximum values to return (default 100)
    """
    try:
        from src.database.duckdb_service import get_duckdb_service
        from src.database.analytics_db import get_data_sources, get_organization_by_id

        # Get org and source slugs
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        org_slug = org.get('slug', org_id)

        sources = get_data_sources(org_id)
        source = next((s for s in sources if s['id'] == source_id), None)
        if not source:
            raise HTTPException(status_code=404, detail="Data source not found")

        from src.api.path_generator import slugify
        source_slug = slugify(source['name'])

        # Get values
        duckdb_service = get_duckdb_service()
        result = duckdb_service.get_filter_values(
            org_slug=org_slug,
            source_slug=source_slug,
            column=column,
            search=search,
            limit=limit
        )

        return result

    except FileNotFoundError as e:
        logger.warning(f"Data file not found for filter values: {e}")
        raise HTTPException(status_code=404, detail="Data file not found. Run ETL pipeline first.")
    except Exception as e:
        logger.error(f"Failed to get filter values: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# VISUALIZATION ENDPOINTS
# ============================================================================

@app.get("/api/analytics/visualizations")
async def get_analytics_visualizations(org_id: str, source_id: str = None):
    """
    Get visualizations for a specific data source or all sources for an organization.

    Args:
        org_id: Organization UUID (required)
        source_id: Data source ID (optional - if not provided, returns all for org)
    """
    try:
        from src.database.analytics_db import get_visualizations, get_all_visualizations_for_org

        if source_id:
            visualizations = get_visualizations(org_id, source_id)
        else:
            visualizations = get_all_visualizations_for_org(org_id)

        return {
            "org_id": org_id,
            "source_id": source_id,
            "visualizations": visualizations,
            "count": len(visualizations)
        }
    except Exception as e:
        logger.error(f"Failed to get visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/visualizations/sources")
async def get_visualization_sources(org_id: str):
    """
    Get list of source IDs that have visualizations stored.
    Used to filter the data source dropdown on the Visualizations page.

    Args:
        org_id: Organization UUID (required)
    """
    try:
        from src.database.analytics_db import get_sources_with_visualizations

        source_ids = get_sources_with_visualizations(org_id)

        return {
            "org_id": org_id,
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
        - org_id: Organization UUID
        - file_path: Path to the processed data file (Parquet or CSV)
    """
    try:
        source_id = data.get('source_id')
        org_id = data.get('org_id')
        file_path = data.get('file_path')

        if not all([source_id, org_id, file_path]):
            raise HTTPException(
                status_code=400,
                detail="source_id, org_id, and file_path are required"
            )

        # Generate visualizations in background to not block the response
        background_tasks.add_task(
            _generate_and_save_visualizations,
            source_id, org_id, file_path
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


async def _generate_and_save_visualizations(source_id: str, org_id: str, file_path: str):
    """Background task to generate and save visualizations."""
    try:
        from src.api.visualization_generator import generate_all_charts
        from src.database.analytics_db import save_visualizations

        logger.info(f"Generating visualizations for {source_id} from {file_path}")

        # Generate charts
        charts = generate_all_charts(file_path, max_charts=10)

        if charts:
            # Save to database
            saved_count = save_visualizations(org_id, source_id, charts)
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
        - org_id: Organization UUID
        - prompt: Natural language description of desired chart
    """
    try:
        from src.api.visualization_generator import generate_custom_chart
        from src.database.analytics_db import get_data_sources

        source_id = data.get('source_id')
        org_id = data.get('org_id')
        prompt = data.get('prompt')

        if not all([source_id, org_id, prompt]):
            raise HTTPException(
                status_code=400,
                detail="source_id, org_id, and prompt are required"
            )

        # Get the file path for this source
        sources = get_data_sources(org_id)
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
async def delete_analytics_visualizations(org_id: str, source_id: str):
    """
    Delete all visualizations for a specific data source.

    Args:
        org_id: Organization UUID (query parameter)
        source_id: Data source ID (query parameter)
    """
    try:
        from src.database.analytics_db import delete_visualizations

        deleted_count = delete_visualizations(org_id, source_id)

        return {
            "message": "Visualizations deleted successfully",
            "org_id": org_id,
            "source_id": source_id,
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Failed to delete visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ORGANIZATION ENDPOINTS
# ============================================================================

@app.get("/api/organizations/by-id/{org_id}")
async def get_organization_by_uuid(org_id: str):
    """
    Get an organization by its UUID.
    Used by RAG API to look up org_slug for display name formatting.

    Args:
        org_id: Organization UUID
    """
    try:
        org = get_organization_by_id(org_id)

        if not org:
            raise HTTPException(
                status_code=404,
                detail=f"Organization '{org_id}' not found"
            )

        return org
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get organization by ID: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/organizations/{slug}")
async def get_organization(slug: str):
    """
    Get an organization by its slug.

    Args:
        slug: URL-friendly organization identifier
    """
    try:
        from src.database.analytics_db import get_organization_by_slug

        org = get_organization_by_slug(slug)

        if not org:
            raise HTTPException(
                status_code=404,
                detail=f"Organization '{slug}' not found"
            )

        return org
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get organization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/organizations")
async def create_organization(data: Dict[str, Any]):
    """
    Create a new organization.

    Body should contain:
        - name: Organization display name
        - slug: URL-friendly identifier
        - industry: Industry type (optional)
    """
    try:
        from src.database.analytics_db import create_organization as db_create_org

        name = data.get('name')
        slug = data.get('slug')
        industry = data.get('industry')

        if not name or not slug:
            raise HTTPException(
                status_code=400,
                detail="Both 'name' and 'slug' are required"
            )

        org = db_create_org(name, slug, industry)

        return {
            "message": "Organization created successfully",
            "organization": org
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create organization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/organizations/{org_id}/industry")
async def update_organization_industry(
    org_id: str,
    data: Dict[str, Any],
    authorization: Optional[str] = Header(None)
):
    """
    Update an organization's industry.
    Only owners can update industry.

    Path params:
        - org_id: Organization UUID

    Headers:
        - Authorization: Bearer <token>

    Body:
        - industry: New industry value
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            update_organization_industry as db_update_industry
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can update industry")

        # Get and validate industry
        industry = data.get('industry')
        if not industry:
            raise HTTPException(status_code=400, detail="Industry is required")

        # Update industry
        org = db_update_industry(org_id, industry)

        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")

        return {
            "message": "Industry updated successfully",
            "organization": org
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update organization industry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FILE MANAGEMENT ENDPOINTS
# ============================================================================

@app.post("/api/organizations/{org_id}/files/upload")
async def upload_file(org_id: str, file: UploadFile = File(...)):
    """
    Upload a file to the organization's bronze folder.

    Args:
        org_id: Organization UUID
        file: File to upload (multipart form data)

    Returns:
        File info with name, path, and size
    """
    try:
        # Get organization to get the slug
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(
                status_code=404,
                detail=f"Organization '{org_id}' not found"
            )

        org_slug = org.get('slug')
        if not org_slug:
            raise HTTPException(
                status_code=400,
                detail="Organization has no slug configured"
            )

        # Ensure bronze folder exists
        bronze_path = ensure_bronze_folder(org_slug)

        # Save the file
        import aiofiles
        file_path = f"{bronze_path}/{file.filename}"

        async with aiofiles.open(file_path, 'wb') as out_file:
            content = await file.read()
            await out_file.write(content)

        file_size = len(content)

        logger.info(f"Uploaded file {file.filename} to {file_path} ({file_size} bytes)")

        return {
            "success": True,
            "file": {
                "name": file.filename,
                "path": file_path,
                "size": file_size
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/organizations/{org_id}/files")
async def list_files(org_id: str):
    """
    List files in the organization's bronze folder.

    Args:
        org_id: Organization UUID

    Returns:
        List of files with name, path, size, and modified date
    """
    try:
        # Get organization to get the slug
        org = get_organization_by_id(org_id)
        if not org:
            raise HTTPException(
                status_code=404,
                detail=f"Organization '{org_id}' not found"
            )

        org_slug = org.get('slug')
        if not org_slug:
            raise HTTPException(
                status_code=400,
                detail="Organization has no slug configured"
            )

        # List files in bronze folder
        files = list_bronze_files(org_slug)

        return {
            "files": files
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================

@app.post("/api/auth/signup")
async def signup(data: Dict[str, Any]):
    """
    Register a new user. Either creates a new organization or joins existing via invite code.

    Body:
        - email: User email (required)
        - password: User password (required)
        - name: User display name (optional)
        - invite_code: Invite code to join existing org (optional)
        - org_name: Organization name (required if no invite_code)
        - industry: Organization industry (optional, only for new org)

    Returns:
        - user: User object (without password)
        - organization: Organization (created or joined)
        - token: JWT access token
    """
    try:
        from src.api.auth import hash_password, create_access_token, generate_slug, validate_password
        from src.database.analytics_db import (
            get_user_by_email,
            create_user,
            create_organization as db_create_org,
            add_user_to_organization,
            validate_invite_code,
            use_invite_code,
            get_organization_by_id
        )

        # Validate required fields
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        invite_code = data.get('invite_code', '').strip().upper()

        if not email or not password:
            raise HTTPException(
                status_code=400,
                detail="Email and password are required"
            )

        # If no invite code, org_name is required
        org_name = data.get('org_name', '').strip()
        if not invite_code and not org_name:
            raise HTTPException(
                status_code=400,
                detail="Organization name is required (or use an invite code)"
            )

        # Validate invite code if provided
        invite = None
        if invite_code:
            is_valid, error_msg, invite = validate_invite_code(invite_code)
            if not is_valid:
                raise HTTPException(
                    status_code=400,
                    detail=error_msg
                )

        # Validate password
        is_valid, error_msg = validate_password(password)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Check if email already exists
        existing_user = get_user_by_email(email)
        if existing_user:
            raise HTTPException(
                status_code=409,
                detail="An account with this email already exists"
            )

        # Hash password and create user
        password_hash = hash_password(password)
        user = create_user(
            email=email,
            password_hash=password_hash,
            name=data.get('name', '').strip() or None,
            auth_provider='local'
        )

        if not user:
            raise HTTPException(
                status_code=500,
                detail="Failed to create user account"
            )

        # Either join existing org via invite or create new org
        if invite:
            # Join existing organization as member
            org = get_organization_by_id(str(invite['org_id']))
            add_user_to_organization(user['id'], str(invite['org_id']), role=invite['role'])
            use_invite_code(invite_code)
            message = f"Account created successfully. You've joined {org['name']}"
        else:
            # Create new organization
            org_slug = generate_slug(org_name)
            industry = data.get('industry', '').strip() or None

            org = db_create_org(
                name=org_name,
                slug=org_slug,
                industry=industry
            )

            if not org:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to create organization"
                )

            # Add user to organization as owner
            add_user_to_organization(user['id'], org['id'], role='owner')
            message = "Account created successfully"

        # Generate JWT token
        token = create_access_token({
            "user_id": user['id'],
            "email": user['email']
        })

        # Return user (without password), org, and token
        user_response = {k: v for k, v in user.items() if k != 'password_hash'}

        return {
            "message": message,
            "user": user_response,
            "organization": org,
            "token": token
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signup failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/login")
async def login(data: Dict[str, Any]):
    """
    Authenticate user with email and password.

    Body:
        - email: User email
        - password: User password

    Returns:
        - user: User object (without password)
        - organizations: List of user's organizations
        - token: JWT access token
    """
    try:
        from src.api.auth import verify_password, create_access_token
        from src.database.analytics_db import get_user_by_email, get_user_organizations

        email = data.get('email', '').strip().lower()
        password = data.get('password', '')

        if not email or not password:
            raise HTTPException(
                status_code=400,
                detail="Email and password are required"
            )

        # Get user by email
        user = get_user_by_email(email)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Invalid email or password"
            )

        # Verify password
        if not user.get('password_hash') or not verify_password(password, user['password_hash']):
            raise HTTPException(
                status_code=401,
                detail="Invalid email or password"
            )

        # Get user's organizations
        organizations = get_user_organizations(user['id'])

        # Generate JWT token
        token = create_access_token({
            "user_id": user['id'],
            "email": user['email']
        })

        # Return user (without password), orgs, and token
        user_response = {k: v for k, v in user.items() if k != 'password_hash'}

        return {
            "user": user_response,
            "organizations": organizations,
            "token": token
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/auth/me")
async def get_current_user(authorization: Optional[str] = Header(None)):
    """
    Validate token and return current user info.
    Used for session validation on page load.

    Headers:
        - Authorization: Bearer <token>

    Returns:
        - user: User object
        - organizations: List of user's organizations
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import get_user_by_email, get_user_organizations

        # Extract and verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(
                status_code=401,
                detail="Authorization token required"
            )

        payload = verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )

        # Get user from token payload
        email = payload.get('email')
        if not email:
            raise HTTPException(
                status_code=401,
                detail="Invalid token payload"
            )

        user = get_user_by_email(email)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="User not found"
            )

        # Get user's organizations
        organizations = get_user_organizations(user['id'])

        # Return user (without password) and orgs
        user_response = {k: v for k, v in user.items() if k != 'password_hash'}

        return {
            "user": user_response,
            "organizations": organizations
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get current user failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/logout")
async def logout():
    """
    Logout endpoint.
    Primarily for client-side cleanup confirmation.
    Token invalidation happens client-side by removing stored token.
    """
    return {"message": "Logged out successfully"}


@app.post("/api/auth/change-password")
async def change_password(data: Dict[str, Any], authorization: Optional[str] = Header(None)):
    """
    Change password for authenticated user.

    Headers:
        - Authorization: Bearer <token>

    Body:
        - current_password: Current password
        - new_password: New password

    Returns:
        - message: Success message
    """
    try:
        from src.api.auth import (
            extract_token_from_header,
            verify_token,
            verify_password,
            hash_password,
            validate_password
        )
        from src.database.analytics_db import get_user_by_email, update_user_password

        # Extract and verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(
                status_code=401,
                detail="Authorization token required"
            )

        payload = verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )

        # Get current password and new password
        current_password = data.get('current_password', '')
        new_password = data.get('new_password', '')

        if not current_password or not new_password:
            raise HTTPException(
                status_code=400,
                detail="Current password and new password are required"
            )

        # Validate new password
        is_valid, error_msg = validate_password(new_password)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Get user from token
        email = payload.get('email')
        user = get_user_by_email(email)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="User not found"
            )

        # Verify current password
        if not user.get('password_hash') or not verify_password(current_password, user['password_hash']):
            raise HTTPException(
                status_code=401,
                detail="Current password is incorrect"
            )

        # Hash new password and update
        new_hash = hash_password(new_password)
        update_user_password(user['id'], new_hash)

        return {"message": "Password changed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Change password failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ORGANIZATION INVITE ENDPOINTS
# ============================================================================

@app.post("/api/invites")
async def create_invite(data: Dict[str, Any], authorization: Optional[str] = Header(None)):
    """
    Create an invite code for the organization.
    Only owners can create invites.

    Headers:
        - Authorization: Bearer <token>

    Body:
        - org_id: Organization UUID
        - max_uses: Maximum uses (default: 1, null for unlimited)

    Returns:
        - invite: Created invite object with code
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            create_org_invite
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        org_id = data.get('org_id')
        if not org_id:
            raise HTTPException(status_code=400, detail="org_id is required")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can create invites")

        # Create invite
        max_uses = data.get('max_uses', 1)
        invite = create_org_invite(
            org_id=org_id,
            created_by=user['id'],
            role='member',
            max_uses=max_uses
        )

        return {
            "message": "Invite created successfully",
            "invite": invite
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create invite failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/invites")
async def list_invites(org_id: str, authorization: Optional[str] = Header(None)):
    """
    List all invites for an organization.
    Only owners can view invites.

    Query params:
        - org_id: Organization UUID

    Headers:
        - Authorization: Bearer <token>

    Returns:
        - invites: List of invite objects
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            get_org_invites
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can view invites")

        invites = get_org_invites(org_id)

        return {
            "org_id": org_id,
            "invites": invites,
            "count": len(invites)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List invites failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/invites/{invite_id}")
async def revoke_invite(invite_id: str, org_id: str, authorization: Optional[str] = Header(None)):
    """
    Deactivate an invite code.
    Only owners can revoke invites.

    Path params:
        - invite_id: Invite UUID

    Query params:
        - org_id: Organization UUID

    Headers:
        - Authorization: Bearer <token>
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            deactivate_invite
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can revoke invites")

        deactivate_invite(invite_id, org_id)

        return {"message": "Invite deactivated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Revoke invite failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/invites/validate/{code}")
async def validate_invite(code: str):
    """
    Validate an invite code (public endpoint for signup form).

    Path params:
        - code: Invite code

    Returns:
        - valid: Boolean
        - org_name: Organization name (if valid)
        - error: Error message (if invalid)
    """
    try:
        from src.database.analytics_db import validate_invite_code

        is_valid, error_msg, invite = validate_invite_code(code)

        if is_valid:
            return {
                "valid": True,
                "org_name": invite['org_name'],
                "org_id": str(invite['org_id'])
            }
        else:
            return {
                "valid": False,
                "error": error_msg
            }

    except Exception as e:
        logger.error(f"Validate invite failed: {e}")
        return {
            "valid": False,
            "error": "Failed to validate invite code"
        }


# ============================================================================
# ORGANIZATION MEMBER ENDPOINTS
# ============================================================================

@app.get("/api/organizations/{org_id}/members")
async def list_org_members(org_id: str, authorization: Optional[str] = Header(None)):
    """
    List all members of an organization.
    Only owners can view members.

    Path params:
        - org_id: Organization UUID

    Headers:
        - Authorization: Bearer <token>

    Returns:
        - members: List of member objects
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            get_org_members
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can view members")

        members = get_org_members(org_id)

        return {
            "org_id": org_id,
            "members": members,
            "count": len(members)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List members failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/organizations/{org_id}/members/{user_id}")
async def remove_org_member_endpoint(org_id: str, user_id: str, authorization: Optional[str] = Header(None)):
    """
    Remove a member from an organization.
    Only owners can remove members. Owners cannot remove themselves.

    Path params:
        - org_id: Organization UUID
        - user_id: User UUID to remove

    Headers:
        - Authorization: Bearer <token>
    """
    try:
        from src.api.auth import extract_token_from_header, verify_token
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            remove_org_member
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can remove members")

        # Cannot remove yourself
        if user['id'] == user_id:
            raise HTTPException(status_code=400, detail="You cannot remove yourself from the organization")

        # Check target user is a member (and not owner)
        target_role = get_user_role_in_org(user_id, org_id)
        if not target_role:
            raise HTTPException(status_code=404, detail="User is not a member of this organization")

        if target_role == 'owner':
            raise HTTPException(status_code=400, detail="Cannot remove organization owner")

        remove_org_member(org_id, user_id)

        return {"message": "Member removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Remove member failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/organizations/{org_id}/members/{user_id}/reset-password")
async def reset_member_password(org_id: str, user_id: str, authorization: Optional[str] = Header(None)):
    """
    Reset a member's password (owner only).
    Generates a temporary password that the owner can share with the member.

    Path params:
        - org_id: Organization UUID
        - user_id: User UUID whose password to reset

    Headers:
        - Authorization: Bearer <token>

    Returns:
        - temporary_password: The generated password (shown only once)
    """
    try:
        import secrets
        import string
        from src.api.auth import extract_token_from_header, verify_token, hash_password
        from src.database.analytics_db import (
            get_user_by_email,
            get_user_role_in_org,
            update_user_password
        )

        # Verify token
        token = extract_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Authorization token required")

        payload = verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Get requesting user
        user = get_user_by_email(payload.get('email'))
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        # Check user is owner of org
        role = get_user_role_in_org(user['id'], org_id)
        if role != 'owner':
            raise HTTPException(status_code=403, detail="Only organization owners can reset passwords")

        # Cannot reset your own password this way
        if user['id'] == user_id:
            raise HTTPException(status_code=400, detail="Use the change password feature for your own account")

        # Check target user is a member
        target_role = get_user_role_in_org(user_id, org_id)
        if not target_role:
            raise HTTPException(status_code=404, detail="User is not a member of this organization")

        if target_role == 'owner':
            raise HTTPException(status_code=400, detail="Cannot reset password for organization owner")

        # Generate temporary password (12 chars: letters + digits)
        alphabet = string.ascii_letters + string.digits
        temp_password = ''.join(secrets.choice(alphabet) for _ in range(12))

        # Hash and save
        password_hash = hash_password(temp_password)
        update_user_password(user_id, password_hash)

        return {
            "message": "Password reset successfully",
            "temporary_password": temp_password
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reset member password failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# SUPERUSER / ADMIN ENDPOINTS
# ============================================================================

async def require_superuser(authorization: Optional[str] = None):
    """
    Helper to verify user is a superuser.
    Returns user dict if superuser, raises HTTPException otherwise.
    """
    from src.api.auth import extract_token_from_header, verify_token
    from src.database.analytics_db import get_user_by_email, is_user_superuser

    token = extract_token_from_header(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Authorization token required")

    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    user = get_user_by_email(payload.get('email'))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    if not is_user_superuser(str(user['id'])):
        raise HTTPException(status_code=403, detail="Superuser access required")

    return user


@app.get("/api/admin/stats")
async def get_admin_stats(authorization: Optional[str] = Header(None)):
    """
    Get platform-wide statistics for admin dashboard.
    Superuser only.

    Returns:
        - totalUsers: Number of users
        - superuserCount: Number of superusers
        - totalOrganizations: Number of organizations
        - totalDataSources: Number of data sources
        - totalPipelineRuns: Number of pipeline runs
        - recentUsers: Users created in last 7 days
        - recentOrganizations: Orgs created in last 7 days
    """
    try:
        from src.database.analytics_db import get_admin_stats as db_get_admin_stats

        await require_superuser(authorization)

        stats = db_get_admin_stats()
        return stats

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get admin stats failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/users")
async def get_all_users(authorization: Optional[str] = Header(None)):
    """
    Get all users with their organization memberships.
    Superuser only.

    Returns:
        - users: List of all users with org info
        - count: Total count
    """
    try:
        from src.database.analytics_db import get_all_users as db_get_all_users

        await require_superuser(authorization)

        users = db_get_all_users()

        return {
            "users": users,
            "count": len(users)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get all users failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/organizations")
async def get_all_organizations(authorization: Optional[str] = Header(None)):
    """
    Get all organizations with member counts.
    Superuser only.

    Returns:
        - organizations: List of all organizations
        - count: Total count
    """
    try:
        from src.database.analytics_db import get_all_organizations as db_get_all_organizations

        await require_superuser(authorization)

        organizations = db_get_all_organizations()

        return {
            "organizations": organizations,
            "count": len(organizations)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get all organizations failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/organizations")
async def admin_create_organization(data: Dict[str, Any], authorization: Optional[str] = Header(None)):
    """
    Create a new organization (admin only).
    Superuser only.

    Body:
        - name: Organization name
        - slug: URL-friendly identifier
        - industry: Industry type (optional)

    Returns:
        - organization: Created organization
    """
    try:
        from src.database.analytics_db import create_organization as db_create_org

        await require_superuser(authorization)

        name = data.get('name', '').strip()
        slug = data.get('slug', '').strip()
        industry = data.get('industry', '').strip() or None

        if not name or not slug:
            raise HTTPException(status_code=400, detail="Name and slug are required")

        org = db_create_org(name, slug, industry)

        return {
            "message": "Organization created successfully",
            "organization": org
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Admin create organization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/organizations/{org_id}")
async def admin_delete_organization(org_id: str, authorization: Optional[str] = Header(None)):
    """
    Delete an organization and all associated data.
    Superuser only.

    Path params:
        - org_id: Organization UUID

    Returns:
        - result: Deletion details
    """
    try:
        from src.database.analytics_db import admin_delete_organization as db_delete_org

        await require_superuser(authorization)

        result = db_delete_org(org_id)

        if not result['success']:
            raise HTTPException(status_code=404, detail="Organization not found")

        return {
            "message": "Organization deleted successfully",
            "result": result
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Admin delete organization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: str, authorization: Optional[str] = Header(None)):
    """
    Delete a user and all associated data.
    Superuser only. Cannot delete self.

    Path params:
        - user_id: User UUID

    Returns:
        - result: Deletion details
    """
    try:
        from src.database.analytics_db import admin_delete_user as db_delete_user

        current_user = await require_superuser(authorization)

        # Cannot delete self
        if str(current_user['id']) == user_id:
            raise HTTPException(status_code=400, detail="You cannot delete your own account")

        result = db_delete_user(user_id)

        if not result['success']:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "message": "User deleted successfully",
            "result": result
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Admin delete user failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/admin/users/{user_id}/superuser")
async def toggle_superuser_status(
    user_id: str,
    data: Dict[str, Any],
    authorization: Optional[str] = Header(None)
):
    """
    Grant or revoke superuser status for a user.
    Superuser only. Cannot modify self.

    Path params:
        - user_id: User UUID

    Body:
        - is_superuser: Boolean (true to grant, false to revoke)

    Returns:
        - message: Success message
    """
    try:
        from src.database.analytics_db import set_superuser_status

        current_user = await require_superuser(authorization)

        # Cannot modify self
        if str(current_user['id']) == user_id:
            raise HTTPException(status_code=400, detail="You cannot modify your own superuser status")

        is_superuser = data.get('is_superuser')
        if is_superuser is None:
            raise HTTPException(status_code=400, detail="is_superuser field is required")

        success = set_superuser_status(user_id, is_superuser)

        if not success:
            raise HTTPException(status_code=404, detail="User not found")

        action = "granted" if is_superuser else "revoked"
        return {
            "message": f"Superuser status {action} successfully",
            "user_id": user_id,
            "is_superuser": is_superuser
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Toggle superuser status failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
