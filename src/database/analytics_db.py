"""
Analytics Interface Database Operations
Organization-based storage for data sources and pipeline history
"""

import os
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database connection configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://etl_user:etl_dev_pass@postgres:5432/etl')


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def init_database():
    """
    Initialize database schema.
    Creates tables if they don't exist.
    """
    try:
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(schema_sql)

        logger.info("Analytics database schema initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


# ============================================================================
# Organization Operations
# ============================================================================

def get_organization_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """
    Get an organization by its slug.

    Args:
        slug: URL-friendly organization identifier

    Returns:
        Organization dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, name, slug, industry, created_at, updated_at
                    FROM organizations
                    WHERE slug = %s
                """, (slug,))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to get organization by slug {slug}: {e}")
        raise


def get_organization_by_id(org_id: str) -> Optional[Dict[str, Any]]:
    """
    Get an organization by its ID.

    Args:
        org_id: Organization UUID

    Returns:
        Organization dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, name, slug, industry, created_at, updated_at
                    FROM organizations
                    WHERE id = %s
                """, (org_id,))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to get organization by id {org_id}: {e}")
        raise


def create_organization(name: str, slug: str, industry: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new organization.

    Args:
        name: Organization display name
        slug: URL-friendly identifier
        industry: Industry type (healthcare, finance, etc.)

    Returns:
        Created organization dictionary
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO organizations (name, slug, industry)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, slug, industry, created_at, updated_at
                """, (name, slug, industry))

                row = cursor.fetchone()
                logger.info(f"Created organization: {name} (slug: {slug})")
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to create organization: {e}")
        raise


def get_organization_by_id(org_id: str) -> Optional[Dict[str, Any]]:
    """
    Get an organization by its ID.

    Args:
        org_id: Organization UUID

    Returns:
        Organization dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, name, slug, industry, created_at, updated_at
                    FROM organizations
                    WHERE id = %s
                """, (org_id,))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to get organization by id: {e}")
        raise


def update_organization_industry(org_id: str, industry: str) -> Optional[Dict[str, Any]]:
    """
    Update an organization's industry.

    Args:
        org_id: Organization UUID
        industry: New industry value

    Returns:
        Updated organization dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    UPDATE organizations
                    SET industry = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id, name, slug, industry, created_at, updated_at
                """, (industry, org_id))

                row = cursor.fetchone()
                if row:
                    logger.info(f"Updated industry for org {org_id} to {industry}")
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to update organization industry: {e}")
        raise


# ============================================================================
# Data Sources Operations
# ============================================================================

def get_data_sources(org_id: str) -> List[Dict[str, Any]]:
    """
    Get all data sources for a specific organization.

    Args:
        org_id: Organization UUID

    Returns:
        List of data source dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, org_id, name, icon, description, file_path, config,
                           created_at, updated_at
                    FROM analytics_data_sources
                    WHERE org_id = %s
                    ORDER BY created_at ASC
                """, (org_id,))

                rows = cursor.fetchall()

                # Convert to list of dicts and merge config
                sources = []
                for row in rows:
                    source = dict(row)
                    # Merge config JSONB into the main dict
                    config = source.pop('config', {})
                    source.update(config)

                    # Convert snake_case to camelCase for JavaScript frontend
                    if 'file_path' in source:
                        source['filePath'] = source.pop('file_path')
                    if 'created_at' in source:
                        created = source.pop('created_at')
                        source['createdAt'] = created.isoformat() if created else None
                    if 'updated_at' in source:
                        updated = source.pop('updated_at')
                        source['updatedAt'] = updated.isoformat() if updated else None
                    # Convert org_id to string for JSON
                    if 'org_id' in source:
                        source['orgId'] = str(source.pop('org_id'))

                    sources.append(source)

                return sources
    except Exception as e:
        logger.error(f"Failed to get data sources for org {org_id}: {e}")
        raise


def save_data_source(org_id: str, source: Dict[str, Any]) -> bool:
    """
    Save or update a data source configuration.

    Args:
        org_id: Organization UUID
        source: Data source dictionary

    Returns:
        True if successful
    """
    try:
        # Extract top-level fields
        source_id = source.get('id')
        name = source.get('name')
        icon = source.get('icon')
        description = source.get('description')
        file_path = source.get('filePath') or source.get('file_path')

        # Everything else goes into config JSONB
        config_keys = ['transforms', 'destinations', 'post_processing', 'validation']
        config = {k: v for k, v in source.items() if k in config_keys}

        # Also preserve any other fields not explicitly handled
        excluded_keys = {'id', 'name', 'icon', 'description', 'filePath', 'file_path',
                        'org_id', 'orgId', 'created_at', 'updated_at'}
        for k, v in source.items():
            if k not in excluded_keys and k not in config_keys:
                config[k] = v

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO analytics_data_sources
                        (id, org_id, name, icon, description, file_path, config)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id, org_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        icon = EXCLUDED.icon,
                        description = EXCLUDED.description,
                        file_path = EXCLUDED.file_path,
                        config = EXCLUDED.config,
                        updated_at = CURRENT_TIMESTAMP
                """, (source_id, org_id, name, icon, description, file_path, Json(config)))

        logger.info(f"Saved data source {source_id} for org {org_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to save data source: {e}")
        raise


def delete_data_source(org_id: str, source_id: str) -> bool:
    """
    Delete a data source configuration only (legacy function).
    For complete cleanup, use complete_delete_data_source().

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_data_sources
                    WHERE id = %s AND org_id = %s
                """, (source_id, org_id))

                if cursor.rowcount == 0:
                    logger.warning(f"Data source {source_id} not found for org {org_id}")
                    return False

        logger.info(f"Deleted data source {source_id} for org {org_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete data source: {e}")
        raise


def complete_delete_data_source(org_id: str, source_id: str) -> Dict[str, Any]:
    """
    Completely delete a data source and ALL associated data.

    Deletes:
        - Data source configuration
        - Pipeline history
        - AI-generated insights
        - Visualizations

    Note: File cleanup and index deletion are handled by the API layer.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        Dict with deletion counts for each resource type
    """
    result = {
        'config_deleted': False,
        'history_deleted': 0,
        'insights_deleted': 0,
        'visualizations_deleted': 0
    }

    # 1. Delete insights
    try:
        result['insights_deleted'] = delete_source_insights(org_id, source_id)
        logger.info(f"Deleted {result['insights_deleted']} insights for source {source_id}")
    except Exception as e:
        logger.error(f"Failed to delete insights for source {source_id}: {e}")

    # 2. Delete visualizations
    try:
        result['visualizations_deleted'] = delete_visualizations(org_id, source_id)
        logger.info(f"Deleted {result['visualizations_deleted']} visualizations for source {source_id}")
    except Exception as e:
        logger.error(f"Failed to delete visualizations for source {source_id}: {e}")

    # 3. Delete pipeline history
    try:
        result['history_deleted'] = delete_source_history(org_id, source_id)
        logger.info(f"Deleted {result['history_deleted']} history records for source {source_id}")
    except Exception as e:
        logger.error(f"Failed to delete history for source {source_id}: {e}")

    # 4. Delete data source configuration (last, so we have source info for other deletions)
    try:
        result['config_deleted'] = delete_data_source(org_id, source_id)
        if result['config_deleted']:
            logger.info(f"Deleted data source config for {source_id}")
    except Exception as e:
        logger.error(f"Failed to delete data source config for {source_id}: {e}")

    return result


def update_source_last_run(org_id: str, source_id: str, last_run: Dict[str, Any]) -> bool:
    """
    Update a data source's lastRun field in its config.
    This persists card metadata separately from pipeline history.

    Args:
        org_id: Organization UUID
        source_id: Data source ID
        last_run: Last run data (timestamp, type, records, duration)

    Returns:
        True if successful, False if source not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Update the config JSONB to include/update lastRun
                cursor.execute("""
                    UPDATE analytics_data_sources
                    SET config = config || %s::jsonb,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND org_id = %s
                """, (Json({'lastRun': last_run}), source_id, org_id))

                if cursor.rowcount == 0:
                    logger.warning(f"Data source {source_id} not found for org {org_id}")
                    return False

        logger.info(f"Updated lastRun for data source {source_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to update source lastRun: {e}")
        raise


# ============================================================================
# Pipeline History Operations
# ============================================================================

def get_pipeline_history(org_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get pipeline execution history for a specific organization.

    Args:
        org_id: Organization UUID
        limit: Maximum number of records to return

    Returns:
        List of pipeline run dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, org_id, source_id, source_name, pipeline_id, status,
                           records_processed, duration_seconds, error_message,
                           run_type, timestamp, created_at
                    FROM analytics_pipeline_history
                    WHERE org_id = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (org_id, limit))

                rows = cursor.fetchall()

                # Convert to frontend format
                history = []
                for row in rows:
                    history.append({
                        'id': row['id'],
                        'sourceId': row['source_id'],
                        'sourceName': row['source_name'],
                        'pipelineId': str(row['pipeline_id']) if row['pipeline_id'] else None,
                        'status': row['status'],
                        'records': row['records_processed'],
                        'duration': row['duration_seconds'] / 60.0,  # Convert to minutes
                        'timestamp': row['timestamp'].isoformat(),
                        'type': row['run_type'],
                        'error': row['error_message']
                    })

                return history
    except Exception as e:
        logger.error(f"Failed to get pipeline history for org {org_id}: {e}")
        raise


def save_pipeline_run(org_id: str, run_data: Dict[str, Any]) -> int:
    """
    Save a pipeline run to history.

    Args:
        org_id: Organization UUID
        run_data: Pipeline run data dictionary

    Returns:
        ID of the created record
    """
    try:
        source_id = run_data.get('sourceId')
        source_name = run_data.get('sourceName')
        pipeline_id = run_data.get('pipelineId')
        status = run_data.get('status', 'completed')
        records = run_data.get('records', 0)
        duration_mins = run_data.get('duration', 0)
        duration_secs = duration_mins * 60.0
        run_type = run_data.get('type')
        error_message = run_data.get('error')

        # Parse timestamp
        timestamp_str = run_data.get('timestamp')
        if timestamp_str:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            timestamp = datetime.now()

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO analytics_pipeline_history
                        (org_id, source_id, source_name, pipeline_id, status,
                         records_processed, duration_seconds, run_type,
                         error_message, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (org_id, source_id, source_name, pipeline_id, status,
                      records, duration_secs, run_type, error_message, timestamp))

                run_id = cursor.fetchone()[0]

        logger.info(f"Saved pipeline run {run_id} for org {org_id}")
        return run_id
    except Exception as e:
        logger.error(f"Failed to save pipeline run: {e}")
        raise


def clear_pipeline_history(org_id: str) -> int:
    """
    Clear all pipeline history for a specific organization.

    Args:
        org_id: Organization UUID

    Returns:
        Number of records deleted
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_pipeline_history
                    WHERE org_id = %s
                """, (org_id,))

                deleted_count = cursor.rowcount

        logger.info(f"Cleared {deleted_count} history records for org {org_id}")
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to clear pipeline history: {e}")
        raise


def delete_source_history(org_id: str, source_id: str) -> int:
    """
    Delete pipeline history for a specific data source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        Number of records deleted
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_pipeline_history
                    WHERE org_id = %s AND source_id = %s
                """, (org_id, source_id))

                deleted_count = cursor.rowcount

        logger.info(f"Deleted {deleted_count} history records for source {source_id}")
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to delete source history: {e}")
        raise


# ============================================================================
# Source Insights Operations
# ============================================================================

def get_source_insights(org_id: str) -> List[Dict[str, Any]]:
    """
    Get all AI-generated insights for a specific organization.

    Args:
        org_id: Organization UUID

    Returns:
        List of insight dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, org_id, source_name, source_icon,
                           data_summary, insights, records_analyzed,
                           generated_from, generated_at
                    FROM analytics_source_insights
                    WHERE org_id = %s
                    ORDER BY generated_at DESC
                """, (org_id,))

                rows = cursor.fetchall()

                # Convert to frontend format
                insights = []
                for row in rows:
                    insights.append({
                        'id': row['id'],
                        'sourceId': row['source_id'],
                        'sourceName': row['source_name'],
                        'sourceIcon': row['source_icon'],
                        'dataSummary': row['data_summary'],
                        'insights': row['insights'] or [],
                        'recordsAnalyzed': row['records_analyzed'],
                        'generatedFrom': row['generated_from'],
                        'generatedAt': row['generated_at'].isoformat() if row['generated_at'] else None
                    })

                return insights
    except Exception as e:
        logger.error(f"Failed to get source insights for org {org_id}: {e}")
        raise


def get_source_insight(org_id: str, source_id: str) -> Optional[Dict[str, Any]]:
    """
    Get AI-generated insight for a specific source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        Insight dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, org_id, source_name, source_icon,
                           data_summary, insights, records_analyzed,
                           generated_from, generated_at
                    FROM analytics_source_insights
                    WHERE org_id = %s AND source_id = %s
                """, (org_id, source_id))

                row = cursor.fetchone()

                if not row:
                    return None

                return {
                    'id': row['id'],
                    'sourceId': row['source_id'],
                    'sourceName': row['source_name'],
                    'sourceIcon': row['source_icon'],
                    'dataSummary': row['data_summary'],
                    'insights': row['insights'] or [],
                    'recordsAnalyzed': row['records_analyzed'],
                    'generatedFrom': row['generated_from'],
                    'generatedAt': row['generated_at'].isoformat() if row['generated_at'] else None
                }
    except Exception as e:
        logger.error(f"Failed to get source insight for {source_id}: {e}")
        raise


def save_source_insights(
    org_id: str,
    source_id: str,
    source_name: str,
    source_icon: str,
    data_summary: str,
    insights: List[str],
    records_analyzed: int,
    generated_from: str
) -> int:
    """
    Save or update AI-generated insights for a data source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID
        source_name: Data source name
        source_icon: Data source icon emoji
        data_summary: 2-3 sentence summary of the data
        insights: List of insight strings (3-5 items)
        records_analyzed: Number of records analyzed
        generated_from: 'etl' or 'rag'

    Returns:
        ID of the created/updated record
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO analytics_source_insights
                        (source_id, org_id, source_name, source_icon, data_summary,
                         insights, records_analyzed, generated_from, generated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (source_id, org_id)
                    DO UPDATE SET
                        source_name = EXCLUDED.source_name,
                        source_icon = EXCLUDED.source_icon,
                        data_summary = EXCLUDED.data_summary,
                        insights = EXCLUDED.insights,
                        records_analyzed = EXCLUDED.records_analyzed,
                        generated_from = EXCLUDED.generated_from,
                        generated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (source_id, org_id, source_name, source_icon, data_summary,
                      Json(insights), records_analyzed, generated_from))

                insight_id = cursor.fetchone()[0]

        logger.info(f"Saved insights for source {source_id} (org: {org_id}, from: {generated_from})")
        return insight_id
    except Exception as e:
        logger.error(f"Failed to save source insights: {e}")
        raise


def delete_source_insights(org_id: str, source_id: str) -> bool:
    """
    Delete insights for a specific source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        True if deleted, False if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_source_insights
                    WHERE org_id = %s AND source_id = %s
                """, (org_id, source_id))

                if cursor.rowcount == 0:
                    return False

        logger.info(f"Deleted insights for source {source_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete source insights: {e}")
        raise


# ============================================================================
# Visualization Operations
# ============================================================================

def get_visualizations(org_id: str, source_id: str) -> List[Dict[str, Any]]:
    """
    Get all generated visualizations for a specific data source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        List of visualization dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, org_id, chart_type, chart_title,
                           x_column, y_column, chart_config, generated_at
                    FROM analytics_visualizations
                    WHERE org_id = %s AND source_id = %s
                    ORDER BY id ASC
                """, (org_id, source_id))

                rows = cursor.fetchall()

                # Convert to frontend format
                visualizations = []
                for row in rows:
                    visualizations.append({
                        'id': row['id'],
                        'sourceId': row['source_id'],
                        'chartType': row['chart_type'],
                        'chartTitle': row['chart_title'],
                        'xColumn': row['x_column'],
                        'yColumn': row['y_column'],
                        'chartConfig': row['chart_config'],
                        'generatedAt': row['generated_at'].isoformat() if row['generated_at'] else None
                    })

                return visualizations
    except Exception as e:
        logger.error(f"Failed to get visualizations for source {source_id}: {e}")
        raise


def get_all_visualizations_for_org(org_id: str) -> List[Dict[str, Any]]:
    """
    Get all visualizations for an organization, grouped by source.

    Args:
        org_id: Organization UUID

    Returns:
        List of visualization dictionaries with source info
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT v.id, v.source_id, v.chart_type, v.chart_title,
                           v.x_column, v.y_column, v.chart_config, v.generated_at,
                           s.name as source_name, s.icon as source_icon
                    FROM analytics_visualizations v
                    LEFT JOIN analytics_data_sources s ON v.source_id = s.id AND v.org_id = s.org_id
                    WHERE v.org_id = %s
                    ORDER BY v.source_id, v.id
                """, (org_id,))

                rows = cursor.fetchall()

                visualizations = []
                for row in rows:
                    visualizations.append({
                        'id': row['id'],
                        'sourceId': row['source_id'],
                        'sourceName': row['source_name'],
                        'sourceIcon': row['source_icon'],
                        'chartType': row['chart_type'],
                        'chartTitle': row['chart_title'],
                        'xColumn': row['x_column'],
                        'yColumn': row['y_column'],
                        'chartConfig': row['chart_config'],
                        'generatedAt': row['generated_at'].isoformat() if row['generated_at'] else None
                    })

                return visualizations
    except Exception as e:
        logger.error(f"Failed to get all visualizations for org {org_id}: {e}")
        raise


def save_visualizations(org_id: str, source_id: str, charts: List[Dict[str, Any]]) -> int:
    """
    Save multiple visualizations for a data source.
    Replaces any existing visualizations for this source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID
        charts: List of chart configurations from visualization_generator

    Returns:
        Number of charts saved
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # First, delete existing visualizations for this source
                cursor.execute("""
                    DELETE FROM analytics_visualizations
                    WHERE org_id = %s AND source_id = %s
                """, (org_id, source_id))

                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} existing visualizations for source {source_id}")

                # Insert new visualizations
                saved_count = 0
                for chart in charts:
                    try:
                        cursor.execute("""
                            INSERT INTO analytics_visualizations
                                (source_id, org_id, chart_type, chart_title, x_column, y_column, chart_config)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            source_id,
                            org_id,
                            chart.get('chart_type'),
                            chart.get('title'),
                            chart.get('x_column'),
                            chart.get('y_column'),
                            Json(chart.get('chart_config'))
                        ))
                        saved_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to save chart '{chart.get('title')}': {e}")
                        continue

        logger.info(f"Saved {saved_count} visualizations for source {source_id} (org: {org_id})")
        return saved_count
    except Exception as e:
        logger.error(f"Failed to save visualizations: {e}")
        raise


def delete_visualizations(org_id: str, source_id: str) -> int:
    """
    Delete all visualizations for a specific source.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        Number of visualizations deleted
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_visualizations
                    WHERE org_id = %s AND source_id = %s
                """, (org_id, source_id))

                deleted_count = cursor.rowcount

        logger.info(f"Deleted {deleted_count} visualizations for source {source_id}")
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to delete visualizations: {e}")
        raise


def get_sources_with_visualizations(org_id: str) -> List[str]:
    """
    Get list of source IDs that have visualizations stored.

    Args:
        org_id: Organization UUID

    Returns:
        List of source_id strings that have at least one visualization
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT source_id
                    FROM analytics_visualizations
                    WHERE org_id = %s
                    ORDER BY source_id
                """, (org_id,))

                rows = cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Failed to get sources with visualizations: {e}")
        return []


def cleanup_source_data(org_id: str, source_id: str) -> Dict[str, int]:
    """
    Delete all insights and visualizations for a data source.
    Does NOT delete the data source configuration itself.

    Args:
        org_id: Organization UUID
        source_id: Data source ID

    Returns:
        Dict with counts: {'insights_deleted': int, 'visualizations_deleted': int}
    """
    insights_deleted = 0
    viz_deleted = 0

    try:
        # Delete insights
        insights_deleted = delete_source_insights(org_id, source_id)
        logger.info(f"Cleanup: deleted {insights_deleted} insights for source {source_id}")
    except Exception as e:
        logger.error(f"Cleanup: failed to delete insights for source {source_id}: {e}")

    try:
        # Delete visualizations
        viz_deleted = delete_visualizations(org_id, source_id)
        logger.info(f"Cleanup: deleted {viz_deleted} visualizations for source {source_id}")
    except Exception as e:
        logger.error(f"Cleanup: failed to delete visualizations for source {source_id}: {e}")

    return {
        'insights_deleted': insights_deleted,
        'visualizations_deleted': viz_deleted
    }


# ============================================================================
# User Operations
# ============================================================================

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Get a user by email address.

    Args:
        email: User email address

    Returns:
        User dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, email, name, password_hash, auth_provider, created_at, updated_at
                    FROM users
                    WHERE email = %s
                """, (email,))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to get user by email: {e}")
        raise


def get_user_organizations(user_id: str) -> List[Dict[str, Any]]:
    """
    Get all organizations a user belongs to.

    Args:
        user_id: User UUID

    Returns:
        List of organization dictionaries with membership role
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT o.id, o.name, o.slug, o.industry, m.role as membership_role
                    FROM organizations o
                    JOIN org_memberships m ON o.id = m.org_id
                    WHERE m.user_id = %s
                    ORDER BY o.name
                """, (user_id,))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to get user organizations: {e}")
        raise


def create_user(email: str, name: Optional[str] = None,
                password_hash: Optional[str] = None,
                auth_provider: str = 'local',
                auth_provider_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new user.

    Args:
        email: User email address
        name: User display name
        password_hash: Hashed password (for local auth)
        auth_provider: Authentication provider ('local', 'google', 'github')
        auth_provider_id: External provider user ID

    Returns:
        Created user dictionary
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO users (email, name, password_hash, auth_provider, auth_provider_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id, email, name, auth_provider, created_at, updated_at
                """, (email, name, password_hash, auth_provider, auth_provider_id))

                row = cursor.fetchone()
                logger.info(f"Created user: {email}")
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to create user: {e}")
        raise


def add_user_to_organization(user_id: str, org_id: str, role: str = 'member') -> bool:
    """
    Add a user to an organization.

    Args:
        user_id: User UUID
        org_id: Organization UUID
        role: Membership role ('owner', 'admin', 'member')

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO org_memberships (user_id, org_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, org_id) DO UPDATE SET role = EXCLUDED.role
                """, (user_id, org_id, role))

        logger.info(f"Added user {user_id} to org {org_id} as {role}")
        return True
    except Exception as e:
        logger.error(f"Failed to add user to organization: {e}")
        raise


def update_user_password(user_id: str, password_hash: str) -> bool:
    """
    Update a user's password hash.

    Args:
        user_id: User UUID
        password_hash: New hashed password

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE users
                    SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (password_hash, user_id))

        logger.info(f"Updated password for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to update user password: {e}")
        raise


# ============================================================================
# Organization Invite Operations
# ============================================================================

def generate_invite_code() -> str:
    """Generate a random 8-character invite code."""
    import secrets
    import string
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(8))


def create_org_invite(org_id: str, created_by: str, role: str = 'member',
                      max_uses: int = 1, expires_at: Optional[str] = None) -> Dict[str, Any]:
    """
    Create an invite code for an organization.

    Args:
        org_id: Organization UUID
        created_by: User UUID who created the invite
        role: Role to assign when invite is used
        max_uses: Maximum number of times code can be used (None = unlimited)
        expires_at: Optional expiration timestamp

    Returns:
        Created invite dictionary
    """
    try:
        code = generate_invite_code()

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO org_invites (org_id, code, created_by, role, max_uses, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id, org_id, code, role, max_uses, use_count, expires_at, is_active, created_at
                """, (org_id, code, created_by, role, max_uses, expires_at))

                row = cursor.fetchone()
                logger.info(f"Created invite code {code} for org {org_id}")
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to create org invite: {e}")
        raise


def get_org_invites(org_id: str) -> List[Dict[str, Any]]:
    """
    Get all invites for an organization.

    Args:
        org_id: Organization UUID

    Returns:
        List of invite dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT i.id, i.org_id, i.code, i.role, i.max_uses, i.use_count,
                           i.expires_at, i.is_active, i.created_at,
                           u.name as created_by_name, u.email as created_by_email
                    FROM org_invites i
                    JOIN users u ON i.created_by = u.id
                    WHERE i.org_id = %s
                    ORDER BY i.created_at DESC
                """, (org_id,))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to get org invites: {e}")
        raise


def get_invite_by_code(code: str) -> Optional[Dict[str, Any]]:
    """
    Get an invite by its code.

    Args:
        code: Invite code

    Returns:
        Invite dictionary with org info, or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT i.id, i.org_id, i.code, i.role, i.max_uses, i.use_count,
                           i.expires_at, i.is_active, i.created_at,
                           o.name as org_name, o.slug as org_slug
                    FROM org_invites i
                    JOIN organizations o ON i.org_id = o.id
                    WHERE i.code = %s
                """, (code.upper(),))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Failed to get invite by code: {e}")
        raise


def validate_invite_code(code: str) -> tuple:
    """
    Validate an invite code is usable.

    Args:
        code: Invite code

    Returns:
        (is_valid, error_message, invite_dict)
    """
    invite = get_invite_by_code(code)

    if not invite:
        return False, "Invalid invite code", None

    if not invite['is_active']:
        return False, "This invite has been deactivated", None

    if invite['max_uses'] is not None and invite['use_count'] >= invite['max_uses']:
        return False, "This invite has reached its maximum uses", None

    if invite['expires_at'] is not None:
        from datetime import datetime
        if datetime.utcnow() > invite['expires_at']:
            return False, "This invite has expired", None

    return True, "", invite


def use_invite_code(code: str) -> bool:
    """
    Increment the use count of an invite code.

    Args:
        code: Invite code

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE org_invites
                    SET use_count = use_count + 1
                    WHERE code = %s
                """, (code.upper(),))

        logger.info(f"Used invite code {code}")
        return True
    except Exception as e:
        logger.error(f"Failed to use invite code: {e}")
        raise


def deactivate_invite(invite_id: str, org_id: str) -> bool:
    """
    Deactivate an invite code.

    Args:
        invite_id: Invite UUID
        org_id: Organization UUID (for security check)

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE org_invites
                    SET is_active = false
                    WHERE id = %s AND org_id = %s
                """, (invite_id, org_id))

        logger.info(f"Deactivated invite {invite_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to deactivate invite: {e}")
        raise


def get_user_role_in_org(user_id: str, org_id: str) -> Optional[str]:
    """
    Get a user's role in an organization.

    Args:
        user_id: User UUID
        org_id: Organization UUID

    Returns:
        Role string ('owner', 'member') or None if not a member
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT role FROM org_memberships
                    WHERE user_id = %s AND org_id = %s
                """, (user_id, org_id))

                row = cursor.fetchone()
                if row:
                    return row['role']
                return None
    except Exception as e:
        logger.error(f"Failed to get user role: {e}")
        raise


def get_org_members(org_id: str) -> List[Dict[str, Any]]:
    """
    Get all members of an organization.

    Args:
        org_id: Organization UUID

    Returns:
        List of member dictionaries with user info and role
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT u.id, u.email, u.name, m.role, m.created_at as joined_at
                    FROM users u
                    JOIN org_memberships m ON u.id = m.user_id
                    WHERE m.org_id = %s
                    ORDER BY
                        CASE m.role WHEN 'owner' THEN 0 ELSE 1 END,
                        m.created_at ASC
                """, (org_id,))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to get org members: {e}")
        raise


def remove_org_member(org_id: str, user_id: str) -> bool:
    """
    Remove a member from an organization.

    Args:
        org_id: Organization UUID
        user_id: User UUID to remove

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM org_memberships
                    WHERE org_id = %s AND user_id = %s
                """, (org_id, user_id))

        logger.info(f"Removed user {user_id} from org {org_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to remove org member: {e}")
        raise
