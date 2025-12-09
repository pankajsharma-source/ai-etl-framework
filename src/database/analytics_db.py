"""
Analytics Interface Database Operations
Role-based storage for data sources and pipeline history
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
# Data Sources Operations
# ============================================================================

def get_data_sources(role: str) -> List[Dict[str, Any]]:
    """
    Get all data sources for a specific role.

    Args:
        role: User role (e.g., 'ceo', 'cto', 'analyst')

    Returns:
        List of data source dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, role, name, icon, description, file_path, config,
                           created_at, updated_at
                    FROM analytics_data_sources
                    WHERE role = %s
                    ORDER BY created_at ASC
                """, (role,))

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

                    sources.append(source)

                return sources
    except Exception as e:
        logger.error(f"Failed to get data sources for role {role}: {e}")
        raise


def save_data_source(role: str, source: Dict[str, Any]) -> bool:
    """
    Save or update a data source configuration.

    Args:
        role: User role
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
                        'role', 'created_at', 'updated_at'}
        for k, v in source.items():
            if k not in excluded_keys and k not in config_keys:
                config[k] = v

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO analytics_data_sources
                        (id, role, name, icon, description, file_path, config)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id, role)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        icon = EXCLUDED.icon,
                        description = EXCLUDED.description,
                        file_path = EXCLUDED.file_path,
                        config = EXCLUDED.config,
                        updated_at = CURRENT_TIMESTAMP
                """, (source_id, role, name, icon, description, file_path, Json(config)))

        logger.info(f"Saved data source {source_id} for role {role}")
        return True
    except Exception as e:
        logger.error(f"Failed to save data source: {e}")
        raise


def delete_data_source(role: str, source_id: str) -> bool:
    """
    Delete a data source.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        True if successful
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_data_sources
                    WHERE id = %s AND role = %s
                """, (source_id, role))

                if cursor.rowcount == 0:
                    logger.warning(f"Data source {source_id} not found for role {role}")
                    return False

        logger.info(f"Deleted data source {source_id} for role {role}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete data source: {e}")
        raise


def update_source_last_run(role: str, source_id: str, last_run: Dict[str, Any]) -> bool:
    """
    Update a data source's lastRun field in its config.
    This persists card metadata separately from pipeline history.

    Args:
        role: User role
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
                    WHERE id = %s AND role = %s
                """, (Json({'lastRun': last_run}), source_id, role))

                if cursor.rowcount == 0:
                    logger.warning(f"Data source {source_id} not found for role {role}")
                    return False

        logger.info(f"Updated lastRun for data source {source_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to update source lastRun: {e}")
        raise


# ============================================================================
# Pipeline History Operations
# ============================================================================

def get_pipeline_history(role: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get pipeline execution history for a specific role.

    Args:
        role: User role
        limit: Maximum number of records to return

    Returns:
        List of pipeline run dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, role, source_id, source_name, pipeline_id, status,
                           records_processed, duration_seconds, error_message,
                           run_type, timestamp, created_at
                    FROM analytics_pipeline_history
                    WHERE role = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (role, limit))

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
        logger.error(f"Failed to get pipeline history for role {role}: {e}")
        raise


def save_pipeline_run(role: str, run_data: Dict[str, Any]) -> int:
    """
    Save a pipeline run to history.

    Args:
        role: User role
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
                        (role, source_id, source_name, pipeline_id, status,
                         records_processed, duration_seconds, run_type,
                         error_message, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (role, source_id, source_name, pipeline_id, status,
                      records, duration_secs, run_type, error_message, timestamp))

                run_id = cursor.fetchone()[0]

        logger.info(f"Saved pipeline run {run_id} for role {role}")
        return run_id
    except Exception as e:
        logger.error(f"Failed to save pipeline run: {e}")
        raise


def clear_pipeline_history(role: str) -> int:
    """
    Clear all pipeline history for a specific role.

    Args:
        role: User role

    Returns:
        Number of records deleted
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_pipeline_history
                    WHERE role = %s
                """, (role,))

                deleted_count = cursor.rowcount

        logger.info(f"Cleared {deleted_count} history records for role {role}")
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to clear pipeline history: {e}")
        raise


# ============================================================================
# Source Insights Operations
# ============================================================================

def get_source_insights(role: str) -> List[Dict[str, Any]]:
    """
    Get all AI-generated insights for a specific role.

    Args:
        role: User role

    Returns:
        List of insight dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, role, source_name, source_icon,
                           data_summary, insights, records_analyzed,
                           generated_from, generated_at
                    FROM analytics_source_insights
                    WHERE role = %s
                    ORDER BY generated_at DESC
                """, (role,))

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
        logger.error(f"Failed to get source insights for role {role}: {e}")
        raise


def get_source_insight(role: str, source_id: str) -> Optional[Dict[str, Any]]:
    """
    Get AI-generated insight for a specific source.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        Insight dictionary or None if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, role, source_name, source_icon,
                           data_summary, insights, records_analyzed,
                           generated_from, generated_at
                    FROM analytics_source_insights
                    WHERE role = %s AND source_id = %s
                """, (role, source_id))

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
    role: str,
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
        role: User role
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
                        (source_id, role, source_name, source_icon, data_summary,
                         insights, records_analyzed, generated_from, generated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (source_id, role)
                    DO UPDATE SET
                        source_name = EXCLUDED.source_name,
                        source_icon = EXCLUDED.source_icon,
                        data_summary = EXCLUDED.data_summary,
                        insights = EXCLUDED.insights,
                        records_analyzed = EXCLUDED.records_analyzed,
                        generated_from = EXCLUDED.generated_from,
                        generated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (source_id, role, source_name, source_icon, data_summary,
                      Json(insights), records_analyzed, generated_from))

                insight_id = cursor.fetchone()[0]

        logger.info(f"Saved insights for source {source_id} (role: {role}, from: {generated_from})")
        return insight_id
    except Exception as e:
        logger.error(f"Failed to save source insights: {e}")
        raise


def delete_source_insights(role: str, source_id: str) -> bool:
    """
    Delete insights for a specific source.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        True if deleted, False if not found
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_source_insights
                    WHERE role = %s AND source_id = %s
                """, (role, source_id))

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

def get_visualizations(role: str, source_id: str) -> List[Dict[str, Any]]:
    """
    Get all generated visualizations for a specific data source.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        List of visualization dictionaries
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, source_id, role, chart_type, chart_title,
                           x_column, y_column, chart_config, generated_at
                    FROM analytics_visualizations
                    WHERE role = %s AND source_id = %s
                    ORDER BY id ASC
                """, (role, source_id))

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


def get_all_visualizations_for_role(role: str) -> List[Dict[str, Any]]:
    """
    Get all visualizations for a role, grouped by source.

    Args:
        role: User role

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
                    LEFT JOIN analytics_data_sources s ON v.source_id = s.id AND v.role = s.role
                    WHERE v.role = %s
                    ORDER BY v.source_id, v.id
                """, (role,))

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
        logger.error(f"Failed to get all visualizations for role {role}: {e}")
        raise


def save_visualizations(role: str, source_id: str, charts: List[Dict[str, Any]]) -> int:
    """
    Save multiple visualizations for a data source.
    Replaces any existing visualizations for this source.

    Args:
        role: User role
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
                    WHERE role = %s AND source_id = %s
                """, (role, source_id))

                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} existing visualizations for source {source_id}")

                # Insert new visualizations
                saved_count = 0
                for chart in charts:
                    try:
                        cursor.execute("""
                            INSERT INTO analytics_visualizations
                                (source_id, role, chart_type, chart_title, x_column, y_column, chart_config)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            source_id,
                            role,
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

        logger.info(f"Saved {saved_count} visualizations for source {source_id} (role: {role})")
        return saved_count
    except Exception as e:
        logger.error(f"Failed to save visualizations: {e}")
        raise


def delete_visualizations(role: str, source_id: str) -> int:
    """
    Delete all visualizations for a specific source.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        Number of visualizations deleted
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM analytics_visualizations
                    WHERE role = %s AND source_id = %s
                """, (role, source_id))

                deleted_count = cursor.rowcount

        logger.info(f"Deleted {deleted_count} visualizations for source {source_id}")
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to delete visualizations: {e}")
        raise


def get_sources_with_visualizations(role: str) -> List[str]:
    """
    Get list of source IDs that have visualizations stored.

    Args:
        role: User role

    Returns:
        List of source_id strings that have at least one visualization
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT source_id
                    FROM analytics_visualizations
                    WHERE role = %s
                    ORDER BY source_id
                """, (role,))

                rows = cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Failed to get sources with visualizations: {e}")
        return []


def cleanup_source_data(role: str, source_id: str) -> Dict[str, int]:
    """
    Delete all insights and visualizations for a data source.
    Does NOT delete the data source configuration itself.

    Args:
        role: User role
        source_id: Data source ID

    Returns:
        Dict with counts: {'insights_deleted': int, 'visualizations_deleted': int}
    """
    insights_deleted = 0
    viz_deleted = 0

    try:
        # Delete insights
        insights_deleted = delete_source_insights(role, source_id)
        logger.info(f"Cleanup: deleted {insights_deleted} insights for source {source_id}")
    except Exception as e:
        logger.error(f"Cleanup: failed to delete insights for source {source_id}: {e}")

    try:
        # Delete visualizations
        viz_deleted = delete_visualizations(role, source_id)
        logger.info(f"Cleanup: deleted {viz_deleted} visualizations for source {source_id}")
    except Exception as e:
        logger.error(f"Cleanup: failed to delete visualizations for source {source_id}: {e}")

    return {
        'insights_deleted': insights_deleted,
        'visualizations_deleted': viz_deleted
    }


# ============================================================================
# Migration from localStorage
# ============================================================================

def migrate_from_localstorage(role: str, data_sources: List[Dict], history: List[Dict]) -> Dict[str, Any]:
    """
    Migrate data from localStorage to PostgreSQL.

    Args:
        role: User role
        data_sources: List of data sources from localStorage
        history: List of pipeline runs from localStorage

    Returns:
        Migration result summary
    """
    result = {
        'sources_migrated': 0,
        'history_migrated': 0,
        'errors': []
    }

    try:
        # Migrate data sources
        for source in data_sources:
            try:
                save_data_source(role, source)
                result['sources_migrated'] += 1
            except Exception as e:
                result['errors'].append(f"Source {source.get('id')}: {str(e)}")

        # Migrate history
        for run in history:
            try:
                save_pipeline_run(role, run)
                result['history_migrated'] += 1
            except Exception as e:
                result['errors'].append(f"History entry: {str(e)}")

        logger.info(f"Migration complete for role {role}: "
                   f"{result['sources_migrated']} sources, "
                   f"{result['history_migrated']} history entries")

        return result
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
