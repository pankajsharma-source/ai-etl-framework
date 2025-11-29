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
