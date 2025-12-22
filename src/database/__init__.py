"""Database utilities for analytics interface storage."""

from .analytics_db import (
    # Database initialization
    init_database,

    # Organization operations
    get_organization_by_slug,
    get_organization_by_id,
    create_organization,

    # User operations
    get_user_by_email,
    get_user_organizations,
    create_user,
    add_user_to_organization,

    # Data source operations
    get_data_sources,
    save_data_source,
    delete_data_source,
    update_source_last_run,

    # Pipeline history operations
    get_pipeline_history,
    save_pipeline_run,
    clear_pipeline_history,

    # Insights operations
    get_source_insights,
    get_source_insight,
    save_source_insights,
    delete_source_insights,

    # Visualization operations
    get_visualizations,
    get_all_visualizations_for_org,
    save_visualizations,
    delete_visualizations,
    get_sources_with_visualizations,

    # Cleanup
    cleanup_source_data
)

__all__ = [
    # Database initialization
    'init_database',

    # Organization operations
    'get_organization_by_slug',
    'get_organization_by_id',
    'create_organization',

    # User operations
    'get_user_by_email',
    'get_user_organizations',
    'create_user',
    'add_user_to_organization',

    # Data source operations
    'get_data_sources',
    'save_data_source',
    'delete_data_source',
    'update_source_last_run',

    # Pipeline history operations
    'get_pipeline_history',
    'save_pipeline_run',
    'clear_pipeline_history',

    # Insights operations
    'get_source_insights',
    'get_source_insight',
    'save_source_insights',
    'delete_source_insights',

    # Visualization operations
    'get_visualizations',
    'get_all_visualizations_for_org',
    'save_visualizations',
    'delete_visualizations',
    'get_sources_with_visualizations',

    # Cleanup
    'cleanup_source_data',

    # DuckDB Analytics
    'get_duckdb_service',
    'DuckDBService',
    'Filter',
    'AggregationSpec'
]

# DuckDB Analytics Service
from .duckdb_service import (
    get_duckdb_service,
    DuckDBService,
    Filter,
    AggregationSpec
)
