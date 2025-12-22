"""
Path Generator Service for ETL Multi-Tenancy

Auto-generates org-isolated paths and RAG index names based on
organization slug and data source name.

Path Structure:
/app/data/
└── {org_slug}/                          # Per-org isolated data
    ├── bronze/                          # Source files (user uploads)
    │   ├── sales-data.csv
    │   └── customers.csv
    ├── gold/
    │   ├── bi/{ds_slug}/{ds_slug}.{parquet|csv}
    │   └── rag/{ds_slug}/{ds_slug}.csv
    ├── silver/                          # Intermediate processing
    └── quarantine/{ds_slug}_anomalies.csv
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Base data directory (can be overridden via environment variable)
DATA_BASE_PATH = os.getenv('DATA_BASE_PATH', '/app/data')


def slugify(text: str) -> str:
    """
    Convert text to URL/path-safe slug.

    Examples:
        "Sales Data" -> "sales-data"
        "Claims Master 2024" -> "claims-master-2024"
        "Healthcare_Claims" -> "healthcare-claims"
    """
    if not text:
        return "unnamed"

    # Convert to lowercase
    slug = text.lower()

    # Replace underscores and spaces with hyphens
    slug = re.sub(r'[_\s]+', '-', slug)

    # Remove any characters that aren't alphanumeric or hyphens
    slug = re.sub(r'[^a-z0-9-]', '', slug)

    # Remove multiple consecutive hyphens
    slug = re.sub(r'-+', '-', slug)

    # Remove leading/trailing hyphens
    slug = slug.strip('-')

    return slug or "unnamed"


def generate_outputs(
    org_slug: str,
    data_source_name: str,
    etl_output_type: str = 'parquet'
) -> Dict[str, str]:
    """
    Generate org-isolated paths and index names for a data source.

    Args:
        org_slug: Organization slug (e.g., "acme-corp")
        data_source_name: Name of the data source (e.g., "Sales Data")
        etl_output_type: Output format for ETL ("parquet" or "csv")

    Returns:
        Dictionary containing:
            - etl_path: Path for BI-ready output
            - rag_path: Path for RAG-ready CSV output
            - quarantine_path: Path for anomaly/rejected records
            - rag_index: OpenSearch index name for RAG

    Example:
        >>> generate_outputs("acme-corp", "Sales Data", "parquet")
        {
            'etl_path': '/app/data/acme-corp/gold/bi/sales-data/sales-data.parquet',
            'rag_path': '/app/data/acme-corp/gold/rag/sales-data/sales-data.csv',
            'quarantine_path': '/app/data/acme-corp/quarantine/sales-data_anomalies.csv',
            'rag_index': 'sales-data'
        }

    Note:
        The rag_index is just the data source slug. The RAG API will prepend
        the org_id (UUID) when creating the actual OpenSearch index. This allows
        organization names to change without requiring reindexing.
    """
    # Slugify both org and data source names
    org = slugify(org_slug)
    ds_slug = slugify(data_source_name)

    # Validate output type
    if etl_output_type not in ('parquet', 'csv'):
        etl_output_type = 'parquet'

    # Path structure: org at top level for easy cleanup
    # /app/data/{org}/gold/bi/... , /app/data/{org}/quarantine/...
    return {
        # File paths (org isolation at TOP level)
        'etl_path': f'{DATA_BASE_PATH}/{org}/gold/bi/{ds_slug}/{ds_slug}.{etl_output_type}',
        'rag_path': f'{DATA_BASE_PATH}/{org}/gold/rag/{ds_slug}/{ds_slug}.csv',
        'quarantine_path': f'{DATA_BASE_PATH}/{org}/quarantine/{ds_slug}_anomalies.csv',
        # RAG index name (just ds_slug - RAG API adds org_id prefix)
        'rag_index': ds_slug
    }


def ensure_directories_exist(paths: Dict[str, str]) -> None:
    """
    Create parent directories for all paths if they don't exist.

    Args:
        paths: Dictionary of paths from generate_outputs()
    """
    for key, path in paths.items():
        if key == 'rag_index':
            continue  # Skip index name, it's not a file path

        parent_dir = Path(path).parent
        if not parent_dir.exists():
            logger.info(f"Creating directory: {parent_dir}")
            parent_dir.mkdir(parents=True, exist_ok=True)


def get_org_data_root(org_slug: str) -> str:
    """
    Get the root data directory for an organization.

    Args:
        org_slug: Organization slug

    Returns:
        Path to org's data root directory
    """
    org = slugify(org_slug)
    return f'{DATA_BASE_PATH}/{org}'


def get_source_paths(org_slug: str, data_source_name: str) -> Dict[str, str]:
    """
    Get all file/directory paths for a data source (for cleanup).

    Args:
        org_slug: Organization slug (e.g., "acme-corp")
        data_source_name: Name of the data source (e.g., "Sales Data")

    Returns:
        Dictionary containing paths to delete:
            - etl_dir: Directory containing ETL output
            - rag_dir: Directory containing RAG output
            - quarantine_file: Quarantine anomalies file
    """
    org = slugify(org_slug)
    ds_slug = slugify(data_source_name)

    return {
        'etl_dir': f'{DATA_BASE_PATH}/{org}/gold/bi/{ds_slug}',
        'rag_dir': f'{DATA_BASE_PATH}/{org}/gold/rag/{ds_slug}',
        'quarantine_file': f'{DATA_BASE_PATH}/{org}/quarantine/{ds_slug}_anomalies.csv'
    }


def delete_source_files(org_slug: str, data_source_name: str) -> Dict[str, bool]:
    """
    Delete all files and directories for a data source.

    Args:
        org_slug: Organization slug
        data_source_name: Name of the data source

    Returns:
        Dictionary with deletion status for each path
    """
    import shutil
    import glob

    paths = get_source_paths(org_slug, data_source_name)
    results = {}

    # Delete ETL output directory
    etl_dir = Path(paths['etl_dir'])
    if etl_dir.exists():
        try:
            shutil.rmtree(etl_dir)
            logger.info(f"Deleted ETL directory: {etl_dir}")
            results['etl_dir'] = True
        except Exception as e:
            logger.error(f"Failed to delete ETL directory {etl_dir}: {e}")
            results['etl_dir'] = False
    else:
        results['etl_dir'] = True  # Already doesn't exist

    # Delete RAG output directory
    rag_dir = Path(paths['rag_dir'])
    if rag_dir.exists():
        try:
            shutil.rmtree(rag_dir)
            logger.info(f"Deleted RAG directory: {rag_dir}")
            results['rag_dir'] = True
        except Exception as e:
            logger.error(f"Failed to delete RAG directory {rag_dir}: {e}")
            results['rag_dir'] = False
    else:
        results['rag_dir'] = True  # Already doesn't exist

    # Delete quarantine file
    quarantine_file = Path(paths['quarantine_file'])
    if quarantine_file.exists():
        try:
            quarantine_file.unlink()
            logger.info(f"Deleted quarantine file: {quarantine_file}")
            results['quarantine'] = True
        except Exception as e:
            logger.error(f"Failed to delete quarantine file {quarantine_file}: {e}")
            results['quarantine'] = False
    else:
        results['quarantine'] = True  # Already doesn't exist

    return results


def get_bronze_path(org_slug: str) -> str:
    """
    Get the bronze folder path for an organization.

    Args:
        org_slug: Organization slug (e.g., "acme-corp")

    Returns:
        Path to org's bronze folder (e.g., "/app/data/acme-corp/bronze")
    """
    org = slugify(org_slug)
    return f'{DATA_BASE_PATH}/{org}/bronze'


def ensure_bronze_folder(org_slug: str) -> str:
    """
    Create bronze folder for an organization if it doesn't exist.

    Args:
        org_slug: Organization slug

    Returns:
        Path to the bronze folder
    """
    bronze_path = get_bronze_path(org_slug)
    path = Path(bronze_path)
    if not path.exists():
        logger.info(f"Creating bronze folder: {bronze_path}")
        path.mkdir(parents=True, exist_ok=True)
    return bronze_path


def list_bronze_files(org_slug: str) -> list:
    """
    List all files in an organization's bronze folder.

    Args:
        org_slug: Organization slug

    Returns:
        List of file info dictionaries with name, path, size, modified
    """
    import os
    from datetime import datetime

    bronze_path = get_bronze_path(org_slug)
    path = Path(bronze_path)

    if not path.exists():
        return []

    files = []
    for file_path in path.iterdir():
        if file_path.is_file():
            stat = file_path.stat()
            files.append({
                'name': file_path.name,
                'path': str(file_path),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

    # Sort by modified date, newest first
    files.sort(key=lambda x: x['modified'], reverse=True)
    return files
