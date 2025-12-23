"""
Shared pipeline core functions

These functions are used by Pipeline to reduce code duplication
and ensure consistent behavior.
"""
from typing import List, Optional
from datetime import datetime

from src.adapters.base import DestinationAdapter
from src.transformers.base_transformer import Transformer
from src.common.models import Record, Schema
from src.common.logging import get_logger


def apply_transformers(
    records: List[Record],
    transformers: List[Transformer],
    logger=None
) -> List[Record]:
    """
    Apply transformers with setup/cleanup lifecycle

    Args:
        records: List of records to transform
        transformers: List of transformers to apply in order
        logger: Optional logger for debug output

    Returns:
        Transformed records
    """
    if logger is None:
        logger = get_logger("PipelineCore")

    if not transformers:
        logger.info("No transformers to apply")
        return records

    # Call setup on all transformers first
    for transformer in transformers:
        if hasattr(transformer, 'setup'):
            transformer.setup()

    # Apply each transformer in sequence
    for transformer in transformers:
        transformer_name = transformer.__class__.__name__
        logger.info(f"Applying transformer: {transformer_name}")

        records = transformer.transform_batch(records)

        logger.info(f"After {transformer_name}: {len(records)} records remain")

    # Call cleanup on all transformers
    for transformer in transformers:
        if hasattr(transformer, 'cleanup'):
            transformer.cleanup()

    return records


def resolve_schema(records: List[Record], source_schema: Schema) -> Schema:
    """
    Determine which schema to use for loading

    If transformers modified the schema (e.g., aggregation), use the
    transformed schema. Otherwise, use the source schema.

    Args:
        records: Transformed records (may contain updated schema)
        source_schema: Original schema from source

    Returns:
        Schema to use for loading
    """
    # Check if records have a transformed schema
    if records and records[0].schema:
        return records[0].schema

    return source_schema


def load_to_destinations(
    records: List[Record],
    schema: Schema,
    destinations: List[DestinationAdapter],
    logger=None
) -> int:
    """
    Load records to one or more destinations with transaction support

    Args:
        records: Records to load
        schema: Schema for the data
        destinations: List of destination adapters
        logger: Optional logger for debug output

    Returns:
        Number of records loaded (from first destination)
    """
    if logger is None:
        logger = get_logger("PipelineCore")

    if not destinations:
        logger.warning("No destinations configured")
        return 0

    total_written = 0

    for i, destination in enumerate(destinations):
        dest_name = destination.__class__.__name__
        logger.info(f"Loading to destination {i+1}/{len(destinations)}: {dest_name}")

        with destination:
            # Create schema at destination
            destination.create_schema(schema)

            # Write records with transaction
            destination.begin_transaction()
            try:
                written = destination.write(iter(records))
                destination.commit()

                # Track count from first destination
                if i == 0:
                    total_written = written

                logger.info(f"Loaded {written} records to {dest_name}")

            except Exception as e:
                destination.rollback()
                logger.error(f"Failed to load to {dest_name}: {e}")
                raise

    return total_written
