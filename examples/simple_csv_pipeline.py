"""
Simple CSV to SQLite pipeline example

This example demonstrates the minimal ETL pipeline:
- Extract: Read data from CSV file
- Transform: Remove records with null values
- Load: Write to SQLite database
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover
from src.common.logging import setup_logging


def main():
    """Run the simple CSV pipeline"""

    # Setup logging
    logger = setup_logging(level="INFO")
    logger.info("=" * 60)
    logger.info("Starting Simple CSV to SQLite Pipeline")
    logger.info("=" * 60)

    # Define file paths
    csv_file = Path(__file__).parent.parent / "data" / "sample.csv"
    output_db = Path(__file__).parent.parent / "output" / "sample.db"

    # Create output directory
    output_db.parent.mkdir(parents=True, exist_ok=True)

    # Check if CSV exists
    if not csv_file.exists():
        logger.error(f"Sample CSV not found: {csv_file}")
        logger.info("Please create data/sample.csv first")
        return

    logger.info(f"Input: {csv_file}")
    logger.info(f"Output: {output_db}")
    logger.info("")

    try:
        # Create and run pipeline
        pipeline = (Pipeline()
            .extract(CSVSource(str(csv_file)))
            .transform(NullRemover(strategy="drop"))
            .load(SQLiteLoader(str(output_db), table="clean_data"))
            .run())

        # Print results
        logger.info("")
        logger.info("=" * 60)
        logger.info("Pipeline Results")
        logger.info("=" * 60)

        result = pipeline.result
        logger.info(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Records Extracted: {result.records_extracted}")
        logger.info(f"Records Transformed: {result.records_transformed}")
        logger.info(f"Records Loaded: {result.records_loaded}")
        logger.info(f"Records Filtered: {result.records_extracted - result.records_transformed}")
        logger.info(f"Total Duration: {result.duration_seconds:.2f}s")
        logger.info(f"  - Extract: {result.extract_duration:.2f}s")
        logger.info(f"  - Transform: {result.transform_duration:.2f}s")
        logger.info(f"  - Load: {result.load_duration:.2f}s")

        # Show transformer stats
        stats = pipeline.get_stats()
        if 'transformer_0' in stats:
            t_stats = stats['transformer_0']
            logger.info(f"")
            logger.info(f"Transformer Stats:")
            logger.info(f"  - Processed: {t_stats['records_processed']}")
            logger.info(f"  - Filtered: {t_stats['records_filtered']}")
            logger.info(f"  - Errors: {t_stats['errors']}")

        logger.info("=" * 60)
        logger.info(f"")
        logger.info(f"Data saved to: {output_db}")
        logger.info(f"")
        logger.info(f"To view the data, run:")
        logger.info(f"  sqlite3 {output_db} 'SELECT * FROM clean_data;'")
        logger.info(f"")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
