"""
Advanced JSON to SQLite pipeline with quality scoring

This example demonstrates:
- Extract: Read data from JSON file
- Transform: Remove nulls + Score data quality
- Load: Write to SQLite database
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.validators.quality_scorer import QualityScorer
from src.common.logging import setup_logging


def main():
    """Run the advanced JSON pipeline with quality scoring"""

    # Setup logging
    logger = setup_logging(level="INFO")
    logger.info("=" * 60)
    logger.info("Advanced JSON to SQLite Pipeline with Quality Scoring")
    logger.info("=" * 60)

    # Define file paths
    json_file = Path(__file__).parent.parent / "data" / "sample.json"
    output_db = Path(__file__).parent.parent / "output" / "quality_scored.db"

    # Create output directory
    output_db.parent.mkdir(parents=True, exist_ok=True)

    # Check if JSON exists
    if not json_file.exists():
        logger.error(f"Sample JSON not found: {json_file}")
        logger.info("Please create data/sample.json first")
        return

    logger.info(f"Input: {json_file}")
    logger.info(f"Output: {output_db}")
    logger.info("")

    try:
        # Create and run pipeline with multiple transformers
        pipeline = (Pipeline()
            .extract(JSONSource(str(json_file)))
            .transform(NullRemover(strategy="remove_fields"))  # Keep records, remove null fields
            .transform(QualityScorer(
                min_score=0.5,  # Minimum quality threshold
                filter_low_quality=False,  # Don't filter, just score
                weights={
                    'completeness': 0.5,
                    'validity': 0.3,
                    'consistency': 0.2
                }
            ))
            .load(SQLiteLoader(str(output_db), table="quality_data"))
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
        logger.info(f"Total Duration: {result.duration_seconds:.2f}s")
        logger.info(f"  - Extract: {result.extract_duration:.2f}s")
        logger.info(f"  - Transform: {result.transform_duration:.2f}s")
        logger.info(f"  - Load: {result.load_duration:.2f}s")

        # Show transformer stats
        stats = pipeline.get_stats()
        logger.info("")
        logger.info("Transformer Stats:")

        if 'transformer_0' in stats:
            t0 = stats['transformer_0']
            logger.info(f"  NullRemover:")
            logger.info(f"    - Processed: {t0['records_processed']}")
            logger.info(f"    - Modified: {t0['records_modified']}")
            logger.info(f"    - Filtered: {t0['records_filtered']}")

        if 'transformer_1' in stats:
            t1 = stats['transformer_1']
            logger.info(f"  QualityScorer:")
            logger.info(f"    - Processed: {t1['records_processed']}")
            logger.info(f"    - Modified: {t1['records_modified']}")

        logger.info("=" * 60)
        logger.info("")
        logger.info(f"Data saved to: {output_db}")
        logger.info("")
        logger.info("To view the data with quality scores:")
        logger.info(f"  sqlite3 {output_db} 'SELECT * FROM quality_data;'")
        logger.info("")
        logger.info("Note: Quality scores are stored in record metadata.")
        logger.info("Use a JSON viewer to inspect metadata in the database.")
        logger.info("")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
