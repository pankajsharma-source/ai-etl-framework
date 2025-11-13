"""
Test Staged Pipeline with FileStorage

Demonstrates running E-T-L stages independently
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.staged_pipeline import StagedPipeline
from src.storage.file_storage import FileStorage
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.cleaners.null_remover import NullRemover
from src.common.logging import setup_logging


def main():
    """Test staged pipeline execution"""

    # Setup logging
    logger = setup_logging(level="INFO")
    logger.info("=" * 60)
    logger.info("Testing Staged Pipeline")
    logger.info("=" * 60)

    # Define paths
    csv_file = Path(__file__).parent.parent / "data" / "sample.csv"
    output_db = Path(__file__).parent.parent / "output" / "staged_test.db"
    storage_path = Path(__file__).parent.parent / ".state" / "intermediate"

    # Create output directory
    output_db.parent.mkdir(parents=True, exist_ok=True)

    # Check if CSV exists
    if not csv_file.exists():
        logger.error(f"Sample CSV not found: {csv_file}")
        return

    logger.info(f"Input: {csv_file}")
    logger.info(f"Output: {output_db}")
    logger.info(f"Intermediate Storage: {storage_path}")
    logger.info("")

    try:
        # Initialize storage
        storage = FileStorage(str(storage_path))
        pipeline = StagedPipeline(
            pipeline_id="test_staged_001",
            storage=storage
        )

        # STAGE 1: Extract
        logger.info("▶️  Stage 1: EXTRACT")
        logger.info("-" * 60)
        extract_result = pipeline.run_extract_only(
            CSVSource(str(csv_file))
        )

        if not extract_result.success:
            logger.error(f"Extract failed: {extract_result.error_message}")
            return

        logger.info(f"✅ Extract complete:")
        logger.info(f"   Records: {extract_result.record_count}")
        logger.info(f"   Duration: {extract_result.duration_seconds:.2f}s")
        logger.info(f"   Storage: {extract_result.storage_key}")
        logger.info("")

        # Pause to simulate time between stages
        import time
        logger.info("⏸️  Pausing between stages (simulating user inspection)...")
        time.sleep(1)
        logger.info("")

        # STAGE 2: Transform
        logger.info("▶️  Stage 2: TRANSFORM")
        logger.info("-" * 60)
        transform_result = pipeline.run_transform_only([
            NullRemover(strategy="drop")
        ])

        if not transform_result.success:
            logger.error(f"Transform failed: {transform_result.error_message}")
            return

        logger.info(f"✅ Transform complete:")
        logger.info(f"   Records: {transform_result.record_count}")
        logger.info(f"   Duration: {transform_result.duration_seconds:.2f}s")
        logger.info(f"   Storage: {transform_result.storage_key}")
        logger.info("")

        # Pause again
        logger.info("⏸️  Pausing before load...")
        time.sleep(1)
        logger.info("")

        # STAGE 3: Load
        logger.info("▶️  Stage 3: LOAD")
        logger.info("-" * 60)
        load_result = pipeline.run_load_only(
            SQLiteLoader(str(output_db), table="staged_data")
        )

        if not load_result.success:
            logger.error(f"Load failed: {load_result.error_message}")
            return

        logger.info(f"✅ Load complete:")
        logger.info(f"   Records: {load_result.record_count}")
        logger.info(f"   Duration: {load_result.duration_seconds:.2f}s")
        logger.info("")

        # Summary
        logger.info("=" * 60)
        logger.info("Staged Pipeline Completed Successfully!")
        logger.info("=" * 60)

        total_duration = (
            extract_result.duration_seconds +
            transform_result.duration_seconds +
            load_result.duration_seconds
        )

        logger.info(f"Total Duration: {total_duration:.2f}s")
        logger.info(f"  Extract:   {extract_result.duration_seconds:.2f}s")
        logger.info(f"  Transform: {transform_result.duration_seconds:.2f}s")
        logger.info(f"  Load:      {load_result.duration_seconds:.2f}s")
        logger.info("")
        logger.info(f"Pipeline Status: {pipeline.get_status()}")
        logger.info("")
        logger.info(f"✅ Data saved to: {output_db}")
        logger.info("")

        # Cleanup intermediate storage
        logger.info("🧹 Cleaning up intermediate storage...")
        pipeline.cleanup()
        logger.info("✅ Cleanup complete")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
