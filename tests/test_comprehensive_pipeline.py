"""
Comprehensive ETL Pipeline Test Script

This script tests the ETL framework with ALL available transformers:
1. SchemaInferrer - Analyze data patterns and infer schema
2. NullRemover - Clean null/missing values
3. Deduplicator - Remove duplicate records
4. AnomalyDetector - Detect statistical outliers
5. QualityScorer - Score data quality
6. MetadataToColumnsTransformer - Export metadata flags as CSV columns
7. AnomalySplitter - Separate anomalies to quarantine
8. DashboardAggregator - Generate Parquet files for dashboards
9. ColumnRemover - Remove all metadata columns from clean data

Input: CSV file (via --input argument or from archive)
Output:
  - Clean CSV (processed/) - pure source data only (no metadata)
  - Anomalies CSV (quarantine/) - with all metadata
  - Dashboard Parquet files (analytics/dashboards/)

USAGE:
  # Process new file
  python test_comprehensive_pipeline.py --input /path/to/file.csv

  # Reprocess latest archived file
  python test_comprehensive_pipeline.py

📖 For complete documentation, see: docs/MEDICAL_CLAIMS_PIPELINE.md
"""

import sys
import time
import argparse
import shutil
import json
from datetime import datetime
from pathlib import Path

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.csv_loader import CSVLoader
from src.transformers.analyzers.schema_inferrer import SchemaInferrer
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.cleaners.column_remover import ColumnRemover
from src.transformers.enrichers.deduplicator import Deduplicator
from src.transformers.analyzers.anomaly_detector import AnomalyDetector
from src.transformers.validators.quality_scorer import QualityScorer
from src.transformers.enrichers.metadata_to_columns import MetadataToColumnsTransformer
from src.transformers.routing.anomaly_splitter import AnomalySplitter
from src.transformers.exporters.dashboard_aggregator import DashboardAggregator

# ============================================================================
# CONFIGURATION
# ============================================================================

# Default paths
DEFAULT_BASE_DIR = "../data/etl"
SOURCE_TYPE = "medical-claims"

# CSV Configuration
CSV_ENCODING = "utf-8"       # Try "latin-1" or "cp1252" if you get encoding errors
CSV_DELIMITER = ","          # Use ";" for semicolon-separated, "\t" for tab-separated

# Transformer Configuration
NULL_STRATEGY = "remove_fields"  # Options: "drop", "drop_all", "remove_fields", "fill"
DEDUP_MODE = "exact"             # Options: "exact", "fuzzy"
ANOMALY_METHOD = "combined"      # Options: "statistical", "iqr", "isolation_forest", "combined"
QUALITY_MIN_SCORE = 0.0          # Set higher (e.g., 0.7) to filter low-quality records

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "="*80)
    print(f"  {text}")
    print("="*80 + "\n")


def print_section(title):
    """Print a section header"""
    print(f"\n--- {title} ---")


def setup_directories(base_dir: Path):
    """
    Ensure directory structure exists

    Args:
        base_dir: Base ETL directory
    """
    required_dirs = [
        base_dir / "source" / SOURCE_TYPE,
        base_dir / "processed" / SOURCE_TYPE,
        base_dir / "quarantine" / SOURCE_TYPE,
        base_dir / "analytics" / "dashboards" / SOURCE_TYPE,
    ]

    for dir_path in required_dirs:
        dir_path.mkdir(parents=True, exist_ok=True)

    print(f"✅ Directory structure verified: {base_dir}")


def archive_source_file(input_file: Path, archive_base: Path) -> Path:
    """
    Copy input file to source archive with timestamp

    Args:
        input_file: Path to input file
        archive_base: Base archive directory (e.g., source/medical-claims/)

    Returns:
        Path to archived file
    """
    # Create timestamped directory
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_dir = archive_base / timestamp
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Copy file
    archived_file = archive_dir / input_file.name
    shutil.copy2(input_file, archived_file)

    # Write metadata
    metadata = {
        "original_path": str(input_file.absolute()),
        "archived_at": datetime.now().isoformat(),
        "file_size": archived_file.stat().st_size,
        "file_name": input_file.name
    }

    (archive_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    print(f"✅ Archived: {input_file.name} → {archived_file}")
    return archived_file


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Medical Claims ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process new file
  python test_comprehensive_pipeline.py --input ~/Downloads/medical-claims.csv

  # Reprocess latest archived file
  python test_comprehensive_pipeline.py

  # Custom output directory
  python test_comprehensive_pipeline.py --input data.csv --base-dir /data/etl
        """
    )

    parser.add_argument(
        "--input",
        type=str,
        help="Path to input CSV file (will be archived)"
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=DEFAULT_BASE_DIR,
        help=f"Base output directory (default: {DEFAULT_BASE_DIR})"
    )

    return parser.parse_args()


# ============================================================================
# MAIN PIPELINE EXECUTION
# ============================================================================

def main():
    """Run the comprehensive ETL pipeline"""

    # Parse arguments
    args = parse_args()
    base_dir = Path(args.base_dir)

    print_banner("Comprehensive ETL Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Setup: Ensure directories exist
    print_section("Setup")
    setup_directories(base_dir)

    # Determine source file
    if args.input:
        # User provided file - archive it
        input_file = Path(args.input)
        if not input_file.exists():
            print(f"❌ Error: File not found: {input_file}")
            return 1

        print(f"Input file: {input_file}")

        # Archive to source/
        archived_file = archive_source_file(
            input_file,
            base_dir / "source" / SOURCE_TYPE
        )
        source_file = str(archived_file)
    else:
        # Use latest from source archive
        source_dir = base_dir / "source" / SOURCE_TYPE
        subdirs = sorted([d for d in source_dir.glob("*") if d.is_dir()], reverse=True)

        if not subdirs:
            print(f"\n❌ Error: No files in {source_dir}")
            print("   Provide --input /path/to/file.csv")
            return 1

        # Find the CSV file in the latest subdirectory
        latest_dir = subdirs[0]
        csv_files = list(latest_dir.glob("*.csv"))

        if not csv_files:
            print(f"\n❌ Error: No CSV files in {latest_dir}")
            return 1

        source_file = str(csv_files[0])
        print(f"📂 Using archived file: {source_file}")

    # Define output paths
    processed_file = base_dir / "processed" / SOURCE_TYPE / "clean.csv"
    quarantine_file = base_dir / "quarantine" / SOURCE_TYPE / "anomalies.csv"
    analytics_dir = base_dir / "analytics" / "dashboards" / SOURCE_TYPE

    start_time = time.time()

    try:
        # ====================================================================
        # BUILD PIPELINE
        # ====================================================================

        print_section("Building Pipeline")

        # Create transformers
        schema_inferrer = SchemaInferrer(
            detect_patterns=True,
            infer_constraints=True,
            suggest_enums=True,
            enum_threshold=10
        )
        print("✓ SchemaInferrer configured")

        null_remover = NullRemover(strategy=NULL_STRATEGY)
        print(f"✓ NullRemover configured (strategy: {NULL_STRATEGY})")

        deduplicator = Deduplicator(
            match_mode=DEDUP_MODE,
            merge_strategy="keep_first"
        )
        print(f"✓ Deduplicator configured (mode: {DEDUP_MODE})")

        anomaly_detector = AnomalyDetector(
            method=ANOMALY_METHOD,
            filter_anomalies=False,  # Flag anomalies, don't remove them
            threshold=3.0
        )
        print(f"✓ AnomalyDetector configured (method: {ANOMALY_METHOD})")

        quality_scorer = QualityScorer(
            min_score=QUALITY_MIN_SCORE,
            filter_low_quality=False  # Score all records
        )
        print(f"✓ QualityScorer configured (min_score: {QUALITY_MIN_SCORE})")

        metadata_to_columns = MetadataToColumnsTransformer(
            include_anomaly_info=True,
            include_quality_info=True,
            include_quality_breakdown=True,
            prefix="_meta_"
        )
        print("✓ MetadataToColumnsTransformer configured (adds metadata as columns)")

        anomaly_splitter = AnomalySplitter(
            output_path=str(quarantine_file)
        )
        print(f"✓ AnomalySplitter configured (quarantine: {quarantine_file.name})")

        dashboard_aggregator = DashboardAggregator(
            output_dir=str(analytics_dir)
        )
        print(f"✓ DashboardAggregator configured (output: {analytics_dir.name}/)")

        column_remover = ColumnRemover(
            prefix="_meta_"  # Removes ALL metadata columns automatically
        )
        print("✓ ColumnRemover configured (removes all _meta_* columns from clean data)")

        # ====================================================================
        # RUN PIPELINE
        # ====================================================================

        print_section("Running Pipeline")

        pipeline = (Pipeline()
            .extract(CSVSource(
                file_path=source_file,
                encoding=CSV_ENCODING,
                delimiter=CSV_DELIMITER
            ))
            .transform(schema_inferrer)
            .transform(null_remover)
            .transform(deduplicator)
            .transform(anomaly_detector)
            .transform(quality_scorer)
            .transform(metadata_to_columns)  # Add metadata as CSV columns
            .transform(anomaly_splitter)     # Split anomalies to quarantine
            .transform(dashboard_aggregator) # Generate Parquet files
            .transform(column_remover)       # Remove anomaly columns from clean records
            .load(CSVLoader(
                file_path=str(processed_file),
                mode="overwrite"
            ))
            .run())

        # ====================================================================
        # PRINT RESULTS
        # ====================================================================

        end_time = time.time()
        duration = end_time - start_time

        print_banner("Pipeline Results")

        # Overall statistics
        print_section("Overall Statistics")
        print(f"Records Extracted:   {pipeline.result.records_extracted:,}")
        print(f"Records Transformed: {pipeline.result.records_transformed:,}")
        print(f"Records Loaded:      {pipeline.result.records_loaded:,}")
        print(f"Records Failed:      {pipeline.result.records_failed:,}")

        # Stage timings
        print_section("Stage Timings")
        print(f"Extract:   {pipeline.result.extract_duration:.2f} seconds")
        print(f"Transform: {pipeline.result.transform_duration:.2f} seconds")
        print(f"Load:      {pipeline.result.load_duration:.2f} seconds")
        print(f"Total:     {duration:.2f} seconds")

        # Transformer-specific stats
        print_section("Transformation Details")

        # Schema Inferrer
        inferred_schema = schema_inferrer.get_inferred_schema()
        if inferred_schema:
            print(f"\n📋 Schema Inference:")
            print(f"   Fields detected: {len(inferred_schema.fields)}")
            print(f"   Sample size:     {inferred_schema.sample_size}")

            # Show pattern detection
            patterns_detected = sum(
                1 for field in inferred_schema.fields
                if field.pattern is not None
            )
            if patterns_detected > 0:
                print(f"   Patterns found:  {patterns_detected}")
                for field in inferred_schema.fields:
                    if field.pattern:
                        print(f"      - {field.name}: {field.pattern} (confidence: {field.confidence:.2%})")

        # Null Remover
        null_stats = null_remover.get_stats()
        print(f"\n🧹 Null Removal:")
        print(f"   Records processed: {null_stats['records_processed']}")
        print(f"   Records modified:  {null_stats['records_modified']}")
        print(f"   Records filtered:  {null_stats['records_filtered']}")

        # Deduplicator
        dedup_stats = deduplicator.get_stats()
        duplicates_removed = dedup_stats['records_filtered']
        print(f"\n🔄 Deduplication:")
        print(f"   Records processed: {dedup_stats['records_processed']}")
        print(f"   Duplicates found:  {duplicates_removed}")
        print(f"   Unique records:    {dedup_stats['records_processed']}")

        # Anomaly Detector
        anomaly_stats = anomaly_detector.get_stats()
        print(f"\n⚠️  Anomaly Detection:")
        print(f"   Records processed: {anomaly_stats['records_processed']}")
        print(f"   Records modified:  {anomaly_stats['records_modified']} (flagged as anomalies)")
        print(f"   Records filtered:  {anomaly_stats['records_filtered']}")

        # Quality Scorer
        quality_stats = quality_scorer.get_stats()
        avg_quality = pipeline.result.avg_quality_score
        print(f"\n⭐ Quality Scoring:")
        print(f"   Records processed: {quality_stats['records_processed']}")
        print(f"   Records modified:  {quality_stats['records_modified']} (scored)")
        print(f"   Average quality:   {avg_quality:.2%}" if avg_quality else "   Average quality:   N/A")

        # Metadata to Columns
        metadata_stats = metadata_to_columns.get_stats()
        print(f"\n📊 Metadata Export:")
        print(f"   Records processed: {metadata_stats['records_processed']}")
        print(f"   Added columns:")
        print(f"      - _meta_is_anomaly")
        print(f"      - _meta_anomaly_method")
        print(f"      - _meta_anomaly_reasons")
        print(f"      - _meta_quality_score")
        print(f"      - _meta_completeness")
        print(f"      - _meta_validity")
        print(f"      - _meta_consistency")

        # Anomaly Splitter
        splitter_stats = anomaly_splitter.get_stats()
        print(f"\n🔀 Anomaly Splitting:")
        print(f"   Clean records:    {splitter_stats['clean_count']}")
        print(f"   Anomalies:        {splitter_stats['anomaly_count']}")
        print(f"   Quarantine file:  {quarantine_file.name}")

        # Dashboard Aggregator
        aggregator_stats = dashboard_aggregator.get_stats()
        print(f"\n📈 Dashboard Aggregations:")
        print(f"   Records aggregated: {aggregator_stats['records_aggregated']}")
        print(f"   Files generated:")
        for file in aggregator_stats['files_generated']:
            print(f"      - {file}")

        # Column Remover
        remover_stats = column_remover.get_stats()
        print(f"\n🗑️  Column Removal:")
        print(f"   Records processed: {remover_stats['records_processed']}")
        print(f"   Records modified:  {remover_stats['records_modified']}")
        print(f"   Columns removed:")
        for col in remover_stats['columns_removed']:
            print(f"      - {col}")

        # Output files
        print_section("Output Files")
        print(f"✅ Clean records:  {processed_file}")
        print(f"   (pure source data - all _meta_* columns removed)")
        print(f"✅ Anomalies:      {quarantine_file}")
        print(f"   (source data + all metadata for investigation)")
        print(f"✅ Dashboards:     {analytics_dir}/")
        print(f"   • claims_summary.parquet")
        print(f"   • claims_by_provider.parquet")
        print(f"   • claims_by_diagnosis.parquet")
        print(f"   • claims_by_date.parquet")

        # Performance metrics
        if pipeline.result.records_loaded > 0:
            throughput = pipeline.result.records_loaded / duration
            print_section("Performance")
            print(f"Throughput: {throughput:.0f} records/second")

        print_banner("✅ Pipeline Completed Successfully!")

        # Next steps
        print("\nNext Steps:")
        print(f"1. View clean data:       python scripts/view_dashboard_data.py")
        print(f"2. Review anomalies:      python scripts/review_anomalies.py")
        print(f"3. Check clean records:   head {processed_file}")
        print(f"4. RAG indexing:          Use {processed_file} as input")
        print(f"5. Dashboard analytics:   Read Parquet files from {analytics_dir}")

        return 0

    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found")
        print(f"   {str(e)}")
        print(f"\n💡 Make sure the input file path is correct!")
        return 1

    except UnicodeDecodeError as e:
        print(f"\n❌ ERROR: Encoding issue")
        print(f"   {str(e)}")
        print(f"\n💡 Try changing CSV_ENCODING to 'latin-1' or 'cp1252' at the top of this script")
        return 1

    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed")
        print(f"   {type(e).__name__}: {str(e)}")

        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
