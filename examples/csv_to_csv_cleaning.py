"""
Example: CSV to CSV Cleaning Pipeline

Demonstrates reading a dirty CSV file, cleaning it, and writing a clean CSV output.

Use case: Clean and deduplicate customer data
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.csv_loader import CSVLoader
from src.transformers.cleaners.null_remover import NullRemover
from src.transformers.enrichers.deduplicator import Deduplicator
from src.transformers.validators.quality_scorer import QualityScorer


def main():
    """Run CSV cleaning pipeline"""

    print("=" * 60)
    print("CSV to CSV Cleaning Pipeline")
    print("=" * 60)

    # Input and output paths
    input_file = "data/sample.csv"
    output_file = "output/sample_clean.csv"

    print(f"\nInput:  {input_file}")
    print(f"Output: {output_file}")

    # Create pipeline
    print("\nBuilding pipeline...")
    print("  1. Extract from CSV")
    print("  2. Remove null values (remove_fields strategy)")
    print("  3. Deduplicate (exact matching)")
    print("  4. Score quality")
    print("  5. Load to clean CSV")

    try:
        # Build and run pipeline
        pipeline = (
            Pipeline()
            .extract(CSVSource(input_file))
            .transform(NullRemover(strategy="remove_fields"))
            .transform(Deduplicator(match_mode="exact"))
            .transform(QualityScorer(min_score=0.5, filter_low_quality=False))
            .load(CSVLoader(
                output_file,
                mode="overwrite",
                pretty=False
            ))
            .run()
        )

        # Get stats
        stats = pipeline.get_stats()

        print("\n" + "=" * 60)
        print("Pipeline Results")
        print("=" * 60)
        print(f"Records extracted: {stats['extracted']}")
        print(f"Records loaded:    {stats['loaded']}")
        print(f"Success:           {stats['success']}")

        if stats['success']:
            print(f"\n✅ Clean CSV written to: {output_file}")
            print("\nYou can now:")
            print(f"  - Open the file: cat {output_file}")
            print(f"  - Compare with original: diff {input_file} {output_file}")
        else:
            print(f"\n❌ Pipeline failed: {stats.get('error')}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
