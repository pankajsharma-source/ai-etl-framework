"""
Example: Deduplication Pipeline

Demonstrates exact and fuzzy matching deduplication
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.enrichers.deduplicator import Deduplicator


def run_exact_deduplication():
    """Example: Exact matching deduplication"""
    print("\n" + "=" * 60)
    print("EXACT DEDUPLICATION PIPELINE")
    print("=" * 60)

    # Paths
    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "duplicates.json"
    output_db = output_dir / "exact_dedup.db"

    # Remove existing output
    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")

    # Build pipeline with exact deduplication
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Deduplicator(
            match_mode="exact",
            merge_strategy="keep_first"
        ))
        .load(SQLiteLoader(str(output_db), table="deduplicated_data"))
        .run()
    )

    print(f"\n✅ Pipeline completed!")
    print(f"   Records extracted:  {pipeline.result.records_extracted}")
    print(f"   Records transformed: {pipeline.result.records_transformed}")
    print(f"   Records loaded:     {pipeline.result.records_loaded}")
    print(f"   Duplicates removed: {pipeline.result.records_extracted - pipeline.result.records_loaded}")

    return pipeline


def run_fuzzy_deduplication():
    """Example: Fuzzy matching deduplication"""
    print("\n" + "=" * 60)
    print("FUZZY DEDUPLICATION PIPELINE")
    print("=" * 60)

    # Paths
    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "duplicates.json"
    output_db = output_dir / "fuzzy_dedup.db"

    # Remove existing output
    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🤖 Using sentence-transformers for fuzzy matching...")

    # Build pipeline with fuzzy deduplication
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Deduplicator(
            match_mode="fuzzy",
            similarity_threshold=0.90,  # 90% similarity threshold
            merge_strategy="keep_first",
            model_name="all-MiniLM-L6-v2"  # Lightweight model
        ))
        .load(SQLiteLoader(str(output_db), table="deduplicated_data"))
        .run()
    )

    print(f"\n✅ Pipeline completed!")
    print(f"   Records extracted:  {pipeline.result.records_extracted}")
    print(f"   Records transformed: {pipeline.result.records_transformed}")
    print(f"   Records loaded:     {pipeline.result.records_loaded}")
    print(f"   Duplicates removed: {pipeline.result.records_extracted - pipeline.result.records_loaded}")

    return pipeline


def run_field_specific_deduplication():
    """Example: Deduplication on specific fields only"""
    print("\n" + "=" * 60)
    print("FIELD-SPECIFIC DEDUPLICATION PIPELINE")
    print("=" * 60)

    # Paths
    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "duplicates.json"
    output_db = output_dir / "email_dedup.db"

    # Remove existing output
    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Deduplicating based on email field only...")

    # Build pipeline - deduplicate only by email
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Deduplicator(
            match_mode="exact",
            match_fields=["email"],  # Only check email field
            merge_strategy="keep_first"
        ))
        .load(SQLiteLoader(str(output_db), table="deduplicated_data"))
        .run()
    )

    print(f"\n✅ Pipeline completed!")
    print(f"   Records extracted:  {pipeline.result.records_extracted}")
    print(f"   Records transformed: {pipeline.result.records_transformed}")
    print(f"   Records loaded:     {pipeline.result.records_loaded}")
    print(f"   Duplicates removed: {pipeline.result.records_extracted - pipeline.result.records_loaded}")

    return pipeline


if __name__ == "__main__":
    print("\n🚀 DEDUPLICATION EXAMPLES")
    print("=" * 60)

    # Run all examples
    try:
        # Example 1: Exact matching
        run_exact_deduplication()

        # Example 2: Fuzzy matching (may take longer due to model loading)
        run_fuzzy_deduplication()

        # Example 3: Field-specific matching
        run_field_specific_deduplication()

        print("\n" + "=" * 60)
        print("🎉 All deduplication examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
