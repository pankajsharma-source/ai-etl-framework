"""
Simple deduplication test
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.enrichers.deduplicator import Deduplicator


def main():
    print("\n" + "=" * 60)
    print("DEDUPLICATION TEST - Excluding ID field")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "duplicates.json"
    output_db = output_dir / "test_dedup.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Matching on: name, email, age, city, salary (excluding id)")

    # Deduplicate on all fields except id
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Deduplicator(
            match_mode="exact",
            match_fields=["name", "email", "age", "city", "salary"],
            merge_strategy="keep_first"
        ))
        .load(SQLiteLoader(str(output_db), table="deduplicated"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Extracted:  {pipeline.result.records_extracted}")
    print(f"   Loaded:     {pipeline.result.records_loaded}")
    print(f"   Removed:    {pipeline.result.records_extracted - pipeline.result.records_loaded}")


if __name__ == "__main__":
    main()
