"""
Example: Format Conversion Pipelines

Demonstrates converting data between different formats:
- CSV → JSON
- JSON → CSV
- CSV → JSONL
- JSON → JSONL

Use case: Convert data formats for different applications
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.json_loader import JSONLoader


def csv_to_json():
    """Convert CSV to JSON array"""
    print("\n" + "=" * 60)
    print("CSV → JSON (Array Format)")
    print("=" * 60)

    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))
        .load(JSONLoader(
            "output/sample_from_csv.json",
            mode="array",
            pretty=True,
            export_schema=True
        ))
        .run()
    )

    stats = pipeline.get_stats()
    print(f"✅ Converted {stats['loaded']} records to JSON array")
    print(f"   Output: output/sample_from_csv.json")
    print(f"   Schema: output/sample_from_csv.schema.json")


def json_to_csv():
    """Convert JSON to CSV"""
    print("\n" + "=" * 60)
    print("JSON → CSV")
    print("=" * 60)

    pipeline = (
        Pipeline()
        .extract(JSONSource("data/sample.json"))
        .load(CSVLoader(
            "output/sample_from_json.csv",
            mode="overwrite"
        ))
        .run()
    )

    stats = pipeline.get_stats()
    print(f"✅ Converted {stats['loaded']} records to CSV")
    print(f"   Output: output/sample_from_json.csv")


def csv_to_jsonl():
    """Convert CSV to JSONL (line-delimited JSON)"""
    print("\n" + "=" * 60)
    print("CSV → JSONL (Line Format)")
    print("=" * 60)

    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))
        .load(JSONLoader(
            "output/sample.jsonl",
            mode="lines",  # JSONL format
            export_schema=False
        ))
        .run()
    )

    stats = pipeline.get_stats()
    print(f"✅ Converted {stats['loaded']} records to JSONL")
    print(f"   Output: output/sample.jsonl")
    print(f"   (One JSON object per line)")


def json_to_jsonl():
    """Convert JSON array to JSONL"""
    print("\n" + "=" * 60)
    print("JSON Array → JSONL")
    print("=" * 60)

    pipeline = (
        Pipeline()
        .extract(JSONSource("data/sample.json", mode="array"))
        .load(JSONLoader(
            "output/sample_array_to_lines.jsonl",
            mode="lines"
        ))
        .run()
    )

    stats = pipeline.get_stats()
    print(f"✅ Converted {stats['loaded']} records from array to JSONL")
    print(f"   Output: output/sample_array_to_lines.jsonl")


def main():
    """Run all format conversion examples"""

    print("=" * 60)
    print("Format Conversion Examples")
    print("=" * 60)
    print("\nDemonstrating various format conversions...")

    try:
        # Run conversions
        csv_to_json()
        json_to_csv()
        csv_to_jsonl()
        json_to_jsonl()

        print("\n" + "=" * 60)
        print("All Conversions Complete!")
        print("=" * 60)
        print("\nGenerated files in output/:")
        print("  - sample_from_csv.json (pretty JSON array)")
        print("  - sample_from_csv.schema.json (schema metadata)")
        print("  - sample_from_json.csv (CSV format)")
        print("  - sample.jsonl (line-delimited JSON)")
        print("  - sample_array_to_lines.jsonl (converted JSONL)")

        print("\nYou can inspect the files:")
        print("  cat output/sample_from_csv.json")
        print("  head output/sample.jsonl")
        print("  wc -l output/sample_from_json.csv")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
