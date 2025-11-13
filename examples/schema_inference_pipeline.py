"""
Example: Schema Inference Pipeline

Demonstrates automatic schema inference from data samples
"""
import sys
from pathlib import Path
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.analyzers.schema_inferrer import SchemaInferrer


def run_basic_inference():
    """Example: Basic schema inference"""
    print("\n" + "=" * 60)
    print("BASIC SCHEMA INFERENCE")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "mixed_data.json"
    output_db = output_dir / "inferred_schema.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Inferring schema from data...")

    # Create inferrer
    inferrer = SchemaInferrer(
        sample_size=1000,
        confidence_threshold=0.8,
        detect_patterns=True,
        infer_constraints=True,
        suggest_enums=True,
        enum_threshold=10
    )

    # Build pipeline
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(inferrer)
        .load(SQLiteLoader(str(output_db), table="employees"))
        .run()
    )

    print(f"\n✅ Pipeline completed!")
    print(f"   Records processed: {pipeline.result.records_extracted}")

    # Get inferred schema
    schema = inferrer.get_inferred_schema()

    print(f"\n📋 INFERRED SCHEMA ({len(schema.fields)} fields):")
    print("=" * 60)

    for field in schema.fields:
        print(f"\n🔸 {field.name}")
        print(f"   Type:        {field.type.value}")
        print(f"   Nullable:    {field.nullable}")
        print(f"   Confidence:  {field.confidence:.2%}")

        if field.description:
            print(f"   Description: {field.description}")

        if field.pattern:
            print(f"   Pattern:     {field.pattern}")

        if field.min_value is not None:
            print(f"   Min:         {field.min_value}")

        if field.max_value is not None:
            print(f"   Max:         {field.max_value}")

        if field.enum_values:
            print(f"   Enum values: {', '.join(str(v) for v in field.enum_values[:5])}")
            if len(field.enum_values) > 5:
                print(f"                ... and {len(field.enum_values) - 5} more")

    # Show null statistics
    print(f"\n📊 NULL STATISTICS:")
    print("=" * 60)
    for field_name, null_count in schema.null_counts.items():
        total = schema.sample_size
        null_pct = (null_count / total * 100) if total > 0 else 0
        print(f"   {field_name:20s} {null_count:3d} nulls ({null_pct:5.1f}%)")

    return pipeline, schema


def export_schema_to_json():
    """Export inferred schema to JSON file"""
    print("\n" + "=" * 60)
    print("EXPORT SCHEMA TO JSON")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"

    json_file = data_dir / "mixed_data.json"
    schema_file = output_dir / "inferred_schema.json"

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {schema_file}")

    # Create inferrer and process data
    inferrer = SchemaInferrer()

    # Read and process records
    from src.adapters.sources.json_source import JSONSource
    with JSONSource(str(json_file)) as source:
        records = list(source.read())
        inferrer.transform_batch(records)

    # Get schema
    schema = inferrer.get_inferred_schema()

    # Convert schema to dict for JSON export
    schema_dict = {
        'name': schema.name,
        'version': schema.version,
        'inferred': schema.inferred,
        'sample_size': schema.sample_size,
        'fields': [
            {
                'name': f.name,
                'type': f.type.value,
                'nullable': f.nullable,
                'description': f.description,
                'pattern': f.pattern,
                'min_value': f.min_value,
                'max_value': f.max_value,
                'enum_values': f.enum_values,
                'confidence': f.confidence,
            }
            for f in schema.fields
        ],
        'null_counts': schema.null_counts,
    }

    # Write to JSON
    with open(schema_file, 'w') as f:
        json.dump(schema_dict, f, indent=2, default=str)

    print(f"\n✅ Schema exported to {schema_file}")
    print(f"   Fields: {len(schema.fields)}")

    return schema_dict


def run_pattern_detection_focus():
    """Example: Focus on pattern detection"""
    print("\n" + "=" * 60)
    print("PATTERN DETECTION FOCUS")
    print("=" * 60)

    data_dir = project_root / "data"
    json_file = data_dir / "mixed_data.json"

    print(f"\n📁 Input: {json_file}")
    print(f"🔍 Detecting patterns in fields...")

    # Create inferrer with pattern detection enabled
    inferrer = SchemaInferrer(
        detect_patterns=True,
        confidence_threshold=0.7
    )

    # Process data
    from src.adapters.sources.json_source import JSONSource
    with JSONSource(str(json_file)) as source:
        records = list(source.read())
        inferrer.transform_batch(records)

    schema = inferrer.get_inferred_schema()

    # Show fields with detected patterns
    print(f"\n📋 DETECTED PATTERNS:")
    print("=" * 60)

    pattern_fields = [f for f in schema.fields if f.pattern]

    if pattern_fields:
        for field in pattern_fields:
            print(f"\n🔸 {field.name}")
            print(f"   Pattern:     {field.pattern}")
            print(f"   Confidence:  {field.confidence:.2%}")
            print(f"   Description: {field.description}")
    else:
        print("   No patterns detected with current threshold")

    return schema


if __name__ == "__main__":
    print("\n🚀 SCHEMA INFERENCE EXAMPLES")
    print("=" * 60)

    try:
        # Example 1: Basic inference
        pipeline, schema = run_basic_inference()

        # Example 2: Export schema to JSON
        schema_dict = export_schema_to_json()

        # Example 3: Pattern detection focus
        run_pattern_detection_focus()

        print("\n" + "=" * 60)
        print("🎉 All schema inference examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
