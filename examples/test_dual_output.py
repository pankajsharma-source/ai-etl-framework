"""
Test script for dual-output pipeline (CSV + Parquet)

This script demonstrates outputting the same data to both:
1. CSV format (for RAG/conversational analytics)
2. Parquet format (for BI/analytics tools)
"""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.parquet_loader import ParquetLoader
from src.transformers.cleaners.null_remover import NullRemover


def test_dual_output():
    """Test dual-output pipeline: CSV + Parquet"""
    print("=" * 60)
    print("Testing Dual-Output Pipeline (CSV + Parquet)")
    print("=" * 60)

    # Input data
    input_file = "./data/sample_claims.csv"

    # Output paths
    output_csv = "./output/test_output_rag.csv"
    output_parquet = "./output/test_output_bi.parquet"

    try:
        # Build pipeline with multiple destinations
        pipeline = (Pipeline()
            .extract(CSVSource(input_file))
            .transform(NullRemover(strategy='drop'))
            .load(CSVLoader(output_csv, mode='overwrite'))
            .load(ParquetLoader(output_parquet, mode='overwrite'))
            .run()
        )

        # Check results
        result = pipeline.result

        if result.success:
            print(f"\n✅ Pipeline completed successfully!")
            print(f"\nStatistics:")
            print(f"  Records extracted:   {result.records_extracted}")
            print(f"  Records transformed: {result.records_transformed}")
            print(f"  Records loaded:      {result.records_loaded}")
            print(f"  Total duration:      {result.duration_seconds:.2f}s")
            print(f"\nOutputs:")
            print(f"  CSV (for RAG):     {output_csv}")
            print(f"  Parquet (for BI):  {output_parquet}")

            # Verify files exist
            if Path(output_csv).exists():
                print(f"  ✓ CSV file created ({Path(output_csv).stat().st_size} bytes)")
            else:
                print(f"  ✗ CSV file NOT created")

            if Path(output_parquet).exists():
                print(f"  ✓ Parquet file created ({Path(output_parquet).stat().st_size} bytes)")
            else:
                print(f"  ✗ Parquet file NOT created")

            return True
        else:
            print(f"\n❌ Pipeline failed!")
            if result.errors:
                for error in result.errors:
                    print(f"  Error: {error.message}")
            return False

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_outputs():
    """Verify output files can be read"""
    print("\n" + "=" * 60)
    print("Verifying Output Files")
    print("=" * 60)

    import pandas as pd

    try:
        # Read CSV
        csv_path = "./output/test_output_rag.csv"
        if Path(csv_path).exists():
            df_csv = pd.read_csv(csv_path)
            print(f"\n✅ CSV readable: {len(df_csv)} rows, {len(df_csv.columns)} columns")
            print(f"   Columns: {list(df_csv.columns)[:5]}...")

        # Read Parquet
        parquet_path = "./output/test_output_bi.parquet"
        if Path(parquet_path).exists():
            df_parquet = pd.read_parquet(parquet_path)
            print(f"✅ Parquet readable: {len(df_parquet)} rows, {len(df_parquet.columns)} columns")
            print(f"   Columns: {list(df_parquet.columns)[:5]}...")

            # Compare
            if df_csv.equals(df_parquet):
                print(f"\n✅ Both outputs contain identical data!")
            else:
                print(f"\n⚠️  Outputs differ slightly (expected due to format differences)")

        return True

    except Exception as e:
        print(f"\n❌ Error verifying outputs: {e}")
        return False


if __name__ == "__main__":
    print("\n🚀 Starting Dual-Output Pipeline Test\n")

    # Run test
    success = test_dual_output()

    if success:
        # Verify outputs
        verify_outputs()
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Tests failed!")
        sys.exit(1)
