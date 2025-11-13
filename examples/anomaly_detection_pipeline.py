"""
Example: Anomaly Detection Pipeline

Demonstrates anomaly detection using statistical and ML methods
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.analyzers.anomaly_detector import AnomalyDetector


def run_statistical_detection():
    """Example: Statistical anomaly detection using Z-score"""
    print("\n" + "=" * 60)
    print("STATISTICAL ANOMALY DETECTION (Z-Score)")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_flagged.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Method: Z-score (threshold=3.0)")
    print(f"📌 Action: Flag anomalies (not filtering)")

    # Build pipeline - flag anomalies but don't filter
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='statistical',
            threshold=3.0,
            filter_anomalies=False  # Keep anomalies, just flag them
        ))
        .load(SQLiteLoader(str(output_db), table="transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output records: {pipeline.result.records_loaded}")

    # Read back and show flagged anomalies
    import sqlite3
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

    # Note: SQLite doesn't have the metadata, so we'll just show counts
    print(f"\n💡 All records saved with anomaly flags in metadata")

    conn.close()
    return pipeline


def run_iqr_detection():
    """Example: IQR-based anomaly detection"""
    print("\n" + "=" * 60)
    print("IQR ANOMALY DETECTION")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_iqr.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Method: IQR (Interquartile Range, threshold=1.5)")

    # Build pipeline
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='iqr',
            threshold=1.5,
            filter_anomalies=False
        ))
        .load(SQLiteLoader(str(output_db), table="transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output records: {pipeline.result.records_loaded}")

    return pipeline


def run_isolation_forest_detection():
    """Example: ML-based anomaly detection using Isolation Forest"""
    print("\n" + "=" * 60)
    print("ISOLATION FOREST ANOMALY DETECTION (ML)")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_ml.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Method: Isolation Forest (contamination=0.2)")

    # Build pipeline
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='isolation_forest',
            contamination=0.2,  # Expect ~20% anomalies
            filter_anomalies=False
        ))
        .load(SQLiteLoader(str(output_db), table="transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output records: {pipeline.result.records_loaded}")

    return pipeline


def run_filtering_anomalies():
    """Example: Filter out anomalies instead of flagging"""
    print("\n" + "=" * 60)
    print("FILTER ANOMALIES (Remove from dataset)")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_clean.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Method: Statistical Z-score")
    print(f"📌 Action: FILTER (remove anomalies)")

    # Build pipeline - filter out anomalies
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='statistical',
            threshold=2.5,  # More sensitive
            filter_anomalies=True  # REMOVE anomalies
        ))
        .load(SQLiteLoader(str(output_db), table="clean_transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:    {pipeline.result.records_extracted}")
    print(f"   Output records:   {pipeline.result.records_loaded}")
    print(f"   Filtered out:     {pipeline.result.records_extracted - pipeline.result.records_loaded}")

    return pipeline


def run_combined_detection():
    """Example: Combined detection methods"""
    print("\n" + "=" * 60)
    print("COMBINED ANOMALY DETECTION")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_combined.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Method: Combined (requires 2+ methods to agree)")

    # Build pipeline
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='combined',
            filter_anomalies=False
        ))
        .load(SQLiteLoader(str(output_db), table="transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output records: {pipeline.result.records_loaded}")

    return pipeline


def analyze_specific_fields():
    """Example: Analyze specific numeric fields only"""
    print("\n" + "=" * 60)
    print("FIELD-SPECIFIC ANOMALY DETECTION")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    output_db = output_dir / "transactions_amount.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Fields: Only 'amount' field")

    # Build pipeline - only analyze amount field
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(AnomalyDetector(
            method='statistical',
            numeric_fields=['amount'],  # Only check amount
            threshold=2.0,
            filter_anomalies=False
        ))
        .load(SQLiteLoader(str(output_db), table="transactions"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output records: {pipeline.result.records_loaded}")

    return pipeline


if __name__ == "__main__":
    print("\n🚀 ANOMALY DETECTION EXAMPLES")
    print("=" * 60)
    print("\n💡 Test data includes 4 intentional anomalies:")
    print("   - txn_006: amount=9999.99 (very high)")
    print("   - txn_009: amount=0.01 (very low)")
    print("   - txn_013: amount=5000.00, quantity=100 (bulk order)")
    print("   - txn_019: amount=-50.00 (negative)")

    try:
        # Example 1: Statistical (Z-score)
        run_statistical_detection()

        # Example 2: IQR
        run_iqr_detection()

        # Example 3: Isolation Forest
        run_isolation_forest_detection()

        # Example 4: Filter anomalies
        run_filtering_anomalies()

        # Example 5: Combined methods
        run_combined_detection()

        # Example 6: Field-specific
        analyze_specific_fields()

        print("\n" + "=" * 60)
        print("🎉 All anomaly detection examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
