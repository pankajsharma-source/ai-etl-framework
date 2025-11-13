"""
Example: AutoTuner Demo

Demonstrates automatic pipeline optimization using ML
"""
import sys
import time
import psutil
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.ml.auto_tuner import AutoTuner, PerformanceMetrics


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def run_pipeline_with_metrics(
    pipeline_id: str,
    json_file: Path,
    output_db: Path,
    batch_size: int,
    tuner: AutoTuner
) -> PerformanceMetrics:
    """
    Run pipeline and collect performance metrics

    Args:
        pipeline_id: Pipeline identifier
        json_file: Input JSON file
        output_db: Output database
        batch_size: Batch size to use
        tuner: AutoTuner instance

    Returns:
        PerformanceMetrics
    """
    # Remove existing output
    if output_db.exists():
        output_db.unlink()

    # Measure initial memory
    start_memory = get_memory_usage_mb()
    start_time = time.time()

    try:
        # Run pipeline
        pipeline = (
            Pipeline()
            .extract(JSONSource(str(json_file)))
            .load(SQLiteLoader(str(output_db), table="data"))
            .run()
        )

        duration = time.time() - start_time
        end_memory = get_memory_usage_mb()
        memory_used = end_memory - start_memory

        # Create metrics
        metrics = PerformanceMetrics(
            pipeline_id=pipeline_id,
            records_processed=pipeline.result.records_extracted,
            duration_seconds=duration,
            batch_size=batch_size,
            memory_mb=max(memory_used, 1.0),  # At least 1 MB
            success=pipeline.result.success
        )

        # Record in tuner
        tuner.record_performance(metrics)

        return metrics

    except Exception as e:
        duration = time.time() - start_time

        metrics = PerformanceMetrics(
            pipeline_id=pipeline_id,
            records_processed=0,
            duration_seconds=duration,
            batch_size=batch_size,
            memory_mb=1.0,
            success=False,
            error=str(e)
        )

        tuner.record_performance(metrics)
        raise


def demo_exploration():
    """Demo: Explore different batch sizes and learn optimal settings"""
    print("\n" + "=" * 60)
    print("AUTOTUNER DEMO: BATCH SIZE EXPLORATION")
    print("=" * 60)

    # Setup
    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "sales.json"
    pipeline_id = "sales_pipeline"

    # Create tuner
    tuner = AutoTuner(
        state_path="./.state/autotuner",
        optimization_target="throughput",
        min_samples=3
    )

    print(f"\n📁 Input: {json_file}")
    print(f"🎯 Pipeline ID: {pipeline_id}")
    print(f"🔍 Optimization Target: throughput")

    # Test different batch sizes
    batch_sizes = [100, 500, 1000, 2500, 5000]

    print(f"\n🧪 Testing {len(batch_sizes)} batch sizes...")
    print("=" * 60)

    for i, batch_size in enumerate(batch_sizes, 1):
        print(f"\n[{i}/{len(batch_sizes)}] Testing batch_size={batch_size}")

        output_db = output_dir / f"sales_batch_{batch_size}.db"

        try:
            metrics = run_pipeline_with_metrics(
                pipeline_id=pipeline_id,
                json_file=json_file,
                output_db=output_db,
                batch_size=batch_size,
                tuner=tuner
            )

            print(f"   ✅ Success!")
            print(f"      Records:    {metrics.records_processed}")
            print(f"      Duration:   {metrics.duration_seconds:.3f}s")
            print(f"      Throughput: {metrics.throughput:.2f} records/sec")
            print(f"      Memory:     {metrics.memory_mb:.2f} MB")

        except Exception as e:
            print(f"   ❌ Failed: {e}")

    # Get recommendations
    print("\n" + "=" * 60)
    print("📊 ANALYSIS & RECOMMENDATIONS")
    print("=" * 60)

    summary = tuner.get_performance_summary(pipeline_id)

    if 'analysis' in summary:
        analysis = summary['analysis']
        print(f"\n📈 Performance Stats:")
        print(f"   Total runs:      {analysis['total_runs']}")
        print(f"   Successful:      {analysis['successful_runs']}")
        print(f"   Avg throughput:  {analysis['avg_throughput']:.2f} records/sec")
        print(f"   Max throughput:  {analysis['max_throughput']:.2f} records/sec")
        print(f"   Avg memory:      {analysis['avg_memory_mb']:.2f} MB")
        print(f"   Batch sizes tried: {analysis['batch_sizes_tried']}")

    if 'recommendations' in summary:
        rec = summary['recommendations']

        print(f"\n💡 Recommendations:")
        if rec['has_recommendations']:
            print(f"   ✅ {rec['reason']}")
            print(f"   Recommended batch size: {rec['batch_size']}")
            print(f"   Confidence: {rec['confidence']:.1%}")
            print(f"   Expected improvement: {rec['expected_improvement']:.1f}%")
        else:
            print(f"   ℹ️  {rec['reason']}")


def demo_adaptive_tuning():
    """Demo: Adaptive tuning that learns over time"""
    print("\n" + "=" * 60)
    print("AUTOTUNER DEMO: ADAPTIVE TUNING")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "transactions.json"
    pipeline_id = "transactions_pipeline"

    tuner = AutoTuner(
        optimization_target="throughput",
        min_samples=2
    )

    print(f"\n📁 Input: {json_file}")
    print(f"🎯 Pipeline ID: {pipeline_id}")
    print(f"🔄 Running adaptive exploration...")

    # Start with default
    current_batch_size = 1000

    for iteration in range(1, 6):
        print(f"\n[Iteration {iteration}]")
        print(f"   Current batch size: {current_batch_size}")

        output_db = output_dir / f"transactions_iter_{iteration}.db"

        # Run pipeline
        metrics = run_pipeline_with_metrics(
            pipeline_id=pipeline_id,
            json_file=json_file,
            output_db=output_db,
            batch_size=current_batch_size,
            tuner=tuner
        )

        print(f"   Throughput: {metrics.throughput:.2f} records/sec")

        # Get recommendations
        recommendations = tuner.get_recommendations(pipeline_id, current_batch_size)

        if recommendations['has_recommendations']:
            print(f"   💡 Recommendation: Try batch_size={recommendations['batch_size']}")
            # Try next suggestion
            current_batch_size = tuner.suggest_next_batch_size(
                pipeline_id,
                current_batch_size
            )
        else:
            print(f"   ✓ Settings are optimal")

    # Final summary
    print("\n" + "=" * 60)
    summary = tuner.get_performance_summary(pipeline_id)

    if 'analysis' in summary:
        analysis = summary['analysis']
        print(f"\n📊 Final Performance:")
        print(f"   Runs completed:  {analysis['successful_runs']}")
        print(f"   Best throughput: {analysis['max_throughput']:.2f} records/sec")
        print(f"   Avg throughput:  {analysis['avg_throughput']:.2f} records/sec")
        print(f"   Improvement:     {((analysis['max_throughput'] - analysis['min_throughput']) / analysis['min_throughput'] * 100):.1f}%")


def demo_load_historical():
    """Demo: Load historical data and get recommendations"""
    print("\n" + "=" * 60)
    print("AUTOTUNER DEMO: HISTORICAL ANALYSIS")
    print("=" * 60)

    tuner = AutoTuner()

    # Check if we have historical data
    history_file = Path("./.state/autotuner/performance_history.json")

    if not history_file.exists():
        print("\n⚠️  No historical data found.")
        print("   Run exploration demo first to generate history.")
        return

    print(f"\n📂 Loading historical data from: {history_file}")

    # Get all pipeline IDs
    pipeline_ids = list(tuner.history.keys())

    if not pipeline_ids:
        print("\n⚠️  No pipelines found in history.")
        return

    print(f"   Found {len(pipeline_ids)} pipeline(s) in history")

    # Show summary for each
    for pipeline_id in pipeline_ids:
        print(f"\n📊 Pipeline: {pipeline_id}")
        print("=" * 60)

        summary = tuner.get_performance_summary(pipeline_id)

        if 'analysis' in summary:
            analysis = summary['analysis']
            print(f"   Total runs:      {analysis.get('total_runs', 0)}")
            print(f"   Avg throughput:  {analysis.get('avg_throughput', 0):.2f} records/sec")
            print(f"   Batch sizes:     {analysis.get('batch_sizes_tried', [])}")

        if 'recommendations' in summary:
            rec = summary['recommendations']
            if rec['has_recommendations']:
                print(f"\n   💡 Recommended batch size: {rec['batch_size']}")
                print(f"      Confidence: {rec['confidence']:.1%}")


if __name__ == "__main__":
    print("\n🚀 AUTOTUNER EXAMPLES")
    print("=" * 60)

    try:
        # Demo 1: Explore different batch sizes
        demo_exploration()

        # Demo 2: Adaptive tuning
        demo_adaptive_tuning()

        # Demo 3: Load and analyze historical data
        demo_load_historical()

        print("\n" + "=" * 60)
        print("🎉 All AutoTuner examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
