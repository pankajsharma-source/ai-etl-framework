"""
AutoTuner - ML-based pipeline optimization

Automatically tunes pipeline parameters based on performance history
"""
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import numpy as np

from src.common.logging import get_logger


class PerformanceMetrics:
    """Container for pipeline performance metrics"""

    def __init__(
        self,
        pipeline_id: str,
        records_processed: int,
        duration_seconds: float,
        batch_size: int,
        memory_mb: float,
        success: bool = True,
        error: Optional[str] = None
    ):
        self.pipeline_id = pipeline_id
        self.records_processed = records_processed
        self.duration_seconds = duration_seconds
        self.batch_size = batch_size
        self.memory_mb = memory_mb
        self.success = success
        self.error = error
        self.timestamp = datetime.now()

        # Derived metrics
        self.throughput = records_processed / duration_seconds if duration_seconds > 0 else 0
        self.memory_per_record = memory_mb / records_processed if records_processed > 0 else 0


class AutoTuner:
    """
    Automatic pipeline parameter optimization using ML

    Features:
    - Batch size optimization
    - Parallelism tuning
    - Memory optimization
    - Performance prediction
    - Historical learning
    """

    def __init__(
        self,
        state_path: str = "./.state/autotuner",
        history_size: int = 100,
        min_samples: int = 5,
        optimization_target: str = "throughput",
        **kwargs
    ):
        """
        Initialize AutoTuner

        Args:
            state_path: Path to store performance history
            history_size: Number of runs to keep in history
            min_samples: Minimum samples needed before making recommendations
            optimization_target: What to optimize ('throughput', 'memory', 'cost')
            **kwargs: Additional configuration
        """
        self.state_path = Path(state_path)
        self.state_path.mkdir(parents=True, exist_ok=True)

        self.history_size = history_size
        self.min_samples = min_samples
        self.optimization_target = optimization_target

        self.logger = get_logger("AutoTuner")

        # Performance history: pipeline_id -> list of metrics
        self.history: Dict[str, List[PerformanceMetrics]] = defaultdict(list)

        # Load existing history
        self._load_history()

        # Batch size candidates for testing
        self.batch_size_candidates = [100, 250, 500, 1000, 2500, 5000, 10000]

    def record_performance(self, metrics: PerformanceMetrics) -> None:
        """
        Record performance metrics from a pipeline run

        Args:
            metrics: Performance metrics to record
        """
        pipeline_history = self.history[metrics.pipeline_id]
        pipeline_history.append(metrics)

        # Keep only most recent history_size entries
        if len(pipeline_history) > self.history_size:
            self.history[metrics.pipeline_id] = pipeline_history[-self.history_size:]

        # Persist to disk
        self._save_history()

        self.logger.info(
            f"Recorded performance for {metrics.pipeline_id}: "
            f"{metrics.throughput:.2f} records/sec, "
            f"{metrics.memory_mb:.2f} MB"
        )

    def get_recommendations(
        self,
        pipeline_id: str,
        current_batch_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get optimization recommendations for a pipeline

        Args:
            pipeline_id: Pipeline identifier
            current_batch_size: Current batch size (optional)

        Returns:
            Dict with recommendations
        """
        recommendations = {
            'has_recommendations': False,
            'confidence': 0.0,
            'batch_size': current_batch_size,
            'reason': None,
            'expected_improvement': None,
            'stats': {}
        }

        history = self.history.get(pipeline_id, [])

        if len(history) < self.min_samples:
            recommendations['reason'] = (
                f"Insufficient data: {len(history)}/{self.min_samples} samples collected"
            )
            return recommendations

        # Analyze historical performance
        analysis = self._analyze_history(history)
        recommendations['stats'] = analysis

        # Find optimal batch size
        optimal_batch_size, confidence, improvement = self._find_optimal_batch_size(history)

        if optimal_batch_size and optimal_batch_size != current_batch_size:
            recommendations['has_recommendations'] = True
            recommendations['confidence'] = confidence
            recommendations['batch_size'] = optimal_batch_size
            recommendations['expected_improvement'] = improvement
            recommendations['reason'] = (
                f"Optimal batch size is {optimal_batch_size} based on {len(history)} runs. "
                f"Expected {improvement:.1f}% improvement in {self.optimization_target}."
            )
        else:
            recommendations['reason'] = "Current settings are near optimal"

        return recommendations

    def suggest_next_batch_size(
        self,
        pipeline_id: str,
        current_batch_size: int
    ) -> int:
        """
        Suggest next batch size to try for exploration

        Uses exploration strategy to find optimal parameters

        Args:
            pipeline_id: Pipeline identifier
            current_batch_size: Current batch size

        Returns:
            Suggested batch size to try next
        """
        history = self.history.get(pipeline_id, [])

        # Get batch sizes we've tried
        tried_sizes = {m.batch_size for m in history}

        # If we haven't tried many, explore the space
        untried = [bs for bs in self.batch_size_candidates if bs not in tried_sizes]

        if untried:
            # Try the middle of untried range
            return untried[len(untried) // 2]

        # If we've tried all candidates, stick with best performing
        if history:
            best_metric = max(
                history,
                key=lambda m: self._score_metric(m)
            )
            return best_metric.batch_size

        # Default
        return current_batch_size

    def _analyze_history(self, history: List[PerformanceMetrics]) -> Dict[str, Any]:
        """
        Analyze performance history

        Args:
            history: List of performance metrics

        Returns:
            Analysis summary
        """
        if not history:
            return {}

        # Filter successful runs
        successful = [m for m in history if m.success]

        if not successful:
            return {'error': 'No successful runs in history'}

        throughputs = [m.throughput for m in successful]
        memory_usage = [m.memory_mb for m in successful]
        batch_sizes = [m.batch_size for m in successful]

        return {
            'total_runs': len(history),
            'successful_runs': len(successful),
            'failed_runs': len(history) - len(successful),
            'avg_throughput': float(np.mean(throughputs)),
            'max_throughput': float(np.max(throughputs)),
            'min_throughput': float(np.min(throughputs)),
            'std_throughput': float(np.std(throughputs)),
            'avg_memory_mb': float(np.mean(memory_usage)),
            'max_memory_mb': float(np.max(memory_usage)),
            'batch_sizes_tried': sorted(list(set(batch_sizes))),
        }

    def _find_optimal_batch_size(
        self,
        history: List[PerformanceMetrics]
    ) -> Tuple[Optional[int], float, float]:
        """
        Find optimal batch size from history

        Args:
            history: List of performance metrics

        Returns:
            Tuple of (optimal_batch_size, confidence, expected_improvement_pct)
        """
        if not history:
            return None, 0.0, 0.0

        # Group by batch size
        by_batch_size = defaultdict(list)
        for metric in history:
            if metric.success:
                by_batch_size[metric.batch_size].append(metric)

        if not by_batch_size:
            return None, 0.0, 0.0

        # Calculate average score for each batch size
        batch_scores = {}
        for batch_size, metrics in by_batch_size.items():
            scores = [self._score_metric(m) for m in metrics]
            batch_scores[batch_size] = {
                'avg_score': np.mean(scores),
                'std_score': np.std(scores),
                'count': len(scores)
            }

        # Find best batch size
        best_batch_size = max(
            batch_scores.keys(),
            key=lambda bs: batch_scores[bs]['avg_score']
        )

        best_stats = batch_scores[best_batch_size]

        # Calculate confidence based on:
        # 1. Number of samples
        # 2. Consistency (low std)
        # 3. Margin vs second best
        sample_confidence = min(1.0, best_stats['count'] / 10.0)
        consistency_confidence = 1.0 / (1.0 + best_stats['std_score'])

        # Margin vs second best
        sorted_batch_sizes = sorted(
            batch_scores.keys(),
            key=lambda bs: batch_scores[bs]['avg_score'],
            reverse=True
        )

        if len(sorted_batch_sizes) > 1:
            second_best = sorted_batch_sizes[1]
            margin = (
                batch_scores[best_batch_size]['avg_score'] -
                batch_scores[second_best]['avg_score']
            )
            margin_confidence = min(1.0, margin * 10)
        else:
            margin_confidence = 0.5

        confidence = (sample_confidence + consistency_confidence + margin_confidence) / 3.0

        # Calculate expected improvement vs average
        all_scores = [stats['avg_score'] for stats in batch_scores.values()]
        avg_score = np.mean(all_scores)
        improvement_pct = (
            (best_stats['avg_score'] - avg_score) / avg_score * 100
            if avg_score > 0 else 0
        )

        return best_batch_size, confidence, improvement_pct

    def _score_metric(self, metric: PerformanceMetrics) -> float:
        """
        Score a metric based on optimization target

        Args:
            metric: Performance metric

        Returns:
            Score (higher is better)
        """
        if self.optimization_target == 'throughput':
            return metric.throughput
        elif self.optimization_target == 'memory':
            # Lower memory is better, so invert
            return 1.0 / (metric.memory_per_record + 0.001)
        elif self.optimization_target == 'cost':
            # Cost = memory * time, so invert
            cost = metric.memory_mb * metric.duration_seconds
            return 1.0 / (cost + 0.001)
        else:
            return metric.throughput

    def _load_history(self) -> None:
        """Load performance history from disk"""
        history_file = self.state_path / "performance_history.json"

        if not history_file.exists():
            return

        try:
            with open(history_file, 'r') as f:
                data = json.load(f)

            for pipeline_id, metrics_list in data.items():
                for metric_data in metrics_list:
                    metric = PerformanceMetrics(
                        pipeline_id=metric_data['pipeline_id'],
                        records_processed=metric_data['records_processed'],
                        duration_seconds=metric_data['duration_seconds'],
                        batch_size=metric_data['batch_size'],
                        memory_mb=metric_data['memory_mb'],
                        success=metric_data['success'],
                        error=metric_data.get('error')
                    )
                    self.history[pipeline_id].append(metric)

            self.logger.info(f"Loaded history for {len(self.history)} pipelines")

        except Exception as e:
            self.logger.warning(f"Failed to load history: {e}")

    def _save_history(self) -> None:
        """Save performance history to disk"""
        history_file = self.state_path / "performance_history.json"

        try:
            # Convert to JSON-serializable format
            data = {}
            for pipeline_id, metrics_list in self.history.items():
                data[pipeline_id] = [
                    {
                        'pipeline_id': m.pipeline_id,
                        'records_processed': m.records_processed,
                        'duration_seconds': m.duration_seconds,
                        'batch_size': m.batch_size,
                        'memory_mb': m.memory_mb,
                        'success': m.success,
                        'error': m.error,
                        'timestamp': m.timestamp.isoformat(),
                        'throughput': m.throughput,
                    }
                    for m in metrics_list
                ]

            with open(history_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            self.logger.error(f"Failed to save history: {e}")

    def get_performance_summary(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get performance summary for a pipeline

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Performance summary
        """
        history = self.history.get(pipeline_id, [])

        if not history:
            return {'error': 'No performance data available'}

        analysis = self._analyze_history(history)
        recommendations = self.get_recommendations(pipeline_id)

        return {
            'pipeline_id': pipeline_id,
            'history_count': len(history),
            'analysis': analysis,
            'recommendations': recommendations,
        }
