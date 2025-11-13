"""
Deduplicator transformer for removing duplicate records
"""
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
import numpy as np

from src.transformers.base_transformer import Transformer
from src.common.models import Record
from src.common.exceptions import TransformError


class Deduplicator(Transformer):
    """Transformer that identifies and removes duplicate records"""

    def __init__(
        self,
        match_mode: str = "exact",
        match_fields: Optional[List[str]] = None,
        similarity_threshold: float = 0.95,
        merge_strategy: str = "keep_first",
        model_name: str = "all-MiniLM-L6-v2",
        **kwargs
    ):
        """
        Initialize deduplicator

        Args:
            match_mode: Matching strategy:
                - 'exact': Exact field matching using hash
                - 'fuzzy': Similarity-based matching using embeddings
            match_fields: Fields to consider for matching (None = all fields)
            similarity_threshold: Minimum similarity score for fuzzy matching (0-1)
            merge_strategy: How to handle duplicates:
                - 'keep_first': Keep the first occurrence
                - 'keep_last': Keep the last occurrence
                - 'keep_best_quality': Keep record with highest quality score
            model_name: Sentence transformer model for fuzzy matching
            **kwargs: Additional configuration
        """
        super().__init__({
            'match_mode': match_mode,
            'match_fields': match_fields,
            'similarity_threshold': similarity_threshold,
            'merge_strategy': merge_strategy,
            'model_name': model_name,
            **kwargs
        })

        self.match_mode = match_mode
        self.match_fields = match_fields
        self.similarity_threshold = similarity_threshold
        self.merge_strategy = merge_strategy
        self.model_name = model_name

        # Validate parameters
        if match_mode not in ['exact', 'fuzzy']:
            raise ValueError(
                f"Invalid match_mode: {match_mode}. "
                f"Must be one of: 'exact', 'fuzzy'"
            )

        if merge_strategy not in ['keep_first', 'keep_last', 'keep_best_quality']:
            raise ValueError(
                f"Invalid merge_strategy: {merge_strategy}. "
                f"Must be one of: 'keep_first', 'keep_last', 'keep_best_quality'"
            )

        if not 0 <= similarity_threshold <= 1:
            raise ValueError(
                f"similarity_threshold must be between 0 and 1, got {similarity_threshold}"
            )

        # Lazy load sentence transformer only if needed
        self.model = None
        if match_mode == 'fuzzy':
            self._load_model()

        # Track seen records for deduplication
        self.seen_hashes = set()
        self.seen_records = []

    def _load_model(self):
        """Lazy load sentence transformer model"""
        try:
            from sentence_transformers import SentenceTransformer
            self.logger.info(f"Loading sentence transformer model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            self.logger.info("Model loaded successfully")
        except ImportError:
            raise TransformError(
                "sentence-transformers is required for fuzzy matching. "
                "Install it with: pip install sentence-transformers"
            )
        except Exception as e:
            raise TransformError(f"Failed to load sentence transformer model: {e}")

    def transform(self, record: Record) -> Optional[Record]:
        """
        Single record transform - not used for deduplication
        Deduplication requires batch processing via transform_batch()

        Args:
            record: Input record

        Returns:
            Record: Unchanged record (actual deduplication happens in transform_batch)
        """
        # Single record transform is a no-op for deduplicator
        # All logic is in transform_batch() since we need to compare records
        return record

    def transform_batch(self, records: List[Record]) -> List[Record]:
        """
        Transform a batch of records by removing duplicates

        Args:
            records: Input records

        Returns:
            List[Record]: Deduplicated records
        """
        if not records:
            return []

        try:
            if self.match_mode == 'exact':
                return self._deduplicate_exact(records)
            else:  # fuzzy
                return self._deduplicate_fuzzy(records)

        except Exception as e:
            self.stats.errors += 1
            raise TransformError(f"Error in Deduplicator: {e}")

    def _deduplicate_exact(self, records: List[Record]) -> List[Record]:
        """
        Deduplicate using exact hash matching

        Args:
            records: Input records

        Returns:
            List[Record]: Deduplicated records
        """
        result = []
        seen_hashes = set()
        duplicate_groups = {}  # hash -> list of records

        for record in records:
            # Compute hash of relevant fields
            record_hash = self._compute_hash(record)

            if record_hash in seen_hashes:
                # Duplicate found
                duplicate_groups.setdefault(record_hash, []).append(record)
                self.stats.records_filtered += 1
            else:
                # New unique record
                seen_hashes.add(record_hash)
                duplicate_groups[record_hash] = [record]
                self.stats.records_processed += 1

        # Apply merge strategy for each duplicate group
        for record_hash, group in duplicate_groups.items():
            selected = self._select_record(group)
            result.append(selected)

        return result

    def _deduplicate_fuzzy(self, records: List[Record]) -> List[Record]:
        """
        Deduplicate using fuzzy similarity matching

        Args:
            records: Input records

        Returns:
            List[Record]: Deduplicated records
        """
        if not records:
            return []

        # Convert records to text for embedding
        texts = [self._record_to_text(record) for record in records]

        # Generate embeddings
        self.logger.info(f"Generating embeddings for {len(texts)} records")
        embeddings = self.model.encode(texts, convert_to_numpy=True)

        # Compute pairwise similarities
        similarities = self._compute_similarities(embeddings)

        # Find duplicate groups using similarity threshold
        duplicate_groups = self._find_duplicate_groups(records, similarities)

        # Apply merge strategy for each group
        result = []
        for group in duplicate_groups:
            selected = self._select_record(group)
            result.append(selected)
            self.stats.records_processed += 1

        # Count filtered records
        total_records = len(records)
        kept_records = len(result)
        self.stats.records_filtered = total_records - kept_records

        return result

    def _compute_hash(self, record: Record) -> str:
        """
        Compute hash of record based on match_fields

        Args:
            record: Input record

        Returns:
            str: Hash string
        """
        # Select fields to hash
        if self.match_fields:
            fields = {k: record.data.get(k) for k in self.match_fields if k in record.data}
        else:
            fields = record.data

        # Sort keys for consistent hashing
        sorted_items = sorted(fields.items())

        # Create hash
        hash_input = str(sorted_items).encode('utf-8')
        return hashlib.md5(hash_input).hexdigest()

    def _record_to_text(self, record: Record) -> str:
        """
        Convert record to text for embedding

        Args:
            record: Input record

        Returns:
            str: Text representation
        """
        # Select fields to embed
        if self.match_fields:
            fields = {k: record.data.get(k) for k in self.match_fields if k in record.data}
        else:
            fields = record.data

        # Convert to text
        parts = []
        for key, value in sorted(fields.items()):
            if value is not None and value != "":
                parts.append(f"{key}: {value}")

        return " | ".join(parts)

    def _compute_similarities(self, embeddings: np.ndarray) -> np.ndarray:
        """
        Compute pairwise cosine similarities

        Args:
            embeddings: Numpy array of embeddings (n_records, embedding_dim)

        Returns:
            np.ndarray: Similarity matrix (n_records, n_records)
        """
        # Normalize embeddings
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        normalized = embeddings / (norms + 1e-8)

        # Compute cosine similarity matrix
        similarities = np.dot(normalized, normalized.T)

        return similarities

    def _find_duplicate_groups(
        self,
        records: List[Record],
        similarities: np.ndarray
    ) -> List[List[Record]]:
        """
        Find groups of duplicate records based on similarity threshold

        Args:
            records: Input records
            similarities: Similarity matrix

        Returns:
            List[List[Record]]: Groups of duplicate records
        """
        n = len(records)
        visited = set()
        groups = []

        for i in range(n):
            if i in visited:
                continue

            # Find all records similar to record i
            group = [records[i]]
            visited.add(i)

            for j in range(i + 1, n):
                if j not in visited and similarities[i, j] >= self.similarity_threshold:
                    group.append(records[j])
                    visited.add(j)

            groups.append(group)

        return groups

    def _select_record(self, group: List[Record]) -> Record:
        """
        Select which record to keep from a duplicate group

        Args:
            group: List of duplicate records

        Returns:
            Record: Selected record
        """
        if len(group) == 1:
            return group[0]

        if self.merge_strategy == 'keep_first':
            return group[0]

        elif self.merge_strategy == 'keep_last':
            return group[-1]

        elif self.merge_strategy == 'keep_best_quality':
            # Try to get quality score from metadata
            best_record = group[0]
            best_score = best_record.metadata.quality_score or 0.0

            for record in group[1:]:
                score = record.metadata.quality_score or 0.0
                if score > best_score:
                    best_score = score
                    best_record = record

            return best_record

        return group[0]

    def reset_stats(self) -> None:
        """Reset statistics and seen records"""
        super().reset_stats()
        self.seen_hashes = set()
        self.seen_records = []
