from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.csv_loader import CSVLoader
from src.transformers.validators.quality_scorer import QualityScorer
from src.transformers.enrichers.metadata_to_columns import MetadataToColumnsTransformer

print("Running minimal test...")

pipeline = (Pipeline()
    .extract(CSVSource("../data/etl/source/medical-claims/medical-claims.csv"))
    .transform(QualityScorer())
    .transform(MetadataToColumnsTransformer())
    .load(CSVLoader("../data/etl/destination/test_metadata.csv"))
    .run())

print(f"✅ Loaded {pipeline.result.records_loaded} records")

# Check what was written
import pandas as pd
df = pd.read_csv("../data/etl/destination/test_metadata.csv")
print(f"\nColumns in output: {len(df.columns)}")
print(f"Metadata columns: {[c for c in df.columns if c.startswith('_meta_')]}")
