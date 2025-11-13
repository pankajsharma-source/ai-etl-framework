from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.transformers.validators.quality_scorer import QualityScorer
from src.transformers.enrichers.metadata_to_columns import MetadataToColumnsTransformer

# Extract just 2 records
source = CSVSource("../data/etl/source/medical-claims/medical-claims.csv")
source.connect()

records = []
for i, record in enumerate(source.read()):
    if i >= 2:
        break
    records.append(record)

print(f"Original record fields: {len(records[0].data)}")
print(f"Sample fields: {list(records[0].data.keys())[:5]}")

# Apply quality scorer
scorer = QualityScorer()
scored_records = [scorer.transform(r) for r in records]

print(f"\nAfter QualityScorer:")
print(f"Fields: {len(scored_records[0].data)}")
print(f"Has quality_score in metadata: {scored_records[0].metadata.quality_score}")

# Apply metadata transformer
metadata_transformer = MetadataToColumnsTransformer()
final_records = [metadata_transformer.transform(r) for r in scored_records]

print(f"\nAfter MetadataToColumnsTransformer:")
print(f"Fields: {len(final_records[0].data)}")
print(f"Field names: {list(final_records[0].data.keys())}")
print(f"\nMetadata columns present: {[k for k in final_records[0].data.keys() if k.startswith('_meta_')]}")

source.close()
