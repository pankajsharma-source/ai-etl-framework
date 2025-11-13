# AI-ETL Framework - Architecture Documentation

**Version**: 2.1 (Phase 2 Complete)
**Last Updated**: 2025-10-11
**Status**: Phase 1 Complete ✅ | Phase 2 Complete ✅ (~95%)
**Audience**: Developers, architects, technical stakeholders

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Overview](#system-overview)
3. [Core Architecture](#core-architecture)
4. [Data Flow](#data-flow)
5. [Components Deep Dive](#components-deep-dive)
6. [AI/ML Features](#aiml-features)
7. [Real-World Examples](#real-world-examples)
8. [Extending the Framework](#extending-the-framework)
9. [Deployment & Scaling](#deployment--scaling)
10. [Technology Stack](#technology-stack)

---

## Executive Summary

### What is This?

The AI-ETL Framework is an **intelligent data pipeline** that automatically:
- Reads data from various sources (CSV, JSON, databases)
- Cleans and improves data using AI/ML
- Writes data to destinations (databases, files, cloud)

Think of it as a **self-improving assembly line** for data.

### Why AI-Powered?

Traditional ETL just moves data. This framework uses **machine learning** to:
- Understand data structure automatically (schema inference)
- Remove duplicates intelligently (fuzzy matching)
- Detect unusual data (anomaly detection)
- Summarize data (aggregation)
- Optimize itself (performance tuning)

### Current Status

**✅ Phase 1 Complete**: Foundation (5 adapters, pipeline, core framework)
**✅ Phase 2 Complete**: AI/ML Enhancement (6 AI/ML components, 6 transformers total, 8 examples)
**⏳ Phase 3 Planned**: Cloud adapters (S3, Parquet, MySQL)
**⏳ Phase 4 Planned**: Production deployment (Docker, AWS, monitoring)

---

## System Overview

### The Big Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AI-ETL FRAMEWORK                          │
│                                                                   │
│  ┌──────────┐      ┌──────────────┐      ┌──────────┐          │
│  │  EXTRACT │  →   │  TRANSFORM   │  →   │   LOAD   │          │
│  │ (Sources)│      │ (AI/ML Magic)│      │ (Destinations)      │
│  └──────────┘      └──────────────┘      └──────────┘          │
│       ↑                    ↑                     ↑               │
│       │                    │                     │               │
│  ┌────────────────────────────────────────────────────┐        │
│  │            PIPELINE ORCHESTRATOR                     │        │
│  │         (Coordinates Everything)                     │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                   │
│  ┌────────────────────────────────────────────────────┐        │
│  │            AUTOTUNER (ML Optimization)               │        │
│  │         (Learns & Improves Performance)              │        │
│  └────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Pluggable**: Everything can be swapped (sources, transformers, destinations)
2. **Composable**: Chain transformers together like LEGO blocks
3. **AI-First**: Machine learning built into the core, not bolted on
4. **Simple**: Fluent API reads like English
5. **Scalable**: Same code works locally or in the cloud

---

## Core Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: ADAPTERS (Input/Output)                          │
│  • Connect to external systems                              │
│  • Sources: Read data IN (CSV, JSON, PostgreSQL)           │
│  • Destinations: Write data OUT (SQLite, PostgreSQL)        │
└─────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: TRANSFORMERS (Data Operations)                   │
│  • Modify, clean, analyze data                              │
│  • Cleaners: Fix problems (nulls, duplicates)               │
│  • Validators: Check quality                                │
│  • Enrichers: Add value (aggregate, deduplicate)            │
│  • Analyzers: Understand data (patterns, anomalies)         │
└─────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: ORCHESTRATION (Coordination)                     │
│  • Pipeline: Ties everything together                       │
│  • AutoTuner: Optimizes performance                         │
│  • Error handling, logging, metrics                         │
└─────────────────────────────────────────────────────────────┘
```

### The Fluent API

Build pipelines that read like sentences:

```python
Pipeline()
    .extract(CSVSource("data.csv"))           # Read CSV
    .transform(NullRemover())                 # Remove nulls
    .transform(Deduplicator())                # Remove duplicates
    .transform(AnomalyDetector())             # Flag unusual data
    .load(SQLiteLoader("output.db"))          # Write to database
    .run()                                     # Execute!
```

**Why fluent?** Easy to read, easy to modify, hard to mess up.

---

## Data Flow

### How a Record Travels Through the System

```
1. EXTRACT: Read from Source
   ┌─────────────────────────────────┐
   │ CSVSource reads file            │
   │ "Alice,30,alice@example.com"    │
   └─────────────┬───────────────────┘
                 ↓
   ┌─────────────────────────────────┐
   │ Convert to Record object        │
   │ Record(                         │
   │   data={                        │
   │     'name': 'Alice',            │
   │     'age': 30,                  │
   │     'email': 'alice@...com'     │
   │   },                            │
   │   metadata={...}                │
   │ )                               │
   └─────────────┬───────────────────┘

2. TRANSFORM: Process Data
                 ↓
   ┌─────────────────────────────────┐
   │ NullRemover: Check for nulls    │
   │ → No nulls, keep record         │
   └─────────────┬───────────────────┘
                 ↓
   ┌─────────────────────────────────┐
   │ SchemaInferrer: Detect patterns │
   │ → 'email' matches email pattern │
   │ → Add to schema metadata        │
   └─────────────┬───────────────────┘
                 ↓
   ┌─────────────────────────────────┐
   │ QualityScorer: Calculate score  │
   │ → Score = 0.95 (excellent)      │
   │ → Add score to metadata         │
   └─────────────┬───────────────────┘
                 ↓
   ┌─────────────────────────────────┐
   │ AnomalyDetector: Check outliers │
   │ → All values normal             │
   │ → is_anomaly = False            │
   └─────────────┬───────────────────┘

3. LOAD: Write to Destination
                 ↓
   ┌─────────────────────────────────┐
   │ SQLiteLoader: Write to database │
   │ INSERT INTO table VALUES (...)  │
   └─────────────────────────────────┘
```

**Key Points:**
- Each transformer can modify the record
- Transformers add metadata (quality scores, flags)
- Original data preserved unless explicitly modified
- If transformer returns `None`, record is filtered out

---

## Components Deep Dive

### 1. Common Layer (Foundation)

**Location**: `src/common/`

**What it provides:**

```
common/
├── models.py         → Record, Schema, Field (data structures)
├── exceptions.py     → Error types (TransformError, etc.)
├── logging.py        → Structured logging
└── config.py         → Configuration management
```

**Key Classes:**

**Record** - The core data unit:
```python
@dataclass
class Record:
    data: Dict[str, Any]          # Actual data
    metadata: RecordMetadata       # Info about the record
    schema: Optional[Schema]       # Data structure definition
    extracted_at: datetime
    transformed_at: datetime
    loaded_at: datetime
```

**Schema** - Describes data structure:
```python
@dataclass
class Schema:
    name: str
    fields: List[Field]            # List of field definitions
    primary_key: Optional[List[str]]
    inferred: bool                 # True if ML-inferred
```

**Field** - Individual field definition:
```python
@dataclass
class Field:
    name: str
    type: FieldType               # STRING, INTEGER, FLOAT, etc.
    nullable: bool
    pattern: Optional[str]        # e.g., "email", "phone"
    min_value: Optional[Any]
    max_value: Optional[Any]
    enum_values: Optional[List]
    confidence: Optional[float]   # ML confidence score
```

---

### 2. Adapters Layer (I/O)

**Location**: `src/adapters/`

#### Source Adapters (Read Data)

**Base Interface:**
```python
class SourceAdapter(ABC):
    def connect(self) -> None
    def read(self) -> Iterator[Record]
    def get_schema(self) -> Schema
    def close(self) -> None
```

**Implemented Adapters:**

1. **CSVSource** (`sources/csv_source.py`)
   - Reads CSV files with pandas
   - Automatic schema inference
   - Handles missing values
   - Supports chunked reading

2. **JSONSource** (`sources/json_source.py`)
   - Reads JSON and JSONL files
   - Auto-detects format
   - Nested structure support
   - Type inference

3. **PostgreSQLSource** (`sources/postgres_source.py`)
   - Connects to PostgreSQL
   - Custom SQL queries
   - Schema extraction
   - Batched reading

#### Destination Adapters (Write Data)

**Base Interface:**
```python
class DestinationAdapter(ABC):
    def connect(self) -> None
    def create_schema(self, schema: Schema) -> None
    def write(self, records: Iterator[Record]) -> int
    def begin_transaction(self) -> None
    def commit(self) -> None
    def rollback(self) -> None
    def close(self) -> None
```

**Implemented Adapters:**

1. **SQLiteLoader** (`destinations/sqlite_loader.py`)
   - Writes to SQLite database
   - Auto-creates tables
   - Batch insertion
   - Transaction support
   - JSON field support

2. **PostgreSQLLoader** (`destinations/postgres_loader.py`)
   - Writes to PostgreSQL
   - Auto schema/table creation
   - Optimized batch writes
   - Full transaction support

3. **CSVLoader** (`destinations/csv_loader.py`)
   - Writes to CSV files
   - Pandas-based efficient writing
   - Overwrite or append modes
   - Compression support (gzip, bz2, zip, xz)
   - Transaction support (temp file)

4. **JSONLoader** (`destinations/json_loader.py`)
   - Writes to JSON/JSONL files
   - Array mode (JSON array) or lines mode (JSONL)
   - Pretty printing for readability
   - Compression support (gzip, bz2)
   - Optional schema export

---

### 3. Transformers Layer (Data Operations)

**Location**: `src/transformers/`

**Base Interface:**
```python
class Transformer(ABC):
    def transform(self, record: Record) -> Optional[Record]
    def transform_batch(self, records: List[Record]) -> List[Record]
    def get_stats(self) -> Dict
```

#### A. Cleaners (`transformers/cleaners/`)

**1. NullRemover**
- **Purpose**: Handle missing data
- **Strategies**:
  - `drop`: Remove records with any null
  - `drop_all`: Remove only if all nulls
  - `remove_fields`: Keep record, remove null fields
  - `fill`: Fill nulls with specific value

```python
NullRemover(strategy="remove_fields")
```

#### B. Validators (`transformers/validators/`)

**2. QualityScorer**
- **Purpose**: Score data quality (0-1)
- **Criteria**:
  - Completeness (% fields filled)
  - Validity (correct formats)
  - Consistency (logical values)

```python
QualityScorer(
    min_score=0.7,
    filter_low_quality=False,
    weights={'completeness': 0.4, 'validity': 0.3, 'consistency': 0.3}
)
```

#### C. Enrichers (`transformers/enrichers/`)

**3. Deduplicator**
- **Purpose**: Remove duplicate records
- **Methods**:
  - **Exact**: Hash-based matching (fast)
  - **Fuzzy**: AI similarity matching (smart)
- **Merge strategies**: keep_first, keep_last, keep_best_quality

```python
Deduplicator(
    match_mode="fuzzy",              # or "exact"
    similarity_threshold=0.95,       # 95% similar = duplicate
    match_fields=["name", "email"],  # Fields to compare
    merge_strategy="keep_best_quality"
)
```

**4. Aggregator**
- **Purpose**: Group and summarize data (SQL GROUP BY)
- **Functions**: sum, avg, min, max, count, count_distinct, first, last, concat, list

```python
Aggregator(
    group_by=['customer_id', 'region'],
    aggregations={
        'total_sales': {'field': 'amount', 'function': 'sum'},
        'order_count': {'field': 'order_id', 'function': 'count'},
        'avg_order': {'field': 'amount', 'function': 'avg'}
    }
)
```

#### D. Analyzers (`transformers/analyzers/`)

**5. SchemaInferrer**
- **Purpose**: Understand data structure automatically
- **Detects**:
  - Types (integer, float, string, boolean, date)
  - Patterns (email, URL, phone, UUID, IP, dates)
  - Constraints (min/max, nullable, enums)

```python
SchemaInferrer(
    detect_patterns=True,           # Find emails, phones, etc.
    infer_constraints=True,         # Find min/max, nullable
    suggest_enums=True,             # Detect categorical fields
    confidence_threshold=0.8        # Minimum confidence
)
```

**Patterns detected:**
- email, url, phone_us, ipv4
- date_iso, datetime_iso
- uuid, credit_card, ssn

**6. AnomalyDetector**
- **Purpose**: Find unusual/suspicious data
- **Methods**:
  - **Statistical (Z-score)**: Flag values >N std deviations
  - **IQR**: Interquartile range method
  - **Isolation Forest**: ML-based outlier detection
  - **Combined**: Require 2+ methods to agree

```python
AnomalyDetector(
    method='statistical',           # or 'iqr', 'isolation_forest', 'combined'
    threshold=3.0,                 # 3 standard deviations
    numeric_fields=['amount'],     # Fields to analyze
    filter_anomalies=False         # Flag don't remove
)
```

---

### 4. ML Services Layer

**Location**: `src/ml/`

**AutoTuner** (`ml/auto_tuner.py`)
- **Purpose**: Optimize pipeline performance automatically
- **What it does**:
  - Records performance metrics (throughput, memory, duration)
  - Tests different batch sizes
  - Learns optimal settings using ML
  - Recommends improvements with confidence scores

```python
tuner = AutoTuner(
    optimization_target='throughput',  # or 'memory', 'cost'
    min_samples=5                      # Samples before recommending
)

# Run pipeline and collect metrics
metrics = PerformanceMetrics(
    pipeline_id='my_pipeline',
    records_processed=10000,
    duration_seconds=2.5,
    batch_size=1000,
    memory_mb=150
)
tuner.record_performance(metrics)

# Get recommendations
recommendations = tuner.get_recommendations('my_pipeline')
# → "Use batch_size=1000 for 22% better throughput (70% confidence)"
```

---

### 5. Orchestration Layer

**Location**: `src/orchestration/`

**Pipeline** (`orchestration/pipeline.py`)
- **Purpose**: Tie everything together
- **Features**:
  - Fluent API for building pipelines
  - Connects source → transformers → destination
  - Error handling and rollback
  - Transaction management
  - Performance metrics
  - Dynamic schema support

```python
class Pipeline:
    def extract(self, source: SourceAdapter) -> 'Pipeline'
    def transform(self, transformer: Transformer) -> 'Pipeline'
    def load(self, destination: DestinationAdapter) -> 'Pipeline'
    def run(self) -> 'Pipeline'
    def get_stats(self) -> Dict
```

---

## AI/ML Features

### 1. Deduplicator - Smart Duplicate Detection

**Problem**: Customer database has duplicates with slight variations
**Solution**: AI-powered fuzzy matching

**How it works:**
```
Exact Mode (Fast):
  "John Smith" vs "John Smith"     → Hash match → DUPLICATE

Fuzzy Mode (Smart):
  "John Smith" vs "Jon Smith"      → 98% similar → DUPLICATE
  "John Smith" vs "Jane Smith"     → 65% similar → DIFFERENT
```

**Technology**:
- **Exact**: MD5 hash comparison
- **Fuzzy**: sentence-transformers (BERT embeddings)
- **Similarity**: Cosine similarity on embeddings

**Example:**
```python
# Remove exact duplicates by name+email
Deduplicator(
    match_mode="exact",
    match_fields=["name", "email"]
)

# Find similar names using AI
Deduplicator(
    match_mode="fuzzy",
    similarity_threshold=0.90,  # 90% similar
    match_fields=["name"]
)
```

---

### 2. Aggregator - Data Summarization

**Problem**: Have transaction data, need customer summaries
**Solution**: SQL-style GROUP BY with 10 functions

**Functions**:
- **Math**: sum, avg, min, max
- **Counting**: count, count_distinct
- **Selection**: first, last
- **Combination**: concat, list

**Example:**
```python
# Sales by customer
Aggregator(
    group_by=['customer_id'],
    aggregations={
        'total_spent': {'field': 'amount', 'function': 'sum'},
        'order_count': {'field': 'order_id', 'function': 'count'},
        'avg_order': {'field': 'amount', 'function': 'avg'},
        'products_bought': {'field': 'product', 'function': 'list'}
    }
)

# Input: 10 transactions → Output: 3 customer summaries
```

---

### 3. Schema Inferrer - Automatic Understanding

**Problem**: Don't know the structure of your data
**Solution**: ML analyzes samples and detects patterns

**What it detects:**
- **Types**: integer, float, string, boolean, date, json, array
- **Patterns** (9 types):
  - email: `alice@example.com`
  - url: `https://example.com`
  - phone_us: `(555) 123-4567`
  - uuid: `e3f8a9b2-1c4d-4e5f-8a9b-2c3d4e5f6a7b`
  - date_iso: `2024-01-15`
  - datetime_iso: `2024-01-15T10:30:00`
  - ipv4: `192.168.1.1`
  - credit_card: `4111-1111-1111-1111`
  - ssn: `123-45-6789`
- **Constraints**: min/max values, nullable fields
- **Enums**: Low-cardinality fields (Small/Medium/Large)

**Output Example:**
```
Field: email
  Type: string
  Pattern: email (100% confidence)
  Nullable: false
  Description: "Email address field"

Field: age
  Type: integer
  Min: 26
  Max: 45
  Nullable: true (10% null)

Field: department
  Type: string
  Enum: ["Engineering", "Sales", "Marketing"]
```

---

### 4. Anomaly Detector - Find Unusual Data

**Problem**: Need to find errors, fraud, or outliers
**Solution**: Multiple detection methods

**Methods:**

1. **Statistical (Z-score)**
   - Flag values >N standard deviations from mean
   - Fast, simple, effective
   - Example: $9,999 when average is $50

2. **IQR (Interquartile Range)**
   - Flag values outside 1.5 × IQR
   - Robust to outliers
   - Good for skewed data

3. **Isolation Forest (ML)**
   - Scikit-learn ensemble method
   - Detects complex anomalies
   - Handles multiple dimensions

4. **Combined (Consensus)**
   - Requires 2+ methods to agree
   - Higher precision, lower false positives

**Example:**
```python
# Flag transactions >3 std deviations
AnomalyDetector(
    method='statistical',
    threshold=3.0,
    numeric_fields=['amount'],
    filter_anomalies=False  # Keep but flag
)

# Results: $50, $75, $100, $65 → Normal
#          $9,999 → ANOMALY (flagged in metadata)
```

---

### 5. AutoTuner - Self-Optimization

**Problem**: Don't know optimal batch size or settings
**Solution**: ML learns from performance history

**What it tracks:**
- Throughput (records/second)
- Memory usage (MB)
- Duration (seconds)
- Batch size used

**How it learns:**
```
Run 1: batch_size=100  →  4,355 records/sec
Run 2: batch_size=500  → 12,132 records/sec
Run 3: batch_size=1000 → 13,495 records/sec  ← BEST
Run 4: batch_size=2500 → 12,740 records/sec
Run 5: batch_size=5000 → 12,557 records/sec

Analysis:
  Best: 1000
  Confidence: 70%
  Expected improvement: 22%
```

**Storage**: JSON file (`.state/autotuner/performance_history.json`)

---

## Real-World Examples

### Example 1: Customer Data Pipeline

**Scenario**: Import messy customer CSV, clean, deduplicate, score quality

**Input** (`customers.csv`):
```csv
id,name,email,age,purchase_amount
1,John Smith,john@email.com,35,150.00
2,John Smith,john@email.com,35,150.00    ← Duplicate
3,Jane Doe,,28,85.00                     ← Missing email
4,Bob Jones,invalid-email,42,9999.99     ← Bad email, suspicious amount
5,Alice Lee,alice@email.com,25,120.00
```

**Pipeline:**
```python
pipeline = (
    Pipeline()
    .extract(CSVSource("customers.csv"))
    .transform(NullRemover(strategy="remove_fields"))
    .transform(SchemaInferrer(detect_patterns=True))
    .transform(Deduplicator(match_mode="exact"))
    .transform(AnomalyDetector(method="statistical"))
    .transform(QualityScorer(min_score=0.7))
    .load(SQLiteLoader("customers_clean.db"))
    .run()
)
```

**Result:**
```
Extracted: 5 records
Transformed: 4 records  (1 duplicate removed)
Loaded: 4 records
Anomalies: 1 flagged (Record 4: $9,999.99)
Quality: 3 excellent, 1 good

Output database:
  Record 1: John Smith, perfect quality, no issues
  Record 3: Jane Doe, 0.80 quality (missing email)
  Record 4: Bob Jones, 0.65 quality, flagged anomaly
  Record 5: Alice Lee, perfect quality, no issues
```

---

### Example 2: Sales Aggregation

**Scenario**: Summarize sales by customer

**Input** (`sales.json`): 10 transactions from 3 customers

**Pipeline:**
```python
pipeline = (
    Pipeline()
    .extract(JSONSource("sales.json"))
    .transform(Aggregator(
        group_by=['customer'],
        aggregations={
            'total_quantity': {'field': 'quantity', 'function': 'sum'},
            'total_orders': {'field': 'order_id', 'function': 'count'},
            'avg_quantity': {'field': 'quantity', 'function': 'avg'},
            'products': {'field': 'product', 'function': 'list'}
        }
    ))
    .load(SQLiteLoader("sales_summary.db"))
    .run()
)
```

**Result:**
```
Input: 10 transactions
Output: 3 customer summaries

Alice:   14 items, 4 orders, avg 3.5/order
Bob:     16 items, 3 orders, avg 5.3/order
Charlie: 19 items, 3 orders, avg 6.3/order
```

---

## Extending the Framework

### Adding a New Source Adapter

**Example**: Add MongoDB support

```python
# src/adapters/sources/mongo_source.py

from src.adapters.base import SourceAdapter
from src.common.models import Record, Schema
from pymongo import MongoClient

class MongoSource(SourceAdapter):
    def __init__(self, connection_string, database, collection):
        self.connection_string = connection_string
        self.database = database
        self.collection = collection
        self.client = None

    def connect(self):
        self.client = MongoClient(self.connection_string)
        self._connected = True

    def read(self):
        collection = self.client[self.database][self.collection]
        for doc in collection.find():
            yield Record(
                data=doc,
                metadata=RecordMetadata(
                    source_type='mongodb',
                    source_id=self.collection
                )
            )

    def get_schema(self):
        # Use schema inferrer on sample documents
        sample = list(self.read())[:100]
        # ... infer schema logic
        return Schema(...)

    def close(self):
        if self.client:
            self.client.close()
```

**Use it:**
```python
Pipeline().extract(MongoSource("mongodb://localhost", "mydb", "users"))
```

---

### Adding a New Transformer

**Example**: Add a currency converter

```python
# src/transformers/enrichers/currency_converter.py

from src.transformers.base_transformer import Transformer
from src.common.models import Record

class CurrencyConverter(Transformer):
    def __init__(self, from_field, to_field, rate):
        super().__init__()
        self.from_field = from_field
        self.to_field = to_field
        self.rate = rate

    def transform(self, record):
        if self.from_field in record.data:
            amount = record.data[self.from_field]
            record.data[self.to_field] = amount * self.rate
            self.stats.records_modified += 1
        return record
```

**Use it:**
```python
Pipeline().transform(CurrencyConverter('usd', 'eur', 0.85))
```

---

## Deployment & Scaling

### Local Development (Current)

**Target**: 100s - 1000s of records

```
┌────────────────────────────┐
│   Your Laptop              │
│                            │
│   Python Process           │
│   ├─ Extract               │
│   ├─ Transform             │
│   └─ Load                  │
│                            │
│   SQLite Database          │
│   Local Files              │
└────────────────────────────┘
```

**Tools**: Python, SQLite, CSV/JSON files

---

### Small Production (Phase 3/4)

**Target**: 10K - 1M records

```
┌─────────────────────────────────┐
│   Docker Container              │
│                                 │
│   ETL Process                   │
│   • Multi-threaded              │
│   • Scheduled (cron/Airflow)    │
│                                 │
│   PostgreSQL Container          │
│                                 │
└─────────────────────────────────┘
          │
          ↓
     S3 / Cloud Storage
```

**Tools**: Docker, PostgreSQL, S3, Airflow

---

### Cloud Scale (Phase 4)

**Target**: 1M+ records

```
┌────────────────────────────────────┐
│         AWS CLOUD                  │
│                                    │
│   Lambda / ECS (Auto-scaling)      │
│          ↓                         │
│   SQS Queue (Work distribution)    │
│          ↓                         │
│   ┌──────────┐    ┌─────────┐     │
│   │ RDS      │    │ S3      │     │
│   │(Postgres)│    │(Data)   │     │
│   └──────────┘    └─────────┘     │
│                                    │
│   CloudWatch (Monitoring)          │
└────────────────────────────────────┘
```

**Tools**: AWS Lambda/ECS, S3, RDS, CloudWatch

---

## Technology Stack

### Core

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.11+ | Development |
| Data | pandas 2.0+ | Data manipulation |
| Validation | pydantic 2.0+ | Type validation |
| Config | PyYAML 6.0+ | Configuration |

### Database

| Component | Technology | Purpose |
|-----------|-----------|---------|
| ORM | SQLAlchemy 2.0+ | Database abstraction |
| PostgreSQL | psycopg2 | PostgreSQL driver |
| SQLite | Built-in | Local development |

### AI/ML

| Component | Technology | Purpose |
|-----------|-----------|---------|
| ML | scikit-learn 1.3+ | ML algorithms |
| Embeddings | sentence-transformers 3.0+ | Fuzzy matching |
| Monitoring | psutil 5.9+ | Performance metrics |

### Infrastructure (Future)

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Containers | Docker | Packaging |
| Cloud | AWS | Deployment |
| IaC | Terraform | Infrastructure |
| Orchestration | Airflow | Scheduling |

---

## Summary

### What You Have Now

✅ **Complete local ETL framework**
✅ **6 transformers** (2 basic + 4 advanced AI/ML)
✅ **6 AI/ML components** (QualityScorer, Deduplicator, Aggregator, AnomalyDetector, SchemaInferrer, AutoTuner)
✅ **3 source adapters** (CSV, JSON, PostgreSQL)
✅ **4 destination adapters** (SQLite, PostgreSQL, CSV, JSON)
✅ **Fluent API** for building pipelines
✅ **Self-optimization** with AutoTuner
✅ **11 working examples** (including format conversion demos)

### What You Can Do

- Build complex data pipelines locally
- Clean and transform data automatically
- Remove duplicates intelligently
- Aggregate and summarize data
- Detect anomalies and fraud
- Infer schemas automatically
- Optimize performance over time
- Convert between formats (CSV ↔ JSON ↔ JSONL ↔ Database)

### What's Next (Optional)

- **Phase 3**: Cloud adapters (S3, Parquet, MySQL)
- **Phase 4**: Production deployment (Docker, AWS)
- **Phase 5**: Enterprise features (monitoring, API)

---

**The framework is production-ready for local and small-scale use. Extend as needed!**
