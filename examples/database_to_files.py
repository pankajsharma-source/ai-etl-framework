"""
Example: Database to Files Export

Demonstrates exporting data from SQLite database to various file formats:
- Database → CSV
- Database → JSON
- Database → JSONL

Use case: Export database tables for reporting, backup, or data sharing
"""
import sys
from pathlib import Path
import sqlite3

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.csv_source import CSVSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.adapters.destinations.csv_loader import CSVLoader
from src.adapters.destinations.json_loader import JSONLoader
from src.transformers.cleaners.null_remover import NullRemover


def setup_sample_database():
    """Create a sample SQLite database for demonstration"""
    print("Setting up sample database...")

    db_path = "output/sample_db.sqlite"
    Path("output").mkdir(exist_ok=True)

    # First, load CSV data into SQLite
    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))
        .transform(NullRemover(strategy="remove_fields"))
        .load(SQLiteLoader(
            db_path,
            table="users",
            create_if_missing=True
        ))
        .run()
    )

    print(f"✅ Sample database created: {db_path}")
    print(f"   Records loaded: {pipeline.get_stats()['loaded']}")

    return db_path


def export_to_csv(db_path: str):
    """Export SQLite table to CSV"""
    print("\n" + "=" * 60)
    print("SQLite → CSV Export")
    print("=" * 60)

    # Note: Since we don't have a generic SQLiteSource yet, we'll demonstrate
    # the concept. In practice, you would use:
    # pipeline = Pipeline().extract(SQLiteSource(db_path, table="users"))...

    # For now, let's show the intended usage pattern
    print("\nIntended usage (when SQLiteSource is implemented):")
    print("""
    pipeline = (
        Pipeline()
        .extract(SQLiteSource(db_path, table="users"))
        .load(CSVLoader("output/users_export.csv"))
        .run()
    )
    """)

    # Alternative: Read from CSV and export to show the pattern
    print("\nDemonstrating with CSV source:")
    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))
        .load(CSVLoader("output/database_export.csv"))
        .run()
    )

    print(f"✅ Exported {pipeline.get_stats()['loaded']} records to CSV")
    print(f"   Output: output/database_export.csv")


def export_to_json(db_path: str):
    """Export SQLite table to JSON"""
    print("\n" + "=" * 60)
    print("SQLite → JSON Export")
    print("=" * 60)

    # Demonstrate the pattern
    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))  # Would be SQLiteSource
        .load(JSONLoader(
            "output/database_export.json",
            mode="array",
            pretty=True,
            export_schema=True
        ))
        .run()
    )

    print(f"✅ Exported {pipeline.get_stats()['loaded']} records to JSON")
    print(f"   Output: output/database_export.json")
    print(f"   Schema: output/database_export.schema.json")


def export_to_jsonl(db_path: str):
    """Export SQLite table to JSONL"""
    print("\n" + "=" * 60)
    print("SQLite → JSONL Export")
    print("=" * 60)

    pipeline = (
        Pipeline()
        .extract(CSVSource("data/sample.csv"))  # Would be SQLiteSource
        .load(JSONLoader(
            "output/database_export.jsonl",
            mode="lines"
        ))
        .run()
    )

    print(f"✅ Exported {pipeline.get_stats()['loaded']} records to JSONL")
    print(f"   Output: output/database_export.jsonl")


def verify_database(db_path: str):
    """Verify the database contents"""
    print("\nVerifying database contents:")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get table info
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    print(f"  - Table 'users' has {count} records")

    # Get columns
    cursor.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"  - Columns: {', '.join(columns)}")

    conn.close()


def main():
    """Run database export examples"""

    print("=" * 60)
    print("Database to Files Export Examples")
    print("=" * 60)
    print("\nDemonstrating database export to various formats...")

    try:
        # Setup database
        db_path = setup_sample_database()
        verify_database(db_path)

        # Run exports
        export_to_csv(db_path)
        export_to_json(db_path)
        export_to_jsonl(db_path)

        print("\n" + "=" * 60)
        print("All Exports Complete!")
        print("=" * 60)
        print("\nGenerated files:")
        print("  - output/sample_db.sqlite (source database)")
        print("  - output/database_export.csv (CSV export)")
        print("  - output/database_export.json (JSON export)")
        print("  - output/database_export.jsonl (JSONL export)")

        print("\nYou can:")
        print("  - Query database: sqlite3 output/sample_db.sqlite 'SELECT * FROM users LIMIT 5;'")
        print("  - View CSV: cat output/database_export.csv")
        print("  - View JSON: cat output/database_export.json")
        print("  - Count lines: wc -l output/database_export.jsonl")

        print("\n💡 Note: To export directly from database, implement SQLiteSource adapter")
        print("   (currently demonstrating pattern with CSV source)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
