"""
Example: Aggregation Pipeline

Demonstrates grouping and aggregating data (similar to SQL GROUP BY)
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestration.pipeline import Pipeline
from src.adapters.sources.json_source import JSONSource
from src.adapters.destinations.sqlite_loader import SQLiteLoader
from src.transformers.enrichers.aggregator import Aggregator


def run_sales_by_customer():
    """Example: Sales aggregated by customer"""
    print("\n" + "=" * 60)
    print("SALES BY CUSTOMER - Multiple Aggregations")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "sales.json"
    output_db = output_dir / "sales_by_customer.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Group by: customer")

    # Group by customer, calculate total sales and order count
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Aggregator(
            group_by=['customer'],
            aggregations={
                'total_quantity': {'field': 'quantity', 'function': 'sum'},
                'total_orders': {'field': 'order_id', 'function': 'count'},
                'avg_quantity': {'field': 'quantity', 'function': 'avg'},
                'products': {'field': 'product', 'function': 'list'}
            }
        ))
        .load(SQLiteLoader(str(output_db), table="customer_summary"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output groups:  {pipeline.result.records_loaded}")

    return pipeline


def run_sales_by_region_product():
    """Example: Sales aggregated by region and product"""
    print("\n" + "=" * 60)
    print("SALES BY REGION AND PRODUCT")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "sales.json"
    output_db = output_dir / "sales_by_region_product.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Group by: region, product")

    # Group by region and product
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Aggregator(
            group_by=['region', 'product'],
            aggregations={
                'total_quantity': {'field': 'quantity', 'function': 'sum'},
                'min_quantity': {'field': 'quantity', 'function': 'min'},
                'max_quantity': {'field': 'quantity', 'function': 'max'},
                'avg_price': {'field': 'price', 'function': 'avg'},
                'order_count': {'field': 'order_id', 'function': 'count'},
                'customers': {'field': 'customer', 'function': 'concat'}
            }
        ))
        .load(SQLiteLoader(str(output_db), table="region_product_summary"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output groups:  {pipeline.result.records_loaded}")

    return pipeline


def run_sales_by_region():
    """Example: Simple regional sales totals"""
    print("\n" + "=" * 60)
    print("SALES BY REGION - Simple Aggregation")
    print("=" * 60)

    data_dir = project_root / "data"
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)

    json_file = data_dir / "sales.json"
    output_db = output_dir / "sales_by_region.db"

    if output_db.exists():
        output_db.unlink()

    print(f"\n📁 Input:  {json_file}")
    print(f"📁 Output: {output_db}")
    print(f"🔍 Group by: region")

    # Group by region only
    pipeline = (
        Pipeline()
        .extract(JSONSource(str(json_file)))
        .transform(Aggregator(
            group_by=['region'],
            aggregations={
                'total_quantity': {'field': 'quantity', 'function': 'sum'},
                'total_orders': {'field': 'order_id', 'function': 'count'},
                'unique_customers': {'field': 'customer', 'function': 'count_distinct'},
                'unique_products': {'field': 'product', 'function': 'count_distinct'},
                'first_date': {'field': 'date', 'function': 'first'},
                'last_date': {'field': 'date', 'function': 'last'}
            }
        ))
        .load(SQLiteLoader(str(output_db), table="region_summary"))
        .run()
    )

    print(f"\n✅ Results:")
    print(f"   Input records:  {pipeline.result.records_extracted}")
    print(f"   Output groups:  {pipeline.result.records_loaded}")

    return pipeline


if __name__ == "__main__":
    print("\n🚀 AGGREGATION EXAMPLES")
    print("=" * 60)

    try:
        # Example 1: Sales by customer
        run_sales_by_customer()

        # Example 2: Sales by region and product (multi-field grouping)
        run_sales_by_region_product()

        # Example 3: Sales by region
        run_sales_by_region()

        print("\n" + "=" * 60)
        print("🎉 All aggregation examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
