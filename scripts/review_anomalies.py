"""
Review Anomalies

Helper script to review anomalous records quarantined by the ETL pipeline.
Shows breakdown by detection method and reasons.
"""

import sys
from pathlib import Path
import pandas as pd


def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_section(title):
    """Print a section header"""
    print(f"\n--- {title} ---")


def review_anomalies(quarantine_file: str = "../data/etl/quarantine/medical-claims/anomalies.csv"):
    """
    Review quarantined anomalous records

    Args:
        quarantine_file: Path to anomalies CSV file
    """
    quarantine_path = Path(quarantine_file)

    if not quarantine_path.exists():
        print(f"❌ Error: Quarantine file not found: {quarantine_path}")
        print(f"   Run the ETL pipeline first: python test_comprehensive_pipeline.py")
        return 1

    print_banner("Anomaly Review")
    print(f"Quarantine file: {quarantine_path}\n")

    # Load anomalies
    df_anomalies = pd.read_csv(quarantine_path)

    if len(df_anomalies) == 0:
        print("✅ No anomalies detected - all records are clean!")
        return 0

    # ========================================================================
    # 1. Overall Statistics
    # ========================================================================
    print_section("📊 Overall Statistics")
    print(f"Total anomalies: {len(df_anomalies)}")
    print(f"Total columns:   {len(df_anomalies.columns)}")

    # ========================================================================
    # 2. Breakdown by Detection Method
    # ========================================================================
    if '_meta_anomaly_method' in df_anomalies.columns:
        print_section("🔍 Breakdown by Detection Method")
        method_counts = df_anomalies['_meta_anomaly_method'].value_counts()
        print(method_counts.to_string())

    # ========================================================================
    # 3. Breakdown by Reasons
    # ========================================================================
    if '_meta_anomaly_reasons' in df_anomalies.columns:
        print_section("⚠️  Breakdown by Reasons")

        # Count unique reasons
        all_reasons = df_anomalies['_meta_anomaly_reasons'].dropna()

        if len(all_reasons) > 0:
            reason_counts = all_reasons.value_counts().head(10)
            print("Top 10 most common reasons:")
            print(reason_counts.to_string())
        else:
            print("No reason information available")

    # ========================================================================
    # 4. Sample Anomalies
    # ========================================================================
    print_section("📋 Sample Anomalies (First 5)")

    # Show key columns if available
    key_columns = []
    if 'Claim Number' in df_anomalies.columns:
        key_columns.append('Claim Number')
    if 'Billed Amount' in df_anomalies.columns:
        key_columns.append('Billed Amount')
    if '_meta_anomaly_method' in df_anomalies.columns:
        key_columns.append('_meta_anomaly_method')
    if '_meta_anomaly_reasons' in df_anomalies.columns:
        key_columns.append('_meta_anomaly_reasons')
    if '_meta_quality_score' in df_anomalies.columns:
        key_columns.append('_meta_quality_score')

    if key_columns:
        sample = df_anomalies[key_columns].head(5)
        # Set display options for better readability
        pd.set_option('display.max_colwidth', 50)
        print(sample.to_string(index=False))
    else:
        print(df_anomalies.head(5).to_string(index=False))

    # ========================================================================
    # 5. Statistical Summary
    # ========================================================================
    if 'Billed Amount' in df_anomalies.columns:
        print_section("💰 Billed Amount Statistics (Anomalies)")
        billed_stats = df_anomalies['Billed Amount'].describe()
        print(billed_stats.to_string())

    if '_meta_quality_score' in df_anomalies.columns:
        print_section("⭐ Quality Score Statistics (Anomalies)")
        quality_stats = df_anomalies['_meta_quality_score'].describe()
        print(quality_stats.to_string())

    # ========================================================================
    # 6. Action Items
    # ========================================================================
    print_banner("Next Steps")
    print("\n📝 Actions for Claims Adjusters:")
    print("1. Review high-value anomalies (sorted by Billed Amount)")
    print("2. Validate diagnosis codes for unusual patterns")
    print("3. Check provider legitimacy for outlier claims")
    print("4. Investigate statistical outliers for data entry errors")
    print(f"\n💾 Anomaly file location: {quarantine_path}")
    print(f"   Open in Excel/spreadsheet for detailed review")

    return 0


def main():
    """Main entry point"""
    # Accept optional quarantine file as argument
    if len(sys.argv) > 1:
        quarantine_file = sys.argv[1]
    else:
        quarantine_file = "../data/etl/quarantine/medical-claims/anomalies.csv"

    return review_anomalies(quarantine_file)


if __name__ == "__main__":
    sys.exit(main())
