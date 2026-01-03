"""
Day 4 Comprehensive Validation Demo

Demonstrates the full validation capabilities of the ingestion pipeline:
- Schema validation
- Data quality checks
- Business rule validation
"""

import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from ingest import (
    ECCCLoader,
    DataValidator,
    SchemaType,
    validate_raw_data,
    validate_normalized_data,
)


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    """Run comprehensive validation demo."""
    
    project_root = Path(__file__).parent.parent
    input_file = project_root / 'data' / 'sample' / 'sample_water_quality.csv'
    
    print_section("Day 4: Comprehensive Validation Demo")
    print(f"\nInput: {input_file}")
    
    if not input_file.exists():
        print(f"\n❌ File not found: {input_file}")
        sys.exit(1)
    
    # Initialize loader
    loader = ECCCLoader(input_file)
    
    # Load data
    print_section("Step 1: Loading Data")
    loader.load()
    print(f"✓ Loaded {len(loader.raw_df):,} rows")
    
    # Validate raw data with full validation suite
    print_section("Step 2: Raw Data Validation (Full Suite)")
    raw_validation = validate_raw_data(loader.raw_df)
    print(raw_validation)
    
    # Parse dates and normalize
    print_section("Step 3: Processing Data")
    print("- Parsing dates...")
    loader.parse_dates()
    print("- Normalizing columns...")
    loader.normalize_columns()
    print(f"✓ Processing complete")
    
    # Validate normalized data
    print_section("Step 4: Normalized Data Validation (Full Suite)")
    norm_validation = validate_normalized_data(loader.normalized_df)
    print(norm_validation)
    
    # Show detailed summary
    print_section("Step 5: Data Summary")
    summary = loader.get_summary()
    
    print(f"\n📊 Dataset Overview:")
    print(f"   Total Records:      {summary['total_rows']:,}")
    print(f"   Date Range:         {summary['date_range'][0]} to {summary['date_range'][1]}")
    print(f"   Unique Stations:    {summary['unique_stations']}")
    print(f"   Unique Parameters:  {summary['unique_parameters']}")
    
    print(f"\n🏭 Monitoring Stations:")
    for station in summary['stations']:
        count = loader.normalized_df[loader.normalized_df['station_id'] == station].shape[0]
        print(f"   {station}: {count:,} measurements")
    
    print(f"\n📏 Water Quality Parameters:")
    for param in summary['parameters']:
        count = loader.normalized_df[loader.normalized_df['parameter'] == param].shape[0]
        print(f"   {param}: {count:,} measurements")
    
    # Show sample records per station
    print_section("Step 6: Sample Data by Station")
    for station in summary['stations']:
        station_data = loader.normalized_df[loader.normalized_df['station_id'] == station]
        print(f"\n{station} (first 3 records):")
        print(station_data.head(3)[['timestamp', 'parameter', 'value', 'unit']].to_string(index=False))
    
    # Final status
    print_section("Validation Summary")
    
    if raw_validation.is_valid and norm_validation.is_valid:
        print("\n✓ All validations PASSED")
        print("✓ Data is ready for downstream processing")
        status_code = 0
    else:
        print("\n⚠ Some validations FAILED or have warnings")
        print("  Review the validation details above")
        status_code = 1
    
    print_section("Next Steps")
    print("\nDay 5 tasks:")
    print("1. Save normalized data to data/processed/eccc_processed.csv")
    print("2. Implement time-series aggregation")
    print("3. Add basic statistical summaries per parameter")
    
    return loader.normalized_df, status_code


if __name__ == "__main__":
    df, code = main()
    sys.exit(code)
