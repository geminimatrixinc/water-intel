"""
Day 4 Entrypoint: ECCC Data Ingestion & Validation

Single entrypoint that:
1. Loads the sample_water_quality.csv (ECCC 2A format)
2. Validates the column schema
3. Parses dates and normalizes column names
4. Displays summary statistics

Usage:
    python day4_ingest.py
"""

import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from ingest.eccc_loader import ECCCLoader


def main():
    """Run the Day 4 ingestion pipeline."""
    
    # Define paths (data is in project root, not ml subfolder)
    project_root = Path(__file__).parent.parent
    input_file = project_root / 'data' / 'sample' / 'sample_water_quality.csv'
    
    print("=" * 70)
    print("Day 4: ECCC Data Ingestion & Schema Validation")
    print("=" * 70)
    print(f"\nInput file: {input_file}")
    
    # Check if file exists
    if not input_file.exists():
        print(f"\n❌ Error: File not found: {input_file}")
        sys.exit(1)
    
    try:
        # Initialize loader
        print("\n1. Initializing ECCC loader...")
        loader = ECCCLoader(input_file)
        
        # Load CSV
        print("2. Loading CSV file...")
        loader.load()
        print(f"   ✓ Loaded {len(loader.raw_df)} rows")
        
        # Validate schema
        print("3. Validating schema...")
        validation = loader.validate_schema()
        if not validation.is_valid:
            print(f"   ❌ Schema validation FAILED!")
            for error in validation.errors:
                print(f"      - {error}")
            sys.exit(1)
        print(f"   ✓ Schema valid - all required columns present")
        
        # Show warnings if any
        if validation.warnings:
            print(f"   ⚠ Warnings:")
            for warning in validation.warnings:
                print(f"      - {warning}")
        
        # Parse dates
        print("4. Parsing datetime column...")
        loader.parse_dates()
        null_dates = loader.raw_df['sample_datetime'].isna().sum()
        print(f"   ✓ Dates parsed ({null_dates} null values)")
        
        # Normalize columns
        print("5. Normalizing column names...")
        loader.normalize_columns()
        print(f"   ✓ Columns normalized")
        
        # Get summary
        print("\n" + "=" * 70)
        print("DATA SUMMARY")
        print("=" * 70)
        summary = loader.get_summary()
        
        print(f"\nTotal records: {summary['total_rows']:,}")
        print(f"Date range: {summary['date_range'][0]} to {summary['date_range'][1]}")
        print(f"Unique stations: {summary['unique_stations']}")
        print(f"Unique parameters: {summary['unique_parameters']}")
        
        print(f"\nStations ({summary['unique_stations']}):")
        for station in summary['stations']:
            print(f"  - {station}")
        
        print(f"\nParameters ({summary['unique_parameters']}):")
        for param in summary['parameters'][:10]:  # Show first 10
            print(f"  - {param}")
        if summary['unique_parameters'] > 10:
            print(f"  ... and {summary['unique_parameters'] - 10} more")
        
        # Show sample data
        print("\n" + "=" * 70)
        print("SAMPLE DATA (first 5 rows)")
        print("=" * 70)
        print(loader.normalized_df.head().to_string())
        
        print("\n" + "=" * 70)
        print("COLUMN TYPES")
        print("=" * 70)
        print(loader.normalized_df.dtypes)
        
        print("\n" + "=" * 70)
        print("✓ Day 4 ingestion complete!")
        print("=" * 70)
        print("\nNormalized data is ready for Day 5 processing.")
        print("Next step: Save to data/processed/eccc_processed.csv")
        
        return loader.normalized_df
        
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    df = main()
