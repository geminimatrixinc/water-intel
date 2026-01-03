"""
ECCC Water Quality Data Loader

Loads Environment and Climate Change Canada (ECCC) water quality CSV data,
parses dates, and normalizes column names for downstream processing.

Day 4 Deliverable: Ingestion skeleton + schema contract validation
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from .schema import (
    RAW_REQUIRED_COLUMNS,
    RAW_OPTIONAL_COLUMNS,
    COLUMN_MAPPING,
    SchemaType,
)
from .validate import DataValidator, ValidationResult


class ECCCLoader:
    """Loader for ECCC water quality data files."""
    
    # Use schema definitions from schema.py
    REQUIRED_COLUMNS = RAW_REQUIRED_COLUMNS
    OPTIONAL_COLUMNS = RAW_OPTIONAL_COLUMNS
    COLUMN_MAPPING = COLUMN_MAPPING
    
    def __init__(self, file_path: str | Path):
        """
        Initialize the ECCC loader.
        
        Args:
            file_path: Path to the ECCC CSV file
        """
        self.file_path = Path(file_path)
        self.raw_df: Optional[pd.DataFrame] = None
        self.normalized_df: Optional[pd.DataFrame] = None
        self.validator = DataValidator(schema_type=SchemaType.RAW)
        self.validation_result: Optional[ValidationResult] = None
        
    def load(self) -> pd.DataFrame:
        """
        Load the CSV file and return the raw dataframe.
        
        Returns:
            Raw pandas DataFrame
            
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            ValueError: If the CSV is empty or malformed
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        
        try:
            self.raw_df = pd.read_csv(self.file_path)
        except Exception as e:
            raise ValueError(f"Failed to load CSV: {e}")
        
        if self.raw_df.empty:
            raise ValueError("CSV file is empty")
        
        return self.raw_df
    
    def validate_schema(self) -> ValidationResult:
        """
        Validate that the CSV contains all required columns.
        
        Returns:
            ValidationResult with detailed findings
        """
        if self.raw_df is None:
            raise RuntimeError("Must call load() before validate_schema()")
        
        self.validation_result = self.validator.validate_schema(self.raw_df)
        return self.validation_result
    
    def parse_dates(self) -> pd.DataFrame:
        """
        Parse the sample_datetime column to proper datetime objects.
        
        Returns:
            DataFrame with parsed datetime column
        """
        if self.raw_df is None:
            raise RuntimeError("Must call load() before parse_dates()")
        
        # Parse datetime column
        self.raw_df['sample_datetime'] = pd.to_datetime(
            self.raw_df['sample_datetime'],
            errors='coerce'
        )
        
        # Check for any parsing failures
        null_count = self.raw_df['sample_datetime'].isna().sum()
        if null_count > 0:
            print(f"Warning: {null_count} datetime values could not be parsed")
        
        return self.raw_df
    
    def normalize_columns(self) -> pd.DataFrame:
        """
        Normalize column names according to the schema contract.
        
        Returns:
            DataFrame with normalized column names
        """
        if self.raw_df is None:
            raise RuntimeError("Must call load() before normalize_columns()")
        
        # Select only columns we want to keep and rename them
        columns_to_keep = {k: v for k, v in self.COLUMN_MAPPING.items() 
                          if k in self.raw_df.columns}
        
        self.normalized_df = self.raw_df[list(columns_to_keep.keys())].copy()
        self.normalized_df.rename(columns=columns_to_keep, inplace=True)
        
        return self.normalized_df
    
    def process(self) -> pd.DataFrame:
        """
        Execute the full ingestion pipeline:
        1. Load CSV
        2. Validate schema
        3. Parse dates
        4. Normalize columns
        
        Returns:
            Normalized DataFrame ready for downstream processing
            
        Raises:
            ValueError: If schema validation fails
        """
        # Load the data
        self.load()
        
        # Validate schema
        validation = self.validate_schema()
        if not validation.is_valid:
            error_msg = "Schema validation failed:\n" + "\n".join(validation.errors)
            raise ValueError(error_msg)
        
        # Parse dates
        self.parse_dates()
        
        # Normalize column names
        self.normalize_columns()
        
        return self.normalized_df
    
    def get_summary(self) -> dict:
        """
        Get a summary of the loaded data.
        
        Returns:
            Dictionary with data summary statistics
        """
        if self.normalized_df is None:
            raise RuntimeError("Must call process() before get_summary()")
        
        return {
            'total_rows': len(self.normalized_df),
            'date_range': (
                self.normalized_df['timestamp'].min(),
                self.normalized_df['timestamp'].max()
            ),
            'unique_stations': self.normalized_df['station_id'].nunique(),
            'unique_parameters': self.normalized_df['parameter'].nunique(),
            'parameters': sorted(self.normalized_df['parameter'].unique().tolist()),
            'stations': sorted(self.normalized_df['station_id'].unique().tolist()),
        }


def load_eccc_data(file_path: str | Path) -> pd.DataFrame:
    """
    Convenience function to load and process ECCC data in one call.
    
    Args:
        file_path: Path to the ECCC CSV file
        
    Returns:
        Normalized DataFrame
    """
    loader = ECCCLoader(file_path)
    return loader.process()


if __name__ == "__main__":
    # Quick test/demo when run as script
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python eccc_loader.py <path_to_csv>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    print(f"Loading ECCC data from: {csv_path}")
    print("-" * 60)
    
    loader = ECCCLoader(csv_path)
    df = loader.process()
    
    print("\n✓ Data loaded and validated successfully!")
    print("\nSummary:")
    summary = loader.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print(f"\nFirst few rows:")
    print(df.head())
    
    print(f"\nData types:")
    print(df.dtypes)
