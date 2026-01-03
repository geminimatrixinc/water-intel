"""
Validation module for ECCC water quality data.

Provides comprehensive validation with helpful error messages for:
- Schema validation (column presence and types)
- Data quality checks (nulls, ranges, constraints)
- Business rule validation
"""

import pandas as pd
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime

from .schema import (
    RAW_REQUIRED_COLUMNS,
    RAW_OPTIONAL_COLUMNS,
    RAW_DTYPES,
    NORMALIZED_REQUIRED_COLUMNS,
    NORMALIZED_DTYPES,
    DataConstraints,
    SchemaType,
)


@dataclass
class ValidationResult:
    """Container for validation results."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: Dict[str, any] = field(default_factory=dict)
    
    def add_error(self, message: str):
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)
    
    def add_info(self, key: str, value: any):
        """Add informational metadata."""
        self.info[key] = value
    
    def __str__(self) -> str:
        """String representation of validation results."""
        lines = []
        
        if self.is_valid:
            lines.append("✓ Validation PASSED")
        else:
            lines.append("✗ Validation FAILED")
        
        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for i, err in enumerate(self.errors, 1):
                lines.append(f"  {i}. {err}")
        
        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for i, warn in enumerate(self.warnings, 1):
                lines.append(f"  {i}. {warn}")
        
        if self.info:
            lines.append("\nInfo:")
            for key, val in self.info.items():
                lines.append(f"  {key}: {val}")
        
        return "\n".join(lines)


class DataValidator:
    """Validator for ECCC water quality data."""
    
    def __init__(self, schema_type: SchemaType = SchemaType.RAW):
        """
        Initialize the validator.
        
        Args:
            schema_type: Type of schema to validate against
        """
        self.schema_type = schema_type
        self.constraints = DataConstraints()
    
    def validate_schema(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate that dataframe has required columns and correct types.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult with detailed findings
        """
        result = ValidationResult(is_valid=True)
        
        # Check if dataframe is empty
        if df.empty:
            result.add_error("DataFrame is empty (0 rows)")
            return result
        
        result.add_info('row_count', len(df))
        result.add_info('column_count', len(df.columns))
        
        # Determine expected columns based on schema type
        if self.schema_type == SchemaType.RAW:
            required_cols = RAW_REQUIRED_COLUMNS
            expected_dtypes = RAW_DTYPES
        else:
            required_cols = NORMALIZED_REQUIRED_COLUMNS
            expected_dtypes = NORMALIZED_DTYPES
        
        # Check for missing required columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            result.add_error(
                f"Missing required columns: {', '.join(missing_cols)}"
            )
            result.add_info('missing_columns', missing_cols)
        
        # Check for unexpected columns
        expected_all = set(expected_dtypes.keys())
        actual_cols = set(df.columns)
        unexpected = actual_cols - expected_all
        if unexpected:
            result.add_warning(
                f"Unexpected columns found: {', '.join(unexpected)}"
            )
            result.add_info('unexpected_columns', list(unexpected))
        
        # Validate data types for present columns
        dtype_issues = []
        for col in df.columns:
            if col in expected_dtypes:
                expected_dtype = expected_dtypes[col]
                actual_dtype = str(df[col].dtype)
                
                # Check if types are compatible
                if not self._dtypes_compatible(actual_dtype, expected_dtype):
                    dtype_issues.append(
                        f"{col}: expected {expected_dtype}, got {actual_dtype}"
                    )
        
        if dtype_issues:
            result.add_warning("Data type mismatches found:")
            for issue in dtype_issues:
                result.add_warning(f"  - {issue}")
        
        return result
    
    def validate_data_quality(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate data quality: nulls, ranges, constraints.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult with quality findings
        """
        result = ValidationResult(is_valid=True)
        
        if df.empty:
            result.add_error("Cannot validate quality of empty DataFrame")
            return result
        
        # Check for null values in critical columns
        critical_cols = ['timestamp', 'station_id', 'parameter', 'value'] if self.schema_type == SchemaType.NORMALIZED \
                       else ['sample_datetime', 'site_no', 'variable', 'value']
        
        null_counts = {}
        for col in critical_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    null_pct = (null_count / len(df)) * 100
                    null_counts[col] = {'count': null_count, 'percent': null_pct}
                    
                    if null_pct > 50:
                        result.add_error(
                            f"Column '{col}' has {null_count:,} null values ({null_pct:.1f}%)"
                        )
                    elif null_pct > 10:
                        result.add_warning(
                            f"Column '{col}' has {null_count:,} null values ({null_pct:.1f}%)"
                        )
        
        if null_counts:
            result.add_info('null_counts', null_counts)
        
        # Validate value column ranges
        value_col = 'value'
        if value_col in df.columns:
            non_null_values = df[value_col].dropna()
            if len(non_null_values) > 0:
                min_val = non_null_values.min()
                max_val = non_null_values.max()
                
                result.add_info('value_range', {'min': min_val, 'max': max_val})
                
                # Check for values outside reasonable range
                if min_val < self.constraints.MIN_VALUE:
                    count = (non_null_values < self.constraints.MIN_VALUE).sum()
                    result.add_warning(
                        f"{count} values below minimum threshold ({self.constraints.MIN_VALUE})"
                    )
                
                if max_val > self.constraints.MAX_VALUE:
                    count = (non_null_values > self.constraints.MAX_VALUE).sum()
                    result.add_warning(
                        f"{count} values above maximum threshold ({self.constraints.MAX_VALUE})"
                    )
        
        # Validate timestamp/date column
        date_col = 'timestamp' if self.schema_type == SchemaType.NORMALIZED else 'sample_datetime'
        if date_col in df.columns:
            dates = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for invalid dates
            invalid_dates = dates.isna().sum() - df[date_col].isna().sum()
            if invalid_dates > 0:
                result.add_error(
                    f"{invalid_dates} datetime values could not be parsed"
                )
            
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                
                result.add_info('date_range', {
                    'min': str(min_date),
                    'max': str(max_date)
                })
                
                # Check year ranges
                if min_date.year < self.constraints.MIN_YEAR:
                    result.add_warning(
                        f"Dates before {self.constraints.MIN_YEAR} found (earliest: {min_date.year})"
                    )
                
                if max_date.year > self.constraints.MAX_YEAR:
                    result.add_error(
                        f"Dates after {self.constraints.MAX_YEAR} found (latest: {max_date.year})"
                    )
                
                # Check for future dates
                now = pd.Timestamp.now()
                future_count = (valid_dates > now).sum()
                if future_count > 0:
                    result.add_warning(
                        f"{future_count} timestamps are in the future"
                    )
        
        return result
    
    def validate_business_rules(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate business rules and domain-specific constraints.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult with business rule findings
        """
        result = ValidationResult(is_valid=True)
        
        if df.empty:
            return result
        
        # Check for duplicate records
        id_cols = ['timestamp', 'station_id', 'parameter'] if self.schema_type == SchemaType.NORMALIZED \
                 else ['sample_datetime', 'site_no', 'variable']
        
        if all(col in df.columns for col in id_cols):
            duplicate_count = df.duplicated(subset=id_cols).sum()
            if duplicate_count > 0:
                result.add_warning(
                    f"{duplicate_count} duplicate records found (same timestamp + station + parameter)"
                )
                result.add_info('duplicate_count', duplicate_count)
        
        # Validate station IDs
        station_col = 'station_id' if self.schema_type == SchemaType.NORMALIZED else 'site_no'
        if station_col in df.columns:
            unique_stations = df[station_col].nunique()
            result.add_info('unique_stations', unique_stations)
            
            if unique_stations == 0:
                result.add_error("No unique stations found")
            
            # Check station ID format/length
            if df[station_col].dtype == 'object':
                max_len = df[station_col].str.len().max()
                if max_len > self.constraints.MAX_STATION_ID_LENGTH:
                    result.add_warning(
                        f"Station IDs exceed max length ({max_len} > {self.constraints.MAX_STATION_ID_LENGTH})"
                    )
        
        # Validate parameters
        param_col = 'parameter' if self.schema_type == SchemaType.NORMALIZED else 'variable'
        if param_col in df.columns:
            unique_params = df[param_col].nunique()
            result.add_info('unique_parameters', unique_params)
            
            if unique_params == 0:
                result.add_error("No unique parameters found")
            
            param_list = df[param_col].unique().tolist()
            result.add_info('parameters', sorted([p for p in param_list if pd.notna(p)]))
        
        # Validate qualifiers
        qual_col = 'qualifier' if self.schema_type == SchemaType.NORMALIZED else 'qualifier_flag'
        if qual_col in df.columns:
            unique_quals = df[qual_col].dropna().unique()
            invalid_quals = [q for q in unique_quals 
                           if str(q) not in self.constraints.VALID_QUALIFIERS]
            
            if invalid_quals:
                result.add_warning(
                    f"Unknown qualifier codes found: {', '.join(map(str, invalid_quals))}"
                )
        
        # Validate QA status codes
        if 'qa_status' in df.columns:
            unique_qa = df['qa_status'].dropna().unique()
            invalid_qa = [q for q in unique_qa 
                         if str(q) not in self.constraints.VALID_QA_STATUS]
            
            if invalid_qa:
                result.add_warning(
                    f"Unknown QA status codes found: {', '.join(map(str, invalid_qa))}"
                )
        
        return result
    
    def validate_all(self, df: pd.DataFrame) -> ValidationResult:
        """
        Run all validations and combine results.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Combined ValidationResult
        """
        combined = ValidationResult(is_valid=True)
        
        # Run all validation checks
        schema_result = self.validate_schema(df)
        quality_result = self.validate_data_quality(df)
        business_result = self.validate_business_rules(df)
        
        # Combine results
        combined.errors.extend(schema_result.errors)
        combined.errors.extend(quality_result.errors)
        combined.errors.extend(business_result.errors)
        
        combined.warnings.extend(schema_result.warnings)
        combined.warnings.extend(quality_result.warnings)
        combined.warnings.extend(business_result.warnings)
        
        combined.info.update(schema_result.info)
        combined.info.update(quality_result.info)
        combined.info.update(business_result.info)
        
        # Set overall validity
        combined.is_valid = len(combined.errors) == 0
        
        return combined
    
    @staticmethod
    def _dtypes_compatible(actual: str, expected: str) -> bool:
        """
        Check if actual dtype is compatible with expected dtype.
        
        Args:
            actual: Actual dtype string
            expected: Expected dtype string
            
        Returns:
            True if compatible
        """
        # Normalize dtype strings
        actual = actual.lower()
        expected = expected.lower()
        
        # Direct match
        if actual == expected:
            return True
        
        # Float compatibility
        if expected == 'float64' and actual in ['float32', 'float64', 'int64', 'int32']:
            return True
        
        # String/object compatibility
        if expected == 'string' and actual in ['object', 'string']:
            return True
        
        # Datetime compatibility
        if 'datetime' in expected and 'datetime' in actual:
            return True
        
        return False


def validate_raw_data(df: pd.DataFrame) -> ValidationResult:
    """
    Convenience function to validate raw ECCC data.
    
    Args:
        df: Raw DataFrame to validate
        
    Returns:
        ValidationResult
    """
    validator = DataValidator(schema_type=SchemaType.RAW)
    return validator.validate_all(df)


def validate_normalized_data(df: pd.DataFrame) -> ValidationResult:
    """
    Convenience function to validate normalized data.
    
    Args:
        df: Normalized DataFrame to validate
        
    Returns:
        ValidationResult
    """
    validator = DataValidator(schema_type=SchemaType.NORMALIZED)
    return validator.validate_all(df)
