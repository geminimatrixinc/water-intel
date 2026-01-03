"""
Schema definitions for ECCC water quality data.

Defines expected columns, data types, and constraints for both
raw ECCC format and normalized internal format.
"""

from typing import Dict, List, Set
from enum import Enum


class SchemaType(Enum):
    """Schema type enumeration."""
    RAW = "raw"          # Raw ECCC format
    NORMALIZED = "normalized"  # Internal normalized format


# =============================================================================
# RAW ECCC SCHEMA (as received from ECCC data files)
# =============================================================================

RAW_REQUIRED_COLUMNS: List[str] = [
    'site_no',           # Station/site identifier
    'sample_datetime',   # Timestamp of sample collection
    'value',             # Measured value
    'variable',          # Parameter name (e.g., 'TURBIDITY', 'PH (FIELD)')
    'unit',              # Unit of measurement
]

RAW_OPTIONAL_COLUMNS: List[str] = [
    'qualifier_flag',    # Data qualifier (e.g., '<' for below detection)
    'sdl',               # Sample detection limit
    'mdl',               # Method detection limit
    'variable_code',     # Numeric code for the variable
    'variable_fr',       # French name of the variable
    'qa_status',         # Quality assurance status code
    'sample_id',         # Unique sample identifier
]

RAW_ALL_COLUMNS: Set[str] = set(RAW_REQUIRED_COLUMNS + RAW_OPTIONAL_COLUMNS)

RAW_DTYPES: Dict[str, str] = {
    'site_no': 'string',
    'sample_datetime': 'datetime64[ns]',
    'value': 'float64',
    'variable': 'string',
    'unit': 'string',
    'qualifier_flag': 'string',
    'sdl': 'float64',
    'mdl': 'float64',
    'variable_code': 'string',
    'variable_fr': 'string',
    'qa_status': 'string',
    'sample_id': 'string',
}


# =============================================================================
# NORMALIZED SCHEMA (internal format after processing)
# =============================================================================

NORMALIZED_REQUIRED_COLUMNS: List[str] = [
    'station_id',        # Station identifier (formerly site_no)
    'timestamp',         # Datetime of measurement (formerly sample_datetime)
    'parameter',         # Parameter name (formerly variable)
    'value',             # Measured value
    'unit',              # Unit of measurement
]

NORMALIZED_OPTIONAL_COLUMNS: List[str] = [
    'qualifier',         # Data qualifier (formerly qualifier_flag)
    'qa_status',         # QA status code
    'sample_id',         # Sample identifier
]

NORMALIZED_ALL_COLUMNS: Set[str] = set(
    NORMALIZED_REQUIRED_COLUMNS + NORMALIZED_OPTIONAL_COLUMNS
)

NORMALIZED_DTYPES: Dict[str, str] = {
    'station_id': 'string',
    'timestamp': 'datetime64[ns]',
    'parameter': 'string',
    'value': 'float64',
    'unit': 'string',
    'qualifier': 'string',
    'qa_status': 'string',
    'sample_id': 'string',
}


# =============================================================================
# COLUMN MAPPING (raw -> normalized)
# =============================================================================

COLUMN_MAPPING: Dict[str, str] = {
    'site_no': 'station_id',
    'sample_datetime': 'timestamp',
    'variable': 'parameter',
    'value': 'value',
    'unit': 'unit',
    'qualifier_flag': 'qualifier',
    'qa_status': 'qa_status',
    'sample_id': 'sample_id',
}


# =============================================================================
# REVERSE MAPPING (normalized -> raw)
# =============================================================================

REVERSE_COLUMN_MAPPING: Dict[str, str] = {
    v: k for k, v in COLUMN_MAPPING.items()
}


# =============================================================================
# DATA CONSTRAINTS & VALIDATION RULES
# =============================================================================

class DataConstraints:
    """Data validation constraints and business rules."""
    
    # Value constraints
    MIN_VALUE = -1000.0  # Minimum allowed measurement value
    MAX_VALUE = 1e9      # Maximum allowed measurement value
    
    # Temporal constraints
    MIN_YEAR = 1900      # Earliest valid year for measurements
    MAX_YEAR = 2100      # Latest valid year for measurements
    
    # String constraints
    MAX_STATION_ID_LENGTH = 50
    MAX_PARAMETER_LENGTH = 200
    MAX_UNIT_LENGTH = 50
    MAX_QUALIFIER_LENGTH = 10
    
    # Known valid qualifiers
    VALID_QUALIFIERS: Set[str] = {
        '<',    # Below detection limit
        '>',    # Above detection limit
        '~',    # Approximate
        'E',    # Estimated
        'U',    # Uncensored
        'nan',  # Not applicable / missing
    }
    
    # Known QA status codes (ECCC specific)
    VALID_QA_STATUS: Set[str] = {
        'P',    # Provisional
        'V',    # Validated
        'R',    # Rejected
        'E',    # Estimated
        'nan',  # Not applicable
    }


def get_schema_info(schema_type: SchemaType = SchemaType.NORMALIZED) -> dict:
    """
    Get complete schema information for the specified type.
    
    Args:
        schema_type: Type of schema (RAW or NORMALIZED)
        
    Returns:
        Dictionary with schema details
    """
    if schema_type == SchemaType.RAW:
        return {
            'type': 'raw',
            'required_columns': RAW_REQUIRED_COLUMNS,
            'optional_columns': RAW_OPTIONAL_COLUMNS,
            'all_columns': RAW_ALL_COLUMNS,
            'dtypes': RAW_DTYPES,
        }
    else:
        return {
            'type': 'normalized',
            'required_columns': NORMALIZED_REQUIRED_COLUMNS,
            'optional_columns': NORMALIZED_OPTIONAL_COLUMNS,
            'all_columns': NORMALIZED_ALL_COLUMNS,
            'dtypes': NORMALIZED_DTYPES,
        }


def get_column_description(column_name: str, schema_type: SchemaType = SchemaType.NORMALIZED) -> str:
    """
    Get human-readable description for a column.
    
    Args:
        column_name: Name of the column
        schema_type: Schema type (RAW or NORMALIZED)
        
    Returns:
        Description string
    """
    descriptions = {
        # Normalized schema
        'station_id': 'Unique identifier for the monitoring station',
        'timestamp': 'Date and time of sample collection',
        'parameter': 'Name of the water quality parameter being measured',
        'value': 'Numeric measurement value',
        'unit': 'Unit of measurement (e.g., MG/L, DEG C, NTU)',
        'qualifier': 'Data qualifier flag (e.g., < for below detection limit)',
        'qa_status': 'Quality assurance status code (P=Provisional, V=Validated)',
        'sample_id': 'Unique identifier for the sample',
        # Raw schema
        'site_no': 'Station/site identifier code',
        'sample_datetime': 'Timestamp of sample collection',
        'variable': 'Water quality parameter name',
        'variable_code': 'Numeric code for the parameter',
        'variable_fr': 'French translation of parameter name',
        'qualifier_flag': 'Data quality qualifier',
        'sdl': 'Sample detection limit',
        'mdl': 'Method detection limit',
    }
    
    return descriptions.get(column_name, f'Column: {column_name}')
