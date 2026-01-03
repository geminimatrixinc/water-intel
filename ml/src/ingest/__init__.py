"""
Ingestion module for water quality data.

Handles loading and normalizing data from various sources (ECCC, etc.)
"""

from .eccc_loader import ECCCLoader, load_eccc_data
from .schema import (
    SchemaType,
    RAW_REQUIRED_COLUMNS,
    NORMALIZED_REQUIRED_COLUMNS,
    COLUMN_MAPPING,
    get_schema_info,
)
from .validate import (
    DataValidator,
    ValidationResult,
    validate_raw_data,
    validate_normalized_data,
)

__all__ = [
    'ECCCLoader',
    'load_eccc_data',
    'SchemaType',
    'RAW_REQUIRED_COLUMNS',
    'NORMALIZED_REQUIRED_COLUMNS',
    'COLUMN_MAPPING',
    'get_schema_info',
    'DataValidator',
    'ValidationResult',
    'validate_raw_data',
    'validate_normalized_data',
]
