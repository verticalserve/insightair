"""
Database Operations Module
Provides database operations for different cloud platforms and database systems
"""

from .base import BaseDatabaseOperations
from .aws_redshift import RedshiftOperations
from .oci_adw import OCIADWOperations

__all__ = [
    'BaseDatabaseOperations',
    'RedshiftOperations', 
    'OCIADWOperations'
]


def get_database_operations(platform: str, connection_params: dict):
    """
    Factory function to get appropriate database operations class
    
    Args:
        platform: Database platform identifier ('aws_redshift', 'oci_adw')
        connection_params: Database connection parameters
        
    Returns:
        Database operations instance
    """
    platform_map = {
        'aws_redshift': RedshiftOperations,
        'redshift': RedshiftOperations,
        'oci_adw': OCIADWOperations,
        'adw': OCIADWOperations,
        'oracle': OCIADWOperations
    }
    
    platform_key = platform.lower()
    if platform_key not in platform_map:
        raise ValueError(f"Unsupported database platform: {platform}")
        
    return platform_map[platform_key](connection_params)