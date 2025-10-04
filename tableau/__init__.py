"""
Tableau Operations Module
Provides Tableau Server integration with cloud storage platforms for Hyper file management and data pipelines
"""

# Import base classes
from .base_tableau import (
    BaseTableauOperations,
    TableauAuthenticationError,
    TableauOperationError,
    TableauDataError
)

# Import cloud-specific implementations
from .aws_tableau import AWSTableauOperations
from .oci_tableau import OCITableauOperations
from .azure_tableau import AzureTableauOperations
from .gcp_tableau import GCPTableauOperations

__version__ = "1.0.0"

__all__ = [
    # Base classes
    "BaseTableauOperations",
    "TableauAuthenticationError",
    "TableauOperationError",
    "TableauDataError",
    
    # Cloud implementations
    "AWSTableauOperations",
    "OCITableauOperations",
    "AzureTableauOperations",
    "GCPTableauOperations",
]

# Cloud platform mapping for dynamic instantiation
CLOUD_PLATFORMS = {
    'aws': AWSTableauOperations,
    'oci': OCITableauOperations,
    'azure': AzureTableauOperations,
    'gcp': GCPTableauOperations,
}


def create_tableau_operations(cloud_platform: str, config: dict) -> BaseTableauOperations:
    """
    Factory function to create appropriate Tableau operations instance
    
    Args:
        cloud_platform: Cloud platform identifier ('aws', 'oci', 'azure', 'gcp')
        config: Configuration dictionary for the platform
        
    Returns:
        Appropriate Tableau operations instance
        
    Raises:
        ValueError: If cloud platform is not supported
    """
    platform = cloud_platform.lower()
    
    if platform not in CLOUD_PLATFORMS:
        raise ValueError(f"Unsupported cloud platform: {cloud_platform}. "
                        f"Supported platforms: {', '.join(CLOUD_PLATFORMS.keys())}")
    
    return CLOUD_PLATFORMS[platform](config)