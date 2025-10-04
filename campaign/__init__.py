"""
Campaign Management Module
Provides Google Ads API operations for campaign management across cloud platforms
"""

from .base_googleads import BaseGoogleAdsOperations, GoogleAdsError
from .googleads_extensions import GoogleAdsExtensionsOperations
from .googleads_customer_match import GoogleAdsCustomerMatchOperations
from .aws_googleads import AWSGoogleAdsOperations
from .gcp_googleads import GCPGoogleAdsOperations
from .oci_googleads import OCIGoogleAdsOperations
from .azure_googleads import AzureGoogleAdsOperations

__all__ = [
    'BaseGoogleAdsOperations',
    'GoogleAdsError',
    'GoogleAdsExtensionsOperations',
    'GoogleAdsCustomerMatchOperations',
    'AWSGoogleAdsOperations',
    'GCPGoogleAdsOperations',
    'OCIGoogleAdsOperations',
    'AzureGoogleAdsOperations'
]


def get_googleads_client(platform: str, config: dict):
    """
    Factory function to get appropriate Google Ads client for cloud platform
    
    Args:
        platform: Platform identifier ('aws', 'gcp', 'azure', 'oci', 'base')
        config: Google Ads and cloud platform configuration parameters
        
    Returns:
        Google Ads client instance
    """
    platform_map = {
        'aws': AWSGoogleAdsOperations,
        's3': AWSGoogleAdsOperations,
        'amazon': AWSGoogleAdsOperations,
        'gcp': GCPGoogleAdsOperations,
        'google': GCPGoogleAdsOperations,
        'gcs': GCPGoogleAdsOperations,
        'oci': OCIGoogleAdsOperations,
        'oracle': OCIGoogleAdsOperations,
        'azure': AzureGoogleAdsOperations,
        'blob': AzureGoogleAdsOperations,
        'microsoft': AzureGoogleAdsOperations,
        'base': BaseGoogleAdsOperations,
        'generic': BaseGoogleAdsOperations
    }
    
    platform_key = platform.lower()
    if platform_key not in platform_map:
        raise ValueError(f"Unsupported platform: {platform}. Supported platforms: {list(platform_map.keys())}")
        
    return platform_map[platform_key](config)