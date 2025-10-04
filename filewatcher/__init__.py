"""
File Watcher Module
Provides file monitoring operations for different cloud platforms and local file systems
"""

from .base import BaseFileWatcher, TimeOutError
from .aws_s3 import S3FileWatcher
from .oci_storage import OCIObjectStorageWatcher
from .gcp_storage import GCPCloudStorageWatcher
from .azure_blob import AzureBlobStorageWatcher

__all__ = [
    'BaseFileWatcher',
    'TimeOutError',
    'S3FileWatcher',
    'OCIObjectStorageWatcher', 
    'GCPCloudStorageWatcher',
    'AzureBlobStorageWatcher'
]


def get_file_watcher(platform: str, config: dict):
    """
    Factory function to get appropriate file watcher class
    
    Args:
        platform: Platform identifier ('aws_s3', 'oci_storage', 'gcp_storage', 'azure_blob')
        config: File watcher configuration parameters
        
    Returns:
        File watcher instance
    """
    platform_map = {
        'aws_s3': S3FileWatcher,
        's3': S3FileWatcher,
        'aws': S3FileWatcher,
        'oci_storage': OCIObjectStorageWatcher,
        'oci': OCIObjectStorageWatcher,
        'oracle': OCIObjectStorageWatcher,
        'gcp_storage': GCPCloudStorageWatcher,
        'gcp': GCPCloudStorageWatcher,
        'google': GCPCloudStorageWatcher,
        'gcs': GCPCloudStorageWatcher,
        'azure_blob': AzureBlobStorageWatcher,
        'azure': AzureBlobStorageWatcher,
        'blob': AzureBlobStorageWatcher
    }
    
    platform_key = platform.lower()
    if platform_key not in platform_map:
        raise ValueError(f"Unsupported file watcher platform: {platform}")
        
    return platform_map[platform_key](config)