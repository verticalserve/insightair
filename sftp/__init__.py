"""
SFTP Streaming Module
Provides SFTP operations for streaming data from cloud object stores to SFTP locations
"""

from .base_sftp import BaseSFTPOperations, SFTPConnectionError, SFTPTransferError, SFTPOperationError
from .aws_sftp import AWSS3SFTPOperations
from .gcp_sftp import GCPCloudStorageSFTPOperations
from .azure_sftp import AzureBlobStorageSFTPOperations
from .oci_sftp import OCIObjectStorageSFTPOperations

__all__ = [
    'BaseSFTPOperations',
    'SFTPConnectionError',
    'SFTPTransferError',
    'SFTPOperationError',
    'AWSS3SFTPOperations',
    'GCPCloudStorageSFTPOperations',
    'AzureBlobStorageSFTPOperations',
    'OCIObjectStorageSFTPOperations'
]


def get_sftp_client(platform: str, config: dict):
    """
    Factory function to get appropriate SFTP streaming client for cloud platform
    
    Args:
        platform: Platform identifier ('aws', 'gcp', 'azure', 'oci', 'base')
        config: SFTP and cloud platform configuration parameters
        
    Returns:
        SFTP streaming client instance
        
    Raises:
        ValueError: If unsupported platform specified
    """
    platform_map = {
        'aws': AWSS3SFTPOperations,
        's3': AWSS3SFTPOperations,
        'amazon': AWSS3SFTPOperations,
        'gcp': GCPCloudStorageSFTPOperations,
        'google': GCPCloudStorageSFTPOperations,
        'gcs': GCPCloudStorageSFTPOperations,
        'oci': OCIObjectStorageSFTPOperations,
        'oracle': OCIObjectStorageSFTPOperations,
        'azure': AzureBlobStorageSFTPOperations,
        'blob': AzureBlobStorageSFTPOperations,
        'microsoft': AzureBlobStorageSFTPOperations,
        'base': BaseSFTPOperations,
        'generic': BaseSFTPOperations
    }
    
    platform_key = platform.lower()
    if platform_key not in platform_map:
        raise ValueError(f"Unsupported platform: {platform}. Supported platforms: {list(platform_map.keys())}")
        
    return platform_map[platform_key](config)


def create_sftp_config(platform: str, sftp_config: dict, cloud_config: dict = None) -> dict:
    """
    Helper function to create unified SFTP configuration for specified platform
    
    Args:
        platform: Platform identifier
        sftp_config: SFTP-specific configuration (hostname, username, auth, etc.)
        cloud_config: Cloud-specific configuration (optional)
        
    Returns:
        Unified configuration dictionary
        
    Example:
        sftp_cfg = {
            'hostname': 'sftp.example.com',
            'username': 'user',
            'password': 'pass',  # or private_key_path
            'port': 22
        }
        
        aws_cfg = {
            'aws_access_key_id': 'key',
            'aws_secret_access_key': 'secret',
            'region_name': 'us-east-1',
            'default_bucket': 'my-bucket'
        }
        
        config = create_sftp_config('aws', sftp_cfg, aws_cfg)
        client = get_sftp_client('aws', config)
    """
    config = sftp_config.copy()
    
    if cloud_config:
        config.update(cloud_config)
        
    # Add platform identifier
    config['platform'] = platform.lower()
    
    return config


def validate_sftp_config(config: dict) -> bool:
    """
    Validates SFTP configuration dictionary
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid, False otherwise
    """
    required_keys = ['hostname', 'username']
    
    # Check required keys
    for key in required_keys:
        if key not in config:
            return False
            
    # Must have either password or private key
    has_password = config.get('password')
    has_private_key = config.get('private_key_path') or config.get('private_key')
    
    if not has_password and not has_private_key:
        return False
        
    # Validate hostname format
    hostname = config['hostname']
    if not isinstance(hostname, str) or len(hostname.strip()) == 0:
        return False
        
    return True


def get_supported_platforms() -> list:
    """
    Returns list of supported cloud platforms for SFTP streaming
    
    Returns:
        List of platform identifiers
    """
    return ['aws', 'gcp', 'azure', 'oci', 'base']


def get_platform_requirements(platform: str) -> dict:
    """
    Returns configuration requirements for specified platform
    
    Args:
        platform: Platform identifier
        
    Returns:
        Dictionary with required and optional configuration keys
    """
    base_requirements = {
        'required': ['hostname', 'username'],
        'optional': ['password', 'private_key_path', 'private_key', 'port', 'timeout', 'buffer_size']
    }
    
    platform_specific = {
        'aws': {
            'cloud_optional': ['aws_access_key_id', 'aws_secret_access_key', 'region_name', 'default_bucket']
        },
        'gcp': {
            'cloud_optional': ['project_id', 'credentials_path', 'credentials', 'default_bucket']
        },
        'azure': {
            'cloud_optional': ['account_name', 'account_key', 'connection_string', 'default_container']
        },
        'oci': {
            'cloud_optional': ['namespace', 'region', 'config_file', 'default_bucket', 'tenancy', 'user', 'fingerprint']
        }
    }
    
    platform_key = platform.lower()
    requirements = base_requirements.copy()
    
    if platform_key in platform_specific:
        requirements.update(platform_specific[platform_key])
        
    return requirements


def test_all_connections(configs: dict) -> dict:
    """
    Test connections for multiple SFTP configurations
    
    Args:
        configs: Dictionary mapping platform names to configuration dictionaries
        
    Returns:
        Dictionary with test results for each platform
    """
    results = {}
    
    for platform, config in configs.items():
        try:
            client = get_sftp_client(platform, config)
            test_result = client.test_connection()
            results[platform] = {
                'success': test_result,
                'platform': platform,
                'hostname': config.get('hostname'),
                'username': config.get('username')
            }
            client.close_connection()
        except Exception as e:
            results[platform] = {
                'success': False,
                'platform': platform,
                'error': str(e),
                'hostname': config.get('hostname'),
                'username': config.get('username')
            }
            
    return results


def create_transfer_manifest(cloud_objects: list, sftp_base_path: str, 
                           preserve_structure: bool = True) -> list:
    """
    Create transfer manifest for batch operations
    
    Args:
        cloud_objects: List of cloud object information dictionaries
        sftp_base_path: Base SFTP directory path
        preserve_structure: Whether to preserve directory structure
        
    Returns:
        List of transfer specifications
    """
    transfer_list = []
    
    for obj in cloud_objects:
        # Determine source path based on cloud platform
        if 'aws_path' in obj or 's3_path' in obj:
            source_path = obj.get('s3_path') or obj.get('aws_path')
        elif 'gcs_path' in obj:
            source_path = obj['gcs_path']
        elif 'azure_path' in obj:
            source_path = obj['azure_path']
        elif 'oci_path' in obj:
            source_path = obj['oci_path']
        else:
            # Skip objects without recognized path format
            continue
            
        # Determine destination path
        if preserve_structure:
            # Extract relative path from object name/key
            object_name = obj.get('name') or obj.get('key', '')
            destination_path = f"{sftp_base_path.rstrip('/')}/{object_name}"
        else:
            # Flatten structure - use just filename
            object_name = obj.get('name') or obj.get('key', '')
            filename = object_name.split('/')[-1] if '/' in object_name else object_name
            destination_path = f"{sftp_base_path.rstrip('/')}/{filename}"
            
        transfer_spec = {
            'source': source_path,
            'destination': destination_path,
            'size': obj.get('size', 0),
            'last_modified': obj.get('last_modified') or obj.get('time_modified')
        }
        
        transfer_list.append(transfer_spec)
        
    return transfer_list


def estimate_transfer_time(transfer_list: list, bandwidth_mbps: float = 10.0) -> dict:
    """
    Estimate transfer time for batch operations
    
    Args:
        transfer_list: List of transfer specifications
        bandwidth_mbps: Estimated bandwidth in MB/s
        
    Returns:
        Dictionary with time estimates
    """
    total_bytes = sum(transfer.get('size', 0) for transfer in transfer_list)
    total_mb = total_bytes / (1024 * 1024)
    
    estimated_seconds = total_mb / bandwidth_mbps if bandwidth_mbps > 0 else 0
    estimated_minutes = estimated_seconds / 60
    estimated_hours = estimated_minutes / 60
    
    return {
        'total_files': len(transfer_list),
        'total_bytes': total_bytes,
        'total_mb': total_mb,
        'bandwidth_mbps': bandwidth_mbps,
        'estimated_seconds': estimated_seconds,
        'estimated_minutes': estimated_minutes,
        'estimated_hours': estimated_hours,
        'estimated_duration_readable': f"{int(estimated_hours)}h {int(estimated_minutes % 60)}m {int(estimated_seconds % 60)}s"
    }