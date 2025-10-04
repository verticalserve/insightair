"""
Kafka Operations Module
Provides Kafka operations for data streaming and consumption across cloud platforms
"""

from .base_kafka import BaseKafkaOperations, TopicDoesNotExistError, MultipleSchemaUseInTopicError, KafkaOperationError
from .kafka_schema_ops import KafkaSchemaOperations
from .aws_kafka import AWSKafkaOperations
from .gcp_kafka import GCPKafkaOperations
from .azure_kafka import AzureKafkaOperations
from .oci_kafka import OCIKafkaOperations

__all__ = [
    'BaseKafkaOperations',
    'TopicDoesNotExistError',
    'MultipleSchemaUseInTopicError',
    'KafkaOperationError',
    'KafkaSchemaOperations',
    'AWSKafkaOperations',
    'GCPKafkaOperations',
    'AzureKafkaOperations',
    'OCIKafkaOperations'
]


def get_kafka_client(platform: str, config: dict):
    """
    Factory function to get appropriate Kafka client for cloud platform
    
    Args:
        platform: Platform identifier ('aws', 'gcp', 'azure', 'oci', 'base')
        config: Kafka and cloud platform configuration parameters
        
    Returns:
        Kafka client instance
        
    Raises:
        ValueError: If unsupported platform specified
    """
    platform_map = {
        'aws': AWSKafkaOperations,
        's3': AWSKafkaOperations,
        'amazon': AWSKafkaOperations,
        'gcp': GCPKafkaOperations,
        'google': GCPKafkaOperations,
        'gcs': GCPKafkaOperations,
        'oci': OCIKafkaOperations,
        'oracle': OCIKafkaOperations,
        'azure': AzureKafkaOperations,
        'blob': AzureKafkaOperations,
        'microsoft': AzureKafkaOperations,
        'base': KafkaSchemaOperations,
        'generic': KafkaSchemaOperations,
        'schema': KafkaSchemaOperations
    }
    
    platform_key = platform.lower()
    if platform_key not in platform_map:
        raise ValueError(f"Unsupported platform: {platform}. Supported platforms: {list(platform_map.keys())}")
        
    return platform_map[platform_key](config)


def create_kafka_config(platform: str, kafka_config: dict, cloud_config: dict = None) -> dict:
    """
    Helper function to create unified Kafka configuration for specified platform
    
    Args:
        platform: Platform identifier
        kafka_config: Kafka-specific configuration (bootstrap_servers, security, etc.)
        cloud_config: Cloud-specific configuration (optional)
        
    Returns:
        Unified configuration dictionary
        
    Example:
        kafka_cfg = {
            'bootstrap_servers': ['localhost:9092'],
            'security_protocol': 'PLAINTEXT',
            'consumer_group_id': 'my_group'
        }
        
        aws_cfg = {
            'aws_access_key_id': 'key',
            'aws_secret_access_key': 'secret',
            'region_name': 'us-east-1',
            'default_bucket': 'my-bucket'
        }
        
        config = create_kafka_config('aws', kafka_cfg, aws_cfg)
        client = get_kafka_client('aws', config)
    """
    config = kafka_config.copy()
    
    if cloud_config:
        config.update(cloud_config)
        
    # Add platform identifier
    config['platform'] = platform.lower()
    
    return config


def validate_kafka_config(config: dict) -> bool:
    """
    Validates Kafka configuration dictionary
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid, False otherwise
    """
    required_keys = ['bootstrap_servers']
    
    # Check required keys
    for key in required_keys:
        if key not in config:
            return False
            
    # Validate bootstrap_servers format
    bootstrap_servers = config['bootstrap_servers']
    if not isinstance(bootstrap_servers, (list, tuple)):
        return False
        
    if len(bootstrap_servers) == 0:
        return False
        
    # Validate server format (host:port)
    for server in bootstrap_servers:
        if not isinstance(server, str) or ':' not in server:
            return False
            
    return True


def get_supported_platforms() -> list:
    """
    Returns list of supported cloud platforms
    
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
    requirements = {
        'aws': {
            'required': ['bootstrap_servers'],
            'optional': ['aws_access_key_id', 'aws_secret_access_key', 'region_name', 'default_bucket']
        },
        'gcp': {
            'required': ['bootstrap_servers'],
            'optional': ['project_id', 'credentials_path', 'default_bucket']
        },
        'azure': {
            'required': ['bootstrap_servers'],
            'optional': ['account_name', 'account_key', 'connection_string', 'default_container']
        },
        'oci': {
            'required': ['bootstrap_servers'],
            'optional': ['namespace', 'region', 'config_file', 'default_bucket']
        },
        'base': {
            'required': ['bootstrap_servers'],
            'optional': ['schema_registry_url', 'consumer_group_id']
        }
    }
    
    platform_key = platform.lower()
    return requirements.get(platform_key, requirements['base'])