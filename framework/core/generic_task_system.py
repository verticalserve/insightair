# Generic Task Type System - Multi-cloud and multi-database abstraction
"""
Generic task type system that automatically selects appropriate implementations based on:
- Cloud provider (AWS, GCP, Azure, OCI)
- Database type (PostgreSQL, MySQL, Oracle, SQL Server, etc.)
- Storage system (S3, GCS, Azure Blob, OCI Object Storage)
- Messaging system (Kafka, Pub/Sub, Service Bus, etc.)
- Environment configuration and deployment target
"""

import logging
import os
from typing import Dict, Any, Optional, Type, List, Union
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

from airflow.models import Variable
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class CloudProvider(Enum):
    """Supported cloud providers"""
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"
    OCI = "oci"
    ON_PREMISE = "on_premise"


class DatabaseType(Enum):
    """Supported database types"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    SQLITE = "sqlite"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    SYNAPSE = "synapse"
    AUTONOMOUS_DB = "autonomous_db"


class StorageType(Enum):
    """Supported storage types"""
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"
    OCI_OBJECT = "oci_object"
    LOCAL_FS = "local_fs"
    HDFS = "hdfs"


class MessagingType(Enum):
    """Supported messaging systems"""
    KAFKA = "kafka"
    PUBSUB = "pubsub"
    SERVICE_BUS = "service_bus"
    SQS = "sqs"
    RABBITMQ = "rabbitmq"
    KINESIS = "kinesis"


@dataclass
class EnvironmentContext:
    """Environment context for task execution"""
    cloud_provider: CloudProvider
    region: Optional[str] = None
    environment: str = "development"
    deployment_target: Optional[str] = None
    default_database_type: Optional[DatabaseType] = None
    default_storage_type: Optional[StorageType] = None
    default_messaging_type: Optional[MessagingType] = None
    connection_mappings: Optional[Dict[str, str]] = None
    
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'EnvironmentContext':
        """Create EnvironmentContext from configuration"""
        return cls(
            cloud_provider=CloudProvider(config.get('cloud_provider', 'aws')),
            region=config.get('region'),
            environment=config.get('environment', 'development'),
            deployment_target=config.get('deployment_target'),
            default_database_type=DatabaseType(config['default_database_type']) if config.get('default_database_type') else None,
            default_storage_type=StorageType(config['default_storage_type']) if config.get('default_storage_type') else None,
            default_messaging_type=MessagingType(config['default_messaging_type']) if config.get('default_messaging_type') else None,
            connection_mappings=config.get('connection_mappings', {})
        )


class ProviderDetector:
    """Detects cloud provider and environment context"""
    
    @staticmethod
    def detect_cloud_provider() -> CloudProvider:
        """Auto-detect cloud provider from environment"""
        
        # Check environment variables
        if os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION'):
            return CloudProvider.AWS
        
        if os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCP_PROJECT'):
            return CloudProvider.GCP
        
        if os.getenv('AZURE_SUBSCRIPTION_ID') or os.getenv('AZURE_TENANT_ID'):
            return CloudProvider.AZURE
        
        if os.getenv('OCI_TENANCY') or os.getenv('OCI_REGION'):
            return CloudProvider.OCI
        
        # Check Airflow Variables
        try:
            cloud_provider = Variable.get("CLOUD_PROVIDER", default_var=None)
            if cloud_provider:
                return CloudProvider(cloud_provider.lower())
        except:
            pass
        
        # Check instance metadata (would need actual implementation)
        provider = ProviderDetector._check_instance_metadata()
        if provider:
            return provider
        
        logger.warning("Could not detect cloud provider, defaulting to AWS")
        return CloudProvider.AWS
    
    @staticmethod
    def _check_instance_metadata() -> Optional[CloudProvider]:
        """Check instance metadata to determine cloud provider"""
        # This would contain actual HTTP calls to metadata services
        # For now, return None to indicate no detection
        return None
    
    @staticmethod
    def detect_environment_context() -> EnvironmentContext:
        """Detect complete environment context"""
        
        cloud_provider = ProviderDetector.detect_cloud_provider()
        
        # Detect region
        region = None
        if cloud_provider == CloudProvider.AWS:
            region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION')
        elif cloud_provider == CloudProvider.GCP:
            region = os.getenv('GOOGLE_CLOUD_REGION') or os.getenv('GCP_REGION')
        elif cloud_provider == CloudProvider.AZURE:
            region = os.getenv('AZURE_REGION')
        elif cloud_provider == CloudProvider.OCI:
            region = os.getenv('OCI_REGION')
        
        # Get environment from various sources
        environment = (
            os.getenv('ENVIRONMENT') or 
            os.getenv('ENV') or 
            Variable.get("ENVIRONMENT", default_var="development")
        )
        
        # Try to load from Airflow Variable
        try:
            env_config = Variable.get("INSIGHTAIR_ENV_CONFIG", deserialize_json=True, default_var={})
            if env_config:
                return EnvironmentContext.from_config(env_config)
        except:
            pass
        
        return EnvironmentContext(
            cloud_provider=cloud_provider,
            region=region,
            environment=environment
        )


class ConnectionResolver:
    """Resolves connections based on environment and task requirements"""
    
    def __init__(self, env_context: EnvironmentContext):
        self.env_context = env_context
    
    def resolve_database_connection(self, 
                                  connection_id: Optional[str] = None,
                                  database_type: Optional[DatabaseType] = None) -> str:
        """Resolve database connection ID based on environment"""
        
        if connection_id:
            # Check if connection_id is a pattern that needs resolution
            resolved_id = self._resolve_connection_pattern(connection_id)
            if resolved_id != connection_id:
                return resolved_id
            
            # Check if connection exists
            if self._connection_exists(connection_id):
                return connection_id
        
        # Auto-resolve based on database type and cloud provider
        db_type = database_type or self.env_context.default_database_type
        if not db_type:
            raise ValueError("Database type not specified and no default configured")
        
        # Generate connection ID based on patterns
        connection_patterns = {
            (CloudProvider.AWS, DatabaseType.POSTGRESQL): "postgres_aws_default",
            (CloudProvider.AWS, DatabaseType.MYSQL): "mysql_aws_default", 
            (CloudProvider.AWS, DatabaseType.REDSHIFT): "redshift_default",
            (CloudProvider.GCP, DatabaseType.POSTGRESQL): "postgres_gcp_default",
            (CloudProvider.GCP, DatabaseType.MYSQL): "mysql_gcp_default",
            (CloudProvider.GCP, DatabaseType.BIGQUERY): "bigquery_default",
            (CloudProvider.AZURE, DatabaseType.POSTGRESQL): "postgres_azure_default",
            (CloudProvider.AZURE, DatabaseType.SQLSERVER): "sqlserver_azure_default",
            (CloudProvider.AZURE, DatabaseType.SYNAPSE): "synapse_default",
            (CloudProvider.OCI, DatabaseType.ORACLE): "oracle_oci_default",
            (CloudProvider.OCI, DatabaseType.AUTONOMOUS_DB): "autonomous_db_default",
        }
        
        pattern_key = (self.env_context.cloud_provider, db_type)
        resolved_connection = connection_patterns.get(pattern_key)
        
        if not resolved_connection:
            # Fallback to generic pattern
            resolved_connection = f"{db_type.value}_{self.env_context.cloud_provider.value}_default"
        
        logger.info(f"Resolved database connection: {resolved_connection} for {db_type.value} on {self.env_context.cloud_provider.value}")
        return resolved_connection
    
    def resolve_storage_connection(self,
                                 connection_id: Optional[str] = None,
                                 storage_type: Optional[StorageType] = None) -> str:
        """Resolve storage connection ID based on environment"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        storage_type = storage_type or self.env_context.default_storage_type
        
        # Auto-resolve based on cloud provider
        if not storage_type:
            storage_type_mapping = {
                CloudProvider.AWS: StorageType.S3,
                CloudProvider.GCP: StorageType.GCS,
                CloudProvider.AZURE: StorageType.AZURE_BLOB,
                CloudProvider.OCI: StorageType.OCI_OBJECT,
            }
            storage_type = storage_type_mapping.get(self.env_context.cloud_provider, StorageType.S3)
        
        connection_patterns = {
            StorageType.S3: "aws_default",
            StorageType.GCS: "google_cloud_default",
            StorageType.AZURE_BLOB: "azure_default",
            StorageType.OCI_OBJECT: "oci_default",
        }
        
        resolved_connection = connection_patterns.get(storage_type, "aws_default")
        logger.info(f"Resolved storage connection: {resolved_connection} for {storage_type.value}")
        return resolved_connection
    
    def resolve_messaging_connection(self,
                                   connection_id: Optional[str] = None,
                                   messaging_type: Optional[MessagingType] = None) -> str:
        """Resolve messaging system connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        messaging_type = messaging_type or self.env_context.default_messaging_type
        
        if not messaging_type:
            # Auto-resolve based on cloud provider
            messaging_type_mapping = {
                CloudProvider.AWS: MessagingType.SQS,
                CloudProvider.GCP: MessagingType.PUBSUB,
                CloudProvider.AZURE: MessagingType.SERVICE_BUS,
                CloudProvider.OCI: MessagingType.KAFKA,  # OCI often uses Kafka
            }
            messaging_type = messaging_type_mapping.get(self.env_context.cloud_provider, MessagingType.KAFKA)
        
        connection_patterns = {
            MessagingType.KAFKA: "kafka_default",
            MessagingType.PUBSUB: "google_cloud_default",
            MessagingType.SERVICE_BUS: "azure_default",
            MessagingType.SQS: "aws_default",
            MessagingType.KINESIS: "aws_default",
        }
        
        resolved_connection = connection_patterns.get(messaging_type, "kafka_default")
        logger.info(f"Resolved messaging connection: {resolved_connection} for {messaging_type.value}")
        return resolved_connection
    
    def resolve_api_connection(self, connection_id: Optional[str] = None) -> str:
        """Resolve API connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        # Default API connection patterns
        connection_patterns = {
            CloudProvider.AWS: "aws_api_default",
            CloudProvider.GCP: "gcp_api_default",
            CloudProvider.AZURE: "azure_api_default",
            CloudProvider.OCI: "oci_api_default",
        }
        
        resolved_connection = connection_patterns.get(self.env_context.cloud_provider, "http_default")
        logger.info(f"Resolved API connection: {resolved_connection}")
        return resolved_connection
    
    def resolve_transfer_connection(self, connection_id: Optional[str] = None) -> str:
        """Resolve file transfer (SFTP/FTP) connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        # Default transfer connection
        resolved_connection = "sftp_default"
        logger.info(f"Resolved transfer connection: {resolved_connection}")
        return resolved_connection
    
    def resolve_email_connection(self, connection_id: Optional[str] = None) -> str:
        """Resolve email connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        # Default email connection
        resolved_connection = "smtp_default"
        logger.info(f"Resolved email connection: {resolved_connection}")
        return resolved_connection
    
    def resolve_slack_connection(self, connection_id: Optional[str] = None) -> str:
        """Resolve Slack connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        # Default Slack connection
        resolved_connection = "slack_default"
        logger.info(f"Resolved Slack connection: {resolved_connection}")
        return resolved_connection
    
    def resolve_teams_connection(self, connection_id: Optional[str] = None) -> str:
        """Resolve Microsoft Teams connection ID"""
        
        if connection_id and self._connection_exists(connection_id):
            return connection_id
        
        # Default Teams connection
        resolved_connection = "teams_default"
        logger.info(f"Resolved Teams connection: {resolved_connection}")
        return resolved_connection
    
    def _resolve_connection_pattern(self, connection_pattern: str) -> str:
        """Resolve connection patterns like {cloud_provider}_database_default"""
        
        replacements = {
            '{cloud_provider}': self.env_context.cloud_provider.value,
            '{environment}': self.env_context.environment,
            '{region}': self.env_context.region or 'default',
        }
        
        resolved = connection_pattern
        for pattern, replacement in replacements.items():
            resolved = resolved.replace(pattern, replacement)
        
        return resolved
    
    def _connection_exists(self, connection_id: str) -> bool:
        """Check if Airflow connection exists"""
        try:
            BaseHook.get_connection(connection_id)
            return True
        except:
            return False


class GenericTaskTypeRegistry:
    """Registry for generic task types and their implementations"""
    
    def __init__(self):
        self._implementations: Dict[str, Dict[str, Type]] = {}
        self._task_type_mappings: Dict[str, str] = {}
    
    def register_implementation(self,
                              task_type: str,  # e.g., "DATABASE_QUERY"
                              provider_key: str,  # e.g., "aws_postgresql"
                              implementation_class: Type):
        """Register implementation for specific provider/database combination"""
        
        if task_type not in self._implementations:
            self._implementations[task_type] = {}
        
        self._implementations[task_type][provider_key] = implementation_class
        logger.debug(f"Registered {implementation_class.__name__} for {task_type}:{provider_key}")
    
    def register_task_type_mapping(self, generic_type: str, specific_type: str):
        """Register mapping from generic type to specific implementation type"""
        self._task_type_mappings[generic_type] = specific_type
    
    def get_implementation(self,
                          task_type: str,
                          env_context: EnvironmentContext,
                          task_properties: Dict[str, Any]) -> Type:
        """Get appropriate implementation class for task type and environment"""
        
        # Check if we have implementations for this task type
        if task_type not in self._implementations:
            raise ValueError(f"No implementations registered for task type: {task_type}")
        
        implementations = self._implementations[task_type]
        
        # Generate provider keys in order of preference
        provider_keys = self._generate_provider_keys(task_type, env_context, task_properties)
        
        # Find best matching implementation
        for key in provider_keys:
            if key in implementations:
                logger.info(f"Using implementation {implementations[key].__name__} for {task_type}:{key}")
                return implementations[key]
        
        # Fallback to any available implementation
        if implementations:
            fallback_impl = next(iter(implementations.values()))
            logger.warning(f"Using fallback implementation {fallback_impl.__name__} for {task_type}")
            return fallback_impl
        
        raise ValueError(f"No suitable implementation found for task type: {task_type}")
    
    def _generate_provider_keys(self,
                               task_type: str,
                               env_context: EnvironmentContext,
                               task_properties: Dict[str, Any]) -> List[str]:
        """Generate provider keys in order of preference"""
        
        keys = []
        cloud_provider = env_context.cloud_provider.value
        
        # Task-specific provider keys
        if task_type.startswith("DATABASE_"):
            # Database operations
            db_type = task_properties.get('database_type')
            if db_type:
                keys.append(f"{cloud_provider}_{db_type}")
            
            if env_context.default_database_type:
                keys.append(f"{cloud_provider}_{env_context.default_database_type.value}")
            
            # Add generic database keys
            keys.extend([
                f"{cloud_provider}_database",
                "generic_database"
            ])
            
        elif task_type.startswith("STORAGE_"):
            # Storage operations
            storage_type = task_properties.get('storage_type')
            if storage_type:
                keys.append(f"{cloud_provider}_{storage_type}")
            
            if env_context.default_storage_type:
                keys.append(f"{cloud_provider}_{env_context.default_storage_type.value}")
            
            keys.extend([
                f"{cloud_provider}_storage",
                "generic_storage"
            ])
            
        elif task_type.startswith("MESSAGING_"):
            # Messaging operations
            messaging_type = task_properties.get('messaging_type')
            if messaging_type:
                keys.append(f"{cloud_provider}_{messaging_type}")
            
            if env_context.default_messaging_type:
                keys.append(f"{cloud_provider}_{env_context.default_messaging_type.value}")
            
            keys.extend([
                f"{cloud_provider}_messaging",
                "generic_messaging"
            ])
        
        # Add cloud provider generic key
        keys.append(cloud_provider)
        
        # Add completely generic key
        keys.append("generic")
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(keys))
    
    def get_all_implementations(self) -> Dict[str, Dict[str, Type]]:
        """Get all registered implementations"""
        return self._implementations.copy()
    
    def get_supported_task_types(self) -> List[str]:
        """Get list of supported task types"""
        return list(self._implementations.keys())


# Global instances
_environment_context: Optional[EnvironmentContext] = None
_connection_resolver: Optional[ConnectionResolver] = None
_task_type_registry: Optional[GenericTaskTypeRegistry] = None


def get_environment_context() -> EnvironmentContext:
    """Get global environment context"""
    global _environment_context
    if _environment_context is None:
        _environment_context = ProviderDetector.detect_environment_context()
    return _environment_context


def get_connection_resolver() -> ConnectionResolver:
    """Get global connection resolver"""
    global _connection_resolver
    if _connection_resolver is None:
        _connection_resolver = ConnectionResolver(get_environment_context())
    return _connection_resolver


def get_task_type_registry() -> GenericTaskTypeRegistry:
    """Get global task type registry"""
    global _task_type_registry
    if _task_type_registry is None:
        _task_type_registry = GenericTaskTypeRegistry()
    return _task_type_registry


def configure_environment(config: Dict[str, Any]):
    """Configure environment context from configuration"""
    global _environment_context, _connection_resolver
    _environment_context = EnvironmentContext.from_config(config)
    _connection_resolver = ConnectionResolver(_environment_context)
    logger.info(f"Environment configured for {_environment_context.cloud_provider.value}")


def register_generic_implementations():
    """Register all generic task type implementations"""
    # This will be called by individual modules to register their implementations
    registry = get_task_type_registry()
    
    # Import and register all implementations
    try:
        from ..operators.generic import database_operations
        database_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Database operations not available: {e}")
    
    try:
        from ..operators.generic import storage_operations
        storage_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Storage operations not available: {e}")
    
    try:
        from ..operators.generic import messaging_operations
        messaging_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Messaging operations not available: {e}")
    
    try:
        from ..operators.generic import tableau_operations
        tableau_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Tableau operations not available: {e}")
    
    try:
        from ..operators.generic import salesforce_operations
        salesforce_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Salesforce operations not available: {e}")
    
    try:
        from ..operators.generic import api_operations
        api_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"API operations not available: {e}")
    
    try:
        from ..operators.generic import transfer_operations
        transfer_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Transfer operations not available: {e}")
    
    try:
        from ..operators.generic import data_quality_operations
        data_quality_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Data quality operations not available: {e}")
    
    try:
        from ..operators.generic import archive_operations
        archive_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Archive operations not available: {e}")
    
    try:
        from ..operators.generic import notification_operations
        notification_operations.register_implementations(registry)
    except ImportError as e:
        logger.debug(f"Notification operations not available: {e}")


# Initialize on import
register_generic_implementations()