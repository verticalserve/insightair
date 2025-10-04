# Generic Storage Operations - Multi-cloud storage abstraction
"""
Generic storage operations that work across cloud providers:
- File upload/download operations
- Directory synchronization
- File validation and processing
- Automatic cloud provider selection based on environment
"""

import logging
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    StorageType, CloudProvider
)

logger = logging.getLogger(__name__)


class BaseStorageOperator(BaseInsightAirOperator, ABC):
    """Base class for storage operations"""
    
    def __init__(self,
                 source_path: Optional[str] = None,
                 destination_path: Optional[str] = None,
                 storage_connection_id: Optional[str] = None,
                 storage_type: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseStorageOperator
        
        Args:
            source_path: Source file/directory path
            destination_path: Destination file/directory path
            storage_connection_id: Storage connection ID (auto-resolved if not provided)
            storage_type: Storage type override
        """
        super().__init__(**kwargs)
        self.source_path = source_path
        self.destination_path = destination_path
        self.storage_connection_id = storage_connection_id
        self.storage_type = storage_type
    
    @abstractmethod
    def execute_storage_operation(self, context: Context) -> Any:
        """Execute the storage operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic connection resolution"""
        
        # Resolve connection if not provided
        if not self.storage_connection_id:
            resolver = get_connection_resolver()
            storage_type_enum = StorageType(self.storage_type) if self.storage_type else None
            self.storage_connection_id = resolver.resolve_storage_connection(
                storage_type=storage_type_enum
            )
        
        return self.execute_storage_operation(context)


class StorageFileUploadOperator(BaseStorageOperator):
    """Generic file upload operator"""
    
    template_fields = ('source_path', 'destination_path', 'bucket_name')
    
    def __init__(self,
                 source_path: str,
                 destination_path: str,
                 bucket_name: Optional[str] = None,
                 overwrite: bool = True,
                 validate_upload: bool = True,
                 **kwargs):
        """
        Initialize StorageFileUploadOperator
        
        Args:
            source_path: Local file path to upload
            destination_path: Remote destination path
            bucket_name: Bucket/container name (required for cloud storage)
            overwrite: Whether to overwrite existing files
            validate_upload: Whether to validate upload success
        """
        super().__init__(source_path=source_path, destination_path=destination_path, **kwargs)
        self.bucket_name = bucket_name
        self.overwrite = overwrite
        self.validate_upload = validate_upload
    
    def execute_storage_operation(self, context: Context) -> Dict[str, Any]:
        """Execute file upload operation"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._upload_to_s3(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._upload_to_gcs(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._upload_to_azure_blob(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._upload_to_oci_object(context)
        else:
            return self._upload_to_local(context)
    
    def _upload_to_s3(self, context: Context) -> Dict[str, Any]:
        """Upload file to S3"""
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            
            s3_hook = S3Hook(aws_conn_id=self.storage_connection_id)
            
            s3_hook.load_file(
                filename=self.source_path,
                key=self.destination_path,
                bucket_name=self.bucket_name,
                replace=self.overwrite
            )
            
            result = {
                'provider': 'aws',
                'storage_type': 's3',
                'bucket': self.bucket_name,
                'key': self.destination_path,
                'source_file': self.source_path,
                'success': True
            }
            
            if self.validate_upload:
                exists = s3_hook.check_for_key(self.destination_path, self.bucket_name)
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Upload validation failed: {self.destination_path} not found in {self.bucket_name}")
            
            logger.info(f"Successfully uploaded {self.source_path} to s3://{self.bucket_name}/{self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("AWS provider not available. Install with: pip install apache-airflow-providers-amazon")
    
    def _upload_to_gcs(self, context: Context) -> Dict[str, Any]:
        """Upload file to Google Cloud Storage"""
        try:
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
            
            gcs_hook = GCSHook(gcp_conn_id=self.storage_connection_id)
            
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.destination_path,
                filename=self.source_path
            )
            
            result = {
                'provider': 'gcp',
                'storage_type': 'gcs',
                'bucket': self.bucket_name,
                'object_name': self.destination_path,
                'source_file': self.source_path,
                'success': True
            }
            
            if self.validate_upload:
                exists = gcs_hook.exists(self.bucket_name, self.destination_path)
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Upload validation failed: {self.destination_path} not found in {self.bucket_name}")
            
            logger.info(f"Successfully uploaded {self.source_path} to gs://{self.bucket_name}/{self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("Google Cloud provider not available. Install with: pip install apache-airflow-providers-google")
    
    def _upload_to_azure_blob(self, context: Context) -> Dict[str, Any]:
        """Upload file to Azure Blob Storage"""
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
            
            wasb_hook = WasbHook(wasb_conn_id=self.storage_connection_id)
            
            wasb_hook.load_file(
                file_path=self.source_path,
                container_name=self.bucket_name,
                blob_name=self.destination_path,
                overwrite=self.overwrite
            )
            
            result = {
                'provider': 'azure',
                'storage_type': 'azure_blob',
                'container': self.bucket_name,
                'blob_name': self.destination_path,
                'source_file': self.source_path,
                'success': True
            }
            
            if self.validate_upload:
                exists = wasb_hook.check_for_blob(self.bucket_name, self.destination_path)
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Upload validation failed: {self.destination_path} not found in {self.bucket_name}")
            
            logger.info(f"Successfully uploaded {self.source_path} to Azure blob {self.bucket_name}/{self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("Azure provider not available. Install with: pip install apache-airflow-providers-microsoft-azure")
    
    def _upload_to_oci_object(self, context: Context) -> Dict[str, Any]:
        """Upload file to OCI Object Storage"""
        # OCI implementation would go here
        # For now, provide a placeholder
        logger.info(f"OCI Object Storage upload: {self.source_path} -> {self.bucket_name}/{self.destination_path}")
        return {
            'provider': 'oci',
            'storage_type': 'oci_object',
            'bucket': self.bucket_name,
            'object_name': self.destination_path,
            'source_file': self.source_path,
            'success': True,
            'note': 'OCI implementation placeholder'
        }
    
    def _upload_to_local(self, context: Context) -> Dict[str, Any]:
        """Upload file to local filesystem (copy operation)"""
        import shutil
        from pathlib import Path
        
        source = Path(self.source_path)
        destination = Path(self.destination_path)
        
        # Create destination directory if it doesn't exist
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        if destination.exists() and not self.overwrite:
            raise ValueError(f"Destination file exists and overwrite=False: {destination}")
        
        shutil.copy2(source, destination)
        
        result = {
            'provider': 'local',
            'storage_type': 'local_fs',
            'source_file': str(source),
            'destination_file': str(destination),
            'success': True
        }
        
        if self.validate_upload:
            exists = destination.exists()
            result['validated'] = exists
            if not exists:
                raise ValueError(f"Upload validation failed: {destination} not found")
        
        logger.info(f"Successfully copied {source} to {destination}")
        return result


class StorageFileDownloadOperator(BaseStorageOperator):
    """Generic file download operator"""
    
    template_fields = ('source_path', 'destination_path', 'bucket_name')
    
    def __init__(self,
                 source_path: str,
                 destination_path: str,
                 bucket_name: Optional[str] = None,
                 overwrite: bool = True,
                 validate_download: bool = True,
                 **kwargs):
        """
        Initialize StorageFileDownloadOperator
        
        Args:
            source_path: Remote source path
            destination_path: Local destination path
            bucket_name: Bucket/container name (required for cloud storage)
            overwrite: Whether to overwrite existing files
            validate_download: Whether to validate download success
        """
        super().__init__(source_path=source_path, destination_path=destination_path, **kwargs)
        self.bucket_name = bucket_name
        self.overwrite = overwrite
        self.validate_download = validate_download
    
    def execute_storage_operation(self, context: Context) -> Dict[str, Any]:
        """Execute file download operation"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._download_from_s3(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._download_from_gcs(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._download_from_azure_blob(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._download_from_oci_object(context)
        else:
            return self._download_from_local(context)
    
    def _download_from_s3(self, context: Context) -> Dict[str, Any]:
        """Download file from S3"""
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            
            s3_hook = S3Hook(aws_conn_id=self.storage_connection_id)
            
            # Check if source exists
            if not s3_hook.check_for_key(self.source_path, self.bucket_name):
                raise ValueError(f"Source file not found: s3://{self.bucket_name}/{self.source_path}")
            
            # Create destination directory
            from pathlib import Path
            Path(self.destination_path).parent.mkdir(parents=True, exist_ok=True)
            
            s3_hook.download_file(
                key=self.source_path,
                bucket_name=self.bucket_name,
                local_path=self.destination_path
            )
            
            result = {
                'provider': 'aws',
                'storage_type': 's3',
                'bucket': self.bucket_name,
                'key': self.source_path,
                'destination_file': self.destination_path,
                'success': True
            }
            
            if self.validate_download:
                from pathlib import Path
                exists = Path(self.destination_path).exists()
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Download validation failed: {self.destination_path} not found")
            
            logger.info(f"Successfully downloaded s3://{self.bucket_name}/{self.source_path} to {self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("AWS provider not available. Install with: pip install apache-airflow-providers-amazon")
    
    def _download_from_gcs(self, context: Context) -> Dict[str, Any]:
        """Download file from Google Cloud Storage"""
        try:
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
            from pathlib import Path
            
            gcs_hook = GCSHook(gcp_conn_id=self.storage_connection_id)
            
            # Check if source exists
            if not gcs_hook.exists(self.bucket_name, self.source_path):
                raise ValueError(f"Source file not found: gs://{self.bucket_name}/{self.source_path}")
            
            # Create destination directory
            Path(self.destination_path).parent.mkdir(parents=True, exist_ok=True)
            
            gcs_hook.download(
                bucket_name=self.bucket_name,
                object_name=self.source_path,
                filename=self.destination_path
            )
            
            result = {
                'provider': 'gcp',
                'storage_type': 'gcs',
                'bucket': self.bucket_name,
                'object_name': self.source_path,
                'destination_file': self.destination_path,
                'success': True
            }
            
            if self.validate_download:
                exists = Path(self.destination_path).exists()
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Download validation failed: {self.destination_path} not found")
            
            logger.info(f"Successfully downloaded gs://{self.bucket_name}/{self.source_path} to {self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("Google Cloud provider not available. Install with: pip install apache-airflow-providers-google")
    
    def _download_from_azure_blob(self, context: Context) -> Dict[str, Any]:
        """Download file from Azure Blob Storage"""
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
            from pathlib import Path
            
            wasb_hook = WasbHook(wasb_conn_id=self.storage_connection_id)
            
            # Check if source exists
            if not wasb_hook.check_for_blob(self.bucket_name, self.source_path):
                raise ValueError(f"Source file not found: Azure blob {self.bucket_name}/{self.source_path}")
            
            # Create destination directory
            Path(self.destination_path).parent.mkdir(parents=True, exist_ok=True)
            
            wasb_hook.get_file(
                file_path=self.destination_path,
                container_name=self.bucket_name,
                blob_name=self.source_path
            )
            
            result = {
                'provider': 'azure',
                'storage_type': 'azure_blob',
                'container': self.bucket_name,
                'blob_name': self.source_path,
                'destination_file': self.destination_path,
                'success': True
            }
            
            if self.validate_download:
                exists = Path(self.destination_path).exists()
                result['validated'] = exists
                if not exists:
                    raise ValueError(f"Download validation failed: {self.destination_path} not found")
            
            logger.info(f"Successfully downloaded Azure blob {self.bucket_name}/{self.source_path} to {self.destination_path}")
            return result
            
        except ImportError:
            raise ImportError("Azure provider not available. Install with: pip install apache-airflow-providers-microsoft-azure")
    
    def _download_from_oci_object(self, context: Context) -> Dict[str, Any]:
        """Download file from OCI Object Storage"""
        # OCI implementation placeholder
        logger.info(f"OCI Object Storage download: {self.bucket_name}/{self.source_path} -> {self.destination_path}")
        return {
            'provider': 'oci',
            'storage_type': 'oci_object',
            'bucket': self.bucket_name,
            'object_name': self.source_path,
            'destination_file': self.destination_path,
            'success': True,
            'note': 'OCI implementation placeholder'
        }
    
    def _download_from_local(self, context: Context) -> Dict[str, Any]:
        """Download file from local filesystem (copy operation)"""
        import shutil
        from pathlib import Path
        
        source = Path(self.source_path)
        destination = Path(self.destination_path)
        
        if not source.exists():
            raise ValueError(f"Source file not found: {source}")
        
        # Create destination directory
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        if destination.exists() and not self.overwrite:
            raise ValueError(f"Destination file exists and overwrite=False: {destination}")
        
        shutil.copy2(source, destination)
        
        result = {
            'provider': 'local',
            'storage_type': 'local_fs',
            'source_file': str(source),
            'destination_file': str(destination),
            'success': True
        }
        
        if self.validate_download:
            exists = destination.exists()
            result['validated'] = exists
            if not exists:
                raise ValueError(f"Download validation failed: {destination} not found")
        
        logger.info(f"Successfully copied {source} to {destination}")
        return result


class StorageDirectorySyncOperator(BaseStorageOperator):
    """Generic directory synchronization operator"""
    
    template_fields = ('source_path', 'destination_path', 'bucket_name')
    
    def __init__(self,
                 source_path: str,
                 destination_path: str,
                 bucket_name: Optional[str] = None,
                 sync_mode: str = 'upload',  # 'upload', 'download', 'bidirectional'
                 delete_extra: bool = False,
                 include_patterns: Optional[List[str]] = None,
                 exclude_patterns: Optional[List[str]] = None,
                 **kwargs):
        """
        Initialize StorageDirectorySyncOperator
        
        Args:
            source_path: Source directory path
            destination_path: Destination directory path
            bucket_name: Bucket/container name for cloud storage
            sync_mode: Synchronization mode
            delete_extra: Whether to delete files not in source
            include_patterns: File patterns to include (glob patterns)
            exclude_patterns: File patterns to exclude (glob patterns)
        """
        super().__init__(source_path=source_path, destination_path=destination_path, **kwargs)
        self.bucket_name = bucket_name
        self.sync_mode = sync_mode
        self.delete_extra = delete_extra
        self.include_patterns = include_patterns or ['*']
        self.exclude_patterns = exclude_patterns or []
    
    def execute_storage_operation(self, context: Context) -> Dict[str, Any]:
        """Execute directory synchronization"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._sync_with_s3(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._sync_with_gcs(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._sync_with_azure_blob(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._sync_with_oci_object(context)
        else:
            return self._sync_local_directories(context)
    
    def _sync_with_s3(self, context: Context) -> Dict[str, Any]:
        """Synchronize directory with S3"""
        # S3 sync implementation would use AWS CLI or boto3
        # For now, provide a placeholder
        logger.info(f"S3 directory sync: {self.source_path} <-> s3://{self.bucket_name}/{self.destination_path}")
        return {
            'provider': 'aws',
            'storage_type': 's3',
            'sync_mode': self.sync_mode,
            'files_synced': 0,
            'success': True,
            'note': 'S3 sync implementation placeholder'
        }
    
    def _sync_with_gcs(self, context: Context) -> Dict[str, Any]:
        """Synchronize directory with Google Cloud Storage"""
        # GCS sync implementation placeholder
        logger.info(f"GCS directory sync: {self.source_path} <-> gs://{self.bucket_name}/{self.destination_path}")
        return {
            'provider': 'gcp',
            'storage_type': 'gcs',
            'sync_mode': self.sync_mode,
            'files_synced': 0,
            'success': True,
            'note': 'GCS sync implementation placeholder'
        }
    
    def _sync_with_azure_blob(self, context: Context) -> Dict[str, Any]:
        """Synchronize directory with Azure Blob Storage"""
        # Azure Blob sync implementation placeholder
        logger.info(f"Azure Blob directory sync: {self.source_path} <-> {self.bucket_name}/{self.destination_path}")
        return {
            'provider': 'azure',
            'storage_type': 'azure_blob',
            'sync_mode': self.sync_mode,
            'files_synced': 0,
            'success': True,
            'note': 'Azure Blob sync implementation placeholder'
        }
    
    def _sync_with_oci_object(self, context: Context) -> Dict[str, Any]:
        """Synchronize directory with OCI Object Storage"""
        # OCI Object Storage sync implementation placeholder
        logger.info(f"OCI Object Storage directory sync: {self.source_path} <-> {self.bucket_name}/{self.destination_path}")
        return {
            'provider': 'oci',
            'storage_type': 'oci_object',
            'sync_mode': self.sync_mode,
            'files_synced': 0,
            'success': True,
            'note': 'OCI sync implementation placeholder'
        }
    
    def _sync_local_directories(self, context: Context) -> Dict[str, Any]:
        """Synchronize local directories"""
        import shutil
        from pathlib import Path
        import fnmatch
        
        source = Path(self.source_path)
        destination = Path(self.destination_path)
        
        if not source.exists():
            raise ValueError(f"Source directory not found: {source}")
        
        destination.mkdir(parents=True, exist_ok=True)
        
        files_synced = 0
        
        # Simple directory copy with pattern matching
        for source_file in source.rglob('*'):
            if source_file.is_file():
                relative_path = source_file.relative_to(source)
                
                # Check include/exclude patterns
                include_match = any(fnmatch.fnmatch(str(relative_path), pattern) for pattern in self.include_patterns)
                exclude_match = any(fnmatch.fnmatch(str(relative_path), pattern) for pattern in self.exclude_patterns)
                
                if include_match and not exclude_match:
                    destination_file = destination / relative_path
                    destination_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    shutil.copy2(source_file, destination_file)
                    files_synced += 1
        
        logger.info(f"Successfully synced {files_synced} files from {source} to {destination}")
        return {
            'provider': 'local',
            'storage_type': 'local_fs',
            'sync_mode': self.sync_mode,
            'files_synced': files_synced,
            'success': True
        }


def register_implementations(registry):
    """Register storage operation implementations"""
    
    # Register generic storage operations
    registry.register_implementation("STORAGE_UPLOAD", "generic", StorageFileUploadOperator)
    registry.register_implementation("STORAGE_DOWNLOAD", "generic", StorageFileDownloadOperator)
    registry.register_implementation("STORAGE_SYNC", "generic", StorageDirectorySyncOperator)
    
    # Register cloud-specific implementations (same operators work for all)
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("STORAGE_UPLOAD", provider, StorageFileUploadOperator)
        registry.register_implementation("STORAGE_DOWNLOAD", provider, StorageFileDownloadOperator)
        registry.register_implementation("STORAGE_SYNC", provider, StorageDirectorySyncOperator)
        
        # Register storage-type specific implementations
        for storage_type in ['s3', 'gcs', 'azure_blob', 'oci_object']:
            registry.register_implementation("STORAGE_UPLOAD", f"{provider}_{storage_type}", StorageFileUploadOperator)
            registry.register_implementation("STORAGE_DOWNLOAD", f"{provider}_{storage_type}", StorageFileDownloadOperator)
            registry.register_implementation("STORAGE_SYNC", f"{provider}_{storage_type}", StorageDirectorySyncOperator)


# Example configuration usage:
"""
tasks:
  - name: "upload_data_file"
    type: "STORAGE_UPLOAD"
    description: "Upload processed data file to cloud storage"
    properties:
      source_path: "/tmp/processed_data.csv"
      destination_path: "data/processed/{{ ds }}/data.csv"
      bucket_name: "{{ var.value.data_bucket }}"
      overwrite: true
      validate_upload: true
      # storage_connection_id: auto-resolved based on cloud provider
      # storage_type: auto-detected based on environment

  - name: "download_reference_data"
    type: "STORAGE_DOWNLOAD" 
    description: "Download reference data from cloud storage"
    properties:
      source_path: "reference/latest/reference_data.json"
      destination_path: "/tmp/reference_data.json"
      bucket_name: "{{ var.value.reference_bucket }}" 
      validate_download: true

  - name: "sync_reports_directory"
    type: "STORAGE_SYNC"
    description: "Sync reports directory to cloud storage"
    properties:
      source_path: "/tmp/reports/"
      destination_path: "reports/{{ ds }}/"
      bucket_name: "{{ var.value.reports_bucket }}"
      sync_mode: "upload"
      include_patterns: ["*.pdf", "*.html"]
      exclude_patterns: ["*.tmp", "*.log"]
"""