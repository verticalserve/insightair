"""
GCP Cloud Storage File Watcher
Provides file monitoring operations for Google Cloud Platform Cloud Storage
Supports both specific file monitoring and pattern-based searches
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import fnmatch

from .base import BaseFileWatcher, TimeOutError

logger = logging.getLogger(__name__)

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP libraries not available: {e}")
    GCP_AVAILABLE = False


class GCPCloudStorageWatcher(BaseFileWatcher):
    """
    GCP Cloud Storage file watcher class
    Monitors GCS buckets for file arrivals with pattern matching support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Cloud Storage file watcher
        
        Args:
            config: Configuration dictionary containing GCP settings
                - bucket_name: Cloud Storage bucket name (None for local file system)
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON file (optional)
                - credentials: Service account credentials dict (optional)
        """
        super().__init__(config)
        
        self.bucket_name = config.get('bucket_name')
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.credentials = config.get('credentials')
        
        # Initialize GCP client
        self.storage_client = None
        self.bucket = None
        
        if GCP_AVAILABLE and self.bucket_name:
            try:
                self._initialize_gcp_client()
            except Exception as e:
                logger.error(f"Failed to initialize GCP Cloud Storage client: {e}")
                raise
        elif not self.bucket_name:
            logger.info("Bucket name not specified, using local file system monitoring")
        else:
            logger.warning("GCP libraries not available, falling back to local file system")
            
    def _initialize_gcp_client(self):
        """
        Initialize GCP Cloud Storage client
        """
        try:
            # Initialize client with different credential methods
            if self.credentials_path:
                # Use service account file
                self.storage_client = storage.Client.from_service_account_json(
                    json_credentials_path=self.credentials_path,
                    project=self.project_id
                )
            elif self.credentials:
                # Use credentials dict
                self.storage_client = storage.Client.from_service_account_info(
                    info=self.credentials,
                    project=self.project_id
                )
            else:
                # Use default credentials (environment variables, metadata server, etc.)
                self.storage_client = storage.Client(project=self.project_id)
                
            # Get bucket reference
            self.bucket = self.storage_client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist")
                
            logger.info(f"GCP Cloud Storage client initialized successfully for bucket: {self.bucket_name}")
            
        except DefaultCredentialsError:
            logger.error("GCP credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or provide credentials")
            raise
        except NotFound:
            logger.error(f"GCS bucket '{self.bucket_name}' not found")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing GCP client: {e}")
            raise
            
    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a specific file exists in GCS bucket or local file system
        
        Args:
            file_path: Path to the file (blob name or local path)
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.bucket_name:
            # Use local file system
            return self.check_local_file_exists(file_path)
            
        if not self.bucket:
            logger.error("GCS bucket not initialized")
            return False
            
        try:
            # Remove leading slash if present
            blob_name = file_path.lstrip('/')
            
            # Check if blob exists
            blob = self.bucket.blob(blob_name)
            exists = blob.exists()
            
            logger.debug(f"GCS blob exists check: gs://{self.bucket_name}/{blob_name} = {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking GCS blob existence: {e}")
            return False
            
    def search_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files in GCS bucket using a pattern
        
        Args:
            pattern: Pattern to search for (supports wildcards)
            
        Returns:
            List of matching blob names or local file paths
        """
        if not self.bucket_name:
            # Use local file system
            return self.search_local_files_by_pattern(pattern)
            
        if not self.bucket:
            logger.error("GCS bucket not initialized")
            return []
            
        try:
            matching_files = []
            
            # Remove leading slash and extract prefix
            pattern = pattern.lstrip('/')
            
            # Find the fixed prefix (part before first wildcard)
            wildcard_pos = min(
                (pattern.find(char) for char in ['*', '?', '['] if pattern.find(char) != -1),
                default=len(pattern)
            )
            prefix = pattern[:wildcard_pos].rsplit('/', 1)[0] if '/' in pattern[:wildcard_pos] else ''
            
            # List blobs with prefix
            blobs = self.bucket.list_blobs(prefix=prefix)
            
            # Filter blobs matching pattern
            for blob in blobs:
                blob_name = blob.name
                
                # Check if blob name matches pattern
                if self._match_gcs_pattern(blob_name, pattern):
                    matching_files.append(blob_name)
                    
            logger.debug(f"GCS pattern search '{pattern}' found {len(matching_files)} files in bucket '{self.bucket_name}'")
            return matching_files
            
        except Exception as e:
            logger.error(f"Error searching GCS blobs with pattern '{pattern}': {e}")
            return []
            
    def _match_gcs_pattern(self, blob_name: str, pattern: str) -> bool:
        """
        Check if GCS blob name matches the given pattern
        
        Args:
            blob_name: GCS blob name
            pattern: Pattern with wildcards
            
        Returns:
            True if name matches pattern, False otherwise
        """
        try:
            # Use fnmatch for pattern matching
            return fnmatch.fnmatch(blob_name, pattern)
        except Exception as e:
            logger.error(f"Error matching pattern '{pattern}' against blob '{blob_name}': {e}")
            return False
            
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file in GCS or local file system
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        if not self.bucket_name:
            # Use local file system
            try:
                import os
                stat_info = os.stat(file_path)
                return {
                    'path': file_path,
                    'size': stat_info.st_size,
                    'modified': datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                    'is_file': os.path.isfile(file_path),
                    'exists': True
                }
            except Exception as e:
                logger.error(f"Error getting local file info: {e}")
                return {'path': file_path, 'exists': False, 'error': str(e)}
                
        if not self.bucket:
            return {'path': file_path, 'exists': False, 'error': 'GCS bucket not initialized'}
            
        try:
            blob_name = file_path.lstrip('/')
            
            # Get blob
            blob = self.bucket.blob(blob_name)
            
            if not blob.exists():
                return {
                    'bucket': self.bucket_name,
                    'blob_name': blob_name,
                    'exists': False
                }
                
            # Reload to get metadata
            blob.reload()
            
            return {
                'bucket': self.bucket_name,
                'blob_name': blob_name,
                'size': blob.size,
                'time_created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'etag': blob.etag,
                'content_type': blob.content_type,
                'storage_class': blob.storage_class,
                'generation': blob.generation,
                'metageneration': blob.metageneration,
                'exists': True
            }
            
        except Exception as e:
            logger.error(f"Error getting GCS blob info: {e}")
            return {
                'bucket': self.bucket_name,
                'blob_name': file_path,
                'exists': False,
                'error': str(e)
            }
            
    def list_blobs_in_prefix(self, prefix: str, max_blobs: int = 1000) -> List[Dict[str, Any]]:
        """
        List blobs in GCS bucket with given prefix
        
        Args:
            prefix: Blob name prefix to search under
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob information dictionaries
        """
        if not self.bucket_name:
            logger.error("No bucket specified for GCS operations")
            return []
            
        if not self.bucket:
            logger.error("GCS bucket not initialized")
            return []
            
        try:
            blobs = []
            prefix = prefix.lstrip('/')
            
            # List blobs with prefix
            blob_iterator = self.bucket.list_blobs(prefix=prefix, max_results=max_blobs)
            
            for blob in blob_iterator:
                blobs.append({
                    'name': blob.name,
                    'size': blob.size,
                    'time_created': blob.time_created.isoformat() if blob.time_created else None,
                    'updated': blob.updated.isoformat() if blob.updated else None,
                    'etag': blob.etag,
                    'content_type': blob.content_type,
                    'storage_class': blob.storage_class,
                    'generation': blob.generation
                })
                
                if len(blobs) >= max_blobs:
                    break
                    
            logger.debug(f"Listed {len(blobs)} blobs with prefix '{prefix}' in bucket '{self.bucket_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing GCS blobs with prefix '{prefix}': {e}")
            return []
            
    def download_blob(self, blob_name: str, local_path: str) -> bool:
        """
        Download a blob from GCS to local file system
        
        Args:
            blob_name: GCS blob name
            local_path: Local file path to save to
            
        Returns:
            True if download successful, False otherwise
        """
        if not self.bucket_name or not self.bucket:
            logger.error("GCS not configured for download operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            
            # Create local directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download blob
            blob = self.bucket.blob(blob_name)
            blob.download_to_filename(local_path)
            
            logger.info(f"Downloaded gs://{self.bucket_name}/{blob_name} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading GCS blob: {e}")
            return False
            
    def upload_blob(self, local_path: str, blob_name: str) -> bool:
        """
        Upload a file from local file system to GCS
        
        Args:
            local_path: Local file path
            blob_name: GCS blob name to upload to
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.bucket_name or not self.bucket:
            logger.error("GCS not configured for upload operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            
            # Upload blob
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            
            logger.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading blob to GCS: {e}")
            return False
            
    def get_bucket_info(self) -> Dict[str, Any]:
        """
        Get information about the GCS bucket
        
        Returns:
            Dictionary with bucket information
        """
        if not self.bucket_name or not self.bucket:
            return {'error': 'GCS not configured'}
            
        try:
            # Reload bucket to get latest info
            self.bucket.reload()
            
            return {
                'bucket_name': self.bucket_name,
                'project_id': self.project_id,
                'location': self.bucket.location,
                'storage_class': self.bucket.storage_class,
                'time_created': self.bucket.time_created.isoformat() if self.bucket.time_created else None,
                'updated': self.bucket.updated.isoformat() if self.bucket.updated else None,
                'versioning_enabled': self.bucket.versioning_enabled,
                'lifecycle_rules': len(self.bucket.lifecycle_rules) if self.bucket.lifecycle_rules else 0,
                'labels': dict(self.bucket.labels) if self.bucket.labels else {},
                'etag': self.bucket.etag,
                'metageneration': self.bucket.metageneration
            }
            
        except Exception as e:
            logger.error(f"Error getting bucket info: {e}")
            return {'bucket_name': self.bucket_name, 'error': str(e)}
            
    def set_blob_lifecycle(self, blob_name: str, lifecycle_config: Dict[str, Any]) -> bool:
        """
        Set lifecycle configuration for a blob
        
        Args:
            blob_name: GCS blob name
            lifecycle_config: Lifecycle configuration
            
        Returns:
            True if successful, False otherwise
        """
        if not self.bucket_name or not self.bucket:
            logger.error("GCS not configured for lifecycle operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            blob = self.bucket.blob(blob_name)
            
            # Set custom metadata or labels based on lifecycle config
            if 'delete_after_days' in lifecycle_config:
                blob.custom_time = datetime.now()
                blob.patch()
                
            logger.info(f"Set lifecycle configuration for gs://{self.bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting blob lifecycle: {e}")
            return False
            
    def copy_blob(self, source_blob_name: str, destination_blob_name: str, 
                  destination_bucket: str = None) -> bool:
        """
        Copy a blob within GCS
        
        Args:
            source_blob_name: Source blob name
            destination_blob_name: Destination blob name
            destination_bucket: Destination bucket (None for same bucket)
            
        Returns:
            True if copy successful, False otherwise
        """
        if not self.bucket_name or not self.bucket:
            logger.error("GCS not configured for copy operations")
            return False
            
        try:
            source_blob_name = source_blob_name.lstrip('/')
            destination_blob_name = destination_blob_name.lstrip('/')
            
            source_blob = self.bucket.blob(source_blob_name)
            
            if destination_bucket:
                dest_bucket = self.storage_client.bucket(destination_bucket)
            else:
                dest_bucket = self.bucket
                
            # Copy blob
            new_blob = self.bucket.copy_blob(source_blob, dest_bucket, destination_blob_name)
            
            dest_bucket_name = destination_bucket or self.bucket_name
            logger.info(f"Copied gs://{self.bucket_name}/{source_blob_name} to "
                       f"gs://{dest_bucket_name}/{destination_blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error copying GCS blob: {e}")
            return False