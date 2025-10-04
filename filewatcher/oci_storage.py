"""
OCI Object Storage File Watcher
Provides file monitoring operations for Oracle Cloud Infrastructure Object Storage
Supports both specific file monitoring and pattern-based searches
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import fnmatch

from .base import BaseFileWatcher, TimeOutError

logger = logging.getLogger(__name__)

try:
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False


class OCIObjectStorageWatcher(BaseFileWatcher):
    """
    OCI Object Storage file watcher class
    Monitors OCI Object Storage buckets for file arrivals with pattern matching support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Object Storage file watcher
        
        Args:
            config: Configuration dictionary containing OCI settings
                - bucket_name: Object Storage bucket name (None for local file system)
                - namespace: Object Storage namespace
                - region: OCI region
                - compartment_id: OCI compartment ID
                - config_file: Path to OCI config file (optional)
                - profile_name: OCI config profile name (optional)
                - tenancy: OCI tenancy OCID (if not using config file)
                - user: OCI user OCID (if not using config file)
                - fingerprint: OCI key fingerprint (if not using config file)
                - private_key_path: Path to private key file (if not using config file)
        """
        super().__init__(config)
        
        self.bucket_name = config.get('bucket_name')
        self.namespace = config.get('namespace')
        self.region = config.get('region', 'us-phoenix-1')
        self.compartment_id = config.get('compartment_id')
        self.config_file = config.get('config_file', '~/.oci/config')
        self.profile_name = config.get('profile_name', 'DEFAULT')
        
        # Direct authentication parameters
        self.tenancy = config.get('tenancy')
        self.user = config.get('user')
        self.fingerprint = config.get('fingerprint')
        self.private_key_path = config.get('private_key_path')
        
        # Initialize OCI client
        self.object_storage_client = None
        
        if OCI_AVAILABLE and self.bucket_name:
            try:
                self._initialize_oci_client()
            except Exception as e:
                logger.error(f"Failed to initialize OCI Object Storage client: {e}")
                raise
        elif not self.bucket_name:
            logger.info("Bucket name not specified, using local file system monitoring")
        else:
            logger.warning("OCI libraries not available, falling back to local file system")
            
    def _initialize_oci_client(self):
        """
        Initialize OCI Object Storage client
        """
        try:
            # Choose authentication method
            if self.tenancy and self.user and self.fingerprint and self.private_key_path:
                # Use direct authentication
                config = {
                    "user": self.user,
                    "key_file": self.private_key_path,
                    "fingerprint": self.fingerprint,
                    "tenancy": self.tenancy,
                    "region": self.region
                }
                self.object_storage_client = ObjectStorageClient(config)
            else:
                # Use config file
                config = oci.config.from_file(self.config_file, self.profile_name)
                config["region"] = self.region
                self.object_storage_client = ObjectStorageClient(config)
                
            # Test connection by getting namespace info
            if not self.namespace:
                self.namespace = self.object_storage_client.get_namespace().data
                logger.info(f"Auto-detected OCI namespace: {self.namespace}")
                
            # Test bucket access
            self.object_storage_client.head_bucket(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name
            )
            
            logger.info(f"OCI Object Storage client initialized successfully for bucket: {self.bucket_name}")
            
        except ServiceError as e:
            if e.status == 404:
                logger.error(f"OCI bucket '{self.bucket_name}' not found in namespace '{self.namespace}'")
            elif e.status == 403:
                logger.error(f"Access denied to OCI bucket '{self.bucket_name}'")
            else:
                logger.error(f"OCI service error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing OCI client: {e}")
            raise
            
    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a specific file exists in OCI Object Storage or local file system
        
        Args:
            file_path: Path to the file (object name or local path)
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.bucket_name:
            # Use local file system
            return self.check_local_file_exists(file_path)
            
        if not self.object_storage_client:
            logger.error("OCI Object Storage client not initialized")
            return False
            
        try:
            # Remove leading slash if present
            object_name = file_path.lstrip('/')
            
            # Use head_object to check if file exists
            self.object_storage_client.head_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            logger.debug(f"OCI object exists: {self.namespace}/{self.bucket_name}/{object_name}")
            return True
            
        except ServiceError as e:
            if e.status == 404:
                logger.debug(f"OCI object not found: {self.namespace}/{self.bucket_name}/{object_name}")
                return False
            else:
                logger.error(f"Error checking OCI object existence: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking OCI object: {e}")
            return False
            
    def search_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files in OCI Object Storage using a pattern
        
        Args:
            pattern: Pattern to search for (supports wildcards)
            
        Returns:
            List of matching object names or local file paths
        """
        if not self.bucket_name:
            # Use local file system
            return self.search_local_files_by_pattern(pattern)
            
        if not self.object_storage_client:
            logger.error("OCI Object Storage client not initialized")
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
            
            # List objects with prefix
            list_objects_response = self.object_storage_client.list_objects(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                prefix=prefix
            )
            
            # Filter objects matching pattern
            if list_objects_response.data.objects:
                for obj in list_objects_response.data.objects:
                    object_name = obj.name
                    
                    # Check if object name matches pattern
                    if self._match_oci_pattern(object_name, pattern):
                        matching_files.append(object_name)
                        
            # Handle pagination if needed
            next_start_with = list_objects_response.data.next_start_with
            while next_start_with:
                list_objects_response = self.object_storage_client.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    prefix=prefix,
                    start=next_start_with
                )
                
                if list_objects_response.data.objects:
                    for obj in list_objects_response.data.objects:
                        object_name = obj.name
                        
                        if self._match_oci_pattern(object_name, pattern):
                            matching_files.append(object_name)
                            
                next_start_with = list_objects_response.data.next_start_with
                
            logger.debug(f"OCI pattern search '{pattern}' found {len(matching_files)} files in bucket '{self.bucket_name}'")
            return matching_files
            
        except Exception as e:
            logger.error(f"Error searching OCI objects with pattern '{pattern}': {e}")
            return []
            
    def _match_oci_pattern(self, object_name: str, pattern: str) -> bool:
        """
        Check if OCI object name matches the given pattern
        
        Args:
            object_name: OCI object name
            pattern: Pattern with wildcards
            
        Returns:
            True if name matches pattern, False otherwise
        """
        try:
            # Use fnmatch for pattern matching
            return fnmatch.fnmatch(object_name, pattern)
        except Exception as e:
            logger.error(f"Error matching pattern '{pattern}' against object '{object_name}': {e}")
            return False
            
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file in OCI Object Storage or local file system
        
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
                
        if not self.object_storage_client:
            return {'path': file_path, 'exists': False, 'error': 'OCI client not initialized'}
            
        try:
            object_name = file_path.lstrip('/')
            
            # Get object metadata
            response = self.object_storage_client.head_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            
            return {
                'namespace': self.namespace,
                'bucket': self.bucket_name,
                'object_name': object_name,
                'size': int(response.headers.get('content-length', 0)),
                'last_modified': response.headers.get('last-modified', ''),
                'etag': response.headers.get('etag', '').strip('"'),
                'content_type': response.headers.get('content-type', ''),
                'storage_tier': response.headers.get('opc-storage-tier', 'Standard'),
                'exists': True
            }
            
        except ServiceError as e:
            if e.status == 404:
                return {
                    'namespace': self.namespace, 
                    'bucket': self.bucket_name, 
                    'object_name': object_name, 
                    'exists': False
                }
            else:
                logger.error(f"Error getting OCI object info: {e}")
                return {
                    'namespace': self.namespace, 
                    'bucket': self.bucket_name, 
                    'object_name': object_name, 
                    'exists': False, 
                    'error': str(e)
                }
        except Exception as e:
            logger.error(f"Unexpected error getting OCI object info: {e}")
            return {
                'namespace': self.namespace, 
                'bucket': self.bucket_name, 
                'object_name': file_path, 
                'exists': False, 
                'error': str(e)
            }
            
    def list_objects_in_prefix(self, prefix: str, max_objects: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in OCI Object Storage bucket with given prefix
        
        Args:
            prefix: Object name prefix to search under
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object information dictionaries
        """
        if not self.bucket_name:
            logger.error("No bucket specified for OCI operations")
            return []
            
        if not self.object_storage_client:
            logger.error("OCI Object Storage client not initialized")
            return []
            
        try:
            objects = []
            prefix = prefix.lstrip('/')
            
            # List objects with prefix
            list_objects_response = self.object_storage_client.list_objects(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                prefix=prefix,
                limit=min(max_objects, 1000)  # OCI limit is 1000 per request
            )
            
            if list_objects_response.data.objects:
                for obj in list_objects_response.data.objects:
                    objects.append({
                        'name': obj.name,
                        'size': obj.size,
                        'time_created': obj.time_created.isoformat() if obj.time_created else None,
                        'time_modified': obj.time_modified.isoformat() if obj.time_modified else None,
                        'etag': obj.etag.strip('"') if obj.etag else '',
                        'storage_tier': getattr(obj, 'storage_tier', 'Standard')
                    })
                    
                    if len(objects) >= max_objects:
                        break
                        
            # Handle pagination if needed and we haven't reached max_objects
            next_start_with = list_objects_response.data.next_start_with
            while next_start_with and len(objects) < max_objects:
                remaining = max_objects - len(objects)
                list_objects_response = self.object_storage_client.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    prefix=prefix,
                    start=next_start_with,
                    limit=min(remaining, 1000)
                )
                
                if list_objects_response.data.objects:
                    for obj in list_objects_response.data.objects:
                        objects.append({
                            'name': obj.name,
                            'size': obj.size,
                            'time_created': obj.time_created.isoformat() if obj.time_created else None,
                            'time_modified': obj.time_modified.isoformat() if obj.time_modified else None,
                            'etag': obj.etag.strip('"') if obj.etag else '',
                            'storage_tier': getattr(obj, 'storage_tier', 'Standard')
                        })
                        
                        if len(objects) >= max_objects:
                            break
                            
                next_start_with = list_objects_response.data.next_start_with
                
            logger.debug(f"Listed {len(objects)} objects with prefix '{prefix}' in bucket '{self.bucket_name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing OCI objects with prefix '{prefix}': {e}")
            return []
            
    def download_object(self, object_name: str, local_path: str) -> bool:
        """
        Download an object from OCI Object Storage to local file system
        
        Args:
            object_name: OCI object name
            local_path: Local file path to save to
            
        Returns:
            True if download successful, False otherwise
        """
        if not self.bucket_name or not self.object_storage_client:
            logger.error("OCI not configured for download operations")
            return False
            
        try:
            object_name = object_name.lstrip('/')
            
            # Create local directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Get the object
            get_object_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            
            # Write to local file
            with open(local_path, 'wb') as f:
                for chunk in get_object_response.data.raw.stream(1024 * 1024, decode_content=False):
                    f.write(chunk)
                    
            logger.info(f"Downloaded {self.namespace}/{self.bucket_name}/{object_name} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading OCI object: {e}")
            return False
            
    def upload_object(self, local_path: str, object_name: str) -> bool:
        """
        Upload a file from local file system to OCI Object Storage
        
        Args:
            local_path: Local file path
            object_name: OCI object name to upload to
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.bucket_name or not self.object_storage_client:
            logger.error("OCI not configured for upload operations")
            return False
            
        try:
            object_name = object_name.lstrip('/')
            
            # Upload object
            with open(local_path, 'rb') as f:
                self.object_storage_client.put_object(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    put_object_body=f
                )
                
            logger.info(f"Uploaded {local_path} to {self.namespace}/{self.bucket_name}/{object_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading object to OCI: {e}")
            return False
            
    def get_bucket_info(self) -> Dict[str, Any]:
        """
        Get information about the OCI Object Storage bucket
        
        Returns:
            Dictionary with bucket information
        """
        if not self.bucket_name or not self.object_storage_client:
            return {'error': 'OCI not configured'}
            
        try:
            # Get bucket details
            get_bucket_response = self.object_storage_client.get_bucket(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name
            )
            
            bucket = get_bucket_response.data
            
            return {
                'bucket_name': self.bucket_name,
                'namespace': self.namespace,
                'compartment_id': bucket.compartment_id,
                'time_created': bucket.time_created.isoformat() if bucket.time_created else None,
                'etag': bucket.etag,
                'public_access_type': bucket.public_access_type,
                'storage_tier': bucket.storage_tier,
                'object_events_enabled': bucket.object_events_enabled,
                'versioning': bucket.versioning,
                'region': self.region
            }
            
        except Exception as e:
            logger.error(f"Error getting bucket info: {e}")
            return {'bucket_name': self.bucket_name, 'namespace': self.namespace, 'error': str(e)}