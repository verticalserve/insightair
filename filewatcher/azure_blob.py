"""
Azure Blob Storage File Watcher
Provides file monitoring operations for Microsoft Azure Blob Storage
Supports both specific file monitoring and pattern-based searches
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import fnmatch

from .base import BaseFileWatcher, TimeOutError

logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    from azure.core.exceptions import ResourceNotFoundError, AzureError
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure libraries not available: {e}")
    AZURE_AVAILABLE = False


class AzureBlobStorageWatcher(BaseFileWatcher):
    """
    Azure Blob Storage file watcher class
    Monitors Azure containers for file arrivals with pattern matching support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Blob Storage file watcher
        
        Args:
            config: Configuration dictionary containing Azure settings
                - container_name: Blob Storage container name (None for local file system)
                - account_name: Storage account name
                - account_key: Storage account key (optional)
                - connection_string: Storage connection string (optional)
                - sas_token: SAS token (optional)
                - credential: Azure credential object (optional)
                - account_url: Storage account URL (optional)
        """
        super().__init__(config)
        
        self.container_name = config.get('container_name')
        self.account_name = config.get('account_name')
        self.account_key = config.get('account_key')
        self.connection_string = config.get('connection_string')
        self.sas_token = config.get('sas_token')
        self.credential = config.get('credential')
        self.account_url = config.get('account_url')
        
        # Initialize Azure clients
        self.blob_service_client = None
        self.container_client = None
        
        if AZURE_AVAILABLE and self.container_name:
            try:
                self._initialize_azure_clients()
            except Exception as e:
                logger.error(f"Failed to initialize Azure Blob Storage clients: {e}")
                raise
        elif not self.container_name:
            logger.info("Container name not specified, using local file system monitoring")
        else:
            logger.warning("Azure libraries not available, falling back to local file system")
            
    def _initialize_azure_clients(self):
        """
        Initialize Azure Blob Storage clients
        """
        try:
            # Initialize BlobServiceClient with different authentication methods
            if self.connection_string:
                # Use connection string
                self.blob_service_client = BlobServiceClient.from_connection_string(
                    conn_str=self.connection_string
                )
            elif self.account_url and self.credential:
                # Use account URL with credential
                self.blob_service_client = BlobServiceClient(
                    account_url=self.account_url,
                    credential=self.credential
                )
            elif self.account_name and self.account_key:
                # Use account name and key
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.account_key
                )
            elif self.account_name and self.sas_token:
                # Use account name and SAS token
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.sas_token
                )
            elif self.account_name:
                # Use default Azure credential
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=DefaultAzureCredential()
                )
            else:
                raise ValueError("No valid Azure authentication method provided")
                
            # Get container client
            self.container_client = self.blob_service_client.get_container_client(
                container=self.container_name
            )
            
            # Test container access
            container_properties = self.container_client.get_container_properties()
            
            logger.info(f"Azure Blob Storage clients initialized successfully for container: {self.container_name}")
            
        except ResourceNotFoundError:
            logger.error(f"Azure container '{self.container_name}' not found")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing Azure clients: {e}")
            raise
            
    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a specific file exists in Azure container or local file system
        
        Args:
            file_path: Path to the file (blob name or local path)
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.container_name:
            # Use local file system
            return self.check_local_file_exists(file_path)
            
        if not self.container_client:
            logger.error("Azure container client not initialized")
            return False
            
        try:
            # Remove leading slash if present
            blob_name = file_path.lstrip('/')
            
            # Get blob client and check if blob exists
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            exists = blob_client.exists()
            
            logger.debug(f"Azure blob exists check: {self.account_name}/{self.container_name}/{blob_name} = {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking Azure blob existence: {e}")
            return False
            
    def search_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files in Azure container using a pattern
        
        Args:
            pattern: Pattern to search for (supports wildcards)
            
        Returns:
            List of matching blob names or local file paths
        """
        if not self.container_name:
            # Use local file system
            return self.search_local_files_by_pattern(pattern)
            
        if not self.container_client:
            logger.error("Azure container client not initialized")
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
            blobs = self.container_client.list_blobs(name_starts_with=prefix if prefix else None)
            
            # Filter blobs matching pattern
            for blob in blobs:
                blob_name = blob.name
                
                # Check if blob name matches pattern
                if self._match_azure_pattern(blob_name, pattern):
                    matching_files.append(blob_name)
                    
            logger.debug(f"Azure pattern search '{pattern}' found {len(matching_files)} files in container '{self.container_name}'")
            return matching_files
            
        except Exception as e:
            logger.error(f"Error searching Azure blobs with pattern '{pattern}': {e}")
            return []
            
    def _match_azure_pattern(self, blob_name: str, pattern: str) -> bool:
        """
        Check if Azure blob name matches the given pattern
        
        Args:
            blob_name: Azure blob name
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
        Get information about a file in Azure container or local file system
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        if not self.container_name:
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
                
        if not self.container_client:
            return {'path': file_path, 'exists': False, 'error': 'Azure container client not initialized'}
            
        try:
            blob_name = file_path.lstrip('/')
            
            # Get blob client
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            
            if not blob_client.exists():
                return {
                    'account_name': self.account_name,
                    'container': self.container_name,
                    'blob_name': blob_name,
                    'exists': False
                }
                
            # Get blob properties
            blob_properties = blob_client.get_blob_properties()
            
            return {
                'account_name': self.account_name,
                'container': self.container_name,
                'blob_name': blob_name,
                'size': blob_properties.size,
                'creation_time': blob_properties.creation_time.isoformat() if blob_properties.creation_time else None,
                'last_modified': blob_properties.last_modified.isoformat() if blob_properties.last_modified else None,
                'etag': blob_properties.etag,
                'content_type': blob_properties.content_settings.content_type if blob_properties.content_settings else None,
                'blob_type': blob_properties.blob_type,
                'access_tier': blob_properties.blob_tier,
                'lease_status': blob_properties.lease.status if blob_properties.lease else None,
                'exists': True
            }
            
        except ResourceNotFoundError:
            return {
                'account_name': self.account_name,
                'container': self.container_name,
                'blob_name': blob_name,
                'exists': False
            }
        except Exception as e:
            logger.error(f"Error getting Azure blob info: {e}")
            return {
                'account_name': self.account_name,
                'container': self.container_name,
                'blob_name': file_path,
                'exists': False,
                'error': str(e)
            }
            
    def list_blobs_in_prefix(self, prefix: str, max_blobs: int = 1000) -> List[Dict[str, Any]]:
        """
        List blobs in Azure container with given prefix
        
        Args:
            prefix: Blob name prefix to search under
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob information dictionaries
        """
        if not self.container_name:
            logger.error("No container specified for Azure operations")
            return []
            
        if not self.container_client:
            logger.error("Azure container client not initialized")
            return []
            
        try:
            blobs = []
            prefix = prefix.lstrip('/')
            
            # List blobs with prefix
            blob_list = self.container_client.list_blobs(
                name_starts_with=prefix if prefix else None,
                results_per_page=min(max_blobs, 5000)  # Azure limit
            )
            
            for blob in blob_list:
                blobs.append({
                    'name': blob.name,
                    'size': blob.size,
                    'creation_time': blob.creation_time.isoformat() if blob.creation_time else None,
                    'last_modified': blob.last_modified.isoformat() if blob.last_modified else None,
                    'etag': blob.etag,
                    'content_type': blob.content_settings.content_type if blob.content_settings else None,
                    'blob_type': blob.blob_type,
                    'access_tier': blob.blob_tier,
                    'lease_status': blob.lease.status if blob.lease else None
                })
                
                if len(blobs) >= max_blobs:
                    break
                    
            logger.debug(f"Listed {len(blobs)} blobs with prefix '{prefix}' in container '{self.container_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing Azure blobs with prefix '{prefix}': {e}")
            return []
            
    def download_blob(self, blob_name: str, local_path: str) -> bool:
        """
        Download a blob from Azure to local file system
        
        Args:
            blob_name: Azure blob name
            local_path: Local file path to save to
            
        Returns:
            True if download successful, False otherwise
        """
        if not self.container_name or not self.container_client:
            logger.error("Azure not configured for download operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            
            # Create local directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download blob
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            
            with open(local_path, 'wb') as f:
                download_stream = blob_client.download_blob()
                f.write(download_stream.readall())
                
            logger.info(f"Downloaded {self.account_name}/{self.container_name}/{blob_name} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading Azure blob: {e}")
            return False
            
    def upload_blob(self, local_path: str, blob_name: str, overwrite: bool = True) -> bool:
        """
        Upload a file from local file system to Azure
        
        Args:
            local_path: Local file path
            blob_name: Azure blob name to upload to
            overwrite: Whether to overwrite existing blob
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.container_name or not self.container_client:
            logger.error("Azure not configured for upload operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            
            # Upload blob
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            
            with open(local_path, 'rb') as f:
                blob_client.upload_blob(f, overwrite=overwrite)
                
            logger.info(f"Uploaded {local_path} to {self.account_name}/{self.container_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading blob to Azure: {e}")
            return False
            
    def get_container_info(self) -> Dict[str, Any]:
        """
        Get information about the Azure container
        
        Returns:
            Dictionary with container information
        """
        if not self.container_name or not self.container_client:
            return {'error': 'Azure not configured'}
            
        try:
            # Get container properties
            container_properties = self.container_client.get_container_properties()
            
            return {
                'container_name': self.container_name,
                'account_name': self.account_name,
                'last_modified': container_properties.last_modified.isoformat() if container_properties.last_modified else None,
                'etag': container_properties.etag,
                'lease_status': container_properties.lease.status if container_properties.lease else None,
                'lease_state': container_properties.lease.state if container_properties.lease else None,
                'public_access': container_properties.public_access,
                'has_immutability_policy': container_properties.has_immutability_policy,
                'has_legal_hold': container_properties.has_legal_hold,
                'metadata': dict(container_properties.metadata) if container_properties.metadata else {}
            }
            
        except Exception as e:
            logger.error(f"Error getting container info: {e}")
            return {'container_name': self.container_name, 'error': str(e)}
            
    def copy_blob(self, source_blob_name: str, destination_blob_name: str,
                  source_container: str = None) -> bool:
        """
        Copy a blob within Azure Storage
        
        Args:
            source_blob_name: Source blob name
            destination_blob_name: Destination blob name
            source_container: Source container (None for same container)
            
        Returns:
            True if copy successful, False otherwise
        """
        if not self.container_name or not self.container_client:
            logger.error("Azure not configured for copy operations")
            return False
            
        try:
            source_blob_name = source_blob_name.lstrip('/')
            destination_blob_name = destination_blob_name.lstrip('/')
            
            # Build source URL
            if source_container:
                source_url = f"https://{self.account_name}.blob.core.windows.net/{source_container}/{source_blob_name}"
            else:
                source_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{source_blob_name}"
                
            # Copy blob
            dest_blob_client = self.container_client.get_blob_client(blob=destination_blob_name)
            copy_operation = dest_blob_client.start_copy_from_url(source_url)
            
            # Wait for copy to complete (for small blobs this is usually instant)
            copy_props = dest_blob_client.get_blob_properties()
            if copy_props.copy.status == 'success':
                source_container_name = source_container or self.container_name
                logger.info(f"Copied {self.account_name}/{source_container_name}/{source_blob_name} to "
                           f"{self.account_name}/{self.container_name}/{destination_blob_name}")
                return True
            else:
                logger.warning(f"Copy operation status: {copy_props.copy.status}")
                return False
                
        except Exception as e:
            logger.error(f"Error copying Azure blob: {e}")
            return False
            
    def set_blob_tier(self, blob_name: str, access_tier: str) -> bool:
        """
        Set access tier for a blob (Hot, Cool, Archive)
        
        Args:
            blob_name: Azure blob name
            access_tier: Access tier ('Hot', 'Cool', 'Archive')
            
        Returns:
            True if successful, False otherwise
        """
        if not self.container_name or not self.container_client:
            logger.error("Azure not configured for tier operations")
            return False
            
        try:
            blob_name = blob_name.lstrip('/')
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            
            # Set blob tier
            blob_client.set_standard_blob_tier(access_tier)
            
            logger.info(f"Set access tier '{access_tier}' for blob {self.account_name}/{self.container_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting blob tier: {e}")
            return False
            
    def create_snapshot(self, blob_name: str) -> Optional[str]:
        """
        Create a snapshot of a blob
        
        Args:
            blob_name: Azure blob name
            
        Returns:
            Snapshot timestamp if successful, None otherwise
        """
        if not self.container_name or not self.container_client:
            logger.error("Azure not configured for snapshot operations")
            return None
            
        try:
            blob_name = blob_name.lstrip('/')
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            
            # Create snapshot
            snapshot_props = blob_client.create_snapshot()
            snapshot_time = snapshot_props['snapshot']
            
            logger.info(f"Created snapshot {snapshot_time} for blob {self.account_name}/{self.container_name}/{blob_name}")
            return snapshot_time
            
        except Exception as e:
            logger.error(f"Error creating blob snapshot: {e}")
            return None