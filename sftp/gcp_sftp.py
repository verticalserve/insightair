"""
GCP Cloud Storage to SFTP Streaming Operations
Streams data from GCP Cloud Storage to SFTP servers with efficient memory management
"""

import logging
import io
import concurrent.futures
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_sftp import BaseSFTPOperations, SFTPTransferError, SFTPOperationError

logger = logging.getLogger(__name__)

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP libraries not available: {e}")
    GCP_AVAILABLE = False


class GCPCloudStorageSFTPOperations(BaseSFTPOperations):
    """
    GCP Cloud Storage to SFTP streaming operations
    Provides efficient streaming of data from Cloud Storage to SFTP servers
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Cloud Storage to SFTP operations
        
        Args:
            config: Configuration dictionary containing both SFTP and GCP settings
                SFTP settings: hostname, username, password/private_key, etc.
                GCP settings:
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON file (optional)
                - credentials: Service account credentials object (optional)
                - default_bucket: Default Cloud Storage bucket for operations
        """
        super().__init__(config)
        
        # GCP configuration
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.credentials = config.get('credentials')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize GCP clients
        self.storage_client = None
        self.bucket_client = None
        
        if GCP_AVAILABLE:
            try:
                self._initialize_gcp_clients()
            except Exception as e:
                logger.error(f"Failed to initialize GCP clients: {e}")
                # Continue without GCP - operations will fail gracefully
        else:
            logger.warning("GCP libraries not available, Cloud Storage operations will not work")
            
    def _initialize_gcp_clients(self):
        """
        Initialize Google Cloud Storage clients
        """
        try:
            # Initialize storage client with different authentication methods
            if self.credentials:
                # Use provided credentials object
                self.storage_client = storage.Client(
                    project=self.project_id,
                    credentials=self.credentials
                )
            elif self.credentials_path:
                # Use service account file
                self.storage_client = storage.Client.from_service_account_json(
                    self.credentials_path,
                    project=self.project_id
                )
            else:
                # Use default credentials (environment, metadata server, etc.)
                self.storage_client = storage.Client(project=self.project_id)
                
            # Get default bucket client if specified
            if self.default_bucket:
                try:
                    self.bucket_client = self.storage_client.bucket(self.default_bucket)
                    # Test bucket access
                    self.bucket_client.reload()
                    logger.info(f"Verified access to default GCS bucket: {self.default_bucket}")
                except NotFound:
                    logger.warning(f"Default GCS bucket '{self.default_bucket}' not found")
                except Exception as e:
                    logger.warning(f"Could not access default bucket: {e}")
                    
            logger.info("Google Cloud Storage clients initialized successfully")
            
        except DefaultCredentialsError as e:
            logger.error(f"GCP credentials error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing GCP clients: {e}")
            raise
            
    def stream_from_cloud_storage(self, cloud_path: str, remote_path: str, 
                                 chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from Cloud Storage to SFTP server
        
        Args:
            cloud_path: GCS path (gs://bucket/object or just object if default_bucket set)
            remote_path: Remote SFTP path
            chunk_size: Size of chunks to stream (uses buffer_size if not specified)
            
        Returns:
            Dictionary with transfer results
        """
        if not self.storage_client:
            raise SFTPOperationError("GCP Cloud Storage not configured")
            
        try:
            # Parse GCS path
            if cloud_path.startswith('gs://'):
                # Full GCS URL
                path_parts = cloud_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket_client:
                    raise SFTPOperationError("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket_client
                blob_name = cloud_path.lstrip('/')
                
            # Get blob and metadata
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                raise SFTPOperationError(f"GCS object not found: gs://{bucket.name}/{blob_name}")
                
            # Reload to get metadata
            blob.reload()
            file_size = blob.size
            last_modified = blob.updated
            etag = blob.etag
            md5_hash = blob.md5_hash
            
            logger.info(f"Streaming GCS object: gs://{bucket.name}/{blob_name} "
                       f"(size: {file_size} bytes) to SFTP: {remote_path}")
            
            # Use specified chunk size or default buffer size
            stream_chunk_size = chunk_size or self.buffer_size
            
            # Ensure SFTP connection
            self.ensure_connection()
            
            # Ensure remote directory exists
            remote_dir = '/'.join(remote_path.split('/')[:-1])
            if remote_dir:
                self.create_directory(remote_dir)
                
            # Start transfer
            start_time = datetime.now()
            bytes_transferred = 0
            
            try:
                with self.sftp_client.open(remote_path, 'wb') as remote_file:
                    # Download blob in chunks
                    for chunk_start in range(0, file_size, stream_chunk_size):
                        chunk_end = min(chunk_start + stream_chunk_size - 1, file_size - 1)
                        
                        # Download chunk from GCS
                        chunk_data = blob.download_as_bytes(start=chunk_start, end=chunk_end)
                        
                        # Write chunk to SFTP
                        remote_file.write(chunk_data)
                        bytes_transferred += len(chunk_data)
                        
                        # Log progress for large files
                        if bytes_transferred % (10 * 1024 * 1024) == 0:  # Every 10MB
                            progress = (bytes_transferred / file_size) * 100
                            logger.info(f"Transfer progress: {progress:.1f}% "
                                       f"({bytes_transferred:,}/{file_size:,} bytes)")
                            
            except Exception as e:
                logger.error(f"Error during GCS to SFTP transfer: {e}")
                # Try to clean up partial file
                try:
                    if self.file_exists(remote_path):
                        self.sftp_client.remove(remote_path)
                        logger.info(f"Cleaned up partial file: {remote_path}")
                except:
                    pass
                raise SFTPTransferError(f"Transfer failed: {e}")
                
            # Calculate transfer statistics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            transfer_rate = bytes_transferred / (1024 * 1024) / duration if duration > 0 else 0  # MB/s
            
            # Verify file was transferred completely
            if bytes_transferred != file_size:
                raise SFTPTransferError(f"Transfer incomplete: {bytes_transferred}/{file_size} bytes")
                
            result = {
                'success': True,
                'source': f"gs://{bucket.name}/{blob_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'file_size': file_size,
                'duration_seconds': duration,
                'transfer_rate_mbps': transfer_rate,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'source_etag': etag,
                'source_md5_hash': md5_hash,
                'source_last_modified': last_modified.isoformat() if last_modified else None
            }
            
            logger.info(f"Successfully streamed gs://{bucket.name}/{blob_name} to {remote_path} "
                       f"({bytes_transferred:,} bytes in {duration:.2f}s at {transfer_rate:.2f} MB/s)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error streaming from GCS to SFTP: {e}")
            return {
                'success': False,
                'source': cloud_path,
                'destination': remote_path,
                'error': str(e),
                'bytes_transferred': 0
            }
            
    def batch_stream_from_cloud_storage(self, transfer_list: List[Dict[str, str]], 
                                       max_concurrent: int = None) -> Dict[str, Any]:
        """
        Batch stream multiple files from Cloud Storage to SFTP server
        
        Args:
            transfer_list: List of transfer specifications with 'source' and 'destination' keys
            max_concurrent: Maximum concurrent transfers (uses class default if not specified)
            
        Returns:
            Dictionary with batch transfer results
        """
        if not transfer_list:
            return {
                'success': True,
                'total_transfers': 0,
                'successful_transfers': 0,
                'failed_transfers': 0,
                'transfer_details': []
            }
            
        max_workers = max_concurrent or self.max_concurrent_transfers
        
        results = {
            'success': True,
            'total_transfers': len(transfer_list),
            'successful_transfers': 0,
            'failed_transfers': 0,
            'total_bytes_transferred': 0,
            'transfer_details': [],
            'start_time': datetime.now().isoformat()
        }
        
        # Process transfers concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all transfer tasks
            future_to_transfer = {
                executor.submit(
                    self.stream_from_cloud_storage,
                    transfer['source'],
                    transfer['destination'],
                    transfer.get('chunk_size')
                ): transfer for transfer in transfer_list
            }
            
            # Process completed transfers
            for future in concurrent.futures.as_completed(future_to_transfer):
                transfer_spec = future_to_transfer[future]
                
                try:
                    result = future.result()
                    results['transfer_details'].append(result)
                    
                    if result['success']:
                        results['successful_transfers'] += 1
                        results['total_bytes_transferred'] += result.get('bytes_transferred', 0)
                    else:
                        results['failed_transfers'] += 1
                        results['success'] = False
                        
                except Exception as e:
                    error_result = {
                        'success': False,
                        'source': transfer_spec['source'],
                        'destination': transfer_spec['destination'],
                        'error': str(e),
                        'bytes_transferred': 0
                    }
                    results['transfer_details'].append(error_result)
                    results['failed_transfers'] += 1
                    results['success'] = False
                    logger.error(f"Transfer failed: {transfer_spec['source']} -> {transfer_spec['destination']}: {e}")
                    
        results['end_time'] = datetime.now().isoformat()
        
        logger.info(f"Batch transfer completed: {results['successful_transfers']}/{results['total_transfers']} successful, "
                   f"{results['total_bytes_transferred']:,} total bytes transferred")
        
        return results
        
    def list_gcs_objects(self, prefix: str = '', bucket_name: str = None, 
                        max_objects: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in GCS bucket with given prefix
        
        Args:
            prefix: GCS prefix to search under
            bucket_name: GCS bucket name (uses default if None)
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object information dictionaries
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured")
            return []
            
        try:
            if bucket_name:
                bucket = self.storage_client.bucket(bucket_name)
            elif self.bucket_client:
                bucket = self.bucket_client
            else:
                logger.error("No GCS bucket specified")
                return []
                
            objects = []
            
            for blob in bucket.list_blobs(prefix=prefix, max_results=max_objects):
                objects.append({
                    'name': blob.name,
                    'size': blob.size,
                    'last_modified': blob.updated.isoformat() if blob.updated else None,
                    'etag': blob.etag,
                    'md5_hash': blob.md5_hash,
                    'content_type': blob.content_type,
                    'gcs_path': f"gs://{bucket.name}/{blob.name}"
                })
                if len(objects) >= max_objects:
                    break
                    
            logger.info(f"Listed {len(objects)} GCS objects with prefix '{prefix}' in bucket '{bucket.name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing GCS objects: {e}")
            return []
            
    def sync_gcs_directory_to_sftp(self, gcs_prefix: str, sftp_directory: str,
                                  bucket_name: str = None, 
                                  file_pattern: str = None) -> Dict[str, Any]:
        """
        Sync entire GCS directory/prefix to SFTP directory
        
        Args:
            gcs_prefix: GCS prefix/directory to sync
            sftp_directory: SFTP destination directory
            bucket_name: GCS bucket name (uses default if None)
            file_pattern: Optional file pattern to filter files
            
        Returns:
            Dictionary with sync results
        """
        try:
            # List GCS objects
            gcs_objects = self.list_gcs_objects(gcs_prefix, bucket_name)
            
            if not gcs_objects:
                return {
                    'success': True,
                    'message': 'No objects found to sync',
                    'files_synced': 0
                }
                
            # Filter by file pattern if specified
            if file_pattern:
                import fnmatch
                gcs_objects = [obj for obj in gcs_objects 
                              if fnmatch.fnmatch(obj['name'], file_pattern)]
                
            # Prepare transfer list
            transfer_list = []
            for obj in gcs_objects:
                # Calculate relative path
                relative_path = obj['name'][len(gcs_prefix):].lstrip('/')
                sftp_path = f"{sftp_directory.rstrip('/')}/{relative_path}"
                
                transfer_list.append({
                    'source': obj['gcs_path'],
                    'destination': sftp_path
                })
                
            # Execute batch transfer
            result = self.batch_stream_from_cloud_storage(transfer_list)
            
            result['sync_details'] = {
                'gcs_prefix': gcs_prefix,
                'sftp_directory': sftp_directory,
                'bucket_name': bucket_name or (self.bucket_client.name if self.bucket_client else None),
                'file_pattern': file_pattern,
                'objects_found': len(gcs_objects)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing GCS directory to SFTP: {e}")
            return {
                'success': False,
                'error': str(e),
                'files_synced': 0
            }
            
    def stream_with_transformation(self, cloud_path: str, remote_path: str,
                                  transform_function: callable = None,
                                  chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from GCS to SFTP with optional transformation
        
        Args:
            cloud_path: GCS path
            remote_path: Remote SFTP path
            transform_function: Optional function to transform data chunks
            chunk_size: Size of chunks to stream
            
        Returns:
            Dictionary with transfer results
        """
        if not self.storage_client:
            raise SFTPOperationError("GCP Cloud Storage not configured")
            
        try:
            # Parse GCS path
            if cloud_path.startswith('gs://'):
                path_parts = cloud_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                if not self.default_bucket or not self.bucket_client:
                    raise SFTPOperationError("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket_client
                blob_name = cloud_path.lstrip('/')
                
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                raise SFTPOperationError(f"GCS object not found: gs://{bucket.name}/{blob_name}")
                
            blob.reload()
            file_size = blob.size
            stream_chunk_size = chunk_size or self.buffer_size
            
            # Ensure SFTP connection and directory
            self.ensure_connection()
            remote_dir = '/'.join(remote_path.split('/')[:-1])
            if remote_dir:
                self.create_directory(remote_dir)
                
            start_time = datetime.now()
            bytes_transferred = 0
            
            with self.sftp_client.open(remote_path, 'wb') as remote_file:
                for chunk_start in range(0, file_size, stream_chunk_size):
                    chunk_end = min(chunk_start + stream_chunk_size - 1, file_size - 1)
                    chunk_data = blob.download_as_bytes(start=chunk_start, end=chunk_end)
                    
                    # Apply transformation if provided
                    if transform_function:
                        try:
                            chunk_data = transform_function(chunk_data)
                        except Exception as e:
                            logger.warning(f"Transformation failed for chunk: {e}")
                            
                    remote_file.write(chunk_data)
                    bytes_transferred += len(chunk_data)
                    
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                'success': True,
                'source': f"gs://{bucket.name}/{blob_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'duration_seconds': duration,
                'transformation_applied': transform_function is not None
            }
            
        except Exception as e:
            logger.error(f"Error streaming with transformation: {e}")
            return {
                'success': False,
                'source': cloud_path,
                'destination': remote_path,
                'error': str(e),
                'bytes_transferred': 0
            }
            
    def get_gcp_info(self) -> Dict[str, Any]:
        """
        Get GCP configuration information
        
        Returns:
            Dictionary with GCP info (without sensitive data)
        """
        return {
            'gcp_configured': self.storage_client is not None,
            'default_bucket': self.default_bucket,
            'project_id': self.project_id,
            'has_credentials_path': self.credentials_path is not None,
            'has_credentials_object': self.credentials is not None
        }