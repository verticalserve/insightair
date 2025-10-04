"""
OCI Object Storage to SFTP Streaming Operations
Streams data from OCI Object Storage to SFTP servers with efficient memory management
"""

import logging
import io
import concurrent.futures
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_sftp import BaseSFTPOperations, SFTPTransferError, SFTPOperationError

logger = logging.getLogger(__name__)

try:
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False


class OCIObjectStorageSFTPOperations(BaseSFTPOperations):
    """
    OCI Object Storage to SFTP streaming operations
    Provides efficient streaming of data from Object Storage to SFTP servers
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Object Storage to SFTP operations
        
        Args:
            config: Configuration dictionary containing both SFTP and OCI settings
                SFTP settings: hostname, username, password/private_key, etc.
                OCI settings:
                - namespace: Object Storage namespace
                - region: OCI region
                - compartment_id: OCI compartment ID
                - config_file: Path to OCI config file (optional)
                - profile_name: OCI config profile name (optional)
                - tenancy: OCI tenancy OCID (if not using config file)
                - user: OCI user OCID (if not using config file)
                - fingerprint: OCI key fingerprint (if not using config file)
                - private_key_path: Path to private key file (if not using config file)
                - default_bucket: Default Object Storage bucket for operations
        """
        super().__init__(config)
        
        # OCI configuration
        self.namespace = config.get('namespace')
        self.region = config.get('region', 'us-phoenix-1')
        self.compartment_id = config.get('compartment_id')
        self.config_file = config.get('config_file', '~/.oci/config')
        self.profile_name = config.get('profile_name', 'DEFAULT')
        self.default_bucket = config.get('default_bucket')
        
        # Direct authentication parameters
        self.tenancy = config.get('tenancy')
        self.user = config.get('user')
        self.fingerprint = config.get('fingerprint')
        self.private_key_path = config.get('private_key_path')
        
        # Initialize OCI clients
        self.object_storage_client = None
        
        if OCI_AVAILABLE:
            try:
                self._initialize_oci_clients()
            except Exception as e:
                logger.error(f"Failed to initialize OCI clients: {e}")
                # Continue without OCI - operations will fail gracefully
        else:
            logger.warning("OCI libraries not available, Object Storage operations will not work")
            
    def _initialize_oci_clients(self):
        """
        Initialize OCI Object Storage clients
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
                
            # Test bucket access if default bucket specified
            if self.default_bucket:
                try:
                    self.object_storage_client.head_bucket(
                        namespace_name=self.namespace,
                        bucket_name=self.default_bucket
                    )
                    logger.info(f"Verified access to default bucket: {self.default_bucket}")
                except ServiceError as e:
                    if e.status == 404:
                        logger.warning(f"Default OCI bucket '{self.default_bucket}' not found")
                    else:
                        logger.warning(f"Could not access default bucket: {e}")
                        
            logger.info("OCI Object Storage clients initialized successfully")
            
        except ServiceError as e:
            logger.error(f"OCI service error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing OCI clients: {e}")
            raise
            
    def stream_from_cloud_storage(self, cloud_path: str, remote_path: str, 
                                 chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from OCI Object Storage to SFTP server
        
        Args:
            cloud_path: OCI path (oci://bucket/object or just object if default_bucket set)
            remote_path: Remote SFTP path
            chunk_size: Size of chunks to stream (uses buffer_size if not specified)
            
        Returns:
            Dictionary with transfer results
        """
        if not self.object_storage_client:
            raise SFTPOperationError("OCI Object Storage not configured")
            
        try:
            # Parse OCI path
            if cloud_path.startswith('oci://'):
                # Full OCI URL
                path_parts = cloud_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise SFTPOperationError("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = cloud_path.lstrip('/')
                
            # Get object metadata
            try:
                head_object_response = self.object_storage_client.head_object(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    object_name=object_name
                )
                
                file_size = head_object_response.headers['content-length']
                file_size = int(file_size) if file_size else 0
                last_modified = head_object_response.headers.get('last-modified')
                etag = head_object_response.headers.get('etag', '').strip('"')
                content_md5 = head_object_response.headers.get('content-md5')
                
                logger.info(f"Streaming OCI object: oci://{bucket_name}/{object_name} "
                           f"(size: {file_size} bytes) to SFTP: {remote_path}")
                
            except ServiceError as e:
                if e.status == 404:
                    raise SFTPOperationError(f"OCI object not found: oci://{bucket_name}/{object_name}")
                else:
                    raise SFTPOperationError(f"Error accessing OCI object: {e}")
                    
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
                    # Download object in chunks
                    for chunk_start in range(0, file_size, stream_chunk_size):
                        chunk_end = min(chunk_start + stream_chunk_size - 1, file_size - 1)
                        
                        # Create range header for partial content
                        range_header = f"bytes={chunk_start}-{chunk_end}"
                        
                        # Download chunk from OCI Object Storage
                        get_object_response = self.object_storage_client.get_object(
                            namespace_name=self.namespace,
                            bucket_name=bucket_name,
                            object_name=object_name,
                            range=range_header
                        )
                        
                        # Read chunk data
                        chunk_data = get_object_response.data.content
                        
                        # Write chunk to SFTP
                        remote_file.write(chunk_data)
                        bytes_transferred += len(chunk_data)
                        
                        # Log progress for large files
                        if bytes_transferred % (10 * 1024 * 1024) == 0:  # Every 10MB
                            progress = (bytes_transferred / file_size) * 100
                            logger.info(f"Transfer progress: {progress:.1f}% "
                                       f"({bytes_transferred:,}/{file_size:,} bytes)")
                            
            except Exception as e:
                logger.error(f"Error during OCI Object Storage to SFTP transfer: {e}")
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
                'source': f"oci://{bucket_name}/{object_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'file_size': file_size,
                'duration_seconds': duration,
                'transfer_rate_mbps': transfer_rate,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'source_etag': etag,
                'source_content_md5': content_md5,
                'source_last_modified': last_modified
            }
            
            logger.info(f"Successfully streamed oci://{bucket_name}/{object_name} to {remote_path} "
                       f"({bytes_transferred:,} bytes in {duration:.2f}s at {transfer_rate:.2f} MB/s)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error streaming from OCI Object Storage to SFTP: {e}")
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
        Batch stream multiple files from OCI Object Storage to SFTP server
        
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
        
    def list_oci_objects(self, prefix: str = '', bucket_name: str = None, 
                        max_objects: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in OCI Object Storage bucket with given prefix
        
        Args:
            prefix: OCI prefix to search under
            bucket_name: OCI bucket name (uses default if None)
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object information dictionaries
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured")
            return []
            
        try:
            bucket_name = bucket_name or self.default_bucket
            if not bucket_name:
                logger.error("No OCI bucket specified")
                return []
                
            objects = []
            
            # List objects with prefix
            list_objects_response = self.object_storage_client.list_objects(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
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
                        'etag': obj.etag,
                        'md5': obj.md5,
                        'storage_tier': obj.storage_tier,
                        'oci_path': f"oci://{bucket_name}/{obj.name}"
                    })
                    if len(objects) >= max_objects:
                        break
                        
            # Handle pagination if needed
            next_start_with = list_objects_response.data.next_start_with
            while next_start_with and len(objects) < max_objects:
                remaining = max_objects - len(objects)
                list_objects_response = self.object_storage_client.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
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
                            'etag': obj.etag,
                            'md5': obj.md5,
                            'storage_tier': obj.storage_tier,
                            'oci_path': f"oci://{bucket_name}/{obj.name}"
                        })
                        if len(objects) >= max_objects:
                            break
                            
                next_start_with = list_objects_response.data.next_start_with
                
            logger.info(f"Listed {len(objects)} OCI objects with prefix '{prefix}' in bucket '{bucket_name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing OCI objects: {e}")
            return []
            
    def sync_oci_bucket_to_sftp(self, oci_prefix: str, sftp_directory: str,
                               bucket_name: str = None, 
                               file_pattern: str = None) -> Dict[str, Any]:
        """
        Sync entire OCI Object Storage bucket/prefix to SFTP directory
        
        Args:
            oci_prefix: OCI prefix/directory to sync
            sftp_directory: SFTP destination directory
            bucket_name: OCI bucket name (uses default if None)
            file_pattern: Optional file pattern to filter files
            
        Returns:
            Dictionary with sync results
        """
        try:
            # List OCI objects
            oci_objects = self.list_oci_objects(oci_prefix, bucket_name)
            
            if not oci_objects:
                return {
                    'success': True,
                    'message': 'No objects found to sync',
                    'files_synced': 0
                }
                
            # Filter by file pattern if specified
            if file_pattern:
                import fnmatch
                oci_objects = [obj for obj in oci_objects 
                              if fnmatch.fnmatch(obj['name'], file_pattern)]
                
            # Prepare transfer list
            transfer_list = []
            for obj in oci_objects:
                # Calculate relative path
                relative_path = obj['name'][len(oci_prefix):].lstrip('/')
                sftp_path = f"{sftp_directory.rstrip('/')}/{relative_path}"
                
                transfer_list.append({
                    'source': obj['oci_path'],
                    'destination': sftp_path
                })
                
            # Execute batch transfer
            result = self.batch_stream_from_cloud_storage(transfer_list)
            
            result['sync_details'] = {
                'oci_prefix': oci_prefix,
                'sftp_directory': sftp_directory,
                'bucket_name': bucket_name or self.default_bucket,
                'file_pattern': file_pattern,
                'objects_found': len(oci_objects)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing OCI bucket to SFTP: {e}")
            return {
                'success': False,
                'error': str(e),
                'files_synced': 0
            }
            
    def stream_with_checksum_verification(self, cloud_path: str, remote_path: str,
                                         verify_checksum: bool = True,
                                         chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from OCI with checksum verification
        
        Args:
            cloud_path: OCI object path
            remote_path: Remote SFTP path
            verify_checksum: Whether to verify MD5 checksum
            chunk_size: Size of chunks to stream
            
        Returns:
            Dictionary with transfer results including checksum verification
        """
        if not self.object_storage_client:
            raise SFTPOperationError("OCI Object Storage not configured")
            
        try:
            import hashlib
            
            # Parse OCI path
            if cloud_path.startswith('oci://'):
                path_parts = cloud_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                if not self.default_bucket:
                    raise SFTPOperationError("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = cloud_path.lstrip('/')
                
            # Get object metadata for checksum
            head_object_response = self.object_storage_client.head_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            file_size = int(head_object_response.headers['content-length'])
            original_md5 = head_object_response.headers.get('content-md5')
            stream_chunk_size = chunk_size or self.buffer_size
            
            # Ensure SFTP connection and directory
            self.ensure_connection()
            remote_dir = '/'.join(remote_path.split('/')[:-1])
            if remote_dir:
                self.create_directory(remote_dir)
                
            start_time = datetime.now()
            bytes_transferred = 0
            md5_hash = hashlib.md5() if verify_checksum else None
            
            with self.sftp_client.open(remote_path, 'wb') as remote_file:
                for chunk_start in range(0, file_size, stream_chunk_size):
                    chunk_end = min(chunk_start + stream_chunk_size - 1, file_size - 1)
                    range_header = f"bytes={chunk_start}-{chunk_end}"
                    
                    get_object_response = self.object_storage_client.get_object(
                        namespace_name=self.namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        range=range_header
                    )
                    
                    chunk_data = get_object_response.data.content
                    remote_file.write(chunk_data)
                    bytes_transferred += len(chunk_data)
                    
                    # Update checksum
                    if verify_checksum and md5_hash:
                        md5_hash.update(chunk_data)
                        
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Verify checksum if requested
            checksum_valid = True
            computed_md5 = None
            
            if verify_checksum and md5_hash and original_md5:
                import base64
                computed_md5 = base64.b64encode(md5_hash.digest()).decode()
                checksum_valid = computed_md5 == original_md5
                
                if not checksum_valid:
                    logger.warning(f"Checksum mismatch for {remote_path}: "
                                 f"expected {original_md5}, got {computed_md5}")
                    
            return {
                'success': True,
                'source': f"oci://{bucket_name}/{object_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'duration_seconds': duration,
                'checksum_verification': {
                    'enabled': verify_checksum,
                    'original_md5': original_md5,
                    'computed_md5': computed_md5,
                    'valid': checksum_valid
                }
            }
            
        except Exception as e:
            logger.error(f"Error streaming with checksum verification: {e}")
            return {
                'success': False,
                'source': cloud_path,
                'destination': remote_path,
                'error': str(e),
                'bytes_transferred': 0
            }
            
    def get_oci_info(self) -> Dict[str, Any]:
        """
        Get OCI configuration information
        
        Returns:
            Dictionary with OCI info (without sensitive data)
        """
        return {
            'oci_configured': self.object_storage_client is not None,
            'default_bucket': self.default_bucket,
            'namespace': self.namespace,
            'region': self.region,
            'compartment_id': self.compartment_id,
            'config_file': self.config_file,
            'profile_name': self.profile_name
        }