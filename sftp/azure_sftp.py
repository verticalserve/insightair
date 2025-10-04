"""
Azure Blob Storage to SFTP Streaming Operations
Streams data from Azure Blob Storage to SFTP servers with efficient memory management
"""

import logging
import io
import concurrent.futures
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_sftp import BaseSFTPOperations, SFTPTransferError, SFTPOperationError

logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    from azure.core.exceptions import ResourceNotFoundError, AzureError
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure libraries not available: {e}")
    AZURE_AVAILABLE = False


class AzureBlobStorageSFTPOperations(BaseSFTPOperations):
    """
    Azure Blob Storage to SFTP streaming operations
    Provides efficient streaming of data from Blob Storage to SFTP servers
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Blob Storage to SFTP operations
        
        Args:
            config: Configuration dictionary containing both SFTP and Azure settings
                SFTP settings: hostname, username, password/private_key, etc.
                Azure settings:
                - account_name: Storage account name
                - account_key: Storage account key (optional)
                - connection_string: Storage connection string (optional)
                - sas_token: SAS token (optional)
                - credential: Azure credential object (optional)
                - account_url: Storage account URL (optional)
                - default_container: Default container for operations
        """
        super().__init__(config)
        
        # Azure configuration
        self.account_name = config.get('account_name')
        self.account_key = config.get('account_key')
        self.connection_string = config.get('connection_string')
        self.sas_token = config.get('sas_token')
        self.credential = config.get('credential')
        self.account_url = config.get('account_url')
        self.default_container = config.get('default_container')
        
        # Initialize Azure clients
        self.blob_service_client = None
        self.container_client = None
        
        if AZURE_AVAILABLE:
            try:
                self._initialize_azure_clients()
            except Exception as e:
                logger.error(f"Failed to initialize Azure clients: {e}")
                # Continue without Azure - operations will fail gracefully
        else:
            logger.warning("Azure libraries not available, Blob Storage operations will not work")
            
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
                
            # Get default container client if specified
            if self.default_container:
                self.container_client = self.blob_service_client.get_container_client(
                    container=self.default_container
                )
                
                try:
                    # Test container access
                    self.container_client.get_container_properties()
                    logger.info(f"Verified access to default container: {self.default_container}")
                except ResourceNotFoundError:
                    logger.warning(f"Default Azure container '{self.default_container}' not found")
                except Exception as e:
                    logger.warning(f"Could not access default container: {e}")
                    
            logger.info("Azure Blob Storage clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error initializing Azure clients: {e}")
            raise
            
    def stream_from_cloud_storage(self, cloud_path: str, remote_path: str, 
                                 chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from Azure Blob Storage to SFTP server
        
        Args:
            cloud_path: Azure path (azure://container/blob or just blob if default_container set)
            remote_path: Remote SFTP path
            chunk_size: Size of chunks to stream (uses buffer_size if not specified)
            
        Returns:
            Dictionary with transfer results
        """
        if not self.blob_service_client:
            raise SFTPOperationError("Azure Blob Storage not configured")
            
        try:
            # Parse Azure path
            if cloud_path.startswith('azure://'):
                # Full Azure URL
                path_parts = cloud_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    raise SFTPOperationError("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = cloud_path.lstrip('/')
                
            # Get blob client and properties
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            if not blob_client.exists():
                raise SFTPOperationError(f"Azure blob not found: azure://{container_client.container_name}/{blob_name}")
                
            # Get blob properties
            blob_properties = blob_client.get_blob_properties()
            file_size = blob_properties.size
            last_modified = blob_properties.last_modified
            etag = blob_properties.etag
            content_md5 = blob_properties.content_settings.content_md5
            
            logger.info(f"Streaming Azure blob: azure://{container_client.container_name}/{blob_name} "
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
                        
                        # Download chunk from Azure Blob Storage
                        download_stream = blob_client.download_blob(offset=chunk_start, length=chunk_end - chunk_start + 1)
                        chunk_data = download_stream.readall()
                        
                        # Write chunk to SFTP
                        remote_file.write(chunk_data)
                        bytes_transferred += len(chunk_data)
                        
                        # Log progress for large files
                        if bytes_transferred % (10 * 1024 * 1024) == 0:  # Every 10MB
                            progress = (bytes_transferred / file_size) * 100
                            logger.info(f"Transfer progress: {progress:.1f}% "
                                       f"({bytes_transferred:,}/{file_size:,} bytes)")
                            
            except Exception as e:
                logger.error(f"Error during Azure Blob to SFTP transfer: {e}")
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
                'source': f"azure://{container_client.container_name}/{blob_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'file_size': file_size,
                'duration_seconds': duration,
                'transfer_rate_mbps': transfer_rate,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'source_etag': etag,
                'source_content_md5': content_md5,
                'source_last_modified': last_modified.isoformat() if last_modified else None
            }
            
            logger.info(f"Successfully streamed azure://{container_client.container_name}/{blob_name} to {remote_path} "
                       f"({bytes_transferred:,} bytes in {duration:.2f}s at {transfer_rate:.2f} MB/s)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error streaming from Azure Blob Storage to SFTP: {e}")
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
        Batch stream multiple files from Azure Blob Storage to SFTP server
        
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
        
    def list_azure_blobs(self, prefix: str = '', container_name: str = None, 
                        max_blobs: int = 1000) -> List[Dict[str, Any]]:
        """
        List blobs in Azure container with given prefix
        
        Args:
            prefix: Azure prefix to search under
            container_name: Azure container name (uses default if None)
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob information dictionaries
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return []
            
        try:
            if container_name:
                container_client = self.blob_service_client.get_container_client(container_name)
            elif self.container_client:
                container_client = self.container_client
            else:
                logger.error("No Azure container specified")
                return []
                
            blobs = []
            blob_list = container_client.list_blobs(
                name_starts_with=prefix if prefix else None,
                results_per_page=min(max_blobs, 5000)  # Azure limit
            )
            
            for blob in blob_list:
                blobs.append({
                    'name': blob.name,
                    'size': blob.size,
                    'last_modified': blob.last_modified.isoformat() if blob.last_modified else None,
                    'etag': blob.etag,
                    'content_md5': blob.content_settings.content_md5 if blob.content_settings else None,
                    'content_type': blob.content_settings.content_type if blob.content_settings else None,
                    'azure_path': f"azure://{container_client.container_name}/{blob.name}"
                })
                if len(blobs) >= max_blobs:
                    break
                    
            logger.info(f"Listed {len(blobs)} Azure blobs with prefix '{prefix}' in container '{container_client.container_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing Azure blobs: {e}")
            return []
            
    def sync_azure_container_to_sftp(self, azure_prefix: str, sftp_directory: str,
                                    container_name: str = None, 
                                    file_pattern: str = None) -> Dict[str, Any]:
        """
        Sync entire Azure container/prefix to SFTP directory
        
        Args:
            azure_prefix: Azure prefix/directory to sync
            sftp_directory: SFTP destination directory
            container_name: Azure container name (uses default if None)
            file_pattern: Optional file pattern to filter files
            
        Returns:
            Dictionary with sync results
        """
        try:
            # List Azure blobs
            azure_blobs = self.list_azure_blobs(azure_prefix, container_name)
            
            if not azure_blobs:
                return {
                    'success': True,
                    'message': 'No blobs found to sync',
                    'files_synced': 0
                }
                
            # Filter by file pattern if specified
            if file_pattern:
                import fnmatch
                azure_blobs = [blob for blob in azure_blobs 
                              if fnmatch.fnmatch(blob['name'], file_pattern)]
                
            # Prepare transfer list
            transfer_list = []
            for blob in azure_blobs:
                # Calculate relative path
                relative_path = blob['name'][len(azure_prefix):].lstrip('/')
                sftp_path = f"{sftp_directory.rstrip('/')}/{relative_path}"
                
                transfer_list.append({
                    'source': blob['azure_path'],
                    'destination': sftp_path
                })
                
            # Execute batch transfer
            result = self.batch_stream_from_cloud_storage(transfer_list)
            
            result['sync_details'] = {
                'azure_prefix': azure_prefix,
                'sftp_directory': sftp_directory,
                'container_name': container_name or self.default_container,
                'file_pattern': file_pattern,
                'blobs_found': len(azure_blobs)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing Azure container to SFTP: {e}")
            return {
                'success': False,
                'error': str(e),
                'files_synced': 0
            }
            
    def stream_with_compression(self, cloud_path: str, remote_path: str,
                               compression_type: str = 'gzip',
                               chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from Azure with on-the-fly compression
        
        Args:
            cloud_path: Azure blob path
            remote_path: Remote SFTP path
            compression_type: Compression type ('gzip', 'bz2', 'lzma')
            chunk_size: Size of chunks to stream
            
        Returns:
            Dictionary with transfer results
        """
        if not self.blob_service_client:
            raise SFTPOperationError("Azure Blob Storage not configured")
            
        try:
            import gzip
            import bz2
            import lzma
            
            # Parse Azure path
            if cloud_path.startswith('azure://'):
                path_parts = cloud_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                if not self.default_container or not self.container_client:
                    raise SFTPOperationError("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = cloud_path.lstrip('/')
                
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            if not blob_client.exists():
                raise SFTPOperationError(f"Azure blob not found: azure://{container_client.container_name}/{blob_name}")
                
            blob_properties = blob_client.get_blob_properties()
            file_size = blob_properties.size
            stream_chunk_size = chunk_size or self.buffer_size
            
            # Ensure SFTP connection and directory
            self.ensure_connection()
            remote_dir = '/'.join(remote_path.split('/')[:-1])
            if remote_dir:
                self.create_directory(remote_dir)
                
            start_time = datetime.now()
            bytes_transferred = 0
            compressed_bytes = 0
            
            # Select compression method
            if compression_type == 'gzip':
                compressor = gzip.GzipFile(fileobj=io.BytesIO(), mode='wb')
            elif compression_type == 'bz2':
                compressor = bz2.BZ2Compressor()
            elif compression_type == 'lzma':
                compressor = lzma.LZMACompressor()
            else:
                raise ValueError(f"Unsupported compression type: {compression_type}")
                
            with self.sftp_client.open(remote_path, 'wb') as remote_file:
                for chunk_start in range(0, file_size, stream_chunk_size):
                    chunk_end = min(chunk_start + stream_chunk_size - 1, file_size - 1)
                    download_stream = blob_client.download_blob(offset=chunk_start, length=chunk_end - chunk_start + 1)
                    chunk_data = download_stream.readall()
                    
                    # Compress chunk
                    if compression_type == 'gzip':
                        compressor.write(chunk_data)
                        compressed_chunk = compressor.flush()
                    elif compression_type in ['bz2', 'lzma']:
                        compressed_chunk = compressor.compress(chunk_data)
                    else:
                        compressed_chunk = chunk_data
                        
                    if compressed_chunk:
                        remote_file.write(compressed_chunk)
                        compressed_bytes += len(compressed_chunk)
                        
                    bytes_transferred += len(chunk_data)
                    
                # Finalize compression
                if compression_type == 'gzip':
                    compressor.close()
                elif compression_type in ['bz2', 'lzma']:
                    final_chunk = compressor.flush()
                    if final_chunk:
                        remote_file.write(final_chunk)
                        compressed_bytes += len(final_chunk)
                        
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            compression_ratio = (1 - compressed_bytes / bytes_transferred) * 100 if bytes_transferred > 0 else 0
            
            return {
                'success': True,
                'source': f"azure://{container_client.container_name}/{blob_name}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'compressed_bytes': compressed_bytes,
                'compression_ratio_percent': compression_ratio,
                'compression_type': compression_type,
                'duration_seconds': duration
            }
            
        except Exception as e:
            logger.error(f"Error streaming with compression: {e}")
            return {
                'success': False,
                'source': cloud_path,
                'destination': remote_path,
                'error': str(e),
                'bytes_transferred': 0
            }
            
    def get_azure_info(self) -> Dict[str, Any]:
        """
        Get Azure configuration information
        
        Returns:
            Dictionary with Azure info (without sensitive data)
        """
        return {
            'azure_configured': self.blob_service_client is not None,
            'default_container': self.default_container,
            'account_name': self.account_name,
            'account_url': self.account_url,
            'has_account_key': self.account_key is not None,
            'has_sas_token': self.sas_token is not None,
            'has_connection_string': self.connection_string is not None
        }