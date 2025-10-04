"""
AWS S3 to SFTP Streaming Operations
Streams data from AWS S3 to SFTP servers with efficient memory management
"""

import logging
import io
import concurrent.futures
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_sftp import BaseSFTPOperations, SFTPTransferError, SFTPOperationError

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False


class AWSS3SFTPOperations(BaseSFTPOperations):
    """
    AWS S3 to SFTP streaming operations
    Provides efficient streaming of data from S3 to SFTP servers
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AWS S3 to SFTP operations
        
        Args:
            config: Configuration dictionary containing both SFTP and AWS settings
                SFTP settings: hostname, username, password/private_key, etc.
                AWS settings:
                - aws_access_key_id: AWS access key (optional, uses boto3 default chain)
                - aws_secret_access_key: AWS secret key (optional)
                - aws_session_token: AWS session token (optional)
                - region_name: AWS region (default: us-east-1)
                - default_bucket: Default S3 bucket for operations
        """
        super().__init__(config)
        
        # AWS configuration
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.region_name = config.get('region_name', 'us-east-1')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize AWS clients
        self.s3_client = None
        self.s3_resource = None
        
        if AWS_AVAILABLE:
            try:
                self._initialize_aws_clients()
            except Exception as e:
                logger.error(f"Failed to initialize AWS clients: {e}")
                # Continue without AWS - operations will fail gracefully
        else:
            logger.warning("AWS libraries not available, S3 operations will not work")
            
    def _initialize_aws_clients(self):
        """
        Initialize AWS S3 clients
        """
        try:
            # Prepare session kwargs
            session_kwargs = {'region_name': self.region_name}
            
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs.update({
                    'aws_access_key_id': self.aws_access_key_id,
                    'aws_secret_access_key': self.aws_secret_access_key
                })
                
            if self.aws_session_token:
                session_kwargs['aws_session_token'] = self.aws_session_token
                
            # Create session and clients
            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client('s3')
            self.s3_resource = session.resource('s3')
            
            # Test default bucket access if specified
            if self.default_bucket:
                try:
                    self.s3_client.head_bucket(Bucket=self.default_bucket)
                    logger.info(f"Verified access to default S3 bucket: {self.default_bucket}")
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.warning(f"Default S3 bucket '{self.default_bucket}' not found")
                    else:
                        logger.warning(f"Could not access default bucket: {e}")
                        
            logger.info("AWS S3 clients initialized successfully")
            
        except (NoCredentialsError, ClientError) as e:
            logger.error(f"AWS credentials or permissions error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS clients: {e}")
            raise
            
    def stream_from_cloud_storage(self, cloud_path: str, remote_path: str, 
                                 chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from S3 to SFTP server
        
        Args:
            cloud_path: S3 path (s3://bucket/key or just key if default_bucket set)
            remote_path: Remote SFTP path
            chunk_size: Size of chunks to stream (uses buffer_size if not specified)
            
        Returns:
            Dictionary with transfer results
        """
        if not self.s3_client:
            raise SFTPOperationError("AWS S3 not configured")
            
        try:
            # Parse S3 path
            if cloud_path.startswith('s3://'):
                # Full S3 URL
                path_parts = cloud_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise SFTPOperationError("No S3 bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_key = cloud_path.lstrip('/')
                
            # Get object metadata
            try:
                response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
                file_size = response['ContentLength']
                last_modified = response['LastModified']
                etag = response['ETag'].strip('"')
                
                logger.info(f"Streaming S3 object: s3://{bucket_name}/{object_key} "
                           f"(size: {file_size} bytes) to SFTP: {remote_path}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    raise SFTPOperationError(f"S3 object not found: s3://{bucket_name}/{object_key}")
                else:
                    raise SFTPOperationError(f"Error accessing S3 object: {e}")
                    
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
                # Stream object from S3
                s3_object = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                
                with self.sftp_client.open(remote_path, 'wb') as remote_file:
                    body = s3_object['Body']
                    
                    while True:
                        chunk = body.read(stream_chunk_size)
                        if not chunk:
                            break
                            
                        remote_file.write(chunk)
                        bytes_transferred += len(chunk)
                        
                        # Log progress for large files
                        if bytes_transferred % (10 * 1024 * 1024) == 0:  # Every 10MB
                            progress = (bytes_transferred / file_size) * 100
                            logger.info(f"Transfer progress: {progress:.1f}% "
                                       f"({bytes_transferred:,}/{file_size:,} bytes)")
                            
                # Close the S3 body stream
                body.close()
                
            except Exception as e:
                logger.error(f"Error during S3 to SFTP transfer: {e}")
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
                'source': f"s3://{bucket_name}/{object_key}",
                'destination': remote_path,
                'bytes_transferred': bytes_transferred,
                'file_size': file_size,
                'duration_seconds': duration,
                'transfer_rate_mbps': transfer_rate,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'source_etag': etag,
                'source_last_modified': last_modified.isoformat()
            }
            
            logger.info(f"Successfully streamed s3://{bucket_name}/{object_key} to {remote_path} "
                       f"({bytes_transferred:,} bytes in {duration:.2f}s at {transfer_rate:.2f} MB/s)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error streaming from S3 to SFTP: {e}")
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
        Batch stream multiple files from S3 to SFTP server
        
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
        
    def list_s3_objects(self, prefix: str = '', bucket_name: str = None, 
                       max_objects: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with given prefix
        
        Args:
            prefix: S3 prefix to search under
            bucket_name: S3 bucket name (uses default if None)
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object information dictionaries
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured")
            return []
            
        try:
            bucket_name = bucket_name or self.default_bucket
            if not bucket_name:
                logger.error("No S3 bucket specified")
                return []
                
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'].strip('"'),
                            's3_path': f"s3://{bucket_name}/{obj['Key']}"
                        })
                        if len(objects) >= max_objects:
                            break
                            
                if len(objects) >= max_objects:
                    break
                    
            logger.info(f"Listed {len(objects)} S3 objects with prefix '{prefix}' in bucket '{bucket_name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []
            
    def sync_s3_directory_to_sftp(self, s3_prefix: str, sftp_directory: str,
                                 bucket_name: str = None, 
                                 file_pattern: str = None) -> Dict[str, Any]:
        """
        Sync entire S3 directory/prefix to SFTP directory
        
        Args:
            s3_prefix: S3 prefix/directory to sync
            sftp_directory: SFTP destination directory
            bucket_name: S3 bucket name (uses default if None)
            file_pattern: Optional file pattern to filter files
            
        Returns:
            Dictionary with sync results
        """
        try:
            # List S3 objects
            s3_objects = self.list_s3_objects(s3_prefix, bucket_name)
            
            if not s3_objects:
                return {
                    'success': True,
                    'message': 'No objects found to sync',
                    'files_synced': 0
                }
                
            # Filter by file pattern if specified
            if file_pattern:
                import fnmatch
                s3_objects = [obj for obj in s3_objects 
                             if fnmatch.fnmatch(obj['key'], file_pattern)]
                
            # Prepare transfer list
            transfer_list = []
            for obj in s3_objects:
                # Calculate relative path
                relative_path = obj['key'][len(s3_prefix):].lstrip('/')
                sftp_path = f"{sftp_directory.rstrip('/')}/{relative_path}"
                
                transfer_list.append({
                    'source': obj['s3_path'],
                    'destination': sftp_path
                })
                
            # Execute batch transfer
            result = self.batch_stream_from_cloud_storage(transfer_list)
            
            result['sync_details'] = {
                's3_prefix': s3_prefix,
                'sftp_directory': sftp_directory,
                'bucket_name': bucket_name or self.default_bucket,
                'file_pattern': file_pattern,
                'objects_found': len(s3_objects)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error syncing S3 directory to SFTP: {e}")
            return {
                'success': False,
                'error': str(e),
                'files_synced': 0
            }
            
    def get_aws_info(self) -> Dict[str, Any]:
        """
        Get AWS configuration information
        
        Returns:
            Dictionary with AWS info (without sensitive data)
        """
        return {
            'aws_configured': self.s3_client is not None,
            'default_bucket': self.default_bucket,
            'region_name': self.region_name,
            'has_access_key': self.aws_access_key_id is not None,
            'has_session_token': self.aws_session_token is not None
        }