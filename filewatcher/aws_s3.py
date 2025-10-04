"""
AWS S3 File Watcher
Provides file monitoring operations for Amazon S3 buckets
Supports both specific file monitoring and pattern-based searches
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import fnmatch
import re

from .base import BaseFileWatcher, TimeOutError

logger = logging.getLogger(__name__)

try:
    import boto3
    import s3fs
    from botocore.exceptions import ClientError, NoCredentialsError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False


class S3FileWatcher(BaseFileWatcher):
    """
    AWS S3 file watcher class
    Monitors S3 buckets for file arrivals with pattern matching support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 file watcher
        
        Args:
            config: Configuration dictionary containing S3 settings
                - bucket_name: S3 bucket name (None for local file system)
                - region_name: AWS region
                - aws_access_key_id: AWS access key (optional, uses default credentials)
                - aws_secret_access_key: AWS secret key (optional)
                - aws_session_token: AWS session token (optional)
                - profile_name: AWS profile name (optional)
        """
        super().__init__(config)
        
        self.bucket_name = config.get('bucket_name')
        self.region_name = config.get('region_name', 'us-east-1')
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.profile_name = config.get('profile_name')
        
        # Initialize AWS clients
        self.s3_client = None
        self.s3_resource = None
        self.s3fs = None
        
        if AWS_AVAILABLE and self.bucket_name:
            try:
                self._initialize_aws_clients()
            except Exception as e:
                logger.error(f"Failed to initialize AWS S3 clients: {e}")
                raise
        elif not self.bucket_name:
            logger.info("Bucket name not specified, using local file system monitoring")
        else:
            logger.warning("AWS libraries not available, falling back to local file system")
            
    def _initialize_aws_clients(self):
        """
        Initialize AWS S3 clients and resources
        """
        try:
            # Prepare session arguments
            session_kwargs = {}
            if self.profile_name:
                session_kwargs['profile_name'] = self.profile_name
            if self.region_name:
                session_kwargs['region_name'] = self.region_name
                
            # Create session
            session = boto3.Session(**session_kwargs)
            
            # Prepare client arguments
            client_kwargs = {}
            if self.region_name:
                client_kwargs['region_name'] = self.region_name
            if self.aws_access_key_id and self.aws_secret_access_key:
                client_kwargs['aws_access_key_id'] = self.aws_access_key_id
                client_kwargs['aws_secret_access_key'] = self.aws_secret_access_key
                if self.aws_session_token:
                    client_kwargs['aws_session_token'] = self.aws_session_token
                    
            # Initialize clients
            if client_kwargs:
                self.s3_client = boto3.client('s3', **client_kwargs)
                self.s3_resource = boto3.resource('s3', **client_kwargs)
            else:
                self.s3_client = session.client('s3')
                self.s3_resource = session.resource('s3')
                
            # Initialize s3fs for file system operations
            s3fs_kwargs = {}
            if self.aws_access_key_id and self.aws_secret_access_key:
                s3fs_kwargs['key'] = self.aws_access_key_id
                s3fs_kwargs['secret'] = self.aws_secret_access_key
                if self.aws_session_token:
                    s3fs_kwargs['token'] = self.aws_session_token
                    
            self.s3fs = s3fs.S3FileSystem(**s3fs_kwargs)
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            logger.info(f"AWS S3 clients initialized successfully for bucket: {self.bucket_name}")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{self.bucket_name}' not found")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket '{self.bucket_name}'")
            else:
                logger.error(f"AWS S3 client initialization error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS S3 clients: {e}")
            raise
            
    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a specific file exists in S3 bucket or local file system
        
        Args:
            file_path: Path to the file (S3 key or local path)
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.bucket_name:
            # Use local file system
            return self.check_local_file_exists(file_path)
            
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
            
        try:
            # Remove leading slash if present
            s3_key = file_path.lstrip('/')
            
            # Use head_object to check if file exists
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            logger.debug(f"S3 file exists: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.debug(f"S3 file not found: s3://{self.bucket_name}/{s3_key}")
                return False
            else:
                logger.error(f"Error checking S3 file existence: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking S3 file: {e}")
            return False
            
    def search_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files in S3 bucket using a pattern
        
        Args:
            pattern: Pattern to search for (supports wildcards)
            
        Returns:
            List of matching S3 keys or local file paths
        """
        if not self.bucket_name:
            # Use local file system
            return self.search_local_files_by_pattern(pattern)
            
        if not self.s3_client:
            logger.error("S3 client not initialized")
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
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            # Filter objects matching pattern
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Check if key matches pattern
                        if self._match_s3_pattern(key, pattern):
                            matching_files.append(key)
                            
            logger.debug(f"S3 pattern search '{pattern}' found {len(matching_files)} files in bucket '{self.bucket_name}'")
            return matching_files
            
        except Exception as e:
            logger.error(f"Error searching S3 files with pattern '{pattern}': {e}")
            return []
            
    def _match_s3_pattern(self, s3_key: str, pattern: str) -> bool:
        """
        Check if S3 key matches the given pattern
        
        Args:
            s3_key: S3 object key
            pattern: Pattern with wildcards
            
        Returns:
            True if key matches pattern, False otherwise
        """
        try:
            # Use fnmatch for pattern matching
            return fnmatch.fnmatch(s3_key, pattern)
        except Exception as e:
            logger.error(f"Error matching pattern '{pattern}' against key '{s3_key}': {e}")
            return False
            
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file in S3 or local file system
        
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
                
        if not self.s3_client:
            return {'path': file_path, 'exists': False, 'error': 'S3 client not initialized'}
            
        try:
            s3_key = file_path.lstrip('/')
            
            # Get object metadata
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            return {
                'bucket': self.bucket_name,
                'key': s3_key,
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', '').isoformat() if response.get('LastModified') else None,
                'etag': response.get('ETag', '').strip('"'),
                'content_type': response.get('ContentType', ''),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'exists': True
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return {'bucket': self.bucket_name, 'key': s3_key, 'exists': False}
            else:
                logger.error(f"Error getting S3 file info: {e}")
                return {'bucket': self.bucket_name, 'key': s3_key, 'exists': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error getting S3 file info: {e}")
            return {'bucket': self.bucket_name, 'key': file_path, 'exists': False, 'error': str(e)}
            
    def list_files_in_prefix(self, prefix: str, max_files: int = 1000) -> List[Dict[str, Any]]:
        """
        List files in S3 bucket with given prefix
        
        Args:
            prefix: S3 prefix to search under
            max_files: Maximum number of files to return
            
        Returns:
            List of file information dictionaries
        """
        if not self.bucket_name:
            logger.error("No bucket specified for S3 operations")
            return []
            
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return []
            
        try:
            files = []
            prefix = prefix.lstrip('/')
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_files}
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'].strip('"'),
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })
                        
            logger.debug(f"Listed {len(files)} files with prefix '{prefix}' in bucket '{self.bucket_name}'")
            return files
            
        except Exception as e:
            logger.error(f"Error listing S3 files with prefix '{prefix}': {e}")
            return []
            
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 to local file system
        
        Args:
            s3_key: S3 object key
            local_path: Local file path to save to
            
        Returns:
            True if download successful, False otherwise
        """
        if not self.bucket_name or not self.s3_client:
            logger.error("S3 not configured for download operations")
            return False
            
        try:
            s3_key = s3_key.lstrip('/')
            
            # Create local directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading S3 file: {e}")
            return False
            
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """
        Upload a file from local file system to S3
        
        Args:
            local_path: Local file path
            s3_key: S3 object key to upload to
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.bucket_name or not self.s3_client:
            logger.error("S3 not configured for upload operations")
            return False
            
        try:
            s3_key = s3_key.lstrip('/')
            
            # Upload file
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            
            logger.info(f"Uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False
            
    def wait_for_rna_wizard_files(self, base_pattern: str, max_wait: str, 
                                 expected_count: Optional[int] = None) -> List[str]:
        """
        Special method for waiting for RNA wizard files
        Handles the specific wizard_rna folder structure
        
        Args:
            base_pattern: Base pattern for RNA wizard files
            max_wait: Maximum wait time
            expected_count: Expected number of files
            
        Returns:
            List of found RNA wizard files
        """
        logger.info(f"Starting RNA wizard file watch for pattern: {base_pattern}")
        
        return self.wait_for_files_by_pattern(
            pattern=base_pattern,
            max_wait=max_wait,
            expected_count=expected_count,
            is_rna_wizard=True
        )
        
    def get_bucket_info(self) -> Dict[str, Any]:
        """
        Get information about the S3 bucket
        
        Returns:
            Dictionary with bucket information
        """
        if not self.bucket_name or not self.s3_client:
            return {'error': 'S3 not configured'}
            
        try:
            # Get bucket location
            location_response = self.s3_client.get_bucket_location(Bucket=self.bucket_name)
            region = location_response.get('LocationConstraint') or 'us-east-1'
            
            # Get bucket versioning
            try:
                versioning_response = self.s3_client.get_bucket_versioning(Bucket=self.bucket_name)
                versioning_status = versioning_response.get('Status', 'Disabled')
            except:
                versioning_status = 'Unknown'
                
            return {
                'bucket_name': self.bucket_name,
                'region': region,
                'versioning': versioning_status,
                'configured_region': self.region_name
            }
            
        except Exception as e:
            logger.error(f"Error getting bucket info: {e}")
            return {'bucket_name': self.bucket_name, 'error': str(e)}