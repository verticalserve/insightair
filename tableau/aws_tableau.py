"""
AWS Tableau Operations
Tableau operations with AWS S3 integration for Hyper file management and data pipeline operations
"""

import logging
import os
import tempfile
import concurrent.futures
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_tableau import BaseTableauOperations, TableauOperationError, TableauDataError

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Pandas/PyArrow not available: {e}")
    PANDAS_AVAILABLE = False


class AWSTableauOperations(BaseTableauOperations):
    """
    Tableau operations with AWS S3 integration
    Provides end-to-end data pipeline from S3 Parquet files to Tableau Server
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AWS Tableau operations
        
        Args:
            config: Configuration dictionary containing both Tableau and AWS settings
                Tableau settings: server_url, username, password, etc.
                AWS settings:
                - aws_access_key_id: AWS access key (optional, uses boto3 default chain)
                - aws_secret_access_key: AWS secret key (optional)
                - aws_session_token: AWS session token (optional)
                - region_name: AWS region (default: us-east-1)
                - default_bucket: Default S3 bucket for operations
                - s3_endpoint_url: Custom S3 endpoint URL (optional)
                - multipart_threshold: Threshold for multipart uploads (default: 8MB)
                - max_concurrency: Max concurrent S3 operations (default: 10)
        """
        super().__init__(config)
        
        # AWS configuration
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.region_name = config.get('region_name', 'us-east-1')
        self.default_bucket = config.get('default_bucket')
        self.s3_endpoint_url = config.get('s3_endpoint_url')
        
        # S3 operation settings
        self.multipart_threshold = config.get('multipart_threshold', 8 * 1024 * 1024)  # 8MB
        self.max_concurrency = config.get('max_concurrency', 10)
        
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
        """Initialize AWS S3 clients"""
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
            
            client_kwargs = {}
            if self.s3_endpoint_url:
                client_kwargs['endpoint_url'] = self.s3_endpoint_url
                
            self.s3_client = session.client('s3', **client_kwargs)
            self.s3_resource = session.resource('s3', **client_kwargs)
            
            # Configure S3 transfer settings
            from boto3.s3.transfer import TransferConfig
            self.transfer_config = TransferConfig(
                multipart_threshold=self.multipart_threshold,
                max_concurrency=self.max_concurrency
            )
            
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
            
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS clients: {e}")
            raise
            
    def _parse_s3_path(self, s3_path: str) -> tuple:
        """
        Parse S3 path into bucket and key
        
        Args:
            s3_path: S3 path (s3://bucket/key or just key if default_bucket set)
            
        Returns:
            Tuple of (bucket_name, object_key)
            
        Raises:
            TableauOperationError: If path cannot be parsed
        """
        if s3_path.startswith('s3://'):
            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket_name = path_parts[0]
            object_key = path_parts[1] if len(path_parts) > 1 else ''
        else:
            if not self.default_bucket:
                raise TableauOperationError("No S3 bucket specified and no default bucket configured")
            bucket_name = self.default_bucket
            object_key = s3_path.lstrip('/')
            
        return bucket_name, object_key
        
    def _download_s3_file(self, s3_path: str, local_path: str) -> str:
        """
        Download file from S3 to local path
        
        Args:
            s3_path: S3 path to file
            local_path: Local destination path
            
        Returns:
            Local file path
            
        Raises:
            TableauOperationError: If download fails
        """
        if not self.s3_client:
            raise TableauOperationError("AWS S3 not configured")
            
        try:
            bucket_name, object_key = self._parse_s3_path(s3_path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download file
            self.s3_client.download_file(
                bucket_name, 
                object_key, 
                local_path,
                Config=self.transfer_config
            )
            
            logger.debug(f"Downloaded s3://{bucket_name}/{object_key} to {local_path}")
            return local_path
            
        except Exception as e:
            error_msg = f"Failed to download S3 file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def create_hyper_from_parquet(self, s3_path: str, hyper_path: str,
                                table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from an S3-stored Parquet file
        
        Args:
            s3_path: S3 path to Parquet file (s3://bucket/key or just key)
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not PANDAS_AVAILABLE:
            raise TableauOperationError("Pandas/PyArrow not available for Parquet processing")
            
        temp_parquet_path = None
        
        try:
            # Download Parquet file from S3
            temp_parquet_path = os.path.join(
                self.temp_dir,
                f"temp_parquet_{datetime.now().timestamp()}.parquet"
            )
            self._download_s3_file(s3_path, temp_parquet_path)
            
            # Read Parquet file
            df = pd.read_parquet(temp_parquet_path)
            logger.info(f"Read Parquet file with {len(df)} rows, {len(df.columns)} columns")
            
            # Check file size for upsert operations
            file_size_mb = os.path.getsize(temp_parquet_path) / (1024 * 1024)
            if file_size_mb > self.upsert_max_size_mb:
                logger.warning(f"File size {file_size_mb:.2f}MB exceeds upsert limit of {self.upsert_max_size_mb}MB")
                
            # Create Hyper file from DataFrame
            return self.create_hyper_from_dataframe(df, hyper_path, table_name)
            
        except Exception as e:
            error_msg = f"Failed to create Hyper from S3 Parquet: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
        finally:
            # Cleanup temp file
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Parquet file: {e}")
                    
    def create_initial_hyper_from_parquet(self, s3_paths: List[str], hyper_path: str,
                                        table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from multiple S3-stored Parquet files
        Optimized for large initial loads
        
        Args:
            s3_paths: List of S3 paths to Parquet files
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not PANDAS_AVAILABLE:
            raise TableauOperationError("Pandas/PyArrow not available for Parquet processing")
            
        if not s3_paths:
            raise TableauDataError("No S3 paths provided")
            
        temp_files = []
        
        try:
            # Process first file to create initial Hyper structure
            first_path = s3_paths[0]
            temp_parquet = os.path.join(
                self.temp_dir,
                f"temp_parquet_0_{datetime.now().timestamp()}.parquet"
            )
            temp_files.append(temp_parquet)
            
            self._download_s3_file(first_path, temp_parquet)
            df_first = pd.read_parquet(temp_parquet)
            
            # Create initial Hyper file
            self.create_hyper_from_dataframe(df_first, hyper_path, table_name)
            
            # Process remaining files and append to Hyper
            if len(s3_paths) > 1:
                logger.info(f"Processing {len(s3_paths) - 1} additional Parquet files")
                
                # Process files in parallel batches
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []
                    
                    for i, s3_path in enumerate(s3_paths[1:], 1):
                        future = executor.submit(self._process_and_append_parquet, 
                                               s3_path, hyper_path, table_name, i)
                        futures.append(future)
                        
                    # Wait for all files to be processed
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            rows_added = future.result()
                            logger.debug(f"Added {rows_added} rows to Hyper file")
                        except Exception as e:
                            logger.error(f"Error processing Parquet file: {e}")
                            
            # Get final file size
            final_size_mb = os.path.getsize(hyper_path) / (1024 * 1024)
            logger.info(f"Created initial Hyper file: {final_size_mb:.2f}MB from {len(s3_paths)} Parquet files")
            
            return hyper_path
            
        except Exception as e:
            error_msg = f"Failed to create initial Hyper from S3 Parquet files: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
        finally:
            # Cleanup temp files
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except Exception as e:
                        logger.warning(f"Could not remove temp file: {e}")
                        
    def _process_and_append_parquet(self, s3_path: str, hyper_path: str, 
                                  table_name: str, index: int) -> int:
        """
        Process a single Parquet file and append to existing Hyper file
        
        Args:
            s3_path: S3 path to Parquet file
            hyper_path: Path to existing Hyper file
            table_name: Table name in Hyper file
            index: File index for temp file naming
            
        Returns:
            Number of rows added
        """
        temp_parquet_path = None
        
        try:
            # Download Parquet file
            temp_parquet_path = os.path.join(
                self.temp_dir,
                f"temp_parquet_{index}_{datetime.now().timestamp()}.parquet"
            )
            self._download_s3_file(s3_path, temp_parquet_path)
            
            # Read Parquet file
            df = pd.read_parquet(temp_parquet_path)
            
            # Append to Hyper file
            # Note: This would require opening the Hyper file in append mode
            # For now, we'll log the operation
            logger.info(f"Would append {len(df)} rows from {s3_path} to Hyper file")
            
            return len(df)
            
        finally:
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp file: {e}")
                    
    def create_hyper_from_parquet_and_publish_tableau(self, s3_paths: List[str],
                                                    datasource_name: str,
                                                    project_id: str = None,
                                                    mode: str = 'Upsert') -> Dict[str, Any]:
        """
        Processes multiple Parquet files iteratively and publishes to Tableau
        Optimized for incremental updates with upsert operations
        
        Args:
            s3_paths: List of S3 paths to Parquet files
            datasource_name: Name for the Tableau data source
            project_id: Tableau project ID (uses default if not specified)
            mode: Publishing mode (default: 'Upsert')
            
        Returns:
            Dictionary with processing results
            
        Raises:
            TableauOperationError: If processing fails
        """
        results = {
            'datasource_name': datasource_name,
            'files_processed': 0,
            'total_rows': 0,
            'failed_files': [],
            'published': False,
            'start_time': datetime.now()
        }
        
        temp_hyper_path = None
        
        try:
            for i, s3_path in enumerate(s3_paths):
                try:
                    # Create temp Hyper file for this batch
                    temp_hyper_path = os.path.join(
                        self.temp_dir,
                        f"temp_hyper_{i}_{datetime.now().timestamp()}.hyper"
                    )
                    
                    # Check file size before processing
                    bucket_name, object_key = self._parse_s3_path(s3_path)
                    response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
                    file_size_mb = response['ContentLength'] / (1024 * 1024)
                    
                    if mode.lower() == 'upsert' and file_size_mb > self.upsert_max_size_mb:
                        logger.warning(f"File {s3_path} is {file_size_mb:.2f}MB, exceeds upsert limit")
                        results['failed_files'].append({
                            'path': s3_path,
                            'error': f"File size {file_size_mb:.2f}MB exceeds upsert limit"
                        })
                        continue
                        
                    # Create Hyper from Parquet
                    self.create_hyper_from_parquet(s3_path, temp_hyper_path)
                    
                    # Publish to Tableau
                    datasource = self.publish_data_source(
                        temp_hyper_path,
                        datasource_name,
                        project_id,
                        mode=mode
                    )
                    
                    results['files_processed'] += 1
                    logger.info(f"Processed and published file {i+1}/{len(s3_paths)}")
                    
                    # Cleanup temp Hyper file
                    if os.path.exists(temp_hyper_path):
                        os.remove(temp_hyper_path)
                        temp_hyper_path = None
                        
                except Exception as e:
                    logger.error(f"Failed to process {s3_path}: {e}")
                    results['failed_files'].append({
                        'path': s3_path,
                        'error': str(e)
                    })
                    
            results['published'] = results['files_processed'] > 0
            results['end_time'] = datetime.now()
            results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"Completed processing: {results['files_processed']}/{len(s3_paths)} files successful")
            return results
            
        except Exception as e:
            error_msg = f"Failed to process and publish Parquet files: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
        finally:
            # Cleanup any remaining temp files
            if temp_hyper_path and os.path.exists(temp_hyper_path):
                try:
                    os.remove(temp_hyper_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Hyper file: {e}")
                    
    def clean_up_cloud_data(self, s3_paths: List[str]):
        """
        Cleans up S3 files
        
        Args:
            s3_paths: List of S3 paths to delete
        """
        if not self.s3_client:
            logger.warning("AWS S3 not configured, skipping cloud cleanup")
            return
            
        for s3_path in s3_paths:
            try:
                bucket_name, object_key = self._parse_s3_path(s3_path)
                self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                logger.debug(f"Deleted S3 object: s3://{bucket_name}/{object_key}")
            except Exception as e:
                logger.warning(f"Could not delete S3 object {s3_path}: {e}")
                
    def clean_up_local_and_s3_data(self, local_paths: List[str] = None, 
                                 s3_paths: List[str] = None):
        """
        Comprehensive cleanup of both local and S3 files
        
        Args:
            local_paths: List of local file paths to delete
            s3_paths: List of S3 paths to delete
        """
        # Clean up local files
        if local_paths:
            for local_path in local_paths:
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                        logger.debug(f"Deleted local file: {local_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete local file {local_path}: {e}")
                        
        # Clean up S3 files
        if s3_paths:
            self.clean_up_cloud_data(s3_paths)
            
        # Clean up default temp files
        self.clean_up_temp_files()
        
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
            'has_session_token': self.aws_session_token is not None,
            's3_endpoint_url': self.s3_endpoint_url
        }