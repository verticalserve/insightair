"""
AWS Google Ads Operations
Google Ads operations with AWS S3 integration for data storage and processing
"""

import logging
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Union

from .googleads_extensions import GoogleAdsExtensionsOperations
from .googleads_customer_match import GoogleAdsCustomerMatchOperations

logger = logging.getLogger(__name__)

try:
    import boto3
    import s3fs
    from botocore.exceptions import ClientError, NoCredentialsError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False


class AWSGoogleAdsOperations(GoogleAdsExtensionsOperations, GoogleAdsCustomerMatchOperations):
    """
    Google Ads operations with AWS S3 integration
    Combines all Google Ads functionality with AWS cloud storage capabilities
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AWS Google Ads operations
        
        Args:
            config: Configuration dictionary containing both Google Ads and AWS settings
                Google Ads settings:
                - developer_token, client_id, client_secret, refresh_token, customer_id
                AWS settings:
                - aws_access_key_id, aws_secret_access_key, aws_session_token
                - region_name, profile_name
                - default_bucket: Default S3 bucket for operations
        """
        super().__init__(config)
        
        # AWS configuration
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.region_name = config.get('region_name', 'us-east-1')
        self.profile_name = config.get('profile_name')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize AWS clients
        self.s3_client = None
        self.s3_resource = None
        self.s3fs = None
        
        if AWS_AVAILABLE:
            try:
                self._initialize_aws_clients()
            except Exception as e:
                logger.error(f"Failed to initialize AWS clients: {e}")
                # Continue without AWS - operations will fall back gracefully
        else:
            logger.warning("AWS libraries not available, S3 operations will not work")
            
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
                
            # Initialize s3fs for pandas integration
            s3fs_kwargs = {}
            if self.aws_access_key_id and self.aws_secret_access_key:
                s3fs_kwargs['key'] = self.aws_access_key_id
                s3fs_kwargs['secret'] = self.aws_secret_access_key
                if self.aws_session_token:
                    s3fs_kwargs['token'] = self.aws_session_token
                    
            self.s3fs = s3fs.S3FileSystem(**s3fs_kwargs)
            
            logger.info("AWS S3 clients initialized successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS clients: {e}")
            raise
            
    def export_to_cloud_storage(self, data: Union[pd.DataFrame, List[Dict]], 
                               storage_path: str, file_format: str = 'csv') -> bool:
        """
        Exports Google Ads data to S3 in CSV/Parquet format
        
        Args:
            data: Data to export (DataFrame or list of dictionaries)
            storage_path: S3 path (s3://bucket/key or just key if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured for export operations")
            return False
            
        try:
            # Convert to DataFrame if needed
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data
                
            if df.empty:
                logger.warning("No data to export")
                return False
                
            # Parse S3 path
            if storage_path.startswith('s3://'):
                # Full S3 URL
                path_parts = storage_path.replace('s3://', '').split('/', 1)
                bucket = path_parts[0]
                key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No S3 bucket specified and no default bucket configured")
                    return False
                bucket = self.default_bucket
                key = storage_path.lstrip('/')
                
            # Export based on format
            if file_format.lower() == 'csv':
                # Export to CSV
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )
                
            elif file_format.lower() == 'parquet':
                # Export to Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=parquet_buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            logger.info(f"Exported {len(df)} rows to s3://{bucket}/{key} in {file_format} format")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to S3: {e}")
            return False
            
    def read_from_cloud_storage(self, storage_path: str, 
                               file_format: str = 'csv') -> pd.DataFrame:
        """
        Read data from S3
        
        Args:
            storage_path: S3 path (s3://bucket/key or just key if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            DataFrame with data
            
        Raises:
            Exception: If read operation fails
        """
        if not self.s3_client:
            raise Exception("AWS S3 not configured for read operations")
            
        try:
            # Parse S3 path
            if storage_path.startswith('s3://'):
                # Use s3fs for pandas integration
                s3_path = storage_path
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise Exception("No S3 bucket specified and no default bucket configured")
                s3_path = f"s3://{self.default_bucket}/{storage_path.lstrip('/')}"
                
            # Read based on format
            if file_format.lower() == 'csv':
                df = pd.read_csv(s3_path, storage_options={
                    'key': self.aws_access_key_id,
                    'secret': self.aws_secret_access_key,
                    'token': self.aws_session_token
                } if self.aws_access_key_id else {})
                
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(s3_path, storage_options={
                    'key': self.aws_access_key_id,
                    'secret': self.aws_secret_access_key,
                    'token': self.aws_session_token
                } if self.aws_access_key_id else {})
                
            else:
                raise Exception(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from {s3_path} in {file_format} format")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from S3: {e}")
            raise
            
    def export_query_results_to_s3(self, query: str, s3_path: str, 
                                   file_format: str = 'csv', customer_id: str = None) -> Dict[str, Any]:
        """
        Execute GAQL query and export results directly to S3
        
        Args:
            query: GAQL query string
            s3_path: S3 path for export
            file_format: File format ('csv' or 'parquet')
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with export results
        """
        try:
            # Execute query
            results = self._query_data_all(query, customer_id, return_format='dataframe')
            
            if results.empty:
                logger.warning("Query returned no results")
                return {
                    'success': False,
                    'rows_exported': 0,
                    'message': 'No data to export'
                }
                
            # Export to S3
            success = self.export_to_cloud_storage(results, s3_path, file_format)
            
            return {
                'success': success,
                'rows_exported': len(results) if success else 0,
                's3_path': s3_path,
                'file_format': file_format,
                'columns': list(results.columns) if success else []
            }
            
        except Exception as e:
            logger.error(f"Error exporting query results to S3: {e}")
            return {
                'success': False,
                'rows_exported': 0,
                'error': str(e)
            }
            
    def batch_upload_custom_assets_from_s3(self, asset_sets_config: List[Dict[str, Any]],
                                          customer_id: str = None) -> Dict[str, Any]:
        """
        Batch upload multiple custom asset sets from S3
        
        Args:
            asset_sets_config: List of asset set configurations with S3 paths
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with batch upload results
        """
        results = {
            'total_asset_sets': len(asset_sets_config),
            'successful_uploads': 0,
            'failed_uploads': 0,
            'upload_details': []
        }
        
        for config in asset_sets_config:
            try:
                asset_set_name = config['asset_set_name']
                s3_path = config['s3_path']
                
                # Upload custom asset set
                upload_result = self.upload_custom_asset_set(
                    asset_set_name=asset_set_name,
                    assets_data=s3_path,
                    customer_id=customer_id
                )
                
                results['successful_uploads'] += 1
                results['upload_details'].append({
                    'asset_set_name': asset_set_name,
                    'success': True,
                    'result': upload_result
                })
                
            except Exception as e:
                results['failed_uploads'] += 1
                results['upload_details'].append({
                    'asset_set_name': config.get('asset_set_name', 'Unknown'),
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to upload asset set {config.get('asset_set_name')}: {e}")
                
        logger.info(f"Batch upload completed: {results['successful_uploads']} successful, "
                   f"{results['failed_uploads']} failed")
        
        return results
        
    def manage_customer_match_from_s3(self, jobs_config: List[Dict[str, Any]],
                                     customer_id: str = None) -> Dict[str, Any]:
        """
        Manage multiple customer match jobs from S3 data
        
        Args:
            jobs_config: List of job configurations with S3 paths
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with job management results
        """
        results = {
            'total_jobs': len(jobs_config),
            'successful_jobs': 0,
            'failed_jobs': 0,
            'job_details': []
        }
        
        for config in jobs_config:
            try:
                user_list_name = config['user_list_name']
                s3_path = config['s3_path']
                job_type = config.get('job_type', 'CREATE')
                
                # Manage customer match job
                job_result = self.customer_match_manage_jobs(
                    user_list_name=user_list_name,
                    user_data_path=s3_path,
                    job_type=job_type,
                    customer_id=customer_id
                )
                
                results['successful_jobs'] += 1
                results['job_details'].append({
                    'user_list_name': user_list_name,
                    'job_type': job_type,
                    'success': True,
                    'result': job_result
                })
                
            except Exception as e:
                results['failed_jobs'] += 1
                results['job_details'].append({
                    'user_list_name': config.get('user_list_name', 'Unknown'),
                    'job_type': config.get('job_type', 'CREATE'),
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to manage job for {config.get('user_list_name')}: {e}")
                
        logger.info(f"Customer match jobs completed: {results['successful_jobs']} successful, "
                   f"{results['failed_jobs']} failed")
        
        return results
        
    def s3_list_files(self, prefix: str, bucket: str = None, max_files: int = 1000) -> List[str]:
        """
        List files in S3 bucket with given prefix
        
        Args:
            prefix: S3 prefix to search under
            bucket: S3 bucket name (uses default if None)
            max_files: Maximum number of files to return
            
        Returns:
            List of S3 keys
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured")
            return []
            
        bucket = bucket or self.default_bucket
        if not bucket:
            logger.error("No S3 bucket specified")
            return []
            
        try:
            files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_files}
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append(obj['Key'])
                        
            logger.info(f"Listed {len(files)} files with prefix '{prefix}' in bucket '{bucket}'")
            return files
            
        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
            return []
            
    def get_s3_info(self) -> Dict[str, Any]:
        """
        Get S3 configuration information
        
        Returns:
            Dictionary with S3 info
        """
        return {
            'aws_configured': self.s3_client is not None,
            'default_bucket': self.default_bucket,
            'region_name': self.region_name,
            'profile_name': self.profile_name
        }