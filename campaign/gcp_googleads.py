"""
GCP Google Ads Operations
Google Ads operations with GCP Cloud Storage integration for data storage and processing
"""

import logging
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Union

from .googleads_extensions import GoogleAdsExtensionsOperations
from .googleads_customer_match import GoogleAdsCustomerMatchOperations

logger = logging.getLogger(__name__)

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_STORAGE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP Storage libraries not available: {e}")
    GCP_STORAGE_AVAILABLE = False


class GCPGoogleAdsOperations(GoogleAdsExtensionsOperations, GoogleAdsCustomerMatchOperations):
    """
    Google Ads operations with GCP Cloud Storage integration
    Combines all Google Ads functionality with GCP cloud storage capabilities
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Google Ads operations
        
        Args:
            config: Configuration dictionary containing both Google Ads and GCP settings
                Google Ads settings:
                - developer_token, client_id, client_secret, refresh_token, customer_id
                GCP settings:
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON file (optional)
                - credentials: Service account credentials dict (optional)
                - default_bucket: Default GCS bucket for operations
        """
        super().__init__(config)
        
        # GCP configuration
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.credentials = config.get('credentials')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize GCP clients
        self.storage_client = None
        self.bucket = None
        
        if GCP_STORAGE_AVAILABLE:
            try:
                self._initialize_gcp_clients()
            except Exception as e:
                logger.error(f"Failed to initialize GCP clients: {e}")
                # Continue without GCP - operations will fall back gracefully
        else:
            logger.warning("GCP Storage libraries not available, GCS operations will not work")
            
    def _initialize_gcp_clients(self):
        """
        Initialize GCP Cloud Storage clients
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
                # Use default credentials
                self.storage_client = storage.Client(project=self.project_id)
                
            # Get default bucket if specified
            if self.default_bucket:
                self.bucket = self.storage_client.bucket(self.default_bucket)
                if not self.bucket.exists():
                    logger.warning(f"Default GCS bucket '{self.default_bucket}' does not exist")
                    
            logger.info("GCP Cloud Storage clients initialized successfully")
            
        except DefaultCredentialsError:
            logger.error("GCP credentials not found")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing GCP clients: {e}")
            raise
            
    def export_to_cloud_storage(self, data: Union[pd.DataFrame, List[Dict]], 
                               storage_path: str, file_format: str = 'csv') -> bool:
        """
        Exports Google Ads data to GCS in CSV/Parquet format
        
        Args:
            data: Data to export (DataFrame or list of dictionaries)
            storage_path: GCS path (gs://bucket/blob or just blob if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured for export operations")
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
                
            # Parse GCS path
            if storage_path.startswith('gs://'):
                # Full GCS URL
                path_parts = storage_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket:
                    logger.error("No GCS bucket specified and no default bucket configured")
                    return False
                bucket = self.bucket
                blob_name = storage_path.lstrip('/')
                
            # Create blob
            blob = bucket.blob(blob_name)
            
            # Export based on format
            if file_format.lower() == 'csv':
                # Export to CSV
                csv_data = df.to_csv(index=False)
                blob.upload_from_string(csv_data, content_type='text/csv')
                
            elif file_format.lower() == 'parquet':
                # Export to Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                blob.upload_from_string(parquet_buffer.getvalue(), content_type='application/octet-stream')
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            logger.info(f"Exported {len(df)} rows to gs://{bucket.name}/{blob_name} in {file_format} format")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to GCS: {e}")
            return False
            
    def read_from_cloud_storage(self, storage_path: str, 
                               file_format: str = 'csv') -> pd.DataFrame:
        """
        Read data from GCS
        
        Args:
            storage_path: GCS path (gs://bucket/blob or just blob if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            DataFrame with data
            
        Raises:
            Exception: If read operation fails
        """
        if not self.storage_client:
            raise Exception("GCP Cloud Storage not configured for read operations")
            
        try:
            # Parse GCS path
            if storage_path.startswith('gs://'):
                # Full GCS URL
                path_parts = storage_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket:
                    raise Exception("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket
                blob_name = storage_path.lstrip('/')
                
            # Get blob
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                raise Exception(f"Blob not found: gs://{bucket.name}/{blob_name}")
                
            # Read based on format
            if file_format.lower() == 'csv':
                csv_data = blob.download_as_text()
                df = pd.read_csv(io.StringIO(csv_data))
                
            elif file_format.lower() == 'parquet':
                parquet_data = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
            else:
                raise Exception(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from gs://{bucket.name}/{blob_name} in {file_format} format")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from GCS: {e}")
            raise
            
    def export_query_results_to_gcs(self, query: str, gcs_path: str, 
                                    file_format: str = 'csv', customer_id: str = None) -> Dict[str, Any]:
        """
        Execute GAQL query and export results directly to GCS
        
        Args:
            query: GAQL query string
            gcs_path: GCS path for export
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
                
            # Export to GCS
            success = self.export_to_cloud_storage(results, gcs_path, file_format)
            
            return {
                'success': success,
                'rows_exported': len(results) if success else 0,
                'gcs_path': gcs_path,
                'file_format': file_format,
                'columns': list(results.columns) if success else []
            }
            
        except Exception as e:
            logger.error(f"Error exporting query results to GCS: {e}")
            return {
                'success': False,
                'rows_exported': 0,
                'error': str(e)
            }
            
    def batch_upload_custom_assets_from_gcs(self, asset_sets_config: List[Dict[str, Any]],
                                           customer_id: str = None) -> Dict[str, Any]:
        """
        Batch upload multiple custom asset sets from GCS
        
        Args:
            asset_sets_config: List of asset set configurations with GCS paths
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
                gcs_path = config['gcs_path']
                
                # Upload custom asset set
                upload_result = self.upload_custom_asset_set(
                    asset_set_name=asset_set_name,
                    assets_data=gcs_path,
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
        
    def manage_customer_match_from_gcs(self, jobs_config: List[Dict[str, Any]],
                                      customer_id: str = None) -> Dict[str, Any]:
        """
        Manage multiple customer match jobs from GCS data
        
        Args:
            jobs_config: List of job configurations with GCS paths
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
                gcs_path = config['gcs_path']
                job_type = config.get('job_type', 'CREATE')
                
                # Manage customer match job
                job_result = self.customer_match_manage_jobs(
                    user_list_name=user_list_name,
                    user_data_path=gcs_path,
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
        
    def gcs_list_blobs(self, prefix: str, bucket_name: str = None, max_blobs: int = 1000) -> List[str]:
        """
        List blobs in GCS bucket with given prefix
        
        Args:
            prefix: GCS prefix to search under
            bucket_name: GCS bucket name (uses default if None)
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob names
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured")
            return []
            
        try:
            if bucket_name:
                bucket = self.storage_client.bucket(bucket_name)
            elif self.bucket:
                bucket = self.bucket
            else:
                logger.error("No GCS bucket specified")
                return []
                
            blobs = []
            blob_iterator = bucket.list_blobs(prefix=prefix, max_results=max_blobs)
            
            for blob in blob_iterator:
                blobs.append(blob.name)
                
            logger.info(f"Listed {len(blobs)} blobs with prefix '{prefix}' in bucket '{bucket.name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing GCS blobs: {e}")
            return []
            
    def get_gcs_info(self) -> Dict[str, Any]:
        """
        Get GCS configuration information
        
        Returns:
            Dictionary with GCS info
        """
        return {
            'gcp_configured': self.storage_client is not None,
            'default_bucket': self.default_bucket,
            'project_id': self.project_id,
            'credentials_path': self.credentials_path
        }