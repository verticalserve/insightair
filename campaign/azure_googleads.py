"""
Azure Google Ads Operations
Google Ads operations with Azure Blob Storage integration for data storage and processing
"""

import logging
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Union

from .googleads_extensions import GoogleAdsExtensionsOperations
from .googleads_customer_match import GoogleAdsCustomerMatchOperations

logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    from azure.core.exceptions import ResourceNotFoundError, AzureError
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure libraries not available: {e}")
    AZURE_AVAILABLE = False


class AzureGoogleAdsOperations(GoogleAdsExtensionsOperations, GoogleAdsCustomerMatchOperations):
    """
    Google Ads operations with Azure Blob Storage integration
    Combines all Google Ads functionality with Azure cloud storage capabilities
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Google Ads operations
        
        Args:
            config: Configuration dictionary containing both Google Ads and Azure settings
                Google Ads settings:
                - developer_token, client_id, client_secret, refresh_token, customer_id
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
                # Continue without Azure - operations will fall back gracefully
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
            
    def export_to_cloud_storage(self, data: Union[pd.DataFrame, List[Dict]], 
                               storage_path: str, file_format: str = 'csv') -> bool:
        """
        Exports Google Ads data to Azure Blob Storage in CSV/Parquet format
        
        Args:
            data: Data to export (DataFrame or list of dictionaries)
            storage_path: Azure path (azure://container/blob or just blob if default_container set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured for export operations")
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
                
            # Parse Azure path
            if storage_path.startswith('azure://'):
                # Full Azure URL
                path_parts = storage_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    logger.error("No Azure container specified and no default container configured")
                    return False
                container_client = self.container_client
                blob_name = storage_path.lstrip('/')
                
            # Get blob client
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            # Export based on format
            if file_format.lower() == 'csv':
                # Export to CSV
                csv_data = df.to_csv(index=False)
                blob_client.upload_blob(csv_data, overwrite=True, content_type='text/csv')
                
            elif file_format.lower() == 'parquet':
                # Export to Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                blob_client.upload_blob(
                    parquet_buffer.getvalue(), 
                    overwrite=True, 
                    content_type='application/octet-stream'
                )
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            container_name = container_client.container_name
            logger.info(f"Exported {len(df)} rows to azure://{container_name}/{blob_name} in {file_format} format")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to Azure Blob Storage: {e}")
            return False
            
    def read_from_cloud_storage(self, storage_path: str, 
                               file_format: str = 'csv') -> pd.DataFrame:
        """
        Read data from Azure Blob Storage
        
        Args:
            storage_path: Azure path (azure://container/blob or just blob if default_container set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            DataFrame with data
            
        Raises:
            Exception: If read operation fails
        """
        if not self.blob_service_client:
            raise Exception("Azure Blob Storage not configured for read operations")
            
        try:
            # Parse Azure path
            if storage_path.startswith('azure://'):
                # Full Azure URL
                path_parts = storage_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    raise Exception("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = storage_path.lstrip('/')
                
            # Get blob client
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            if not blob_client.exists():
                raise Exception(f"Blob not found: {container_client.container_name}/{blob_name}")
                
            # Read based on format
            if file_format.lower() == 'csv':
                csv_data = blob_client.download_blob().readall().decode('utf-8')
                df = pd.read_csv(io.StringIO(csv_data))
                
            elif file_format.lower() == 'parquet':
                parquet_data = blob_client.download_blob().readall()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
            else:
                raise Exception(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from azure://{container_client.container_name}/{blob_name} in {file_format} format")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Azure Blob Storage: {e}")
            raise
            
    def export_query_results_to_azure(self, query: str, azure_path: str, 
                                      file_format: str = 'csv', customer_id: str = None) -> Dict[str, Any]:
        """
        Execute GAQL query and export results directly to Azure Blob Storage
        
        Args:
            query: GAQL query string
            azure_path: Azure path for export
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
                
            # Export to Azure
            success = self.export_to_cloud_storage(results, azure_path, file_format)
            
            return {
                'success': success,
                'rows_exported': len(results) if success else 0,
                'azure_path': azure_path,
                'file_format': file_format,
                'columns': list(results.columns) if success else []
            }
            
        except Exception as e:
            logger.error(f"Error exporting query results to Azure: {e}")
            return {
                'success': False,
                'rows_exported': 0,
                'error': str(e)
            }
            
    def batch_upload_custom_assets_from_azure(self, asset_sets_config: List[Dict[str, Any]],
                                             customer_id: str = None) -> Dict[str, Any]:
        """
        Batch upload multiple custom asset sets from Azure Blob Storage
        
        Args:
            asset_sets_config: List of asset set configurations with Azure paths
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
                azure_path = config['azure_path']
                
                # Upload custom asset set
                upload_result = self.upload_custom_asset_set(
                    asset_set_name=asset_set_name,
                    assets_data=azure_path,
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
        
    def manage_customer_match_from_azure(self, jobs_config: List[Dict[str, Any]],
                                        customer_id: str = None) -> Dict[str, Any]:
        """
        Manage multiple customer match jobs from Azure Blob Storage data
        
        Args:
            jobs_config: List of job configurations with Azure paths
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
                azure_path = config['azure_path']
                job_type = config.get('job_type', 'CREATE')
                
                # Manage customer match job
                job_result = self.customer_match_manage_jobs(
                    user_list_name=user_list_name,
                    user_data_path=azure_path,
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
        
    def azure_list_blobs(self, prefix: str, container_name: str = None, max_blobs: int = 1000) -> List[str]:
        """
        List blobs in Azure container with given prefix
        
        Args:
            prefix: Azure prefix to search under
            container_name: Azure container name (uses default if None)
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob names
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
                blobs.append(blob.name)
                if len(blobs) >= max_blobs:
                    break
                    
            logger.info(f"Listed {len(blobs)} blobs with prefix '{prefix}' in container '{container_client.container_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing Azure blobs: {e}")
            return []
            
    def create_sas_url(self, blob_name: str, container_name: str = None, 
                      expiration_hours: int = 24, permissions: str = 'r') -> Optional[str]:
        """
        Create a SAS URL for Azure Blob Storage blob access
        
        Args:
            blob_name: Azure blob name
            container_name: Azure container name (uses default if None)
            expiration_hours: URL expiration time in hours
            permissions: SAS permissions ('r' for read, 'rw' for read/write)
            
        Returns:
            SAS URL string or None if creation fails
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return None
            
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import datetime, timedelta
            
            container_name = container_name or self.default_container
            if not container_name:
                logger.error("No Azure container specified")
                return None
                
            if not self.account_key:
                logger.error("Account key required for SAS URL generation")
                return None
                
            # Set permissions
            sas_permissions = BlobSasPermissions(read=True)
            if 'w' in permissions.lower():
                sas_permissions.write = True
                
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=self.account_key,
                permission=sas_permissions,
                expiry=datetime.utcnow() + timedelta(hours=expiration_hours)
            )
            
            # Build SAS URL
            sas_url = f"https://{self.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            
            logger.info(f"Generated SAS URL for blob: {container_name}/{blob_name}")
            return sas_url
            
        except Exception as e:
            logger.error(f"Error creating SAS URL: {e}")
            return None
            
    def get_azure_info(self) -> Dict[str, Any]:
        """
        Get Azure configuration information
        
        Returns:
            Dictionary with Azure info
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
        
    def sync_data_between_containers(self, source_path: str, destination_path: str,
                                    source_container: str = None, dest_container: str = None) -> bool:
        """
        Sync data between Azure containers
        
        Args:
            source_path: Source blob path
            destination_path: Destination blob path
            source_container: Source container name (uses default if None)
            dest_container: Destination container name (uses default if None)
            
        Returns:
            True if sync successful, False otherwise
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return False
            
        try:
            source_container = source_container or self.default_container
            dest_container = dest_container or self.default_container
            
            if not source_container or not dest_container:
                logger.error("Source and destination containers must be specified")
                return False
                
            # Get source blob client
            source_blob_client = self.blob_service_client.get_blob_client(
                container=source_container,
                blob=source_path
            )
            
            # Get destination blob client
            dest_blob_client = self.blob_service_client.get_blob_client(
                container=dest_container,
                blob=destination_path
            )
            
            # Copy blob data
            blob_data = source_blob_client.download_blob().readall()
            dest_blob_client.upload_blob(blob_data, overwrite=True)
            
            logger.info(f"Synced blob from {source_container}/{source_path} to {dest_container}/{destination_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error syncing between Azure containers: {e}")
            return False
            
    def set_blob_tier(self, blob_name: str, access_tier: str, container_name: str = None) -> bool:
        """
        Set access tier for a blob (Hot, Cool, Archive)
        
        Args:
            blob_name: Azure blob name
            access_tier: Access tier ('Hot', 'Cool', 'Archive')
            container_name: Container name (uses default if None)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return False
            
        try:
            container_name = container_name or self.default_container
            if not container_name:
                logger.error("No Azure container specified")
                return False
                
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            # Set blob tier
            blob_client.set_standard_blob_tier(access_tier)
            
            logger.info(f"Set access tier '{access_tier}' for blob {container_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting blob tier: {e}")
            return False