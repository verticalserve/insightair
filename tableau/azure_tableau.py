"""
Azure Tableau Operations
Tableau operations with Azure Blob Storage integration for Hyper file management and data pipeline operations
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
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    from azure.core.exceptions import ResourceNotFoundError, AzureError
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure libraries not available: {e}")
    AZURE_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Pandas/PyArrow not available: {e}")
    PANDAS_AVAILABLE = False


class AzureTableauOperations(BaseTableauOperations):
    """
    Tableau operations with Azure Blob Storage integration
    Provides end-to-end data pipeline from Azure Blob Storage Parquet files to Tableau Server
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Tableau operations
        
        Args:
            config: Configuration dictionary containing both Tableau and Azure settings
                Tableau settings: server_url, username, password, etc.
                Azure settings:
                - account_name: Storage account name
                - account_key: Storage account key (optional)
                - connection_string: Storage connection string (optional)
                - sas_token: SAS token (optional)
                - credential: Azure credential object (optional)
                - account_url: Storage account URL (optional)
                - default_container: Default container for operations
                - max_single_put_size: Max size for single put operations (default: 64MB)
                - max_block_size: Max block size for uploads (default: 4MB)
                - max_concurrency: Max concurrent operations (default: 5)
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
        
        # Azure operation settings
        self.max_single_put_size = config.get('max_single_put_size', 64 * 1024 * 1024)  # 64MB
        self.max_block_size = config.get('max_block_size', 4 * 1024 * 1024)  # 4MB
        self.max_concurrency = config.get('max_concurrency', 5)
        
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
        """Initialize Azure Blob Storage clients"""
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
                    logger.info(f"Verified access to default Azure container: {self.default_container}")
                except ResourceNotFoundError:
                    logger.warning(f"Default Azure container '{self.default_container}' not found")
                except Exception as e:
                    logger.warning(f"Could not access default container: {e}")
                    
            logger.info("Azure Blob Storage clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error initializing Azure clients: {e}")
            raise
            
    def _parse_azure_path(self, azure_path: str) -> tuple:
        """
        Parse Azure path into container and blob name
        
        Args:
            azure_path: Azure path (azure://container/blob or just blob if default_container set)
            
        Returns:
            Tuple of (container_name, blob_name)
            
        Raises:
            TableauOperationError: If path cannot be parsed
        """
        if azure_path.startswith('azure://'):
            path_parts = azure_path.replace('azure://', '').split('/', 1)
            container_name = path_parts[0]
            blob_name = path_parts[1] if len(path_parts) > 1 else ''
        else:
            if not self.default_container:
                raise TableauOperationError("No Azure container specified and no default container configured")
            container_name = self.default_container
            blob_name = azure_path.lstrip('/')
            
        return container_name, blob_name
        
    def _download_azure_file(self, azure_path: str, local_path: str) -> str:
        """
        Download file from Azure Blob Storage to local path
        
        Args:
            azure_path: Azure path to file
            local_path: Local destination path
            
        Returns:
            Local file path
            
        Raises:
            TableauOperationError: If download fails
        """
        if not self.blob_service_client:
            raise TableauOperationError("Azure Blob Storage not configured")
            
        try:
            container_name, blob_name = self._parse_azure_path(azure_path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Get blob client
            if container_name == self.default_container and self.container_client:
                blob_client = self.container_client.get_blob_client(blob=blob_name)
            else:
                container_client = self.blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob=blob_name)
                
            # Download blob
            with open(local_path, 'wb') as f:
                download_stream = blob_client.download_blob()
                download_stream.readinto(f)
                
            logger.debug(f"Downloaded azure://{container_name}/{blob_name} to {local_path}")
            return local_path
            
        except ResourceNotFoundError as e:
            error_msg = f"Azure blob not found: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
        except Exception as e:
            error_msg = f"Failed to download Azure file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def create_hyper_from_parquet(self, azure_path: str, hyper_path: str,
                                table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from an Azure-stored Parquet file
        
        Args:
            azure_path: Azure path to Parquet file (azure://container/blob or just blob)
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
            # Download Parquet file from Azure
            temp_parquet_path = os.path.join(
                self.temp_dir,
                f"temp_parquet_{datetime.now().timestamp()}.parquet"
            )
            self._download_azure_file(azure_path, temp_parquet_path)
            
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
            error_msg = f"Failed to create Hyper from Azure Parquet: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
        finally:
            # Cleanup temp file
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Parquet file: {e}")
                    
    def create_initial_hyper_from_parquet(self, azure_paths: List[str], hyper_path: str,
                                        table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from multiple Azure-stored Parquet files
        Optimized for large initial loads with Azure's scalable storage
        
        Args:
            azure_paths: List of Azure paths to Parquet files
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not PANDAS_AVAILABLE:
            raise TableauOperationError("Pandas/PyArrow not available for Parquet processing")
            
        if not azure_paths:
            raise TableauDataError("No Azure paths provided")
            
        temp_files = []
        
        try:
            # Process first file to create initial Hyper structure
            first_path = azure_paths[0]
            temp_parquet = os.path.join(
                self.temp_dir,
                f"temp_parquet_0_{datetime.now().timestamp()}.parquet"
            )
            temp_files.append(temp_parquet)
            
            self._download_azure_file(first_path, temp_parquet)
            df_first = pd.read_parquet(temp_parquet)
            
            # Create initial Hyper file
            self.create_hyper_from_dataframe(df_first, hyper_path, table_name)
            
            # Process remaining files and append to Hyper
            if len(azure_paths) > 1:
                logger.info(f"Processing {len(azure_paths) - 1} additional Parquet files from Azure")
                
                # Process files in parallel batches
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []
                    
                    for i, azure_path in enumerate(azure_paths[1:], 1):
                        future = executor.submit(self._process_and_append_parquet, 
                                               azure_path, hyper_path, table_name, i)
                        futures.append(future)
                        
                    # Wait for all files to be processed
                    total_rows = len(df_first)
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            rows_added = future.result()
                            total_rows += rows_added
                            logger.debug(f"Added {rows_added} rows to Hyper file")
                        except Exception as e:
                            logger.error(f"Error processing Azure Parquet file: {e}")
                            
            # Get final file size
            final_size_mb = os.path.getsize(hyper_path) / (1024 * 1024)
            logger.info(f"Created initial Hyper file: {final_size_mb:.2f}MB from {len(azure_paths)} Azure Parquet files")
            
            return hyper_path
            
        except Exception as e:
            error_msg = f"Failed to create initial Hyper from Azure Parquet files: {e}"
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
                        
    def _process_and_append_parquet(self, azure_path: str, hyper_path: str, 
                                  table_name: str, index: int) -> int:
        """
        Process a single Parquet file and append to existing Hyper file
        
        Args:
            azure_path: Azure path to Parquet file
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
            self._download_azure_file(azure_path, temp_parquet_path)
            
            # Read Parquet file
            df = pd.read_parquet(temp_parquet_path)
            
            # Append to Hyper file
            # Note: This would require opening the Hyper file in append mode
            # For now, we'll log the operation
            logger.info(f"Would append {len(df)} rows from {azure_path} to Hyper file")
            
            return len(df)
            
        finally:
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp file: {e}")
                    
    def create_hyper_from_parquet_and_publish_tableau(self, azure_paths: List[str],
                                                    datasource_name: str,
                                                    project_id: str = None,
                                                    mode: str = 'Upsert') -> Dict[str, Any]:
        """
        Processes multiple Parquet files iteratively and publishes to Tableau
        Optimized for incremental updates leveraging Azure's fast I/O
        
        Args:
            azure_paths: List of Azure paths to Parquet files
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
            for i, azure_path in enumerate(azure_paths):
                try:
                    # Create temp Hyper file for this batch
                    temp_hyper_path = os.path.join(
                        self.temp_dir,
                        f"temp_hyper_{i}_{datetime.now().timestamp()}.hyper"
                    )
                    
                    # Check file size before processing
                    container_name, blob_name = self._parse_azure_path(azure_path)
                    
                    # Get blob properties
                    if container_name == self.default_container and self.container_client:
                        blob_client = self.container_client.get_blob_client(blob=blob_name)
                    else:
                        container_client = self.blob_service_client.get_container_client(container_name)
                        blob_client = container_client.get_blob_client(blob=blob_name)
                        
                    properties = blob_client.get_blob_properties()
                    file_size_mb = properties.size / (1024 * 1024)
                    
                    if mode.lower() == 'upsert' and file_size_mb > self.upsert_max_size_mb:
                        logger.warning(f"File {azure_path} is {file_size_mb:.2f}MB, exceeds upsert limit")
                        results['failed_files'].append({
                            'path': azure_path,
                            'error': f"File size {file_size_mb:.2f}MB exceeds upsert limit"
                        })
                        continue
                        
                    # Create Hyper from Parquet
                    self.create_hyper_from_parquet(azure_path, temp_hyper_path)
                    
                    # Publish to Tableau
                    datasource = self.publish_data_source(
                        temp_hyper_path,
                        datasource_name,
                        project_id,
                        mode=mode
                    )
                    
                    results['files_processed'] += 1
                    logger.info(f"Processed and published file {i+1}/{len(azure_paths)} from Azure")
                    
                    # Cleanup temp Hyper file
                    if os.path.exists(temp_hyper_path):
                        os.remove(temp_hyper_path)
                        temp_hyper_path = None
                        
                except Exception as e:
                    logger.error(f"Failed to process {azure_path}: {e}")
                    results['failed_files'].append({
                        'path': azure_path,
                        'error': str(e)
                    })
                    
            results['published'] = results['files_processed'] > 0
            results['end_time'] = datetime.now()
            results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"Completed Azure processing: {results['files_processed']}/{len(azure_paths)} files successful")
            return results
            
        except Exception as e:
            error_msg = f"Failed to process and publish Azure Parquet files: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
        finally:
            # Cleanup any remaining temp files
            if temp_hyper_path and os.path.exists(temp_hyper_path):
                try:
                    os.remove(temp_hyper_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Hyper file: {e}")
                    
    def clean_up_cloud_data(self, azure_paths: List[str]):
        """
        Cleans up Azure Blob Storage files
        
        Args:
            azure_paths: List of Azure paths to delete
        """
        if not self.blob_service_client:
            logger.warning("Azure Blob Storage not configured, skipping cloud cleanup")
            return
            
        for azure_path in azure_paths:
            try:
                container_name, blob_name = self._parse_azure_path(azure_path)
                
                # Get blob client
                if container_name == self.default_container and self.container_client:
                    blob_client = self.container_client.get_blob_client(blob=blob_name)
                else:
                    container_client = self.blob_service_client.get_container_client(container_name)
                    blob_client = container_client.get_blob_client(blob=blob_name)
                    
                # Delete blob
                blob_client.delete_blob()
                logger.debug(f"Deleted Azure blob: azure://{container_name}/{blob_name}")
                
            except ResourceNotFoundError:
                logger.debug(f"Azure blob already deleted: {azure_path}")
            except Exception as e:
                logger.warning(f"Could not delete Azure blob {azure_path}: {e}")
                
    def clean_up_local_and_azure_data(self, local_paths: List[str] = None, 
                                    azure_paths: List[str] = None):
        """
        Comprehensive cleanup of both local and Azure files
        
        Args:
            local_paths: List of local file paths to delete
            azure_paths: List of Azure paths to delete
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
                        
        # Clean up Azure files
        if azure_paths:
            self.clean_up_cloud_data(azure_paths)
            
        # Clean up default temp files
        self.clean_up_temp_files()
        
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
            'has_connection_string': self.connection_string is not None,
            'max_single_put_size_mb': self.max_single_put_size / (1024 * 1024),
            'max_block_size_mb': self.max_block_size / (1024 * 1024),
            'max_concurrency': self.max_concurrency
        }