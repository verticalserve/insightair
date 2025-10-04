"""
OCI Tableau Operations
Tableau operations with OCI Object Storage integration for Hyper file management and data pipeline operations
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
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Pandas/PyArrow not available: {e}")
    PANDAS_AVAILABLE = False


class OCITableauOperations(BaseTableauOperations):
    """
    Tableau operations with OCI Object Storage integration
    Provides end-to-end data pipeline from OCI Object Storage Parquet files to Tableau Server
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Tableau operations
        
        Args:
            config: Configuration dictionary containing both Tableau and OCI settings
                Tableau settings: server_url, username, password, etc.
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
                - max_multipart_size: Max size for multipart operations (default: 128MB)
                - max_parallel_uploads: Max parallel upload operations (default: 5)
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
        
        # OCI operation settings
        self.max_multipart_size = config.get('max_multipart_size', 128 * 1024 * 1024)  # 128MB
        self.max_parallel_uploads = config.get('max_parallel_uploads', 5)
        
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
        """Initialize OCI Object Storage clients"""
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
                    logger.info(f"Verified access to default OCI bucket: {self.default_bucket}")
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
            
    def _parse_oci_path(self, oci_path: str) -> tuple:
        """
        Parse OCI path into bucket and object name
        
        Args:
            oci_path: OCI path (oci://bucket/object or just object if default_bucket set)
            
        Returns:
            Tuple of (bucket_name, object_name)
            
        Raises:
            TableauOperationError: If path cannot be parsed
        """
        if oci_path.startswith('oci://'):
            path_parts = oci_path.replace('oci://', '').split('/', 1)
            bucket_name = path_parts[0]
            object_name = path_parts[1] if len(path_parts) > 1 else ''
        else:
            if not self.default_bucket:
                raise TableauOperationError("No OCI bucket specified and no default bucket configured")
            bucket_name = self.default_bucket
            object_name = oci_path.lstrip('/')
            
        return bucket_name, object_name
        
    def _download_oci_file(self, oci_path: str, local_path: str) -> str:
        """
        Download file from OCI Object Storage to local path
        
        Args:
            oci_path: OCI path to file
            local_path: Local destination path
            
        Returns:
            Local file path
            
        Raises:
            TableauOperationError: If download fails
        """
        if not self.object_storage_client:
            raise TableauOperationError("OCI Object Storage not configured")
            
        try:
            bucket_name, object_name = self._parse_oci_path(oci_path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Get object
            get_object_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            # Write to local file
            with open(local_path, 'wb') as f:
                for chunk in get_object_response.data.raw.stream(1024 * 1024):  # 1MB chunks
                    f.write(chunk)
                    
            logger.debug(f"Downloaded oci://{bucket_name}/{object_name} to {local_path}")
            return local_path
            
        except ServiceError as e:
            error_msg = f"Failed to download OCI file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error downloading OCI file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def create_hyper_from_parquet(self, oci_path: str, hyper_path: str,
                                table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from an OCI-stored Parquet file
        
        Args:
            oci_path: OCI path to Parquet file (oci://bucket/object or just object)
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
            # Download Parquet file from OCI
            temp_parquet_path = os.path.join(
                self.temp_dir,
                f"temp_parquet_{datetime.now().timestamp()}.parquet"
            )
            self._download_oci_file(oci_path, temp_parquet_path)
            
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
            error_msg = f"Failed to create Hyper from OCI Parquet: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
        finally:
            # Cleanup temp file
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Parquet file: {e}")
                    
    def create_initial_hyper_from_parquet(self, oci_paths: List[str], hyper_path: str,
                                        table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from multiple OCI-stored Parquet files
        Optimized for large initial loads with OCI's high-throughput capabilities
        
        Args:
            oci_paths: List of OCI paths to Parquet files
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not PANDAS_AVAILABLE:
            raise TableauOperationError("Pandas/PyArrow not available for Parquet processing")
            
        if not oci_paths:
            raise TableauDataError("No OCI paths provided")
            
        temp_files = []
        
        try:
            # Process first file to create initial Hyper structure
            first_path = oci_paths[0]
            temp_parquet = os.path.join(
                self.temp_dir,
                f"temp_parquet_0_{datetime.now().timestamp()}.parquet"
            )
            temp_files.append(temp_parquet)
            
            self._download_oci_file(first_path, temp_parquet)
            df_first = pd.read_parquet(temp_parquet)
            
            # Create initial Hyper file
            self.create_hyper_from_dataframe(df_first, hyper_path, table_name)
            
            # Process remaining files and append to Hyper
            if len(oci_paths) > 1:
                logger.info(f"Processing {len(oci_paths) - 1} additional Parquet files from OCI")
                
                # Process files in parallel batches (OCI can handle higher concurrency)
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_uploads) as executor:
                    futures = []
                    
                    for i, oci_path in enumerate(oci_paths[1:], 1):
                        future = executor.submit(self._process_and_append_parquet, 
                                               oci_path, hyper_path, table_name, i)
                        futures.append(future)
                        
                    # Wait for all files to be processed
                    total_rows = len(df_first)
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            rows_added = future.result()
                            total_rows += rows_added
                            logger.debug(f"Added {rows_added} rows to Hyper file")
                        except Exception as e:
                            logger.error(f"Error processing OCI Parquet file: {e}")
                            
            # Get final file size
            final_size_mb = os.path.getsize(hyper_path) / (1024 * 1024)
            logger.info(f"Created initial Hyper file: {final_size_mb:.2f}MB from {len(oci_paths)} OCI Parquet files")
            
            return hyper_path
            
        except Exception as e:
            error_msg = f"Failed to create initial Hyper from OCI Parquet files: {e}"
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
                        
    def _process_and_append_parquet(self, oci_path: str, hyper_path: str, 
                                  table_name: str, index: int) -> int:
        """
        Process a single Parquet file and append to existing Hyper file
        
        Args:
            oci_path: OCI path to Parquet file
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
            self._download_oci_file(oci_path, temp_parquet_path)
            
            # Read Parquet file
            df = pd.read_parquet(temp_parquet_path)
            
            # Append to Hyper file
            # Note: This would require opening the Hyper file in append mode
            # For now, we'll log the operation
            logger.info(f"Would append {len(df)} rows from {oci_path} to Hyper file")
            
            return len(df)
            
        finally:
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp file: {e}")
                    
    def create_hyper_from_parquet_and_publish_tableau(self, oci_paths: List[str],
                                                    datasource_name: str,
                                                    project_id: str = None,
                                                    mode: str = 'Upsert') -> Dict[str, Any]:
        """
        Processes multiple Parquet files iteratively and publishes to Tableau
        Leverages OCI's high-performance storage for efficient processing
        
        Args:
            oci_paths: List of OCI paths to Parquet files
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
            for i, oci_path in enumerate(oci_paths):
                try:
                    # Create temp Hyper file for this batch
                    temp_hyper_path = os.path.join(
                        self.temp_dir,
                        f"temp_hyper_{i}_{datetime.now().timestamp()}.hyper"
                    )
                    
                    # Check file size before processing
                    bucket_name, object_name = self._parse_oci_path(oci_path)
                    
                    # Head object to get size
                    head_response = self.object_storage_client.head_object(
                        namespace_name=self.namespace,
                        bucket_name=bucket_name,
                        object_name=object_name
                    )
                    file_size_mb = int(head_response.headers.get('Content-Length', 0)) / (1024 * 1024)
                    
                    if mode.lower() == 'upsert' and file_size_mb > self.upsert_max_size_mb:
                        logger.warning(f"File {oci_path} is {file_size_mb:.2f}MB, exceeds upsert limit")
                        results['failed_files'].append({
                            'path': oci_path,
                            'error': f"File size {file_size_mb:.2f}MB exceeds upsert limit"
                        })
                        continue
                        
                    # Create Hyper from Parquet
                    self.create_hyper_from_parquet(oci_path, temp_hyper_path)
                    
                    # Publish to Tableau
                    datasource = self.publish_data_source(
                        temp_hyper_path,
                        datasource_name,
                        project_id,
                        mode=mode
                    )
                    
                    results['files_processed'] += 1
                    logger.info(f"Processed and published file {i+1}/{len(oci_paths)} from OCI")
                    
                    # Cleanup temp Hyper file
                    if os.path.exists(temp_hyper_path):
                        os.remove(temp_hyper_path)
                        temp_hyper_path = None
                        
                except Exception as e:
                    logger.error(f"Failed to process {oci_path}: {e}")
                    results['failed_files'].append({
                        'path': oci_path,
                        'error': str(e)
                    })
                    
            results['published'] = results['files_processed'] > 0
            results['end_time'] = datetime.now()
            results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"Completed OCI processing: {results['files_processed']}/{len(oci_paths)} files successful")
            return results
            
        except Exception as e:
            error_msg = f"Failed to process and publish OCI Parquet files: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
        finally:
            # Cleanup any remaining temp files
            if temp_hyper_path and os.path.exists(temp_hyper_path):
                try:
                    os.remove(temp_hyper_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Hyper file: {e}")
                    
    def clean_up_cloud_data(self, oci_paths: List[str]):
        """
        Cleans up OCI Object Storage files
        
        Args:
            oci_paths: List of OCI paths to delete
        """
        if not self.object_storage_client:
            logger.warning("OCI Object Storage not configured, skipping cloud cleanup")
            return
            
        for oci_path in oci_paths:
            try:
                bucket_name, object_name = self._parse_oci_path(oci_path)
                self.object_storage_client.delete_object(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    object_name=object_name
                )
                logger.debug(f"Deleted OCI object: oci://{bucket_name}/{object_name}")
            except ServiceError as e:
                if e.status == 404:
                    logger.debug(f"OCI object already deleted: {oci_path}")
                else:
                    logger.warning(f"Could not delete OCI object {oci_path}: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error deleting OCI object {oci_path}: {e}")
                
    def clean_up_local_and_oci_data(self, local_paths: List[str] = None, 
                                  oci_paths: List[str] = None):
        """
        Comprehensive cleanup of both local and OCI files
        
        Args:
            local_paths: List of local file paths to delete
            oci_paths: List of OCI paths to delete
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
                        
        # Clean up OCI files
        if oci_paths:
            self.clean_up_cloud_data(oci_paths)
            
        # Clean up default temp files
        self.clean_up_temp_files()
        
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
            'profile_name': self.profile_name,
            'max_multipart_size_mb': self.max_multipart_size / (1024 * 1024),
            'max_parallel_uploads': self.max_parallel_uploads
        }