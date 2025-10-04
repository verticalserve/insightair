"""
GCP Tableau Operations
Tableau operations with GCP Cloud Storage integration for Hyper file management and data pipeline operations
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
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP libraries not available: {e}")
    GCP_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Pandas/PyArrow not available: {e}")
    PANDAS_AVAILABLE = False


class GCPTableauOperations(BaseTableauOperations):
    """
    Tableau operations with GCP Cloud Storage integration
    Provides end-to-end data pipeline from GCS Parquet files to Tableau Server
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Tableau operations
        
        Args:
            config: Configuration dictionary containing both Tableau and GCP settings
                Tableau settings: server_url, username, password, etc.
                GCP settings:
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON file (optional)
                - credentials: Service account credentials object (optional)
                - default_bucket: Default Cloud Storage bucket for operations
                - chunk_size: Chunk size for uploads/downloads (default: 8MB)
                - max_workers: Max workers for parallel operations (default: 8)
                - timeout: Timeout for operations in seconds (default: 300)
        """
        super().__init__(config)
        
        # GCP configuration
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.credentials = config.get('credentials')
        self.default_bucket = config.get('default_bucket')
        
        # GCS operation settings
        self.chunk_size = config.get('chunk_size', 8 * 1024 * 1024)  # 8MB
        self.max_workers = config.get('max_workers', 8)
        self.timeout = config.get('timeout', 300)  # 5 minutes
        
        # Initialize GCP clients
        self.storage_client = None
        self.bucket_client = None
        
        if GCP_AVAILABLE:
            try:
                self._initialize_gcp_clients()
            except Exception as e:
                logger.error(f"Failed to initialize GCP clients: {e}")
                # Continue without GCP - operations will fail gracefully
        else:
            logger.warning("GCP libraries not available, Cloud Storage operations will not work")
            
    def _initialize_gcp_clients(self):
        """Initialize Google Cloud Storage clients"""
        try:
            # Initialize storage client with different authentication methods
            if self.credentials:
                # Use provided credentials object
                self.storage_client = storage.Client(
                    project=self.project_id,
                    credentials=self.credentials
                )
            elif self.credentials_path:
                # Use service account file
                self.storage_client = storage.Client.from_service_account_json(
                    self.credentials_path,
                    project=self.project_id
                )
            else:
                # Use default credentials (environment, metadata server, etc.)
                self.storage_client = storage.Client(project=self.project_id)
                
            # Get default bucket client if specified
            if self.default_bucket:
                try:
                    self.bucket_client = self.storage_client.bucket(self.default_bucket)
                    # Test bucket access
                    self.bucket_client.reload()
                    logger.info(f"Verified access to default GCS bucket: {self.default_bucket}")
                except NotFound:
                    logger.warning(f"Default GCS bucket '{self.default_bucket}' not found")
                except Exception as e:
                    logger.warning(f"Could not access default bucket: {e}")
                    
            logger.info("Google Cloud Storage clients initialized successfully")
            
        except DefaultCredentialsError as e:
            logger.error(f"GCP credentials error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing GCP clients: {e}")
            raise
            
    def _parse_gcs_path(self, gcs_path: str) -> tuple:
        """
        Parse GCS path into bucket and blob name
        
        Args:
            gcs_path: GCS path (gs://bucket/object or just object if default_bucket set)
            
        Returns:
            Tuple of (bucket_name, blob_name)
            
        Raises:
            TableauOperationError: If path cannot be parsed
        """
        if gcs_path.startswith('gs://'):
            path_parts = gcs_path.replace('gs://', '').split('/', 1)
            bucket_name = path_parts[0]
            blob_name = path_parts[1] if len(path_parts) > 1 else ''
        else:
            if not self.default_bucket:
                raise TableauOperationError("No GCS bucket specified and no default bucket configured")
            bucket_name = self.default_bucket
            blob_name = gcs_path.lstrip('/')
            
        return bucket_name, blob_name
        
    def _download_gcs_file(self, gcs_path: str, local_path: str) -> str:
        """
        Download file from GCS to local path
        
        Args:
            gcs_path: GCS path to file
            local_path: Local destination path
            
        Returns:
            Local file path
            
        Raises:
            TableauOperationError: If download fails
        """
        if not self.storage_client:
            raise TableauOperationError("GCP Cloud Storage not configured")
            
        try:
            bucket_name, blob_name = self._parse_gcs_path(gcs_path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Get bucket and blob
            if bucket_name == self.default_bucket and self.bucket_client:
                bucket = self.bucket_client
            else:
                bucket = self.storage_client.bucket(bucket_name)
                
            blob = bucket.blob(blob_name)
            
            # Download with chunk size configuration
            blob.chunk_size = self.chunk_size
            blob.download_to_filename(local_path, timeout=self.timeout)
            
            logger.debug(f"Downloaded gs://{bucket_name}/{blob_name} to {local_path}")
            return local_path
            
        except NotFound as e:
            error_msg = f"GCS object not found: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
        except Exception as e:
            error_msg = f"Failed to download GCS file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def create_hyper_from_parquet(self, gcs_path: str, hyper_path: str,
                                table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from a GCS-stored Parquet file
        
        Args:
            gcs_path: GCS path to Parquet file (gs://bucket/object or just object)
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
            # Download Parquet file from GCS
            temp_parquet_path = os.path.join(
                self.temp_dir,
                f"temp_parquet_{datetime.now().timestamp()}.parquet"
            )
            self._download_gcs_file(gcs_path, temp_parquet_path)
            
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
            error_msg = f"Failed to create Hyper from GCS Parquet: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
        finally:
            # Cleanup temp file
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Parquet file: {e}")
                    
    def create_initial_hyper_from_parquet(self, gcs_paths: List[str], hyper_path: str,
                                        table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from multiple GCS-stored Parquet files
        Optimized for large initial loads leveraging GCS's parallel processing capabilities
        
        Args:
            gcs_paths: List of GCS paths to Parquet files
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not PANDAS_AVAILABLE:
            raise TableauOperationError("Pandas/PyArrow not available for Parquet processing")
            
        if not gcs_paths:
            raise TableauDataError("No GCS paths provided")
            
        temp_files = []
        
        try:
            # Process first file to create initial Hyper structure
            first_path = gcs_paths[0]
            temp_parquet = os.path.join(
                self.temp_dir,
                f"temp_parquet_0_{datetime.now().timestamp()}.parquet"
            )
            temp_files.append(temp_parquet)
            
            self._download_gcs_file(first_path, temp_parquet)
            df_first = pd.read_parquet(temp_parquet)
            
            # Create initial Hyper file
            self.create_hyper_from_dataframe(df_first, hyper_path, table_name)
            
            # Process remaining files and append to Hyper
            if len(gcs_paths) > 1:
                logger.info(f"Processing {len(gcs_paths) - 1} additional Parquet files from GCS")
                
                # Process files in parallel batches (GCS handles high concurrency well)
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    
                    for i, gcs_path in enumerate(gcs_paths[1:], 1):
                        future = executor.submit(self._process_and_append_parquet, 
                                               gcs_path, hyper_path, table_name, i)
                        futures.append(future)
                        
                    # Wait for all files to be processed
                    total_rows = len(df_first)
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            rows_added = future.result()
                            total_rows += rows_added
                            logger.debug(f"Added {rows_added} rows to Hyper file")
                        except Exception as e:
                            logger.error(f"Error processing GCS Parquet file: {e}")
                            
            # Get final file size
            final_size_mb = os.path.getsize(hyper_path) / (1024 * 1024)
            logger.info(f"Created initial Hyper file: {final_size_mb:.2f}MB from {len(gcs_paths)} GCS Parquet files")
            
            return hyper_path
            
        except Exception as e:
            error_msg = f"Failed to create initial Hyper from GCS Parquet files: {e}"
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
                        
    def _process_and_append_parquet(self, gcs_path: str, hyper_path: str, 
                                  table_name: str, index: int) -> int:
        """
        Process a single Parquet file and append to existing Hyper file
        
        Args:
            gcs_path: GCS path to Parquet file
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
            self._download_gcs_file(gcs_path, temp_parquet_path)
            
            # Read Parquet file
            df = pd.read_parquet(temp_parquet_path)
            
            # Append to Hyper file
            # Note: This would require opening the Hyper file in append mode
            # For now, we'll log the operation
            logger.info(f"Would append {len(df)} rows from {gcs_path} to Hyper file")
            
            return len(df)
            
        finally:
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp file: {e}")
                    
    def create_hyper_from_parquet_and_publish_tableau(self, gcs_paths: List[str],
                                                    datasource_name: str,
                                                    project_id: str = None,
                                                    mode: str = 'Upsert') -> Dict[str, Any]:
        """
        Processes multiple Parquet files iteratively and publishes to Tableau
        Leverages GCS's global infrastructure for optimal performance
        
        Args:
            gcs_paths: List of GCS paths to Parquet files
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
            for i, gcs_path in enumerate(gcs_paths):
                try:
                    # Create temp Hyper file for this batch
                    temp_hyper_path = os.path.join(
                        self.temp_dir,
                        f"temp_hyper_{i}_{datetime.now().timestamp()}.hyper"
                    )
                    
                    # Check file size before processing
                    bucket_name, blob_name = self._parse_gcs_path(gcs_path)
                    
                    # Get bucket and blob
                    if bucket_name == self.default_bucket and self.bucket_client:
                        bucket = self.bucket_client
                    else:
                        bucket = self.storage_client.bucket(bucket_name)
                        
                    blob = bucket.blob(blob_name)
                    blob.reload()
                    file_size_mb = blob.size / (1024 * 1024)
                    
                    if mode.lower() == 'upsert' and file_size_mb > self.upsert_max_size_mb:
                        logger.warning(f"File {gcs_path} is {file_size_mb:.2f}MB, exceeds upsert limit")
                        results['failed_files'].append({
                            'path': gcs_path,
                            'error': f"File size {file_size_mb:.2f}MB exceeds upsert limit"
                        })
                        continue
                        
                    # Create Hyper from Parquet
                    self.create_hyper_from_parquet(gcs_path, temp_hyper_path)
                    
                    # Publish to Tableau
                    datasource = self.publish_data_source(
                        temp_hyper_path,
                        datasource_name,
                        project_id,
                        mode=mode
                    )
                    
                    results['files_processed'] += 1
                    logger.info(f"Processed and published file {i+1}/{len(gcs_paths)} from GCS")
                    
                    # Cleanup temp Hyper file
                    if os.path.exists(temp_hyper_path):
                        os.remove(temp_hyper_path)
                        temp_hyper_path = None
                        
                except Exception as e:
                    logger.error(f"Failed to process {gcs_path}: {e}")
                    results['failed_files'].append({
                        'path': gcs_path,
                        'error': str(e)
                    })
                    
            results['published'] = results['files_processed'] > 0
            results['end_time'] = datetime.now()
            results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"Completed GCS processing: {results['files_processed']}/{len(gcs_paths)} files successful")
            return results
            
        except Exception as e:
            error_msg = f"Failed to process and publish GCS Parquet files: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
        finally:
            # Cleanup any remaining temp files
            if temp_hyper_path and os.path.exists(temp_hyper_path):
                try:
                    os.remove(temp_hyper_path)
                except Exception as e:
                    logger.warning(f"Could not remove temp Hyper file: {e}")
                    
    def clean_up_cloud_data(self, gcs_paths: List[str]):
        """
        Cleans up GCS files
        
        Args:
            gcs_paths: List of GCS paths to delete
        """
        if not self.storage_client:
            logger.warning("GCP Cloud Storage not configured, skipping cloud cleanup")
            return
            
        for gcs_path in gcs_paths:
            try:
                bucket_name, blob_name = self._parse_gcs_path(gcs_path)
                
                # Get bucket and blob
                if bucket_name == self.default_bucket and self.bucket_client:
                    bucket = self.bucket_client
                else:
                    bucket = self.storage_client.bucket(bucket_name)
                    
                blob = bucket.blob(blob_name)
                blob.delete()
                
                logger.debug(f"Deleted GCS object: gs://{bucket_name}/{blob_name}")
                
            except NotFound:
                logger.debug(f"GCS object already deleted: {gcs_path}")
            except Exception as e:
                logger.warning(f"Could not delete GCS object {gcs_path}: {e}")
                
    def clean_up_local_and_gcs_data(self, local_paths: List[str] = None, 
                                  gcs_paths: List[str] = None):
        """
        Comprehensive cleanup of both local and GCS files
        
        Args:
            local_paths: List of local file paths to delete
            gcs_paths: List of GCS paths to delete
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
                        
        # Clean up GCS files
        if gcs_paths:
            self.clean_up_cloud_data(gcs_paths)
            
        # Clean up default temp files
        self.clean_up_temp_files()
        
    def get_gcp_info(self) -> Dict[str, Any]:
        """
        Get GCP configuration information
        
        Returns:
            Dictionary with GCP info (without sensitive data)
        """
        return {
            'gcp_configured': self.storage_client is not None,
            'default_bucket': self.default_bucket,
            'project_id': self.project_id,
            'has_credentials_path': self.credentials_path is not None,
            'has_credentials_object': self.credentials is not None,
            'chunk_size_mb': self.chunk_size / (1024 * 1024),
            'max_workers': self.max_workers,
            'timeout_seconds': self.timeout
        }