"""
Base Tableau Operations Class
Provides core Tableau Server operations for Hyper file management, TDSX packaging, and data publishing
"""

import logging
import os
import shutil
import zipfile
import tempfile
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, TableDefinition, SqlType, NOT_NULLABLE, NULLABLE, Inserter, TableName
    HYPER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Tableau Hyper API not available: {e}")
    HYPER_AVAILABLE = False

try:
    import tableauserverclient as TSC
    TSC_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Tableau Server Client not available: {e}")
    TSC_AVAILABLE = False


class TableauAuthenticationError(Exception):
    """Exception raised for Tableau authentication errors"""
    pass


class TableauOperationError(Exception):
    """Exception raised for general Tableau operation errors"""
    pass


class TableauDataError(Exception):
    """Exception raised for Tableau data operation errors"""
    pass


class BaseTableauOperations(ABC):
    """
    Base class for Tableau operations across different cloud platforms
    Provides common functionality for Hyper file management, TDSX packaging, and publishing
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base Tableau operations
        
        Args:
            config: Configuration dictionary containing Tableau and cloud settings
                Tableau settings:
                - server_url: Tableau Server URL
                - username: Tableau Server username
                - password: Tableau Server password (optional if using token)
                - token_name: Personal access token name (optional)
                - token_value: Personal access token value (optional)
                - site_id: Tableau site ID (empty string for default site)
                - project_name: Default project name for publishing
                - hyper_process_parameters: Dict of Hyper process parameters
                - max_file_size_mb: Maximum file size in MB (default: 1024)
                - upsert_max_size_mb: Maximum file size for upsert operations (default: 64)
                - temp_dir: Temporary directory for file operations
                - cleanup_temp_files: Whether to cleanup temp files (default: True)
                - job_timeout_seconds: Timeout for async jobs (default: 1800)
                - retry_attempts: Number of retry attempts (default: 3)
                - retry_delay_seconds: Delay between retries (default: 5)
        """
        self.config = config
        
        # Tableau configuration
        self.server_url = config.get('server_url')
        self.username = config.get('username')
        self.password = config.get('password')
        self.token_name = config.get('token_name')
        self.token_value = config.get('token_value')
        self.site_id = config.get('site_id', '')
        self.project_name = config.get('project_name', 'Default')
        
        # Hyper process parameters
        self.hyper_process_parameters = config.get('hyper_process_parameters', {
            'log_config': '',
            'max_log_file_size': '100M',
            'max_log_file_count': '5'
        })
        
        # File size limits
        self.max_file_size_mb = config.get('max_file_size_mb', 1024)  # 1GB default
        self.upsert_max_size_mb = config.get('upsert_max_size_mb', 64)  # 64MB for upserts
        
        # Operation settings
        self.temp_dir = config.get('temp_dir', tempfile.gettempdir())
        self.cleanup_temp_files = config.get('cleanup_temp_files', True)
        self.job_timeout_seconds = config.get('job_timeout_seconds', 1800)  # 30 minutes
        self.retry_attempts = config.get('retry_attempts', 3)
        self.retry_delay_seconds = config.get('retry_delay_seconds', 5)
        
        # Runtime state
        self.server = None
        self.auth_token = None
        self.hyper_process = None
        
        # Validate configuration
        if not self.server_url:
            raise TableauOperationError("Tableau server_url is required")
            
        if not ((self.username and self.password) or (self.token_name and self.token_value)):
            raise TableauOperationError("Either username/password or token_name/token_value is required")
            
    def sign_in(self) -> TSC.Server:
        """
        Authenticates with Tableau Server
        
        Returns:
            Authenticated Tableau Server instance
            
        Raises:
            TableauAuthenticationError: If authentication fails
        """
        if not TSC_AVAILABLE:
            raise TableauOperationError("Tableau Server Client library not available")
            
        try:
            # Create server instance
            self.server = TSC.Server(self.server_url, use_server_version=True)
            
            # Create authentication object
            if self.token_name and self.token_value:
                # Use personal access token
                tableau_auth = TSC.PersonalAccessTokenAuth(
                    self.token_name,
                    self.token_value,
                    self.site_id
                )
            else:
                # Use username/password
                tableau_auth = TSC.TableauAuth(
                    self.username,
                    self.password,
                    self.site_id
                )
                
            # Sign in
            self.auth_token = self.server.auth.sign_in(tableau_auth)
            logger.info(f"Successfully signed in to Tableau Server at {self.server_url}")
            
            return self.server
            
        except Exception as e:
            error_msg = f"Failed to authenticate with Tableau Server: {e}"
            logger.error(error_msg)
            raise TableauAuthenticationError(error_msg)
            
    def sign_out(self):
        """Signs out from Tableau Server"""
        try:
            if self.server and self.auth_token:
                self.server.auth.sign_out()
                logger.info("Successfully signed out from Tableau Server")
                self.server = None
                self.auth_token = None
        except Exception as e:
            logger.warning(f"Error signing out from Tableau Server: {e}")
            
    def _ensure_signed_in(self):
        """Ensures we're signed in to Tableau Server"""
        if not self.server or not self.auth_token:
            self.sign_in()
            
    def _start_hyper_process(self) -> HyperProcess:
        """
        Starts a Hyper process
        
        Returns:
            HyperProcess instance
            
        Raises:
            TableauOperationError: If Hyper is not available
        """
        if not HYPER_AVAILABLE:
            raise TableauOperationError("Tableau Hyper API not available")
            
        try:
            if not self.hyper_process:
                self.hyper_process = HyperProcess(
                    telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU,
                    parameters=self.hyper_process_parameters
                )
            return self.hyper_process
            
        except Exception as e:
            error_msg = f"Failed to start Hyper process: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def _stop_hyper_process(self):
        """Stops the Hyper process if running"""
        try:
            if self.hyper_process:
                self.hyper_process.close()
                self.hyper_process = None
                logger.debug("Hyper process stopped")
        except Exception as e:
            logger.warning(f"Error stopping Hyper process: {e}")
            
    def _get_hyper_sql_type(self, pandas_dtype) -> SqlType:
        """
        Maps pandas dtype to Hyper SQL type
        
        Args:
            pandas_dtype: Pandas data type
            
        Returns:
            Hyper SqlType
        """
        dtype_str = str(pandas_dtype)
        
        if 'int64' in dtype_str:
            return SqlType.big_int()
        elif 'int' in dtype_str:
            return SqlType.int()
        elif 'float' in dtype_str:
            return SqlType.double()
        elif 'bool' in dtype_str:
            return SqlType.bool()
        elif 'datetime' in dtype_str:
            return SqlType.timestamp()
        elif 'date' in dtype_str:
            return SqlType.date()
        else:
            return SqlType.text()
            
    def create_hyper_from_dataframe(self, df: pd.DataFrame, hyper_path: str,
                                  table_name: str = 'Extract', schema_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from a pandas DataFrame
        
        Args:
            df: DataFrame to convert
            hyper_path: Path for the output Hyper file
            table_name: Name of the table in Hyper file
            schema_name: Schema name in Hyper file
            
        Returns:
            Path to created Hyper file
            
        Raises:
            TableauDataError: If creation fails
        """
        if not HYPER_AVAILABLE:
            raise TableauOperationError("Tableau Hyper API not available")
            
        try:
            # Start Hyper process
            hyper = self._start_hyper_process()
            
            # Create Hyper file
            with Connection(
                endpoint=hyper.endpoint,
                database=hyper_path,
                create_mode=CreateMode.CREATE_AND_REPLACE
            ) as connection:
                
                # Create table definition based on DataFrame
                columns = []
                for col_name, dtype in df.dtypes.items():
                    sql_type = self._get_hyper_sql_type(dtype)
                    # Check for nulls
                    nullability = NULLABLE if df[col_name].isnull().any() else NOT_NULLABLE
                    columns.append(TableDefinition.Column(col_name, sql_type, nullability))
                    
                # Create table
                table_def = TableDefinition(
                    table_name=TableName(schema_name, table_name),
                    columns=columns
                )
                connection.catalog.create_schema(schema_name)
                connection.catalog.create_table(table_def)
                
                # Insert data
                with Inserter(connection, table_def) as inserter:
                    # Convert DataFrame to list of tuples
                    data_tuples = [tuple(row) for row in df.values]
                    inserter.add_rows(data_tuples)
                    inserter.execute()
                    
            logger.info(f"Created Hyper file with {len(df)} rows at {hyper_path}")
            return hyper_path
            
        except Exception as e:
            error_msg = f"Failed to create Hyper file from DataFrame: {e}"
            logger.error(error_msg)
            raise TableauDataError(error_msg)
            
    def prepare_tdsx_file(self, hyper_path: str, tdsx_template_path: str,
                         output_tdsx_path: str, connection_credentials: Dict[str, str] = None) -> str:
        """
        Embeds a Hyper file into a TDSX template
        
        Args:
            hyper_path: Path to Hyper file
            tdsx_template_path: Path to TDSX template
            output_tdsx_path: Path for output TDSX file
            connection_credentials: Optional connection credentials to embed
            
        Returns:
            Path to created TDSX file
            
        Raises:
            TableauOperationError: If packaging fails
        """
        try:
            # Create temp directory for extraction
            temp_extract_dir = os.path.join(self.temp_dir, f"tdsx_extract_{datetime.now().timestamp()}")
            os.makedirs(temp_extract_dir, exist_ok=True)
            
            try:
                # Extract TDSX template
                with zipfile.ZipFile(tdsx_template_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_extract_dir)
                    
                # Find and replace the Hyper file
                data_dir = os.path.join(temp_extract_dir, 'Data')
                if os.path.exists(data_dir):
                    # Remove existing Hyper files
                    for file in os.listdir(data_dir):
                        if file.endswith('.hyper'):
                            os.remove(os.path.join(data_dir, file))
                            
                    # Copy new Hyper file
                    hyper_filename = os.path.basename(hyper_path)
                    shutil.copy2(hyper_path, os.path.join(data_dir, hyper_filename))
                else:
                    # Create Data directory if it doesn't exist
                    os.makedirs(data_dir)
                    shutil.copy2(hyper_path, data_dir)
                    
                # Update connection credentials if provided
                if connection_credentials:
                    self._update_tdsx_credentials(temp_extract_dir, connection_credentials)
                    
                # Repackage as TDSX
                with zipfile.ZipFile(output_tdsx_path, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                    for root, dirs, files in os.walk(temp_extract_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, temp_extract_dir)
                            zip_out.write(file_path, arcname)
                            
                logger.info(f"Created TDSX package at {output_tdsx_path}")
                return output_tdsx_path
                
            finally:
                # Cleanup temp directory
                if os.path.exists(temp_extract_dir):
                    shutil.rmtree(temp_extract_dir)
                    
        except Exception as e:
            error_msg = f"Failed to prepare TDSX file: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def _update_tdsx_credentials(self, extract_dir: str, credentials: Dict[str, str]):
        """
        Updates connection credentials in TDSX files
        
        Args:
            extract_dir: Directory containing extracted TDSX files
            credentials: Dictionary of credentials to update
        """
        # This is a placeholder - actual implementation would modify the TWB/TWBX XML files
        logger.debug(f"Updating TDSX credentials in {extract_dir}")
        
    def publish_data_source(self, file_path: str, name: str, project_id: str = None,
                          mode: str = 'Publish', description: str = None,
                          tags: List[str] = None) -> TSC.DatasourceItem:
        """
        Publishes a Hyper or TDSX file to Tableau Server
        
        Args:
            file_path: Path to Hyper or TDSX file
            name: Name for the data source
            project_id: Project ID (uses default if not specified)
            mode: Publishing mode ('Publish', 'Append', 'Upsert', 'Delete')
            description: Data source description
            tags: List of tags to apply
            
        Returns:
            Published datasource item
            
        Raises:
            TableauOperationError: If publishing fails
        """
        self._ensure_signed_in()
        
        try:
            # Get project ID if not specified
            if not project_id:
                project_id = self._get_default_project_id()
                
            # Create datasource item
            new_datasource = TSC.DatasourceItem(project_id, name=name)
            
            if description:
                new_datasource.description = description
                
            if tags:
                new_datasource.tags = tags
                
            # Set publish mode
            publish_mode = TSC.Server.PublishMode.Overwrite
            if mode.lower() == 'append':
                publish_mode = TSC.Server.PublishMode.Append
                
            # For Upsert and Delete, we need to handle differently
            if mode.lower() in ['upsert', 'delete']:
                # These require special handling via REST API
                return self._publish_with_action(file_path, new_datasource, mode)
                
            # Publish datasource
            datasource = self.server.datasources.publish(
                new_datasource,
                file_path,
                publish_mode,
                connection_credentials=None
            )
            
            logger.info(f"Published data source '{name}' to project {project_id}")
            return datasource
            
        except Exception as e:
            error_msg = f"Failed to publish data source: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def _get_default_project_id(self) -> str:
        """Gets the default project ID"""
        try:
            for project in TSC.Pager(self.server.projects):
                if project.name == self.project_name:
                    return project.id
                    
            # If not found, use the first project
            for project in TSC.Pager(self.server.projects):
                return project.id
                
            raise TableauOperationError("No projects found on Tableau Server")
            
        except Exception as e:
            raise TableauOperationError(f"Failed to get project ID: {e}")
            
    def _publish_with_action(self, file_path: str, datasource: TSC.DatasourceItem, 
                           action: str) -> TSC.DatasourceItem:
        """
        Publishes with special actions (Upsert/Delete) using REST API
        
        Args:
            file_path: Path to data file
            datasource: Datasource item
            action: Action type ('Upsert' or 'Delete')
            
        Returns:
            Published datasource item
        """
        # This would require direct REST API calls as TSC doesn't support these modes
        # For now, we'll use standard publish and log a warning
        logger.warning(f"Action '{action}' not fully supported, using standard publish")
        return self.server.datasources.publish(
            datasource,
            file_path,
            TSC.Server.PublishMode.Overwrite
        )
        
    def wait_for_job_to_complete(self, job_id: str, timeout_seconds: int = None) -> Dict[str, Any]:
        """
        Monitors a job until completion
        
        Args:
            job_id: Job ID to monitor
            timeout_seconds: Timeout in seconds (uses default if not specified)
            
        Returns:
            Job completion status
            
        Raises:
            TableauOperationError: If job fails or times out
        """
        self._ensure_signed_in()
        
        timeout = timeout_seconds or self.job_timeout_seconds
        start_time = time.time()
        
        try:
            while True:
                # Get job status
                job = self.server.jobs.get_by_id(job_id)
                
                if job.completed_at:
                    # Job completed
                    duration = time.time() - start_time
                    logger.info(f"Job {job_id} completed in {duration:.2f} seconds")
                    
                    return {
                        'job_id': job_id,
                        'status': 'completed',
                        'duration_seconds': duration,
                        'completed_at': job.completed_at
                    }
                    
                # Check timeout
                if time.time() - start_time > timeout:
                    raise TableauOperationError(f"Job {job_id} timed out after {timeout} seconds")
                    
                # Wait before next check
                time.sleep(5)
                
        except Exception as e:
            error_msg = f"Error waiting for job completion: {e}"
            logger.error(error_msg)
            raise TableauOperationError(error_msg)
            
    def clean_up_temp_files(self, file_patterns: List[str] = None):
        """
        Cleans up temporary files
        
        Args:
            file_patterns: List of file patterns to clean (uses defaults if not specified)
        """
        if not self.cleanup_temp_files:
            logger.debug("Temp file cleanup is disabled")
            return
            
        patterns = file_patterns or ['*.hyper', '*.tdsx', '*.parquet']
        
        try:
            for pattern in patterns:
                for file_path in os.listdir(self.temp_dir):
                    if file_path.endswith(pattern.replace('*', '')):
                        full_path = os.path.join(self.temp_dir, file_path)
                        try:
                            os.remove(full_path)
                            logger.debug(f"Cleaned up temp file: {full_path}")
                        except Exception as e:
                            logger.warning(f"Could not remove temp file {full_path}: {e}")
                            
        except Exception as e:
            logger.warning(f"Error during temp file cleanup: {e}")
            
    @abstractmethod
    def create_hyper_from_parquet(self, cloud_path: str, hyper_path: str,
                                table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from a cloud-stored Parquet file
        
        Args:
            cloud_path: Cloud storage path to Parquet file
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
        """
        pass
        
    @abstractmethod
    def create_initial_hyper_from_parquet(self, cloud_paths: List[str], hyper_path: str,
                                        table_name: str = 'Extract') -> str:
        """
        Creates a Hyper file from multiple cloud-stored Parquet files
        
        Args:
            cloud_paths: List of cloud storage paths to Parquet files
            hyper_path: Local path for output Hyper file
            table_name: Table name in Hyper file
            
        Returns:
            Path to created Hyper file
        """
        pass
        
    @abstractmethod
    def clean_up_cloud_data(self, cloud_paths: List[str]):
        """
        Cleans up cloud storage files
        
        Args:
            cloud_paths: List of cloud storage paths to clean up
        """
        pass
        
    def __enter__(self):
        """Context manager entry"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        self.sign_out()
        self._stop_hyper_process()
        if self.cleanup_temp_files:
            self.clean_up_temp_files()