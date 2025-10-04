"""
Azure Salesforce Operations
Salesforce Marketing Cloud operations with Azure Blob Storage integration for data export and import
"""

import logging
import pandas as pd
import io
import json
import time
import concurrent.futures
from typing import Dict, List, Any, Optional, Union, Generator
from datetime import datetime, timedelta

from .base_salesforce import BaseSalesforceOperations, SalesforceDataError, SalesforceOperationError

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
    import requests
    REQUESTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Requests library not available: {e}")
    REQUESTS_AVAILABLE = False


class AzureSalesforceOperations(BaseSalesforceOperations):
    """
    Salesforce Marketing Cloud operations with Azure Blob Storage integration
    Combines Salesforce functionality with Azure cloud storage capabilities for data pipelines
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Salesforce operations
        
        Args:
            config: Configuration dictionary containing both Salesforce and Azure settings
                Salesforce settings: client_id, client_secret, account_id, etc.
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
            
    def export_data_extension(self, data_extension_key: str, azure_path: str, 
                            file_format: str = 'csv', filters: Dict[str, Any] = None,
                            batch_size: int = None) -> Dict[str, Any]:
        """
        Exports data extension content to Azure Blob Storage in CSV/Parquet format
        
        Args:
            data_extension_key: Data extension customer key
            azure_path: Azure path for export (azure://container/blob or just blob if default_container set)
            file_format: Export format ('csv' or 'parquet')
            filters: Optional filters to apply
            batch_size: Batch size for data retrieval
            
        Returns:
            Dictionary with export results
        """
        if not self.blob_service_client:
            raise SalesforceOperationError("Azure Blob Storage not configured for export operations")
            
        try:
            start_time = datetime.now()
            
            # Parse Azure path
            if azure_path.startswith('azure://'):
                path_parts = azure_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                if not self.default_container or not self.container_client:
                    raise SalesforceOperationError("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = azure_path.lstrip('/')
                
            # Get data extension rows
            batch_sz = batch_size or self.batch_size
            all_rows = []
            
            # Use generator to handle large datasets
            for batch_rows in self._get_data_extensions_rows_soap_gen(
                data_extension_key, filters, batch_sz
            ):
                all_rows.extend(batch_rows)
                
            if not all_rows:
                logger.warning(f"No data found for data extension {data_extension_key}")
                return {
                    'success': True,
                    'message': 'No data to export',
                    'rows_exported': 0,
                    'azure_path': f"azure://{container_client.container_name}/{blob_name}"
                }
                
            # Convert to DataFrame
            df = self.transform_dtypes(all_rows)
            
            # Get blob client
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            # Export based on format
            if file_format.lower() == 'csv':
                csv_data = df.to_csv(index=False)
                blob_client.upload_blob(csv_data, overwrite=True, content_type='text/csv')
                
            elif file_format.lower() == 'parquet':
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                blob_client.upload_blob(
                    parquet_buffer.getvalue(), 
                    overwrite=True,
                    content_type='application/octet-stream'
                )
                
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'data_extension_key': data_extension_key,
                'azure_path': f"azure://{container_client.container_name}/{blob_name}",
                'file_format': file_format,
                'rows_exported': len(all_rows),
                'columns_exported': len(df.columns) if not df.empty else 0,
                'export_duration_seconds': duration,
                'filters_applied': filters,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully exported {len(all_rows)} rows from {data_extension_key} "
                       f"to azure://{container_client.container_name}/{blob_name} in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting data extension to Azure: {e}")
            return {
                'success': False,
                'data_extension_key': data_extension_key,
                'azure_path': azure_path,
                'error': str(e),
                'rows_exported': 0
            }
            
    def get_data_extensions_rows_soap(self, data_extension_key: str, 
                                    filters: Dict[str, Any] = None,
                                    batch_size: int = None) -> List[Dict[str, Any]]:
        """
        Retrieves data extension rows with filtering capabilities
        
        Args:
            data_extension_key: Data extension customer key
            filters: Optional filters (date_range, columns, conditions)
            batch_size: Batch size for retrieval
            
        Returns:
            List of row dictionaries
        """
        try:
            batch_sz = batch_size or self.batch_size
            all_rows = []
            
            for batch_rows in self._get_data_extensions_rows_soap_gen(
                data_extension_key, filters, batch_sz
            ):
                all_rows.extend(batch_rows)
                
            logger.info(f"Retrieved {len(all_rows)} rows from data extension {data_extension_key}")
            return all_rows
            
        except Exception as e:
            logger.error(f"Error retrieving data extension rows: {e}")
            raise SalesforceDataError(f"Failed to retrieve rows: {e}")
            
    def _get_data_extensions_rows_soap_gen(self, data_extension_key: str,
                                         filters: Dict[str, Any] = None,
                                         batch_size: int = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generator for batch processing large datasets with Azure optimizations
        
        Args:
            data_extension_key: Data extension customer key
            filters: Optional filters
            batch_size: Batch size for processing
            
        Yields:
            Batches of row dictionaries
        """
        try:
            if not self.soap_client:
                self._initialize_soap_client()
                
            batch_sz = batch_size or self.batch_size
            current_batch = 0
            
            while True:
                # Prepare SOAP request
                retrieve_request = self.soap_client.factory.create('RetrieveRequest')
                retrieve_request.ObjectType = 'DataExtensionObject[' + data_extension_key + ']'
                
                # Set properties (get all columns)
                columns = self._get_data_extension_columns_soap(data_extension_key)
                if columns:
                    retrieve_request.Properties = [col['Name'] for col in columns]
                else:
                    # Fallback to common properties
                    retrieve_request.Properties = ['*']
                    
                # Apply filters if specified (enhanced for Azure operations)
                if filters:
                    filter_part = self._build_soap_filter_azure(filters)
                    if filter_part:
                        retrieve_request.Filter = filter_part
                        
                # Set pagination with Azure optimizations
                retrieve_request.Options = self.soap_client.factory.create('RetrieveOptions')
                retrieve_request.Options.BatchSize = min(batch_sz, 2000)  # Azure optimization
                retrieve_request.Options.StartIndex = current_batch * batch_sz
                
                # Execute request with retry and Azure-specific timeout
                response = self._retry_operation(
                    self.soap_client.service.Retrieve, retrieve_request
                )
                
                if response.OverallStatus != 'OK':
                    if 'MoreDataAvailable' not in str(response.OverallStatus):
                        logger.error(f"SOAP retrieve failed: {response.OverallStatus}")
                        break
                        
                # Process results
                batch_rows = []
                if hasattr(response, 'Results') and response.Results:
                    for result in response.Results:
                        row_dict = {}
                        if hasattr(result, 'Properties') and result.Properties:
                            for prop in result.Properties:
                                if hasattr(prop, 'Name') and hasattr(prop, 'Value'):
                                    # Azure-specific data type handling
                                    value = prop.Value
                                    if isinstance(value, str) and len(value) > 8000:  # Azure text limit
                                        value = value[:8000]  # Truncate for Azure compatibility
                                    row_dict[prop.Name] = value
                        batch_rows.append(row_dict)
                        
                if not batch_rows:
                    break
                    
                yield batch_rows
                current_batch += 1
                
                # Check if more data is available
                if not hasattr(response, 'MoreDataAvailable') or not response.MoreDataAvailable:
                    break
                    
                # Refresh token if needed for long-running operations
                if current_batch % 10 == 0:  # Every 10 batches
                    self.get_auth_token()
                    
        except Exception as e:
            logger.error(f"Error in SOAP data generator: {e}")
            raise SalesforceDataError(f"SOAP data retrieval failed: {e}")
            
    def _build_soap_filter_azure(self, filters: Dict[str, Any]):
        """
        Build SOAP filter with Azure-specific optimizations
        
        Args:
            filters: Filter specifications
            
        Returns:
            SOAP filter object optimized for Azure operations
        """
        try:
            if not filters:
                return None
                
            # Handle date range filters with Azure timezone considerations
            if 'date_range' in filters:
                date_filter = filters['date_range']
                if 'start_date' in date_filter and 'end_date' in date_filter:
                    # Create date range filter
                    complex_filter = self.soap_client.factory.create('ComplexFilterPart')
                    complex_filter.LogicalOperator = 'AND'
                    
                    # Start date filter with UTC normalization for Azure
                    start_filter = self.soap_client.factory.create('SimpleFilterPart')
                    start_filter.Property = date_filter.get('date_field', 'CreatedDate')
                    start_filter.SimpleOperator = 'greaterThan'
                    
                    # Ensure ISO format for Azure compatibility
                    start_date = date_filter['start_date']
                    if isinstance(start_date, str) and 'T' not in start_date:
                        start_date += 'T00:00:00.000Z'
                    start_filter.Value = start_date
                    
                    # End date filter
                    end_filter = self.soap_client.factory.create('SimpleFilterPart')
                    end_filter.Property = date_filter.get('date_field', 'CreatedDate')
                    end_filter.SimpleOperator = 'lessThan'
                    
                    end_date = date_filter['end_date']
                    if isinstance(end_date, str) and 'T' not in end_date:
                        end_date += 'T23:59:59.999Z'
                    end_filter.Value = end_date
                    
                    complex_filter.LeftOperand = start_filter
                    complex_filter.RightOperand = end_filter
                    
                    return complex_filter
                    
            # Handle Azure-optimized field filters
            if 'conditions' in filters:
                conditions = filters['conditions']
                if len(conditions) == 1:
                    # Single condition with Azure data type handling
                    condition = conditions[0]
                    simple_filter = self.soap_client.factory.create('SimpleFilterPart')
                    simple_filter.Property = condition['field']
                    simple_filter.SimpleOperator = condition.get('operator', 'equals')
                    
                    # Azure-specific value formatting
                    value = condition['value']
                    if isinstance(value, str) and len(value) > 4000:  # Azure varchar limit
                        value = value[:4000]
                    simple_filter.Value = value
                    
                    return simple_filter
                    
            return None
            
        except Exception as e:
            logger.warning(f"Error building Azure SOAP filter: {e}")
            return None
            
    def _get_data_extension_columns_soap(self, data_extension_key: str) -> List[Dict[str, Any]]:
        """
        Get data extension column metadata via SOAP with Azure caching
        
        Args:
            data_extension_key: Data extension customer key
            
        Returns:
            List of column metadata dictionaries
        """
        try:
            if not self.soap_client:
                self._initialize_soap_client()
                
            # Prepare SOAP request for data extension fields
            retrieve_request = self.soap_client.factory.create('RetrieveRequest')
            retrieve_request.ObjectType = 'DataExtensionField'
            
            properties = ['Name', 'FieldType', 'IsPrimaryKey', 'IsRequired', 'MaxLength', 'Ordinal']
            retrieve_request.Properties = properties
            
            # Filter by data extension
            simple_filter = self.soap_client.factory.create('SimpleFilterPart')
            simple_filter.Property = 'DataExtension.CustomerKey'
            simple_filter.SimpleOperator = 'equals'
            simple_filter.Value = data_extension_key
            retrieve_request.Filter = simple_filter
            
            response = self.soap_client.service.Retrieve(retrieve_request)
            
            columns = []
            if response.OverallStatus == 'OK' and hasattr(response, 'Results'):
                for result in response.Results:
                    column_dict = {}
                    for prop in properties:
                        if hasattr(result, prop):
                            column_dict[prop] = getattr(result, prop)
                    columns.append(column_dict)
                    
            return columns
            
        except Exception as e:
            logger.warning(f"Could not get SOAP column metadata: {e}")
            return []
            
    def send_data_to_salesforce(self, df: pd.DataFrame, data_extension_key: str,
                               batch_size: int = None) -> Dict[str, Any]:
        """
        Uploads DataFrame data to data extensions via REST API with Azure optimizations
        
        Args:
            df: DataFrame with data to upload
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            
        Returns:
            Dictionary with upload results
        """
        try:
            if df.empty:
                return {
                    'success': True,
                    'message': 'No data to upload',
                    'rows_processed': 0,
                    'rows_successful': 0,
                    'rows_failed': 0
                }
                
            # Format DataFrame for Salesforce with Azure considerations
            formatted_df = self.format_df_azure(df)
            batch_sz = batch_size or min(self.batch_size, 1000)  # Azure optimization
            
            total_rows = len(formatted_df)
            rows_successful = 0
            rows_failed = 0
            batch_results = []
            
            # Process data in batches with Azure-optimized concurrency
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = []
                
                for i in range(0, total_rows, batch_sz):
                    batch_df = formatted_df.iloc[i:i + batch_sz]
                    future = executor.submit(self._upload_batch_rest, batch_df, data_extension_key)
                    futures.append((future, len(batch_df)))
                    
                # Process completed futures
                for future, batch_size in futures:
                    try:
                        batch_result = future.result(timeout=300)  # 5 minute timeout
                        batch_results.append(batch_result)
                        
                        if batch_result.get('success', False):
                            rows_successful += batch_size
                        else:
                            rows_failed += batch_size
                            
                    except Exception as e:
                        logger.error(f"Batch upload failed: {e}")
                        rows_failed += batch_size
                        batch_results.append({
                            'success': False,
                            'error': str(e),
                            'batch_size': batch_size
                        })
                        
            result = {
                'success': rows_failed == 0,
                'data_extension_key': data_extension_key,
                'rows_processed': total_rows,
                'rows_successful': rows_successful,
                'rows_failed': rows_failed,
                'batch_count': len(batch_results),
                'batch_results': batch_results,
                'azure_optimized': True
            }
            
            logger.info(f"Upload completed: {rows_successful}/{total_rows} rows successful")
            return result
            
        except Exception as e:
            logger.error(f"Error uploading data to Salesforce: {e}")
            return {
                'success': False,
                'data_extension_key': data_extension_key,
                'error': str(e),
                'rows_processed': len(df) if not df.empty else 0,
                'rows_successful': 0,
                'rows_failed': len(df) if not df.empty else 0
            }
            
    def format_df_azure(self, df: pd.DataFrame, datetime_format: str = '%m/%d/%Y %I:%M:%S %p') -> pd.DataFrame:
        """
        Formats DataFrames for Salesforce compatibility with Azure-specific optimizations
        
        Args:
            df: DataFrame to format
            datetime_format: Format string for datetime conversion
            
        Returns:
            Formatted DataFrame optimized for Azure operations
        """
        try:
            formatted_df = df.copy()
            
            # Handle datetime columns
            for col in formatted_df.columns:
                if pd.api.types.is_datetime64_any_dtype(formatted_df[col]):
                    formatted_df[col] = formatted_df[col].dt.strftime(datetime_format)
                    
            # Handle null values
            formatted_df = formatted_df.fillna('')
            
            # Azure-specific string length limitations
            for col in formatted_df.columns:
                if formatted_df[col].dtype == 'object':  # String columns
                    formatted_df[col] = formatted_df[col].astype(str)
                    # Limit string length for Azure compatibility
                    formatted_df[col] = formatted_df[col].str.slice(0, 4000)
                else:
                    formatted_df[col] = formatted_df[col].astype(str)
                    
            logger.debug(f"Formatted DataFrame with Azure optimizations: {len(formatted_df)} rows, {len(formatted_df.columns)} columns")
            return formatted_df
            
        except Exception as e:
            error_msg = f"Error formatting DataFrame for Azure: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def _upload_batch_rest(self, batch_df: pd.DataFrame, data_extension_key: str) -> Dict[str, Any]:
        """
        Upload a batch of data via REST API with Azure optimizations
        
        Args:
            batch_df: Batch DataFrame
            data_extension_key: Data extension customer key
            
        Returns:
            Batch upload result
        """
        try:
            headers = self._get_auth_headers()
            url = f"{self.rest_base_url.rstrip('/')}/hub/v1/dataevents/key:{data_extension_key}/rowset"
            
            # Convert DataFrame to list of dictionaries
            records = batch_df.to_dict('records')
            
            payload = records
            
            # Azure-optimized request with longer timeout
            response = requests.post(url, json=payload, headers=headers, timeout=180)
            
            if response.status_code in [200, 201, 202]:
                return {
                    'success': True,
                    'batch_size': len(records),
                    'status_code': response.status_code,
                    'azure_optimized': True
                }
            else:
                error_msg = f"REST upload failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'batch_size': len(records),
                    'error': error_msg,
                    'status_code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error in batch REST upload: {e}")
            return {
                'success': False,
                'batch_size': len(batch_df),
                'error': str(e)
            }
            
    def send_s3_data_to_salesforce(self, azure_path: str, data_extension_key: str,
                                  batch_size: int = None, file_format: str = 'csv') -> Dict[str, Any]:
        """
        Bulk uploads data from Azure Blob Storage files to Salesforce with batching
        
        Args:
            azure_path: Azure path to data file (azure://container/blob or just blob if default_container set)
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            Dictionary with upload results
        """
        if not self.blob_service_client:
            raise SalesforceOperationError("Azure Blob Storage not configured for data import operations")
            
        try:
            # Parse Azure path
            if azure_path.startswith('azure://'):
                path_parts = azure_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                if not self.default_container or not self.container_client:
                    raise SalesforceOperationError("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = azure_path.lstrip('/')
                
            # Read data from Azure Blob Storage
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            if not blob_client.exists():
                raise SalesforceOperationError(f"Azure blob not found: azure://{container_client.container_name}/{blob_name}")
                
            blob_data = blob_client.download_blob().readall()
            
            if file_format.lower() == 'csv':
                df = pd.read_csv(io.BytesIO(blob_data))
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(io.BytesIO(blob_data))
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from azure://{container_client.container_name}/{blob_name}")
            
            # Upload data to Salesforce
            result = self.send_data_to_salesforce(df, data_extension_key, batch_size)
            result['azure_source'] = f"azure://{container_client.container_name}/{blob_name}"
            result['file_format'] = file_format
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading Azure data to Salesforce: {e}")
            return {
                'success': False,
                'azure_source': azure_path,
                'data_extension_key': data_extension_key,
                'error': str(e),
                'rows_processed': 0,
                'rows_successful': 0,
                'rows_failed': 0
            }
            
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
            'has_connection_string': self.connection_string is not None
        }