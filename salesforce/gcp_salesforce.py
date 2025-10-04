"""
GCP Salesforce Operations
Salesforce Marketing Cloud operations with GCP Cloud Storage integration for data export and import
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
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP libraries not available: {e}")
    GCP_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Requests library not available: {e}")
    REQUESTS_AVAILABLE = False


class GCPSalesforceOperations(BaseSalesforceOperations):
    """
    Salesforce Marketing Cloud operations with GCP Cloud Storage integration
    Combines Salesforce functionality with GCP cloud storage capabilities for data pipelines
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Salesforce operations
        
        Args:
            config: Configuration dictionary containing both Salesforce and GCP settings
                Salesforce settings: client_id, client_secret, account_id, etc.
                GCP settings:
                - project_id: GCP project ID
                - credentials_path: Path to service account JSON file (optional)
                - credentials: Service account credentials object (optional)
                - default_bucket: Default Cloud Storage bucket for operations
        """
        super().__init__(config)
        
        # GCP configuration
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.credentials = config.get('credentials')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize GCP clients
        self.storage_client = None
        self.bucket_client = None
        
        if GCP_AVAILABLE:
            try:
                self._initialize_gcp_clients()
            except Exception as e:
                logger.error(f"Failed to initialize GCP clients: {e}")
                # Continue without GCP - operations will fall back gracefully
        else:
            logger.warning("GCP libraries not available, Cloud Storage operations will not work")
            
    def _initialize_gcp_clients(self):
        """
        Initialize Google Cloud Storage clients
        """
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
            
    def export_data_extension(self, data_extension_key: str, gcs_path: str, 
                            file_format: str = 'csv', filters: Dict[str, Any] = None,
                            batch_size: int = None) -> Dict[str, Any]:
        """
        Exports data extension content to Google Cloud Storage in CSV/Parquet format
        
        Args:
            data_extension_key: Data extension customer key
            gcs_path: GCS path for export (gs://bucket/object or just object if default_bucket set)
            file_format: Export format ('csv' or 'parquet')
            filters: Optional filters to apply
            batch_size: Batch size for data retrieval
            
        Returns:
            Dictionary with export results
        """
        if not self.storage_client:
            raise SalesforceOperationError("GCP Cloud Storage not configured for export operations")
            
        try:
            start_time = datetime.now()
            
            # Parse GCS path
            if gcs_path.startswith('gs://'):
                path_parts = gcs_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                if not self.default_bucket or not self.bucket_client:
                    raise SalesforceOperationError("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket_client
                blob_name = gcs_path.lstrip('/')
                
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
                    'gcs_path': f"gs://{bucket.name}/{blob_name}"
                }
                
            # Convert to DataFrame
            df = self.transform_dtypes(all_rows)
            
            # Create blob
            blob = bucket.blob(blob_name)
            
            # Export based on format
            if file_format.lower() == 'csv':
                csv_data = df.to_csv(index=False)
                blob.upload_from_string(csv_data, content_type='text/csv')
                
            elif file_format.lower() == 'parquet':
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                blob.upload_from_string(
                    parquet_buffer.getvalue(), 
                    content_type='application/octet-stream'
                )
                
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'data_extension_key': data_extension_key,
                'gcs_path': f"gs://{bucket.name}/{blob_name}",
                'file_format': file_format,
                'rows_exported': len(all_rows),
                'columns_exported': len(df.columns) if not df.empty else 0,
                'export_duration_seconds': duration,
                'filters_applied': filters,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully exported {len(all_rows)} rows from {data_extension_key} "
                       f"to gs://{bucket.name}/{blob_name} in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting data extension to GCS: {e}")
            return {
                'success': False,
                'data_extension_key': data_extension_key,
                'gcs_path': gcs_path,
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
        Generator for batch processing large datasets
        
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
                    
                # Apply filters if specified
                if filters:
                    filter_part = self._build_soap_filter(filters)
                    if filter_part:
                        retrieve_request.Filter = filter_part
                        
                # Set pagination
                retrieve_request.Options = self.soap_client.factory.create('RetrieveOptions')
                retrieve_request.Options.BatchSize = batch_sz
                retrieve_request.Options.StartIndex = current_batch * batch_sz
                
                # Execute request with retry
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
                                    row_dict[prop.Name] = prop.Value
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
            
    def _get_data_extension_columns_soap(self, data_extension_key: str) -> List[Dict[str, Any]]:
        """
        Get data extension column metadata via SOAP
        
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
            
    def _build_soap_filter(self, filters: Dict[str, Any]):
        """
        Build SOAP filter from filter dictionary with advanced GCP-specific optimizations
        
        Args:
            filters: Filter specifications
            
        Returns:
            SOAP filter object
        """
        try:
            if not filters:
                return None
                
            # Handle date range filters with timezone support
            if 'date_range' in filters:
                date_filter = filters['date_range']
                if 'start_date' in date_filter and 'end_date' in date_filter:
                    # Create date range filter
                    complex_filter = self.soap_client.factory.create('ComplexFilterPart')
                    complex_filter.LogicalOperator = 'AND'
                    
                    # Start date filter
                    start_filter = self.soap_client.factory.create('SimpleFilterPart')
                    start_filter.Property = date_filter.get('date_field', 'CreatedDate')
                    start_filter.SimpleOperator = 'greaterThan'
                    start_filter.Value = date_filter['start_date']
                    
                    # End date filter
                    end_filter = self.soap_client.factory.create('SimpleFilterPart')
                    end_filter.Property = date_filter.get('date_field', 'CreatedDate')
                    end_filter.SimpleOperator = 'lessThan'
                    end_filter.Value = date_filter['end_date']
                    
                    complex_filter.LeftOperand = start_filter
                    complex_filter.RightOperand = end_filter
                    
                    return complex_filter
                    
            # Handle multiple condition filters with OR/AND logic
            if 'conditions' in filters:
                conditions = filters['conditions']
                if len(conditions) == 1:
                    # Single condition
                    condition = conditions[0]
                    simple_filter = self.soap_client.factory.create('SimpleFilterPart')
                    simple_filter.Property = condition['field']
                    simple_filter.SimpleOperator = condition.get('operator', 'equals')
                    simple_filter.Value = condition['value']
                    return simple_filter
                elif len(conditions) > 1:
                    # Multiple conditions with logical operator
                    logical_op = filters.get('logical_operator', 'AND')
                    
                    # Build complex filter
                    complex_filter = self.soap_client.factory.create('ComplexFilterPart')
                    complex_filter.LogicalOperator = logical_op
                    
                    # Create first condition
                    first_condition = conditions[0]
                    left_filter = self.soap_client.factory.create('SimpleFilterPart')
                    left_filter.Property = first_condition['field']
                    left_filter.SimpleOperator = first_condition.get('operator', 'equals')
                    left_filter.Value = first_condition['value']
                    
                    if len(conditions) == 2:
                        # Two conditions
                        second_condition = conditions[1]
                        right_filter = self.soap_client.factory.create('SimpleFilterPart')
                        right_filter.Property = second_condition['field']
                        right_filter.SimpleOperator = second_condition.get('operator', 'equals')
                        right_filter.Value = second_condition['value']
                        
                        complex_filter.LeftOperand = left_filter
                        complex_filter.RightOperand = right_filter
                    else:
                        # More than two conditions - create nested complex filters
                        remaining_conditions = conditions[1:]
                        right_complex = self._build_complex_filter_recursive(remaining_conditions, logical_op)
                        
                        complex_filter.LeftOperand = left_filter
                        complex_filter.RightOperand = right_complex
                        
                    return complex_filter
                    
            return None
            
        except Exception as e:
            logger.warning(f"Error building SOAP filter: {e}")
            return None
            
    def _build_complex_filter_recursive(self, conditions: List[Dict[str, Any]], logical_op: str):
        """
        Recursively build complex filters for multiple conditions
        
        Args:
            conditions: List of conditions
            logical_op: Logical operator ('AND' or 'OR')
            
        Returns:
            Complex filter object
        """
        if len(conditions) == 1:
            condition = conditions[0]
            simple_filter = self.soap_client.factory.create('SimpleFilterPart')
            simple_filter.Property = condition['field']
            simple_filter.SimpleOperator = condition.get('operator', 'equals')
            simple_filter.Value = condition['value']
            return simple_filter
        else:
            complex_filter = self.soap_client.factory.create('ComplexFilterPart')
            complex_filter.LogicalOperator = logical_op
            
            first_condition = conditions[0]
            left_filter = self.soap_client.factory.create('SimpleFilterPart')
            left_filter.Property = first_condition['field']
            left_filter.SimpleOperator = first_condition.get('operator', 'equals')
            left_filter.Value = first_condition['value']
            
            remaining_conditions = conditions[1:]
            right_filter = self._build_complex_filter_recursive(remaining_conditions, logical_op)
            
            complex_filter.LeftOperand = left_filter
            complex_filter.RightOperand = right_filter
            
            return complex_filter
            
    def send_data_to_salesforce(self, df: pd.DataFrame, data_extension_key: str,
                               batch_size: int = None) -> Dict[str, Any]:
        """
        Uploads DataFrame data to data extensions via REST API with GCP optimizations
        
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
                
            # Format DataFrame for Salesforce
            formatted_df = self.format_df(df)
            batch_sz = batch_size or self.batch_size
            
            total_rows = len(formatted_df)
            rows_successful = 0
            rows_failed = 0
            batch_results = []
            
            # Process data in batches with concurrent processing for GCP
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
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
                'concurrent_processing': True
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
            
    def _upload_batch_rest(self, batch_df: pd.DataFrame, data_extension_key: str) -> Dict[str, Any]:
        """
        Upload a batch of data via REST API
        
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
            
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            
            if response.status_code in [200, 201, 202]:
                return {
                    'success': True,
                    'batch_size': len(records),
                    'status_code': response.status_code
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
            
    def send_s3_data_to_salesforce(self, gcs_path: str, data_extension_key: str,
                                  batch_size: int = None, file_format: str = 'csv') -> Dict[str, Any]:
        """
        Bulk uploads data from GCS files to Salesforce with batching
        
        Args:
            gcs_path: GCS path to data file (gs://bucket/object or just object if default_bucket set)
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            Dictionary with upload results
        """
        if not self.storage_client:
            raise SalesforceOperationError("GCP Cloud Storage not configured for data import operations")
            
        try:
            # Parse GCS path
            if gcs_path.startswith('gs://'):
                path_parts = gcs_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                if not self.default_bucket or not self.bucket_client:
                    raise SalesforceOperationError("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket_client
                blob_name = gcs_path.lstrip('/')
                
            # Read data from GCS
            blob = bucket.blob(blob_name)
            if not blob.exists():
                raise SalesforceOperationError(f"GCS object not found: gs://{bucket.name}/{blob_name}")
                
            blob_data = blob.download_as_bytes()
            
            if file_format.lower() == 'csv':
                df = pd.read_csv(io.BytesIO(blob_data))
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(io.BytesIO(blob_data))
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from gs://{bucket.name}/{blob_name}")
            
            # Upload data to Salesforce
            result = self.send_data_to_salesforce(df, data_extension_key, batch_size)
            result['gcs_source'] = f"gs://{bucket.name}/{blob_name}"
            result['file_format'] = file_format
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading GCS data to Salesforce: {e}")
            return {
                'success': False,
                'gcs_source': gcs_path,
                'data_extension_key': data_extension_key,
                'error': str(e),
                'rows_processed': 0,
                'rows_successful': 0,
                'rows_failed': 0
            }
            
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
            'has_credentials_object': self.credentials is not None
        }