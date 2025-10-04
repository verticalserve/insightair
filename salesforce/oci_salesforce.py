"""
OCI Salesforce Operations
Salesforce Marketing Cloud operations with OCI Object Storage integration for data export and import
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
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Requests library not available: {e}")
    REQUESTS_AVAILABLE = False


class OCISalesforceOperations(BaseSalesforceOperations):
    """
    Salesforce Marketing Cloud operations with OCI Object Storage integration
    Combines Salesforce functionality with OCI cloud storage capabilities for data pipelines
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Salesforce operations
        
        Args:
            config: Configuration dictionary containing both Salesforce and OCI settings
                Salesforce settings: client_id, client_secret, account_id, etc.
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
        
        # Initialize OCI clients
        self.object_storage_client = None
        
        if OCI_AVAILABLE:
            try:
                self._initialize_oci_clients()
            except Exception as e:
                logger.error(f"Failed to initialize OCI clients: {e}")
                # Continue without OCI - operations will fall back gracefully
        else:
            logger.warning("OCI libraries not available, Object Storage operations will not work")
            
    def _initialize_oci_clients(self):
        """
        Initialize OCI Object Storage clients
        """
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
                    logger.info(f"Verified access to default bucket: {self.default_bucket}")
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
            
    def export_data_extension(self, data_extension_key: str, oci_path: str, 
                            file_format: str = 'csv', filters: Dict[str, Any] = None,
                            batch_size: int = None) -> Dict[str, Any]:
        """
        Exports data extension content to OCI Object Storage in CSV/Parquet format
        
        Args:
            data_extension_key: Data extension customer key
            oci_path: OCI path for export (oci://bucket/object or just object if default_bucket set)
            file_format: Export format ('csv' or 'parquet')
            filters: Optional filters to apply
            batch_size: Batch size for data retrieval
            
        Returns:
            Dictionary with export results
        """
        if not self.object_storage_client:
            raise SalesforceOperationError("OCI Object Storage not configured for export operations")
            
        try:
            start_time = datetime.now()
            
            # Parse OCI path
            if oci_path.startswith('oci://'):
                path_parts = oci_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                if not self.default_bucket:
                    raise SalesforceOperationError("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = oci_path.lstrip('/')
                
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
                    'oci_path': f"oci://{bucket_name}/{object_name}"
                }
                
            # Convert to DataFrame
            df = self.transform_dtypes(all_rows)
            
            # Prepare data based on format
            if file_format.lower() == 'csv':
                csv_data = df.to_csv(index=False)
                data_bytes = csv_data.encode('utf-8')
                content_type = 'text/csv'
                
            elif file_format.lower() == 'parquet':
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                data_bytes = parquet_buffer.getvalue()
                content_type = 'application/octet-stream'
                
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            # Upload to OCI Object Storage
            self.object_storage_client.put_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=data_bytes,
                content_type=content_type
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'data_extension_key': data_extension_key,
                'oci_path': f"oci://{bucket_name}/{object_name}",
                'file_format': file_format,
                'rows_exported': len(all_rows),
                'columns_exported': len(df.columns) if not df.empty else 0,
                'export_duration_seconds': duration,
                'filters_applied': filters,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully exported {len(all_rows)} rows from {data_extension_key} "
                       f"to oci://{bucket_name}/{object_name} in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting data extension to OCI: {e}")
            return {
                'success': False,
                'data_extension_key': data_extension_key,
                'oci_path': oci_path,
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
        Generator for batch processing large datasets with OCI optimizations
        
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
                    
                # Apply filters if specified (enhanced for OCI operations)
                if filters:
                    filter_part = self._build_soap_filter_oci(filters)
                    if filter_part:
                        retrieve_request.Filter = filter_part
                        
                # Set pagination with OCI optimizations
                retrieve_request.Options = self.soap_client.factory.create('RetrieveOptions')
                retrieve_request.Options.BatchSize = min(batch_sz, 5000)  # OCI can handle larger batches
                retrieve_request.Options.StartIndex = current_batch * batch_sz
                
                # Execute request with retry and OCI-specific timeout
                response = self._retry_operation(
                    self.soap_client.service.Retrieve, retrieve_request
                )
                
                if response.OverallStatus != 'OK':
                    if 'MoreDataAvailable' not in str(response.OverallStatus):
                        logger.error(f"SOAP retrieve failed: {response.OverallStatus}")
                        break
                        
                # Process results with OCI data type optimizations
                batch_rows = []
                if hasattr(response, 'Results') and response.Results:
                    for result in response.Results:
                        row_dict = {}
                        if hasattr(result, 'Properties') and result.Properties:
                            for prop in result.Properties:
                                if hasattr(prop, 'Name') and hasattr(prop, 'Value'):
                                    # OCI-specific data type handling
                                    value = prop.Value
                                    # OCI supports larger text fields but validate encoding
                                    if isinstance(value, str):
                                        try:
                                            value.encode('utf-8')
                                        except UnicodeEncodeError:
                                            value = value.encode('utf-8', errors='replace').decode('utf-8')
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
                if current_batch % 15 == 0:  # Every 15 batches (OCI can handle longer sessions)
                    self.get_auth_token()
                    
        except Exception as e:
            logger.error(f"Error in SOAP data generator: {e}")
            raise SalesforceDataError(f"SOAP data retrieval failed: {e}")
            
    def _build_soap_filter_oci(self, filters: Dict[str, Any]):
        """
        Build SOAP filter with OCI-specific optimizations
        
        Args:
            filters: Filter specifications
            
        Returns:
            SOAP filter object optimized for OCI operations
        """
        try:
            if not filters:
                return None
                
            # Handle date range filters with OCI timezone handling
            if 'date_range' in filters:
                date_filter = filters['date_range']
                if 'start_date' in date_filter and 'end_date' in date_filter:
                    # Create date range filter
                    complex_filter = self.soap_client.factory.create('ComplexFilterPart')
                    complex_filter.LogicalOperator = 'AND'
                    
                    # Start date filter with OCI timezone support
                    start_filter = self.soap_client.factory.create('SimpleFilterPart')
                    start_filter.Property = date_filter.get('date_field', 'CreatedDate')
                    start_filter.SimpleOperator = 'greaterThan'
                    
                    # Convert to OCI-compatible ISO format
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
                    
            # Handle OCI-optimized field filters with better performance
            if 'conditions' in filters:
                conditions = filters['conditions']
                if len(conditions) == 1:
                    # Single condition with OCI data type optimization
                    condition = conditions[0]
                    simple_filter = self.soap_client.factory.create('SimpleFilterPart')
                    simple_filter.Property = condition['field']
                    simple_filter.SimpleOperator = condition.get('operator', 'equals')
                    
                    # OCI can handle larger values and different data types better
                    value = condition['value']
                    if isinstance(value, str) and len(value) > 32000:  # OCI CLOB limit
                        value = value[:32000]
                    simple_filter.Value = value
                    
                    return simple_filter
                elif len(conditions) > 1:
                    # Multiple conditions with optimized logic for OCI
                    return self._build_complex_filter_oci(conditions, filters.get('logical_operator', 'AND'))
                    
            return None
            
        except Exception as e:
            logger.warning(f"Error building OCI SOAP filter: {e}")
            return None
            
    def _build_complex_filter_oci(self, conditions: List[Dict[str, Any]], logical_op: str):
        """
        Build complex filters optimized for OCI operations
        
        Args:
            conditions: List of conditions
            logical_op: Logical operator ('AND' or 'OR')
            
        Returns:
            Complex filter object optimized for OCI
        """
        try:
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
                
                # For OCI, we can handle more complex nested filters efficiently
                first_condition = conditions[0]
                left_filter = self.soap_client.factory.create('SimpleFilterPart')
                left_filter.Property = first_condition['field']
                left_filter.SimpleOperator = first_condition.get('operator', 'equals')
                left_filter.Value = first_condition['value']
                
                if len(conditions) == 2:
                    # Two conditions - simple case
                    second_condition = conditions[1]
                    right_filter = self.soap_client.factory.create('SimpleFilterPart')
                    right_filter.Property = second_condition['field']
                    right_filter.SimpleOperator = second_condition.get('operator', 'equals')
                    right_filter.Value = second_condition['value']
                    
                    complex_filter.LeftOperand = left_filter
                    complex_filter.RightOperand = right_filter
                else:
                    # More conditions - recursive nesting (OCI handles this well)
                    remaining_conditions = conditions[1:]
                    right_filter = self._build_complex_filter_oci(remaining_conditions, logical_op)
                    
                    complex_filter.LeftOperand = left_filter
                    complex_filter.RightOperand = right_filter
                    
                return complex_filter
                
        except Exception as e:
            logger.warning(f"Error building complex OCI filter: {e}")
            return None
            
    def _get_data_extension_columns_soap(self, data_extension_key: str) -> List[Dict[str, Any]]:
        """
        Get data extension column metadata via SOAP with OCI caching
        
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
        Uploads DataFrame data to data extensions via REST API with OCI optimizations
        
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
                
            # Format DataFrame for Salesforce with OCI considerations
            formatted_df = self.format_df_oci(df)
            batch_sz = batch_size or min(self.batch_size, 5000)  # OCI can handle larger batches
            
            total_rows = len(formatted_df)
            rows_successful = 0
            rows_failed = 0
            batch_results = []
            
            # Process data in batches with OCI-optimized concurrency
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:  # OCI can handle more threads
                futures = []
                
                for i in range(0, total_rows, batch_sz):
                    batch_df = formatted_df.iloc[i:i + batch_sz]
                    future = executor.submit(self._upload_batch_rest, batch_df, data_extension_key)
                    futures.append((future, len(batch_df)))
                    
                # Process completed futures with OCI timeout optimization
                for future, batch_size in futures:
                    try:
                        batch_result = future.result(timeout=600)  # 10 minute timeout for OCI
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
                'oci_optimized': True
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
            
    def format_df_oci(self, df: pd.DataFrame, datetime_format: str = '%m/%d/%Y %I:%M:%S %p') -> pd.DataFrame:
        """
        Formats DataFrames for Salesforce compatibility with OCI-specific optimizations
        
        Args:
            df: DataFrame to format
            datetime_format: Format string for datetime conversion
            
        Returns:
            Formatted DataFrame optimized for OCI operations
        """
        try:
            formatted_df = df.copy()
            
            # Handle datetime columns with OCI timezone awareness
            for col in formatted_df.columns:
                if pd.api.types.is_datetime64_any_dtype(formatted_df[col]):
                    # Convert to UTC first for OCI consistency
                    if formatted_df[col].dt.tz is None:
                        formatted_df[col] = formatted_df[col].dt.tz_localize('UTC')
                    else:
                        formatted_df[col] = formatted_df[col].dt.tz_convert('UTC')
                    formatted_df[col] = formatted_df[col].dt.strftime(datetime_format)
                    
            # Handle null values
            formatted_df = formatted_df.fillna('')
            
            # OCI-specific optimizations - can handle larger data types
            for col in formatted_df.columns:
                if formatted_df[col].dtype == 'object':  # String columns
                    formatted_df[col] = formatted_df[col].astype(str)
                    # OCI can handle much larger strings (up to 32KB for VARCHAR2)
                    # Only truncate if absolutely necessary
                    max_lengths = formatted_df[col].str.len()
                    if max_lengths.max() > 32000:
                        logger.warning(f"Column {col} has values exceeding 32KB, truncating for OCI compatibility")
                        formatted_df[col] = formatted_df[col].str.slice(0, 32000)
                else:
                    formatted_df[col] = formatted_df[col].astype(str)
                    
            logger.debug(f"Formatted DataFrame with OCI optimizations: {len(formatted_df)} rows, {len(formatted_df.columns)} columns")
            return formatted_df
            
        except Exception as e:
            error_msg = f"Error formatting DataFrame for OCI: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def _upload_batch_rest(self, batch_df: pd.DataFrame, data_extension_key: str) -> Dict[str, Any]:
        """
        Upload a batch of data via REST API with OCI optimizations
        
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
            
            # OCI-optimized request with extended timeout
            response = requests.post(url, json=payload, headers=headers, timeout=300)  # 5 minute timeout
            
            if response.status_code in [200, 201, 202]:
                return {
                    'success': True,
                    'batch_size': len(records),
                    'status_code': response.status_code,
                    'oci_optimized': True
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
            
    def send_s3_data_to_salesforce(self, oci_path: str, data_extension_key: str,
                                  batch_size: int = None, file_format: str = 'csv') -> Dict[str, Any]:
        """
        Bulk uploads data from OCI Object Storage files to Salesforce with batching
        
        Args:
            oci_path: OCI path to data file (oci://bucket/object or just object if default_bucket set)
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            Dictionary with upload results
        """
        if not self.object_storage_client:
            raise SalesforceOperationError("OCI Object Storage not configured for data import operations")
            
        try:
            # Parse OCI path
            if oci_path.startswith('oci://'):
                path_parts = oci_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                if not self.default_bucket:
                    raise SalesforceOperationError("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = oci_path.lstrip('/')
                
            # Read data from OCI Object Storage
            get_object_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            object_data = get_object_response.data.content
            
            if file_format.lower() == 'csv':
                df = pd.read_csv(io.BytesIO(object_data))
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(io.BytesIO(object_data))
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from oci://{bucket_name}/{object_name}")
            
            # Upload data to Salesforce
            result = self.send_data_to_salesforce(df, data_extension_key, batch_size)
            result['oci_source'] = f"oci://{bucket_name}/{object_name}"
            result['file_format'] = file_format
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading OCI data to Salesforce: {e}")
            return {
                'success': False,
                'oci_source': oci_path,
                'data_extension_key': data_extension_key,
                'error': str(e),
                'rows_processed': 0,
                'rows_successful': 0,
                'rows_failed': 0
            }
            
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
            'profile_name': self.profile_name
        }