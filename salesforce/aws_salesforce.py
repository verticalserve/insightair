"""
AWS Salesforce Operations
Salesforce Marketing Cloud operations with AWS S3 integration for data export and import
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
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Requests library not available: {e}")
    REQUESTS_AVAILABLE = False


class AWSSalesforceOperations(BaseSalesforceOperations):
    """
    Salesforce Marketing Cloud operations with AWS S3 integration
    Combines Salesforce functionality with AWS cloud storage capabilities for data pipelines
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AWS Salesforce operations
        
        Args:
            config: Configuration dictionary containing both Salesforce and AWS settings
                Salesforce settings: client_id, client_secret, account_id, etc.
                AWS settings:
                - aws_access_key_id: AWS access key (optional, uses boto3 default chain)
                - aws_secret_access_key: AWS secret key (optional)
                - aws_session_token: AWS session token (optional)
                - region_name: AWS region (default: us-east-1)
                - default_bucket: Default S3 bucket for operations
        """
        super().__init__(config)
        
        # AWS configuration
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.region_name = config.get('region_name', 'us-east-1')
        self.default_bucket = config.get('default_bucket')
        
        # Initialize AWS clients
        self.s3_client = None
        self.s3_resource = None
        
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
        Initialize AWS S3 clients
        """
        try:
            # Prepare session kwargs
            session_kwargs = {'region_name': self.region_name}
            
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs.update({
                    'aws_access_key_id': self.aws_access_key_id,
                    'aws_secret_access_key': self.aws_secret_access_key
                })
                
            if self.aws_session_token:
                session_kwargs['aws_session_token'] = self.aws_session_token
                
            # Create session and clients
            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client('s3')
            self.s3_resource = session.resource('s3')
            
            # Test default bucket access if specified
            if self.default_bucket:
                try:
                    self.s3_client.head_bucket(Bucket=self.default_bucket)
                    logger.info(f"Verified access to default S3 bucket: {self.default_bucket}")
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.warning(f"Default S3 bucket '{self.default_bucket}' not found")
                    else:
                        logger.warning(f"Could not access default bucket: {e}")
                        
            logger.info("AWS S3 clients initialized successfully")
            
        except (NoCredentialsError, ClientError) as e:
            logger.error(f"AWS credentials or permissions error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS clients: {e}")
            raise
            
    def export_data_extension(self, data_extension_key: str, s3_path: str, 
                            file_format: str = 'csv', filters: Dict[str, Any] = None,
                            batch_size: int = None) -> Dict[str, Any]:
        """
        Exports data extension content to S3 in CSV/Parquet format
        
        Args:
            data_extension_key: Data extension customer key
            s3_path: S3 path for export (s3://bucket/key or just key if default_bucket set)
            file_format: Export format ('csv' or 'parquet')
            filters: Optional filters to apply
            batch_size: Batch size for data retrieval
            
        Returns:
            Dictionary with export results
        """
        if not self.s3_client:
            raise SalesforceOperationError("AWS S3 not configured for export operations")
            
        try:
            start_time = datetime.now()
            
            # Parse S3 path
            if s3_path.startswith('s3://'):
                path_parts = s3_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                if not self.default_bucket:
                    raise SalesforceOperationError("No S3 bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_key = s3_path.lstrip('/')
                
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
                    's3_path': f"s3://{bucket_name}/{object_key}"
                }
                
            # Convert to DataFrame
            df = self.transform_dtypes(all_rows)
            
            # Export based on format
            if file_format.lower() == 'csv':
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=csv_data,
                    ContentType='text/csv'
                )
                
            elif file_format.lower() == 'parquet':
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=parquet_buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
                
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'data_extension_key': data_extension_key,
                's3_path': f"s3://{bucket_name}/{object_key}",
                'file_format': file_format,
                'rows_exported': len(all_rows),
                'columns_exported': len(df.columns) if not df.empty else 0,
                'export_duration_seconds': duration,
                'filters_applied': filters,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully exported {len(all_rows)} rows from {data_extension_key} "
                       f"to s3://{bucket_name}/{object_key} in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting data extension to S3: {e}")
            return {
                'success': False,
                'data_extension_key': data_extension_key,
                's3_path': s3_path,
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
        Build SOAP filter from filter dictionary
        
        Args:
            filters: Filter specifications
            
        Returns:
            SOAP filter object
        """
        try:
            if not filters:
                return None
                
            # Handle date range filters
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
                    
            # Handle simple field filters
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
                    
            return None
            
        except Exception as e:
            logger.warning(f"Error building SOAP filter: {e}")
            return None
            
    def send_data_to_salesforce(self, df: pd.DataFrame, data_extension_key: str,
                               batch_size: int = None) -> Dict[str, Any]:
        """
        Uploads DataFrame data to data extensions via REST API
        
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
            
            # Process data in batches
            for i in range(0, total_rows, batch_sz):
                batch_df = formatted_df.iloc[i:i + batch_sz]
                
                try:
                    batch_result = self._upload_batch_rest(batch_df, data_extension_key)
                    batch_results.append(batch_result)
                    
                    if batch_result.get('success', False):
                        rows_successful += len(batch_df)
                    else:
                        rows_failed += len(batch_df)
                        
                except Exception as e:
                    logger.error(f"Batch upload failed: {e}")
                    rows_failed += len(batch_df)
                    batch_results.append({
                        'success': False,
                        'error': str(e),
                        'batch_size': len(batch_df)
                    })
                    
                # Refresh token periodically
                if i > 0 and i % (batch_sz * 10) == 0:
                    self.get_auth_token()
                    
            result = {
                'success': rows_failed == 0,
                'data_extension_key': data_extension_key,
                'rows_processed': total_rows,
                'rows_successful': rows_successful,
                'rows_failed': rows_failed,
                'batch_count': len(batch_results),
                'batch_results': batch_results
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
            
            response = requests.post(url, json=payload, headers=headers)
            
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
            
    def send_s3_data_to_salesforce(self, s3_path: str, data_extension_key: str,
                                  batch_size: int = None, file_format: str = 'csv') -> Dict[str, Any]:
        """
        Bulk uploads data from S3 files to Salesforce with batching
        
        Args:
            s3_path: S3 path to data file (s3://bucket/key or just key if default_bucket set)
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            Dictionary with upload results
        """
        if not self.s3_client:
            raise SalesforceOperationError("AWS S3 not configured for data import operations")
            
        try:
            # Parse S3 path
            if s3_path.startswith('s3://'):
                path_parts = s3_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                if not self.default_bucket:
                    raise SalesforceOperationError("No S3 bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_key = s3_path.lstrip('/')
                
            # Read data from S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            
            if file_format.lower() == 'csv':
                df = pd.read_csv(io.BytesIO(response['Body'].read()))
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            else:
                raise SalesforceOperationError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from s3://{bucket_name}/{object_key}")
            
            # Upload data to Salesforce
            result = self.send_data_to_salesforce(df, data_extension_key, batch_size)
            result['s3_source'] = f"s3://{bucket_name}/{object_key}"
            result['file_format'] = file_format
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading S3 data to Salesforce: {e}")
            return {
                'success': False,
                's3_source': s3_path,
                'data_extension_key': data_extension_key,
                'error': str(e),
                'rows_processed': 0,
                'rows_successful': 0,
                'rows_failed': 0
            }
            
    def get_accounts_soap(self) -> List[Dict[str, Any]]:
        """
        Retrieves business unit hierarchy and account information
        
        Returns:
            List of account dictionaries with hierarchy information
        """
        try:
            if not self.soap_client:
                self._initialize_soap_client()
                
            # Prepare SOAP request
            retrieve_request = self.soap_client.factory.create('RetrieveRequest')
            retrieve_request.ObjectType = 'Account'
            
            properties = [
                'ID', 'Name', 'Description', 'AccountType', 'ParentID',
                'CustomerID', 'CreatedDate', 'ModifiedDate', 'Client'
            ]
            retrieve_request.Properties = properties
            
            response = self.soap_client.service.Retrieve(retrieve_request)
            
            accounts = []
            if response.OverallStatus == 'OK' and hasattr(response, 'Results'):
                for result in response.Results:
                    account_dict = {}
                    for prop in properties:
                        if hasattr(result, prop):
                            account_dict[prop] = getattr(result, prop)
                    accounts.append(account_dict)
                    
            logger.info(f"Retrieved {len(accounts)} business unit accounts")
            return accounts
            
        except Exception as e:
            logger.error(f"Error retrieving accounts via SOAP: {e}")
            raise SalesforceDataError(f"Failed to retrieve accounts: {e}")
            
    def check_async_status(self, request_id: str) -> Dict[str, Any]:
        """
        Monitors asynchronous upload job status
        
        Args:
            request_id: Async request ID
            
        Returns:
            Dictionary with job status information
        """
        try:
            headers = self._get_auth_headers()
            url = f"{self.rest_base_url.rstrip('/')}/data/v1/async/{request_id}/status"
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                status_data = response.json()
                return {
                    'success': True,
                    'request_id': request_id,
                    'status': status_data.get('status'),
                    'results': status_data
                }
            else:
                error_msg = f"Failed to check async status: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'request_id': request_id,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"Error checking async status: {e}")
            return {
                'success': False,
                'request_id': request_id,
                'error': str(e)
            }
            
    def get_async_results(self, request_id: str) -> Dict[str, Any]:
        """
        Retrieves error details from failed upload operations
        
        Args:
            request_id: Async request ID
            
        Returns:
            Dictionary with operation results and errors
        """
        try:
            headers = self._get_auth_headers()
            url = f"{self.rest_base_url.rstrip('/')}/data/v1/async/{request_id}/results"
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                results_data = response.json()
                return {
                    'success': True,
                    'request_id': request_id,
                    'results': results_data,
                    'error_count': len(results_data.get('errors', [])),
                    'errors': results_data.get('errors', [])
                }
            else:
                error_msg = f"Failed to get async results: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'request_id': request_id,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"Error getting async results: {e}")
            return {
                'success': False,
                'request_id': request_id,
                'error': str(e)
            }
            
    def get_aws_info(self) -> Dict[str, Any]:
        """
        Get AWS configuration information
        
        Returns:
            Dictionary with AWS info (without sensitive data)
        """
        return {
            'aws_configured': self.s3_client is not None,
            'default_bucket': self.default_bucket,
            'region_name': self.region_name,
            'has_access_key': self.aws_access_key_id is not None,
            'has_session_token': self.aws_session_token is not None
        }