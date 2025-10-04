"""
Base Salesforce Operations Class
Provides core Salesforce Marketing Cloud operations for authentication, data extensions, and data management
"""

import logging
import json
import time
import pandas as pd
import io
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple, Generator
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom

logger = logging.getLogger(__name__)

try:
    import suds
    from suds.client import Client
    from suds.wsse import Security, UsernameToken
    SUDS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Suds SOAP library not available: {e}")
    SUDS_AVAILABLE = False


class SalesforceAuthenticationError(Exception):
    """Exception raised for Salesforce authentication errors"""
    pass


class SalesforceDataError(Exception):
    """Exception raised for Salesforce data operation errors"""
    pass


class SalesforceOperationError(Exception):
    """Exception raised for general Salesforce operation errors"""
    pass


class MultipleSchemaUseInTopicError(Exception):
    """Exception raised when multiple schemas are detected in a topic"""
    pass


class BaseSalesforceOperations(ABC):
    """
    Base class for Salesforce Marketing Cloud operations across different cloud platforms
    Provides common functionality for authentication, data extensions, and data management
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base Salesforce operations
        
        Args:
            config: Configuration dictionary containing Salesforce and cloud settings
                Salesforce settings:
                - client_id: Salesforce Marketing Cloud Client ID
                - client_secret: Salesforce Marketing Cloud Client Secret
                - account_id: Salesforce Marketing Cloud Account ID (MID)
                - auth_base_url: Authentication base URL (default: https://YOUR_SUBDOMAIN.auth.marketingcloudapis.com/)
                - rest_base_url: REST API base URL (default: https://YOUR_SUBDOMAIN.rest.marketingcloudapis.com/)
                - soap_base_url: SOAP API base URL (default: https://YOUR_SUBDOMAIN.soap.marketingcloudapis.com/)
                - api_version: API version (default: v2)
                - batch_size: Default batch size for data operations (default: 2500)
                - max_retries: Maximum retry attempts (default: 3)
                - retry_delay: Delay between retries in seconds (default: 5)
        """
        self.config = config
        
        # Salesforce configuration
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.account_id = config.get('account_id')
        self.auth_base_url = config.get('auth_base_url', 'https://YOUR_SUBDOMAIN.auth.marketingcloudapis.com/')
        self.rest_base_url = config.get('rest_base_url', 'https://YOUR_SUBDOMAIN.rest.marketingcloudapis.com/')
        self.soap_base_url = config.get('soap_base_url', 'https://YOUR_SUBDOMAIN.soap.marketingcloudapis.com/')
        self.api_version = config.get('api_version', 'v2')
        
        # Operation settings
        self.batch_size = config.get('batch_size', 2500)
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 5)
        
        # Authentication state
        self.access_token = None
        self.token_expires_at = None
        self.soap_client = None
        
        # Validate configuration
        if not all([self.client_id, self.client_secret, self.account_id]):
            raise SalesforceOperationError("Salesforce client_id, client_secret, and account_id are required")
            
    def get_auth_token(self, api_version: str = None) -> str:
        """
        Authenticates with Salesforce Marketing Cloud and retrieves access tokens
        Supports multiple API versions (v1, v2) and client credential flows
        
        Args:
            api_version: API version to use (v1 or v2)
            
        Returns:
            Access token string
            
        Raises:
            SalesforceAuthenticationError: If authentication fails
        """
        try:
            api_ver = api_version or self.api_version
            
            # Check if current token is still valid
            if (self.access_token and self.token_expires_at and 
                datetime.now() < self.token_expires_at - timedelta(minutes=5)):
                return self.access_token
                
            # Prepare authentication request
            auth_url = f"{self.auth_base_url.rstrip('/')}/{api_ver}/token"
            
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'account_id': self.account_id
            }
            
            headers = {
                'Content-Type': 'application/json'
            }
            
            # Make authentication request
            response = requests.post(auth_url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Authentication failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise SalesforceAuthenticationError(error_msg)
                
            auth_data = response.json()
            
            # Extract token information
            self.access_token = auth_data.get('access_token')
            expires_in = auth_data.get('expires_in', 3600)  # Default 1 hour
            
            if not self.access_token:
                raise SalesforceAuthenticationError("No access token in authentication response")
                
            # Calculate expiration time
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            logger.info(f"Salesforce authentication successful, token expires at {self.token_expires_at}")
            return self.access_token
            
        except requests.RequestException as e:
            error_msg = f"Authentication request failed: {e}"
            logger.error(error_msg)
            raise SalesforceAuthenticationError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected authentication error: {e}"
            logger.error(error_msg)
            raise SalesforceAuthenticationError(error_msg)
            
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for API requests
        
        Returns:
            Dictionary with authorization headers
        """
        token = self.get_auth_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
    def _initialize_soap_client(self):
        """
        Initialize SOAP client for Marketing Cloud API
        
        Raises:
            SalesforceOperationError: If SOAP client initialization fails
        """
        if not SUDS_AVAILABLE:
            raise SalesforceOperationError("Suds SOAP library not available")
            
        try:
            # SOAP endpoint URL
            soap_url = f"{self.soap_base_url.rstrip('/')}/Service.asmx?wsdl"
            
            # Create SOAP client
            self.soap_client = Client(soap_url)
            
            # Set up authentication
            security = Security()
            token = UsernameToken('*', '*')  # Placeholder, actual auth via token
            security.tokens.append(token)
            self.soap_client.set_options(wsse=security)
            
            logger.info("SOAP client initialized successfully")
            
        except Exception as e:
            error_msg = f"Failed to initialize SOAP client: {e}"
            logger.error(error_msg)
            raise SalesforceOperationError(error_msg)
            
    def get_data_extensions_rest(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Lists data extensions using REST API with name filtering
        
        Args:
            name_filter: Optional name filter for data extensions
            
        Returns:
            List of data extension dictionaries
            
        Raises:
            SalesforceDataError: If operation fails
        """
        try:
            headers = self._get_auth_headers()
            url = f"{self.rest_base_url.rstrip('/')}/data/v1/customobjectdata/key/DataExtension/rowset"
            
            params = {}
            if name_filter:
                params['$filter'] = f"Name eq '{name_filter}'"
                
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                error_msg = f"Failed to get data extensions: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise SalesforceDataError(error_msg)
                
            data = response.json()
            extensions = data.get('items', [])
            
            logger.info(f"Retrieved {len(extensions)} data extensions")
            return extensions
            
        except requests.RequestException as e:
            error_msg = f"REST API request failed: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error getting data extensions: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def get_data_extensions_soap(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves data extensions using SOAP API with comprehensive metadata
        
        Args:
            name_filter: Optional name filter for data extensions
            
        Returns:
            List of data extension dictionaries with metadata
            
        Raises:
            SalesforceDataError: If operation fails
        """
        try:
            if not self.soap_client:
                self._initialize_soap_client()
                
            # Prepare SOAP request
            retrieve_request = self.soap_client.factory.create('RetrieveRequest')
            retrieve_request.ObjectType = 'DataExtension'
            
            # Set properties to retrieve
            properties = [
                'ObjectID', 'CustomerKey', 'Name', 'Description', 
                'CategoryID', 'Status', 'CreatedDate', 'ModifiedDate',
                'IsSendable', 'IsTestable', 'SendableDataExtensionField',
                'SendableSubscriberField', 'Template', 'DataRetentionPeriod'
            ]
            retrieve_request.Properties = properties
            
            # Add filter if specified
            if name_filter:
                simple_filter = self.soap_client.factory.create('SimpleFilterPart')
                simple_filter.Property = 'Name'
                simple_filter.SimpleOperator = 'equals'
                simple_filter.Value = name_filter
                retrieve_request.Filter = simple_filter
                
            # Execute SOAP request
            response = self.soap_client.service.Retrieve(retrieve_request)
            
            if response.OverallStatus != 'OK':
                error_msg = f"SOAP retrieve failed: {response.OverallStatus}"
                logger.error(error_msg)
                raise SalesforceDataError(error_msg)
                
            # Convert results to dictionaries
            extensions = []
            if hasattr(response, 'Results') and response.Results:
                for result in response.Results:
                    extension_dict = {}
                    for prop in properties:
                        if hasattr(result, prop):
                            extension_dict[prop] = getattr(result, prop)
                    extensions.append(extension_dict)
                    
            logger.info(f"Retrieved {len(extensions)} data extensions via SOAP")
            return extensions
            
        except Exception as e:
            error_msg = f"SOAP API error getting data extensions: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def get_data_extension_columns_rest(self, data_extension_key: str) -> List[Dict[str, Any]]:
        """
        Gets column metadata including data types, constraints, and ordinal positions
        
        Args:
            data_extension_key: Data extension customer key
            
        Returns:
            List of column metadata dictionaries
            
        Raises:
            SalesforceDataError: If operation fails
        """
        try:
            headers = self._get_auth_headers()
            url = f"{self.rest_base_url.rstrip('/')}/data/v1/customobjectdata/key/{data_extension_key}/fields"
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Failed to get data extension columns: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise SalesforceDataError(error_msg)
                
            data = response.json()
            columns = data.get('items', [])
            
            # Enhance column information
            for i, column in enumerate(columns):
                column['ordinal_position'] = i + 1
                column['data_extension_key'] = data_extension_key
                
            logger.info(f"Retrieved {len(columns)} columns for data extension {data_extension_key}")
            return columns
            
        except requests.RequestException as e:
            error_msg = f"REST API request failed: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error getting data extension columns: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def format_df(self, df: pd.DataFrame, datetime_format: str = '%m/%d/%Y %I:%M:%S %p') -> pd.DataFrame:
        """
        Formats DataFrames for Salesforce compatibility (datetime conversion, null handling)
        
        Args:
            df: DataFrame to format
            datetime_format: Format string for datetime conversion
            
        Returns:
            Formatted DataFrame
        """
        try:
            formatted_df = df.copy()
            
            # Handle datetime columns
            for col in formatted_df.columns:
                if pd.api.types.is_datetime64_any_dtype(formatted_df[col]):
                    formatted_df[col] = formatted_df[col].dt.strftime(datetime_format)
                    
            # Handle null values
            formatted_df = formatted_df.fillna('')
            
            # Ensure all values are strings for Salesforce compatibility
            for col in formatted_df.columns:
                formatted_df[col] = formatted_df[col].astype(str)
                
            logger.debug(f"Formatted DataFrame with {len(formatted_df)} rows and {len(formatted_df.columns)} columns")
            return formatted_df
            
        except Exception as e:
            error_msg = f"Error formatting DataFrame: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def transform_dtypes(self, data: List[Dict[str, Any]], force_text: bool = False) -> pd.DataFrame:
        """
        Converts Salesforce data types to pandas dtypes
        
        Args:
            data: List of data dictionaries
            force_text: Force all columns to text type
            
        Returns:
            DataFrame with appropriate data types
        """
        try:
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            
            if force_text:
                # Convert all columns to string
                for col in df.columns:
                    df[col] = df[col].astype(str)
            else:
                # Attempt to infer appropriate data types
                for col in df.columns:
                    # Try to convert to numeric first
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
                        
                    # Try to convert to datetime
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col], errors='ignore')
                        except:
                            pass
                            
            logger.debug(f"Transformed data types for {len(df)} rows")
            return df
            
        except Exception as e:
            error_msg = f"Error transforming data types: {e}"
            logger.error(error_msg)
            raise SalesforceDataError(error_msg)
            
    def parse_sf_message(self, message: str) -> Dict[str, Any]:
        """
        Parses Salesforce error messages for debugging
        
        Args:
            message: Salesforce error message
            
        Returns:
            Dictionary with parsed error information
        """
        try:
            parsed = {
                'original_message': message,
                'error_code': None,
                'error_type': None,
                'field_name': None,
                'description': message
            }
            
            # Try to extract error code
            import re
            code_match = re.search(r'Error Code:\s*(\w+)', message)
            if code_match:
                parsed['error_code'] = code_match.group(1)
                
            # Try to extract field name
            field_match = re.search(r'Field:\s*(\w+)', message)
            if field_match:
                parsed['field_name'] = field_match.group(1)
                
            # Determine error type
            if 'DUPLICATE' in message.upper():
                parsed['error_type'] = 'DUPLICATE_VALUE'
            elif 'REQUIRED' in message.upper():
                parsed['error_type'] = 'REQUIRED_FIELD'
            elif 'INVALID' in message.upper():
                parsed['error_type'] = 'INVALID_VALUE'
            else:
                parsed['error_type'] = 'UNKNOWN'
                
            return parsed
            
        except Exception as e:
            logger.warning(f"Error parsing Salesforce message: {e}")
            return {
                'original_message': message,
                'error_code': None,
                'error_type': 'PARSE_ERROR',
                'field_name': None,
                'description': message
            }
            
    def _retry_operation(self, operation, *args, **kwargs):
        """
        Retry mechanism for operations with exponential backoff
        
        Args:
            operation: Function to retry
            *args: Arguments for the operation
            **kwargs: Keyword arguments for the operation
            
        Returns:
            Operation result
            
        Raises:
            Exception: Last exception if all retries fail
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Operation failed (attempt {attempt + 1}/{self.max_retries + 1}), "
                                 f"retrying in {delay} seconds: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"Operation failed after {self.max_retries + 1} attempts: {e}")
                    
        raise last_exception
        
    @abstractmethod
    def export_data_extension(self, data_extension_key: str, cloud_path: str, 
                            file_format: str = 'csv', filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Exports data extension content to cloud storage in CSV/Parquet format
        
        Args:
            data_extension_key: Data extension customer key
            cloud_path: Cloud storage path for export
            file_format: Export format ('csv' or 'parquet')
            filters: Optional filters to apply
            
        Returns:
            Dictionary with export results
        """
        pass
        
    @abstractmethod
    def send_s3_data_to_salesforce(self, cloud_path: str, data_extension_key: str,
                                  batch_size: int = None) -> Dict[str, Any]:
        """
        Bulk uploads data from cloud storage files to Salesforce with batching
        
        Args:
            cloud_path: Cloud storage path to data file
            data_extension_key: Target data extension customer key
            batch_size: Batch size for upload
            
        Returns:
            Dictionary with upload results
        """
        pass
        
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information
        
        Returns:
            Dictionary with connection info (without sensitive data)
        """
        return {
            'auth_base_url': self.auth_base_url,
            'rest_base_url': self.rest_base_url,
            'soap_base_url': self.soap_base_url,
            'api_version': self.api_version,
            'account_id': self.account_id,
            'batch_size': self.batch_size,
            'max_retries': self.max_retries,
            'token_valid': (self.access_token is not None and 
                          self.token_expires_at is not None and 
                          datetime.now() < self.token_expires_at),
            'soap_client_initialized': self.soap_client is not None
        }