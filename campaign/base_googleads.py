"""
Base Google Ads Operations Class
Provides core Google Ads API operations for campaign management across cloud platforms
"""

import logging
import pandas as pd
import io
import json
from typing import Dict, List, Any, Optional, Generator, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
    from google.api_core import protobuf_helpers
    GOOGLE_ADS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Google Ads libraries not available: {e}")
    GOOGLE_ADS_AVAILABLE = False


class GoogleAdsError(Exception):
    """Custom exception for Google Ads operations"""
    pass


class BaseGoogleAdsOperations(ABC):
    """
    Base class for Google Ads API operations
    Provides common functionality for all cloud platform implementations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Google Ads operations
        
        Args:
            config: Configuration dictionary containing Google Ads settings
                - developer_token: Google Ads developer token
                - client_id: OAuth2 client ID
                - client_secret: OAuth2 client secret
                - refresh_token: OAuth2 refresh token
                - customer_id: Google Ads customer ID
                - login_customer_id: MCC account ID (optional)
                - use_proto_plus: Whether to use proto-plus (default: True)
        """
        self.config = config
        self.developer_token = config.get('developer_token')
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.refresh_token = config.get('refresh_token')
        self.customer_id = config.get('customer_id')
        self.login_customer_id = config.get('login_customer_id')
        self.use_proto_plus = config.get('use_proto_plus', True)
        
        # Google Ads client
        self.client = None
        
        if GOOGLE_ADS_AVAILABLE:
            try:
                self.set_client()
            except Exception as e:
                logger.error(f"Failed to initialize Google Ads client: {e}")
                raise GoogleAdsError(f"Google Ads client initialization failed: {e}")
        else:
            logger.warning("Google Ads libraries not available")
            
    def set_client(self) -> GoogleAdsClient:
        """
        Establishes connection to Google Ads API with credentials
        
        Returns:
            GoogleAdsClient instance
            
        Raises:
            GoogleAdsError: If client initialization fails
        """
        try:
            # Prepare credentials dictionary
            credentials = {
                "developer_token": self.developer_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "use_proto_plus": self.use_proto_plus
            }
            
            if self.login_customer_id:
                credentials["login_customer_id"] = self.login_customer_id
                
            # Initialize Google Ads client
            self.client = GoogleAdsClient.load_from_dict(credentials)
            
            logger.info(f"Google Ads client initialized successfully for customer: {self.customer_id}")
            return self.client
            
        except Exception as e:
            logger.error(f"Failed to set Google Ads client: {e}")
            raise GoogleAdsError(f"Client initialization failed: {e}")
            
    def get_accounts_hierarchy(self) -> List[Dict[str, Any]]:
        """
        Retrieves MCC account hierarchy
        Lists accessible customer accounts and their details
        
        Returns:
            List of customer account dictionaries
            
        Raises:
            GoogleAdsError: If hierarchy retrieval fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        try:
            customer_service = self.client.get_service("CustomerService")
            
            # Query for accessible customers
            accessible_customers = customer_service.list_accessible_customers()
            
            accounts = []
            for customer_resource in accessible_customers.resource_names:
                customer_id = customer_resource.split("/")[-1]
                
                try:
                    # Get customer details
                    customer = customer_service.get_customer(
                        resource_name=customer_resource
                    )
                    
                    account_info = {
                        'customer_id': customer_id,
                        'resource_name': customer_resource,
                        'descriptive_name': customer.descriptive_name,
                        'currency_code': customer.currency_code,
                        'time_zone': customer.time_zone,
                        'tracking_url_template': customer.tracking_url_template,
                        'auto_tagging_enabled': customer.auto_tagging_enabled,
                        'has_partners_badge': customer.has_partners_badge,
                        'manager': customer.manager,
                        'test_account': customer.test_account,
                        'call_reporting_setting': {
                            'call_reporting_enabled': customer.call_reporting_setting.call_reporting_enabled if customer.call_reporting_setting else None,
                            'call_conversion_reporting_enabled': customer.call_reporting_setting.call_conversion_reporting_enabled if customer.call_reporting_setting else None
                        }
                    }
                    
                    accounts.append(account_info)
                    
                except Exception as e:
                    logger.warning(f"Could not get details for customer {customer_id}: {e}")
                    accounts.append({
                        'customer_id': customer_id,
                        'resource_name': customer_resource,
                        'error': str(e)
                    })
                    
            logger.info(f"Retrieved {len(accounts)} accessible customer accounts")
            return accounts
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error getting accounts hierarchy: {e}")
            raise GoogleAdsError(f"Failed to get accounts hierarchy: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting accounts hierarchy: {e}")
            raise GoogleAdsError(f"Failed to get accounts hierarchy: {e}")
            
    def _query_data_generator(self, query: str, customer_id: str = None, 
                             page_size: int = 10000) -> Generator[Any, None, None]:
        """
        Streams query results in batches
        
        Args:
            query: GAQL query string
            customer_id: Customer ID to query (uses default if None)
            page_size: Number of results per page
            
        Yields:
            Query result rows
            
        Raises:
            GoogleAdsError: If query execution fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            ga_service = self.client.get_service("GoogleAdsService")
            
            # Execute search
            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            search_request.page_size = page_size
            
            results = ga_service.search(request=search_request)
            
            for batch in results:
                for row in batch.results:
                    yield row
                    
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error in query: {e}")
            raise GoogleAdsError(f"Query execution failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in query: {e}")
            raise GoogleAdsError(f"Query execution failed: {e}")
            
    def _query_data_all(self, query: str, customer_id: str = None,
                       return_format: str = 'dict') -> Union[List[Dict], pd.DataFrame]:
        """
        Executes GAQL queries and returns complete datasets
        
        Args:
            query: GAQL query string
            customer_id: Customer ID to query (uses default if None)
            return_format: Return format ('dict' or 'dataframe')
            
        Returns:
            Query results as list of dictionaries or DataFrame
            
        Raises:
            GoogleAdsError: If query execution fails
        """
        try:
            results = []
            
            for row in self._query_data_generator(query, customer_id):
                # Convert protobuf message to dictionary
                row_dict = protobuf_helpers.from_any_pb(row)
                results.append(row_dict)
                
            logger.info(f"Query returned {len(results)} rows")
            
            if return_format == 'dataframe':
                return pd.DataFrame(results)
            else:
                return results
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise GoogleAdsError(f"Query execution failed: {e}")
            
    @abstractmethod
    def export_to_cloud_storage(self, data: Union[pd.DataFrame, List[Dict]], 
                               storage_path: str, file_format: str = 'csv') -> bool:
        """
        Exports Google Ads data to cloud storage
        
        Args:
            data: Data to export (DataFrame or list of dictionaries)
            storage_path: Cloud storage path
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            True if export successful, False otherwise
        """
        pass
        
    @abstractmethod
    def read_from_cloud_storage(self, storage_path: str, 
                               file_format: str = 'csv') -> pd.DataFrame:
        """
        Read data from cloud storage
        
        Args:
            storage_path: Cloud storage path
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            DataFrame with data
        """
        pass
        
    def _create_asset_set(self, asset_set_name: str, asset_set_type: str,
                         customer_id: str = None) -> str:
        """
        Creates dynamic custom asset sets
        
        Args:
            asset_set_name: Name of the asset set
            asset_set_type: Type of asset set
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Asset set resource name
            
        Raises:
            GoogleAdsError: If asset set creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            asset_set_service = self.client.get_service("AssetSetService")
            asset_set_operation = self.client.get_type("AssetSetOperation")
            
            # Create asset set
            asset_set = asset_set_operation.create
            asset_set.name = asset_set_name
            asset_set.type_ = getattr(self.client.enums.AssetSetTypeEnum, asset_set_type)
            
            # Execute mutation
            response = asset_set_service.mutate_asset_sets(
                customer_id=customer_id,
                operations=[asset_set_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created asset set: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating asset set: {e}")
            raise GoogleAdsError(f"Asset set creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating asset set: {e}")
            raise GoogleAdsError(f"Asset set creation failed: {e}")
            
    def _mutate_dynamic_custom_assets(self, assets_data: List[Dict[str, Any]],
                                     customer_id: str = None) -> List[str]:
        """
        Creates/updates dynamic custom assets
        
        Args:
            assets_data: List of asset data dictionaries
            customer_id: Customer ID (uses default if None)
            
        Returns:
            List of asset resource names
            
        Raises:
            GoogleAdsError: If asset mutation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            asset_service = self.client.get_service("AssetService")
            operations = []
            
            for asset_data in assets_data:
                asset_operation = self.client.get_type("AssetOperation")
                asset = asset_operation.create
                
                # Set asset properties based on asset_data
                asset.name = asset_data.get('name', '')
                
                # Handle different asset types
                asset_type = asset_data.get('type', 'TEXT')
                if asset_type == 'TEXT':
                    asset.text_asset.text = asset_data.get('text', '')
                elif asset_type == 'IMAGE':
                    asset.image_asset.data = asset_data.get('data', b'')
                    asset.image_asset.mime_type = asset_data.get('mime_type', 'IMAGE_JPEG')
                elif asset_type == 'MEDIA_BUNDLE':
                    asset.media_bundle_asset.data = asset_data.get('data', b'')
                    
                operations.append(asset_operation)
                
            # Execute mutations
            response = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=operations
            )
            
            resource_names = [result.resource_name for result in response.results]
            logger.info(f"Created/updated {len(resource_names)} assets")
            
            return resource_names
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error mutating assets: {e}")
            raise GoogleAdsError(f"Asset mutation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error mutating assets: {e}")
            raise GoogleAdsError(f"Asset mutation failed: {e}")
            
    def upload_custom_asset_set(self, asset_set_name: str, assets_data: Union[pd.DataFrame, str],
                               customer_id: str = None) -> Dict[str, Any]:
        """
        Uploads custom asset sets from cloud storage or DataFrame
        
        Args:
            asset_set_name: Name of the asset set
            assets_data: DataFrame with asset data or cloud storage path
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with upload results
            
        Raises:
            GoogleAdsError: If upload fails
        """
        try:
            # Load data if path provided
            if isinstance(assets_data, str):
                assets_df = self.read_from_cloud_storage(assets_data)
            else:
                assets_df = assets_data
                
            # Create asset set
            asset_set_resource = self._create_asset_set(
                asset_set_name=asset_set_name,
                asset_set_type='DYNAMIC_CUSTOM',
                customer_id=customer_id
            )
            
            # Prepare assets data
            assets_list = []
            for _, row in assets_df.iterrows():
                asset_data = {
                    'name': row.get('name', ''),
                    'type': row.get('type', 'TEXT'),
                    'text': row.get('text', ''),
                    'data': row.get('data', b''),
                    'mime_type': row.get('mime_type', 'IMAGE_JPEG')
                }
                assets_list.append(asset_data)
                
            # Create assets
            asset_resources = self._mutate_dynamic_custom_assets(
                assets_data=assets_list,
                customer_id=customer_id
            )
            
            # Link assets to asset set
            linked_assets = []
            for asset_resource in asset_resources:
                try:
                    self._mutate_assetset_asset(
                        asset_set_resource=asset_set_resource,
                        asset_resource=asset_resource,
                        operation='CREATE',
                        customer_id=customer_id
                    )
                    linked_assets.append(asset_resource)
                except Exception as e:
                    logger.warning(f"Failed to link asset {asset_resource}: {e}")
                    
            result = {
                'asset_set_resource': asset_set_resource,
                'assets_created': len(asset_resources),
                'assets_linked': len(linked_assets),
                'asset_resources': asset_resources,
                'linked_assets': linked_assets
            }
            
            logger.info(f"Uploaded custom asset set: {asset_set_name}, "
                       f"created {len(asset_resources)} assets, "
                       f"linked {len(linked_assets)} assets")
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading custom asset set: {e}")
            raise GoogleAdsError(f"Asset set upload failed: {e}")
            
    def _mutate_assetset_asset(self, asset_set_resource: str, asset_resource: str,
                              operation: str = 'CREATE', customer_id: str = None) -> str:
        """
        Links/unlinks assets to asset sets
        
        Args:
            asset_set_resource: Asset set resource name
            asset_resource: Asset resource name
            operation: Operation type ('CREATE' or 'REMOVE')
            customer_id: Customer ID (uses default if None)
            
        Returns:
            AssetSetAsset resource name
            
        Raises:
            GoogleAdsError: If operation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            asset_set_asset_service = self.client.get_service("AssetSetAssetService")
            asset_set_asset_operation = self.client.get_type("AssetSetAssetOperation")
            
            if operation == 'CREATE':
                asset_set_asset = asset_set_asset_operation.create
                asset_set_asset.asset_set = asset_set_resource
                asset_set_asset.asset = asset_resource
            elif operation == 'REMOVE':
                # For remove, we need the AssetSetAsset resource name
                # This would typically be retrieved from a previous query
                asset_set_asset_operation.remove = f"{asset_set_resource}/assets/{asset_resource.split('/')[-1]}"
            else:
                raise GoogleAdsError(f"Invalid operation: {operation}")
                
            # Execute mutation
            response = asset_set_asset_service.mutate_asset_set_assets(
                customer_id=customer_id,
                operations=[asset_set_asset_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Asset set asset operation '{operation}' completed: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error in asset set asset operation: {e}")
            raise GoogleAdsError(f"Asset set asset operation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in asset set asset operation: {e}")
            raise GoogleAdsError(f"Asset set asset operation failed: {e}")
            
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information (without sensitive data)
        
        Returns:
            Dictionary with connection info
        """
        safe_config = self.config.copy()
        # Remove sensitive information
        sensitive_keys = ['developer_token', 'client_secret', 'refresh_token']
        for key in sensitive_keys:
            if key in safe_config:
                safe_config[key] = "***"
        return safe_config