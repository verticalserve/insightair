"""
Google Ads Extensions Operations
Handles price extensions, feed operations, customer match, and campaign management
"""

import logging
import pandas as pd
import hashlib
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta

from .base_googleads import BaseGoogleAdsOperations, GoogleAdsError

logger = logging.getLogger(__name__)

try:
    from google.ads.googleads.errors import GoogleAdsException
    from google.api_core import protobuf_helpers
    GOOGLE_ADS_AVAILABLE = True
except ImportError:
    GOOGLE_ADS_AVAILABLE = False


class GoogleAdsExtensionsOperations(BaseGoogleAdsOperations):
    """
    Extended Google Ads operations for price extensions, feeds, and customer match
    """
    
    def _create_price_asset(self, price_data: Dict[str, Any], customer_id: str = None) -> str:
        """
        Creates price assets for extensions
        
        Args:
            price_data: Dictionary containing price asset data
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Price asset resource name
            
        Raises:
            GoogleAdsError: If price asset creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            asset_service = self.client.get_service("AssetService")
            asset_operation = self.client.get_type("AssetOperation")
            
            # Create price asset
            price_asset = asset_operation.create
            price_asset.name = price_data.get('name', 'Price Asset')
            price_asset.type_ = self.client.enums.AssetTypeEnum.PRICE
            
            # Set price asset details
            price_asset_info = price_asset.price_asset
            price_asset_info.type_ = getattr(
                self.client.enums.PriceExtensionTypeEnum, 
                price_data.get('type', 'SERVICES')
            )
            price_asset_info.price_qualifier = getattr(
                self.client.enums.PriceExtensionPriceQualifierEnum,
                price_data.get('price_qualifier', 'NONE')
            )
            price_asset_info.language_code = price_data.get('language_code', 'en')
            price_asset_info.tracking_url_template = price_data.get('tracking_url_template', '')
            
            # Add price offerings
            offerings = price_data.get('offerings', [])
            for offering_data in offerings:
                offering = self._create_price_offering(offering_data)
                price_asset_info.price_offerings.append(offering)
                
            # Execute mutation
            response = asset_service.mutate_assets(
                customer_id=customer_id,
                operations=[asset_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created price asset: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating price asset: {e}")
            raise GoogleAdsError(f"Price asset creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating price asset: {e}")
            raise GoogleAdsError(f"Price asset creation failed: {e}")
            
    def _create_price_offering(self, offering_data: Dict[str, Any]) -> Any:
        """
        Creates individual price offerings
        
        Args:
            offering_data: Dictionary containing price offering data
            
        Returns:
            PriceOffering object
        """
        price_offering = self.client.get_type("PriceOffering")
        
        price_offering.header = offering_data.get('header', '')
        price_offering.description = offering_data.get('description', '')
        price_offering.final_url = offering_data.get('final_url', '')
        price_offering.final_mobile_url = offering_data.get('final_mobile_url', '')
        
        # Set price
        price = price_offering.price
        price.amount_micros = int(offering_data.get('amount_micros', 0))
        price.currency_code = offering_data.get('currency_code', 'USD')
        
        # Set unit if provided
        if 'unit' in offering_data:
            price_offering.unit = getattr(
                self.client.enums.PriceExtensionPriceUnitEnum,
                offering_data['unit']
            )
            
        return price_offering
        
    def create_price_assets(self, price_assets_data: Union[List[Dict], pd.DataFrame, str],
                           customer_id: str = None) -> List[str]:
        """
        Bulk creates price extensions from data
        
        Args:
            price_assets_data: Price assets data (list, DataFrame, or cloud storage path)
            customer_id: Customer ID (uses default if None)
            
        Returns:
            List of created price asset resource names
            
        Raises:
            GoogleAdsError: If bulk creation fails
        """
        try:
            # Load data if path provided
            if isinstance(price_assets_data, str):
                df = self.read_from_cloud_storage(price_assets_data)
                price_assets_list = df.to_dict('records')
            elif isinstance(price_assets_data, pd.DataFrame):
                price_assets_list = price_assets_data.to_dict('records')
            else:
                price_assets_list = price_assets_data
                
            created_assets = []
            for price_data in price_assets_list:
                try:
                    asset_resource = self._create_price_asset(price_data, customer_id)
                    created_assets.append(asset_resource)
                except Exception as e:
                    logger.error(f"Failed to create price asset: {e}")
                    
            logger.info(f"Created {len(created_assets)} price assets out of {len(price_assets_list)}")
            return created_assets
            
        except Exception as e:
            logger.error(f"Error in bulk price asset creation: {e}")
            raise GoogleAdsError(f"Bulk price asset creation failed: {e}")
            
    def _associate_asset_to_campaign(self, campaign_resource: str, asset_resource: str,
                                   field_type: str = 'PRICE', customer_id: str = None) -> str:
        """
        Associates price assets to campaigns
        
        Args:
            campaign_resource: Campaign resource name
            asset_resource: Asset resource name
            field_type: Asset field type
            customer_id: Customer ID (uses default if None)
            
        Returns:
            CampaignAsset resource name
            
        Raises:
            GoogleAdsError: If association fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            campaign_asset_service = self.client.get_service("CampaignAssetService")
            campaign_asset_operation = self.client.get_type("CampaignAssetOperation")
            
            # Create campaign asset association
            campaign_asset = campaign_asset_operation.create
            campaign_asset.campaign = campaign_resource
            campaign_asset.asset = asset_resource
            campaign_asset.field_type = getattr(
                self.client.enums.AssetFieldTypeEnum,
                field_type
            )
            
            # Execute mutation
            response = campaign_asset_service.mutate_campaign_assets(
                customer_id=customer_id,
                operations=[campaign_asset_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Associated asset to campaign: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error associating asset to campaign: {e}")
            raise GoogleAdsError(f"Asset association failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error associating asset to campaign: {e}")
            raise GoogleAdsError(f"Asset association failed: {e}")
            
    def remove_price_assets_from_campaigns(self, campaign_ids: List[str],
                                         customer_id: str = None) -> Dict[str, Any]:
        """
        Removes existing price extensions from campaigns
        
        Args:
            campaign_ids: List of campaign IDs
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with removal results
            
        Raises:
            GoogleAdsError: If removal fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            # Query existing price assets for campaigns
            query = f"""
                SELECT 
                    campaign_asset.resource_name,
                    campaign_asset.campaign,
                    campaign_asset.asset,
                    campaign_asset.field_type
                FROM campaign_asset 
                WHERE campaign_asset.field_type = 'PRICE'
                AND campaign.id IN ({','.join(campaign_ids)})
            """
            
            existing_assets = self._query_data_all(query, customer_id)
            
            if not existing_assets:
                logger.info("No existing price assets found to remove")
                return {'removed_count': 0, 'assets_removed': []}
                
            # Remove existing price assets
            campaign_asset_service = self.client.get_service("CampaignAssetService")
            operations = []
            
            for asset_data in existing_assets:
                remove_operation = self.client.get_type("CampaignAssetOperation")
                remove_operation.remove = asset_data['campaign_asset']['resource_name']
                operations.append(remove_operation)
                
            # Execute removal
            response = campaign_asset_service.mutate_campaign_assets(
                customer_id=customer_id,
                operations=operations
            )
            
            removed_resources = [result.resource_name for result in response.results]
            
            result = {
                'removed_count': len(removed_resources),
                'assets_removed': removed_resources,
                'campaigns_processed': campaign_ids
            }
            
            logger.info(f"Removed {len(removed_resources)} price assets from campaigns")
            return result
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error removing price assets: {e}")
            raise GoogleAdsError(f"Price asset removal failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error removing price assets: {e}")
            raise GoogleAdsError(f"Price asset removal failed: {e}")
            
    def create_custom_feed(self, feed_name: str, attributes: List[Dict[str, Any]],
                          customer_id: str = None) -> str:
        """
        Creates custom feeds with attributes
        
        Args:
            feed_name: Name of the feed
            attributes: List of feed attribute dictionaries
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Feed resource name
            
        Raises:
            GoogleAdsError: If feed creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            feed_service = self.client.get_service("FeedService")
            feed_operation = self.client.get_type("FeedOperation")
            
            # Create feed
            feed = feed_operation.create
            feed.name = feed_name
            feed.origin = self.client.enums.FeedOriginEnum.USER
            
            # Add attributes
            for attr_data in attributes:
                attribute = feed.attributes.add()
                attribute.name = attr_data['name']
                attribute.type_ = getattr(
                    self.client.enums.FeedAttributeTypeEnum,
                    attr_data.get('type', 'STRING')
                )
                attribute.is_part_of_key = attr_data.get('is_part_of_key', False)
                
            # Execute mutation
            response = feed_service.mutate_feeds(
                customer_id=customer_id,
                operations=[feed_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created custom feed: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating feed: {e}")
            raise GoogleAdsError(f"Feed creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating feed: {e}")
            raise GoogleAdsError(f"Feed creation failed: {e}")
            
    def create_custom_feed_mapping(self, feed_resource: str, 
                                  placeholder_type: str, attribute_mappings: List[Dict[str, Any]],
                                  customer_id: str = None) -> str:
        """
        Maps feed attributes to placeholder fields
        
        Args:
            feed_resource: Feed resource name
            placeholder_type: Placeholder type (e.g., 'SITELINKS', 'CALLOUTS')
            attribute_mappings: List of attribute mapping dictionaries
            customer_id: Customer ID (uses default if None)
            
        Returns:
            FeedMapping resource name
            
        Raises:
            GoogleAdsError: If feed mapping creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            feed_mapping_service = self.client.get_service("FeedMappingService")
            feed_mapping_operation = self.client.get_type("FeedMappingOperation")
            
            # Create feed mapping
            feed_mapping = feed_mapping_operation.create
            feed_mapping.feed = feed_resource
            feed_mapping.placeholder_type = getattr(
                self.client.enums.PlaceholderTypeEnum,
                placeholder_type
            )
            
            # Add attribute field mappings
            for mapping_data in attribute_mappings:
                mapping = feed_mapping.attribute_field_mappings.add()
                mapping.feed_attribute_id = mapping_data['feed_attribute_id']
                mapping.field_id = mapping_data['field_id']
                
            # Execute mutation
            response = feed_mapping_service.mutate_feed_mappings(
                customer_id=customer_id,
                operations=[feed_mapping_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created feed mapping: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating feed mapping: {e}")
            raise GoogleAdsError(f"Feed mapping creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating feed mapping: {e}")
            raise GoogleAdsError(f"Feed mapping creation failed: {e}")
            
    def create_feed_item(self, feed_resource: str, attribute_values: Dict[str, Any],
                        customer_id: str = None) -> str:
        """
        Adds items to custom feeds
        
        Args:
            feed_resource: Feed resource name
            attribute_values: Dictionary of attribute values
            customer_id: Customer ID (uses default if None)
            
        Returns:
            FeedItem resource name
            
        Raises:
            GoogleAdsError: If feed item creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            feed_item_service = self.client.get_service("FeedItemService")
            feed_item_operation = self.client.get_type("FeedItemOperation")
            
            # Create feed item
            feed_item = feed_item_operation.create
            feed_item.feed = feed_resource
            
            # Add attribute values
            for attr_id, value in attribute_values.items():
                attribute_value = feed_item.attribute_values.add()
                attribute_value.feed_attribute_id = int(attr_id)
                attribute_value.string_value = str(value)
                
            # Execute mutation
            response = feed_item_service.mutate_feed_items(
                customer_id=customer_id,
                operations=[feed_item_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created feed item: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating feed item: {e}")
            raise GoogleAdsError(f"Feed item creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating feed item: {e}")
            raise GoogleAdsError(f"Feed item creation failed: {e}")
            
    def remove_feed(self, feed_resource: str, customer_id: str = None) -> bool:
        """
        Removes feeds
        
        Args:
            feed_resource: Feed resource name
            customer_id: Customer ID (uses default if None)
            
        Returns:
            True if removal successful
            
        Raises:
            GoogleAdsError: If feed removal fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            feed_service = self.client.get_service("FeedService")
            feed_operation = self.client.get_type("FeedOperation")
            
            # Remove feed
            feed_operation.remove = feed_resource
            
            # Execute mutation
            response = feed_service.mutate_feeds(
                customer_id=customer_id,
                operations=[feed_operation]
            )
            
            logger.info(f"Removed feed: {feed_resource}")
            return True
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error removing feed: {e}")
            raise GoogleAdsError(f"Feed removal failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error removing feed: {e}")
            raise GoogleAdsError(f"Feed removal failed: {e}")
            
    def remove_feed_items(self, feed_item_resources: List[str], customer_id: str = None) -> int:
        """
        Removes items from feeds
        
        Args:
            feed_item_resources: List of feed item resource names
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Number of items removed
            
        Raises:
            GoogleAdsError: If feed item removal fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            feed_item_service = self.client.get_service("FeedItemService")
            operations = []
            
            for feed_item_resource in feed_item_resources:
                feed_item_operation = self.client.get_type("FeedItemOperation")
                feed_item_operation.remove = feed_item_resource
                operations.append(feed_item_operation)
                
            # Execute mutations
            response = feed_item_service.mutate_feed_items(
                customer_id=customer_id,
                operations=operations
            )
            
            removed_count = len(response.results)
            logger.info(f"Removed {removed_count} feed items")
            
            return removed_count
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error removing feed items: {e}")
            raise GoogleAdsError(f"Feed item removal failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error removing feed items: {e}")
            raise GoogleAdsError(f"Feed item removal failed: {e}")
            
    def upload_custom_feed(self, feed_name: str, feed_data: Union[pd.DataFrame, str],
                          feed_attributes: List[Dict[str, Any]], customer_id: str = None) -> Dict[str, Any]:
        """
        Uploads complete custom feed from cloud storage or DataFrame
        
        Args:
            feed_name: Name of the feed
            feed_data: DataFrame with feed data or cloud storage path
            feed_attributes: List of feed attribute configurations
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with upload results
            
        Raises:
            GoogleAdsError: If upload fails
        """
        try:
            # Load data if path provided
            if isinstance(feed_data, str):
                df = self.read_from_cloud_storage(feed_data)
            else:
                df = feed_data
                
            # Create feed
            feed_resource = self.create_custom_feed(
                feed_name=feed_name,
                attributes=feed_attributes,
                customer_id=customer_id
            )
            
            # Create feed items
            created_items = []
            for _, row in df.iterrows():
                try:
                    # Map row data to attribute values
                    attribute_values = {}
                    for i, attr in enumerate(feed_attributes):
                        attr_name = attr['name']
                        if attr_name in row:
                            attribute_values[str(i + 1)] = row[attr_name]
                            
                    feed_item_resource = self.create_feed_item(
                        feed_resource=feed_resource,
                        attribute_values=attribute_values,
                        customer_id=customer_id
                    )
                    created_items.append(feed_item_resource)
                    
                except Exception as e:
                    logger.error(f"Failed to create feed item: {e}")
                    
            result = {
                'feed_resource': feed_resource,
                'items_created': len(created_items),
                'total_rows': len(df),
                'feed_items': created_items
            }
            
            logger.info(f"Uploaded custom feed: {feed_name}, "
                       f"created {len(created_items)} items out of {len(df)} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading custom feed: {e}")
            raise GoogleAdsError(f"Custom feed upload failed: {e}")