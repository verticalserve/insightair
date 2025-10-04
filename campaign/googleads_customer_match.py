"""
Google Ads Customer Match Operations
Handles customer match audience management and offline user data jobs
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


class GoogleAdsCustomerMatchOperations(BaseGoogleAdsOperations):
    """
    Google Ads Customer Match operations for audience management
    """
    
    def _hash_email(self, email: str) -> str:
        """
        Hash email address for customer match
        
        Args:
            email: Email address to hash
            
        Returns:
            SHA256 hashed email
        """
        # Normalize email (lowercase, strip whitespace)
        normalized_email = email.lower().strip()
        
        # Hash with SHA256
        return hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
        
    def _hash_phone(self, phone: str) -> str:
        """
        Hash phone number for customer match
        
        Args:
            phone: Phone number to hash
            
        Returns:
            SHA256 hashed phone number
        """
        # Remove all non-digit characters and add country code if missing
        phone_digits = ''.join(filter(str.isdigit, phone))
        
        # Assume US if no country code and 10 digits
        if len(phone_digits) == 10:
            phone_digits = '1' + phone_digits
            
        # Hash with SHA256
        return hashlib.sha256(phone_digits.encode('utf-8')).hexdigest()
        
    def _customer_match_create_list(self, list_name: str, list_description: str = "",
                                   membership_lifespan: int = 30, customer_id: str = None) -> str:
        """
        Creates CRM-based user lists for customer match
        
        Args:
            list_name: Name of the user list
            list_description: Description of the user list
            membership_lifespan: Membership lifespan in days
            customer_id: Customer ID (uses default if None)
            
        Returns:
            User list resource name
            
        Raises:
            GoogleAdsError: If user list creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            user_list_service = self.client.get_service("UserListService")
            user_list_operation = self.client.get_type("UserListOperation")
            
            # Create user list
            user_list = user_list_operation.create
            user_list.name = list_name
            user_list.description = list_description
            user_list.membership_status = self.client.enums.UserListMembershipStatusEnum.OPEN
            user_list.membership_life_span = membership_lifespan
            
            # Set as CRM-based user list
            user_list.crm_based_user_list.upload_key_type = self.client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
            user_list.crm_based_user_list.data_source_type = self.client.enums.UserListCrmDataSourceTypeEnum.FIRST_PARTY
            
            # Execute mutation
            response = user_list_service.mutate_user_lists(
                customer_id=customer_id,
                operations=[user_list_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Created customer match user list: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating user list: {e}")
            raise GoogleAdsError(f"User list creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating user list: {e}")
            raise GoogleAdsError(f"User list creation failed: {e}")
            
    def _offline_job_create(self, user_list_resource: str, job_type: str = "CREATE",
                           customer_id: str = None) -> str:
        """
        Creates offline user data jobs
        
        Args:
            user_list_resource: User list resource name
            job_type: Job type ("CREATE", "ADD", "REMOVE")
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Offline user data job resource name
            
        Raises:
            GoogleAdsError: If job creation fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            offline_user_data_job_service = self.client.get_service("OfflineUserDataJobService")
            
            # Create offline user data job
            offline_user_data_job = self.client.get_type("OfflineUserDataJob")
            offline_user_data_job.type_ = getattr(
                self.client.enums.OfflineUserDataJobTypeEnum,
                "CUSTOMER_MATCH_USER_LIST"
            )
            
            # Set job configuration
            job_info = offline_user_data_job.customer_match_user_list_metadata
            job_info.user_list = user_list_resource
            
            # Execute creation
            response = offline_user_data_job_service.create_offline_user_data_job(
                customer_id=customer_id,
                job=offline_user_data_job
            )
            
            resource_name = response.resource_name
            logger.info(f"Created offline user data job: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error creating offline job: {e}")
            raise GoogleAdsError(f"Offline job creation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating offline job: {e}")
            raise GoogleAdsError(f"Offline job creation failed: {e}")
            
    def _offline_job_add_operations(self, job_resource: str, user_data: List[Dict[str, Any]],
                                   customer_id: str = None) -> bool:
        """
        Adds operations to offline jobs
        
        Args:
            job_resource: Offline user data job resource name
            user_data: List of user data dictionaries
            customer_id: Customer ID (uses default if None)
            
        Returns:
            True if operations added successfully
            
        Raises:
            GoogleAdsError: If adding operations fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            offline_user_data_job_service = self.client.get_service("OfflineUserDataJobService")
            operations = []
            
            for user_info in user_data:
                operation = self.client.get_type("OfflineUserDataJobOperation")
                user_data_obj = operation.create
                
                # Add hashed email if provided
                if 'email' in user_info:
                    user_identifier = user_data_obj.user_identifiers.add()
                    user_identifier.hashed_email = self._hash_email(user_info['email'])
                    
                # Add hashed phone if provided
                if 'phone' in user_info:
                    user_identifier = user_data_obj.user_identifiers.add()
                    user_identifier.hashed_phone_number = self._hash_phone(user_info['phone'])
                    
                # Add first name if provided
                if 'first_name' in user_info:
                    address_info = user_data_obj.user_identifiers.add().address_info
                    address_info.hashed_first_name = hashlib.sha256(
                        user_info['first_name'].lower().strip().encode('utf-8')
                    ).hexdigest()
                    
                # Add last name if provided
                if 'last_name' in user_info:
                    if not hasattr(address_info, 'hashed_last_name'):
                        address_info = user_data_obj.user_identifiers.add().address_info
                    address_info.hashed_last_name = hashlib.sha256(
                        user_info['last_name'].lower().strip().encode('utf-8')
                    ).hexdigest()
                    
                # Add other address info if provided
                if any(field in user_info for field in ['country_code', 'postal_code']):
                    if 'country_code' in user_info:
                        address_info.country_code = user_info['country_code']
                    if 'postal_code' in user_info:
                        address_info.postal_code = user_info['postal_code']
                        
                operations.append(operation)
                
            # Add operations to job
            response = offline_user_data_job_service.add_offline_user_data_job_operations(
                resource_name=job_resource,
                operations=operations
            )
            
            logger.info(f"Added {len(operations)} operations to offline job: {job_resource}")
            return True
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error adding operations to offline job: {e}")
            raise GoogleAdsError(f"Adding operations to offline job failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error adding operations to offline job: {e}")
            raise GoogleAdsError(f"Adding operations to offline job failed: {e}")
            
    def _offline_job_run(self, job_resource: str, customer_id: str = None) -> bool:
        """
        Executes offline user data jobs
        
        Args:
            job_resource: Offline user data job resource name
            customer_id: Customer ID (uses default if None)
            
        Returns:
            True if job started successfully
            
        Raises:
            GoogleAdsError: If job execution fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            offline_user_data_job_service = self.client.get_service("OfflineUserDataJobService")
            
            # Run the job
            response = offline_user_data_job_service.run_offline_user_data_job(
                resource_name=job_resource
            )
            
            logger.info(f"Started offline user data job: {job_resource}")
            return True
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error running offline job: {e}")
            raise GoogleAdsError(f"Offline job execution failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error running offline job: {e}")
            raise GoogleAdsError(f"Offline job execution failed: {e}")
            
    def _offline_job_check_status(self, job_resource: str, customer_id: str = None) -> Dict[str, Any]:
        """
        Monitors offline job status
        
        Args:
            job_resource: Offline user data job resource name
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with job status information
            
        Raises:
            GoogleAdsError: If status check fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            # Query job status
            query = f"""
                SELECT 
                    offline_user_data_job.resource_name,
                    offline_user_data_job.id,
                    offline_user_data_job.status,
                    offline_user_data_job.type,
                    offline_user_data_job.failure_reason,
                    offline_user_data_job.customer_match_user_list_metadata.user_list
                FROM offline_user_data_job 
                WHERE offline_user_data_job.resource_name = '{job_resource}'
            """
            
            results = self._query_data_all(query, customer_id)
            
            if not results:
                raise GoogleAdsError(f"Job not found: {job_resource}")
                
            job_info = results[0]['offline_user_data_job']
            
            status_info = {
                'resource_name': job_info['resource_name'],
                'job_id': job_info['id'],
                'status': job_info['status'],
                'type': job_info['type'],
                'failure_reason': job_info.get('failure_reason', ''),
                'user_list': job_info.get('customer_match_user_list_metadata', {}).get('user_list', '')
            }
            
            logger.info(f"Job status: {job_resource} - {status_info['status']}")
            return status_info
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error checking job status: {e}")
            raise GoogleAdsError(f"Job status check failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error checking job status: {e}")
            raise GoogleAdsError(f"Job status check failed: {e}")
            
    def _customer_match_get_user_lists(self, customer_id: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves user list information
        
        Args:
            customer_id: Customer ID (uses default if None)
            
        Returns:
            List of user list dictionaries
            
        Raises:
            GoogleAdsError: If retrieval fails
        """
        try:
            query = """
                SELECT 
                    user_list.resource_name,
                    user_list.id,
                    user_list.name,
                    user_list.description,
                    user_list.membership_status,
                    user_list.membership_life_span,
                    user_list.size_for_display,
                    user_list.size_for_search,
                    user_list.type,
                    user_list.crm_based_user_list.upload_key_type,
                    user_list.crm_based_user_list.data_source_type
                FROM user_list 
                WHERE user_list.type = 'CRM_BASED'
            """
            
            results = self._query_data_all(query, customer_id)
            
            user_lists = []
            for result in results:
                user_list_info = result['user_list']
                user_lists.append({
                    'resource_name': user_list_info['resource_name'],
                    'id': user_list_info['id'],
                    'name': user_list_info['name'],
                    'description': user_list_info.get('description', ''),
                    'membership_status': user_list_info['membership_status'],
                    'membership_life_span': user_list_info['membership_life_span'],
                    'size_for_display': user_list_info.get('size_for_display', 0),
                    'size_for_search': user_list_info.get('size_for_search', 0),
                    'upload_key_type': user_list_info.get('crm_based_user_list', {}).get('upload_key_type', ''),
                    'data_source_type': user_list_info.get('crm_based_user_list', {}).get('data_source_type', '')
                })
                
            logger.info(f"Retrieved {len(user_lists)} customer match user lists")
            return user_lists
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error getting user lists: {e}")
            raise GoogleAdsError(f"User list retrieval failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting user lists: {e}")
            raise GoogleAdsError(f"User list retrieval failed: {e}")
            
    def customer_match_delta_emails(self, current_data_path: str, previous_data_path: str,
                                   output_path: str) -> Dict[str, Any]:
        """
        Generates delta files for email updates
        
        Args:
            current_data_path: Path to current email data
            previous_data_path: Path to previous email data
            output_path: Path to save delta files
            
        Returns:
            Dictionary with delta processing results
            
        Raises:
            GoogleAdsError: If delta processing fails
        """
        try:
            # Read current and previous data
            current_df = self.read_from_cloud_storage(current_data_path)
            previous_df = self.read_from_cloud_storage(previous_data_path)
            
            # Assume email column exists
            email_col = 'email'
            if email_col not in current_df.columns or email_col not in previous_df.columns:
                raise GoogleAdsError(f"Email column '{email_col}' not found in data")
                
            # Find new emails (to add)
            current_emails = set(current_df[email_col].dropna())
            previous_emails = set(previous_df[email_col].dropna())
            
            new_emails = current_emails - previous_emails
            removed_emails = previous_emails - current_emails
            
            # Create delta DataFrames
            add_df = current_df[current_df[email_col].isin(new_emails)]
            remove_df = previous_df[previous_df[email_col].isin(removed_emails)]
            
            # Save delta files
            add_path = f"{output_path}/add_emails.csv"
            remove_path = f"{output_path}/remove_emails.csv"
            
            if not add_df.empty:
                self.export_to_cloud_storage(add_df, add_path, 'csv')
            if not remove_df.empty:
                self.export_to_cloud_storage(remove_df, remove_path, 'csv')
                
            result = {
                'new_emails_count': len(new_emails),
                'removed_emails_count': len(removed_emails),
                'add_file_path': add_path if not add_df.empty else None,
                'remove_file_path': remove_path if not remove_df.empty else None,
                'add_df_size': len(add_df),
                'remove_df_size': len(remove_df)
            }
            
            logger.info(f"Generated delta files: {len(new_emails)} new, {len(removed_emails)} removed")
            return result
            
        except Exception as e:
            logger.error(f"Error generating delta files: {e}")
            raise GoogleAdsError(f"Delta file generation failed: {e}")
            
    def customer_match_manage_jobs(self, user_list_name: str, user_data_path: str,
                                  job_type: str = "CREATE", customer_id: str = None) -> Dict[str, Any]:
        """
        Manages customer match job lifecycle
        
        Args:
            user_list_name: Name of the user list
            user_data_path: Path to user data file
            job_type: Job type ("CREATE", "ADD", "REMOVE")
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with job management results
            
        Raises:
            GoogleAdsError: If job management fails
        """
        try:
            # Load user data
            user_df = self.read_from_cloud_storage(user_data_path)
            
            # Convert DataFrame to list of dictionaries
            user_data_list = user_df.to_dict('records')
            
            # Create or get user list
            if job_type == "CREATE":
                user_list_resource = self._customer_match_create_list(
                    list_name=user_list_name,
                    list_description=f"Customer match list created on {datetime.now().isoformat()}",
                    customer_id=customer_id
                )
            else:
                # Find existing user list
                user_lists = self._customer_match_get_user_lists(customer_id)
                matching_lists = [ul for ul in user_lists if ul['name'] == user_list_name]
                
                if not matching_lists:
                    raise GoogleAdsError(f"User list '{user_list_name}' not found")
                    
                user_list_resource = matching_lists[0]['resource_name']
                
            # Create offline job
            job_resource = self._offline_job_create(
                user_list_resource=user_list_resource,
                job_type=job_type,
                customer_id=customer_id
            )
            
            # Add operations to job
            self._offline_job_add_operations(
                job_resource=job_resource,
                user_data=user_data_list,
                customer_id=customer_id
            )
            
            # Run the job
            self._offline_job_run(job_resource, customer_id)
            
            # Get initial status
            job_status = self._offline_job_check_status(job_resource, customer_id)
            
            result = {
                'user_list_resource': user_list_resource,
                'job_resource': job_resource,
                'job_type': job_type,
                'user_count': len(user_data_list),
                'job_status': job_status,
                'created_at': datetime.now().isoformat()
            }
            
            logger.info(f"Managed customer match job: {job_type} for list '{user_list_name}', "
                       f"job: {job_resource}, users: {len(user_data_list)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error managing customer match job: {e}")
            raise GoogleAdsError(f"Customer match job management failed: {e}")
            
    def campaign_update_strategy(self, campaign_id: str, bidding_strategy: Dict[str, Any],
                               customer_id: str = None) -> str:
        """
        Updates campaign bidding strategies
        
        Args:
            campaign_id: Campaign ID
            bidding_strategy: Bidding strategy configuration
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Campaign resource name
            
        Raises:
            GoogleAdsError: If strategy update fails
        """
        if not self.client:
            raise GoogleAdsError("Google Ads client not initialized")
            
        customer_id = customer_id or self.customer_id
        if not customer_id:
            raise GoogleAdsError("Customer ID not provided")
            
        try:
            campaign_service = self.client.get_service("CampaignService")
            campaign_operation = self.client.get_type("CampaignOperation")
            
            # Update campaign
            campaign_resource = f"customers/{customer_id}/campaigns/{campaign_id}"
            campaign = campaign_operation.update
            campaign.resource_name = campaign_resource
            
            # Set bidding strategy based on type
            strategy_type = bidding_strategy.get('type', 'MANUAL_CPC')
            
            if strategy_type == 'MANUAL_CPC':
                campaign.manual_cpc.enhanced_cpc_enabled = bidding_strategy.get('enhanced_cpc', False)
            elif strategy_type == 'TARGET_CPA':
                campaign.target_cpa.target_cpa_micros = int(bidding_strategy.get('target_cpa_micros', 0))
            elif strategy_type == 'TARGET_ROAS':
                campaign.target_roas.target_roas = float(bidding_strategy.get('target_roas', 0.0))
            elif strategy_type == 'MAXIMIZE_CONVERSIONS':
                if 'target_cpa_micros' in bidding_strategy:
                    campaign.maximize_conversions.target_cpa_micros = int(bidding_strategy['target_cpa_micros'])
            elif strategy_type == 'MAXIMIZE_CONVERSION_VALUE':
                if 'target_roas' in bidding_strategy:
                    campaign.maximize_conversion_value.target_roas = float(bidding_strategy['target_roas'])
                    
            # Set update mask
            campaign_operation.update_mask = "manual_cpc,target_cpa,target_roas,maximize_conversions,maximize_conversion_value"
            
            # Execute mutation
            response = campaign_service.mutate_campaigns(
                customer_id=customer_id,
                operations=[campaign_operation]
            )
            
            resource_name = response.results[0].resource_name
            logger.info(f"Updated campaign bidding strategy: {resource_name}")
            
            return resource_name
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error updating campaign strategy: {e}")
            raise GoogleAdsError(f"Campaign strategy update failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error updating campaign strategy: {e}")
            raise GoogleAdsError(f"Campaign strategy update failed: {e}")