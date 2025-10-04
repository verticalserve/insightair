"""
OCI Google Ads Operations
Google Ads operations with OCI Object Storage integration for data storage and processing
"""

import logging
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Union

from .googleads_extensions import GoogleAdsExtensionsOperations
from .googleads_customer_match import GoogleAdsCustomerMatchOperations

logger = logging.getLogger(__name__)

try:
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False


class OCIGoogleAdsOperations(GoogleAdsExtensionsOperations, GoogleAdsCustomerMatchOperations):
    """
    Google Ads operations with OCI Object Storage integration
    Combines all Google Ads functionality with OCI cloud storage capabilities
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Google Ads operations
        
        Args:
            config: Configuration dictionary containing both Google Ads and OCI settings
                Google Ads settings:
                - developer_token, client_id, client_secret, refresh_token, customer_id
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
            
    def export_to_cloud_storage(self, data: Union[pd.DataFrame, List[Dict]], 
                               storage_path: str, file_format: str = 'csv') -> bool:
        """
        Exports Google Ads data to OCI Object Storage in CSV/Parquet format
        
        Args:
            data: Data to export (DataFrame or list of dictionaries)
            storage_path: OCI path (oci://bucket/object or just object if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured for export operations")
            return False
            
        try:
            # Convert to DataFrame if needed
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data
                
            if df.empty:
                logger.warning("No data to export")
                return False
                
            # Parse OCI path
            if storage_path.startswith('oci://'):
                # Full OCI URL
                path_parts = storage_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No OCI bucket specified and no default bucket configured")
                    return False
                bucket_name = self.default_bucket
                object_name = storage_path.lstrip('/')
                
            # Prepare data based on format
            if file_format.lower() == 'csv':
                # Export to CSV
                csv_data = df.to_csv(index=False)
                data_bytes = csv_data.encode('utf-8')
                content_type = 'text/csv'
                
            elif file_format.lower() == 'parquet':
                # Export to Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                data_bytes = parquet_buffer.getvalue()
                content_type = 'application/octet-stream'
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            # Upload to OCI Object Storage
            self.object_storage_client.put_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=data_bytes,
                content_type=content_type
            )
            
            logger.info(f"Exported {len(df)} rows to oci://{bucket_name}/{object_name} in {file_format} format")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to OCI Object Storage: {e}")
            return False
            
    def read_from_cloud_storage(self, storage_path: str, 
                               file_format: str = 'csv') -> pd.DataFrame:
        """
        Read data from OCI Object Storage
        
        Args:
            storage_path: OCI path (oci://bucket/object or just object if default_bucket set)
            file_format: File format ('csv' or 'parquet')
            
        Returns:
            DataFrame with data
            
        Raises:
            Exception: If read operation fails
        """
        if not self.object_storage_client:
            raise Exception("OCI Object Storage not configured for read operations")
            
        try:
            # Parse OCI path
            if storage_path.startswith('oci://'):
                # Full OCI URL
                path_parts = storage_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise Exception("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = storage_path.lstrip('/')
                
            # Get object from OCI Object Storage
            get_object_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            # Read based on format
            if file_format.lower() == 'csv':
                csv_data = get_object_response.data.content.decode('utf-8')
                df = pd.read_csv(io.StringIO(csv_data))
                
            elif file_format.lower() == 'parquet':
                parquet_data = get_object_response.data.content
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
            else:
                raise Exception(f"Unsupported file format: {file_format}")
                
            logger.info(f"Read {len(df)} rows from oci://{bucket_name}/{object_name} in {file_format} format")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from OCI Object Storage: {e}")
            raise
            
    def export_query_results_to_oci(self, query: str, oci_path: str, 
                                    file_format: str = 'csv', customer_id: str = None) -> Dict[str, Any]:
        """
        Execute GAQL query and export results directly to OCI Object Storage
        
        Args:
            query: GAQL query string
            oci_path: OCI path for export
            file_format: File format ('csv' or 'parquet')
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with export results
        """
        try:
            # Execute query
            results = self._query_data_all(query, customer_id, return_format='dataframe')
            
            if results.empty:
                logger.warning("Query returned no results")
                return {
                    'success': False,
                    'rows_exported': 0,
                    'message': 'No data to export'
                }
                
            # Export to OCI
            success = self.export_to_cloud_storage(results, oci_path, file_format)
            
            return {
                'success': success,
                'rows_exported': len(results) if success else 0,
                'oci_path': oci_path,
                'file_format': file_format,
                'columns': list(results.columns) if success else []
            }
            
        except Exception as e:
            logger.error(f"Error exporting query results to OCI: {e}")
            return {
                'success': False,
                'rows_exported': 0,
                'error': str(e)
            }
            
    def batch_upload_custom_assets_from_oci(self, asset_sets_config: List[Dict[str, Any]],
                                           customer_id: str = None) -> Dict[str, Any]:
        """
        Batch upload multiple custom asset sets from OCI Object Storage
        
        Args:
            asset_sets_config: List of asset set configurations with OCI paths
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with batch upload results
        """
        results = {
            'total_asset_sets': len(asset_sets_config),
            'successful_uploads': 0,
            'failed_uploads': 0,
            'upload_details': []
        }
        
        for config in asset_sets_config:
            try:
                asset_set_name = config['asset_set_name']
                oci_path = config['oci_path']
                
                # Upload custom asset set
                upload_result = self.upload_custom_asset_set(
                    asset_set_name=asset_set_name,
                    assets_data=oci_path,
                    customer_id=customer_id
                )
                
                results['successful_uploads'] += 1
                results['upload_details'].append({
                    'asset_set_name': asset_set_name,
                    'success': True,
                    'result': upload_result
                })
                
            except Exception as e:
                results['failed_uploads'] += 1
                results['upload_details'].append({
                    'asset_set_name': config.get('asset_set_name', 'Unknown'),
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to upload asset set {config.get('asset_set_name')}: {e}")
                
        logger.info(f"Batch upload completed: {results['successful_uploads']} successful, "
                   f"{results['failed_uploads']} failed")
        
        return results
        
    def manage_customer_match_from_oci(self, jobs_config: List[Dict[str, Any]],
                                      customer_id: str = None) -> Dict[str, Any]:
        """
        Manage multiple customer match jobs from OCI Object Storage data
        
        Args:
            jobs_config: List of job configurations with OCI paths
            customer_id: Customer ID (uses default if None)
            
        Returns:
            Dictionary with job management results
        """
        results = {
            'total_jobs': len(jobs_config),
            'successful_jobs': 0,
            'failed_jobs': 0,
            'job_details': []
        }
        
        for config in jobs_config:
            try:
                user_list_name = config['user_list_name']
                oci_path = config['oci_path']
                job_type = config.get('job_type', 'CREATE')
                
                # Manage customer match job
                job_result = self.customer_match_manage_jobs(
                    user_list_name=user_list_name,
                    user_data_path=oci_path,
                    job_type=job_type,
                    customer_id=customer_id
                )
                
                results['successful_jobs'] += 1
                results['job_details'].append({
                    'user_list_name': user_list_name,
                    'job_type': job_type,
                    'success': True,
                    'result': job_result
                })
                
            except Exception as e:
                results['failed_jobs'] += 1
                results['job_details'].append({
                    'user_list_name': config.get('user_list_name', 'Unknown'),
                    'job_type': config.get('job_type', 'CREATE'),
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"Failed to manage job for {config.get('user_list_name')}: {e}")
                
        logger.info(f"Customer match jobs completed: {results['successful_jobs']} successful, "
                   f"{results['failed_jobs']} failed")
        
        return results
        
    def oci_list_objects(self, prefix: str, bucket_name: str = None, max_objects: int = 1000) -> List[str]:
        """
        List objects in OCI Object Storage bucket with given prefix
        
        Args:
            prefix: OCI prefix to search under
            bucket_name: OCI bucket name (uses default if None)
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object names
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured")
            return []
            
        try:
            bucket_name = bucket_name or self.default_bucket
            if not bucket_name:
                logger.error("No OCI bucket specified")
                return []
                
            objects = []
            
            # List objects with prefix
            list_objects_response = self.object_storage_client.list_objects(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                prefix=prefix,
                limit=min(max_objects, 1000)  # OCI limit is 1000 per request
            )
            
            if list_objects_response.data.objects:
                for obj in list_objects_response.data.objects:
                    objects.append(obj.name)
                    if len(objects) >= max_objects:
                        break
                        
            # Handle pagination if needed
            next_start_with = list_objects_response.data.next_start_with
            while next_start_with and len(objects) < max_objects:
                remaining = max_objects - len(objects)
                list_objects_response = self.object_storage_client.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    prefix=prefix,
                    start=next_start_with,
                    limit=min(remaining, 1000)
                )
                
                if list_objects_response.data.objects:
                    for obj in list_objects_response.data.objects:
                        objects.append(obj.name)
                        if len(objects) >= max_objects:
                            break
                            
                next_start_with = list_objects_response.data.next_start_with
                
            logger.info(f"Listed {len(objects)} objects with prefix '{prefix}' in bucket '{bucket_name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing OCI objects: {e}")
            return []
            
    def create_presigned_url(self, object_name: str, bucket_name: str = None, 
                           expiration_hours: int = 24) -> Optional[str]:
        """
        Create a presigned URL for OCI Object Storage object access
        
        Args:
            object_name: OCI object name
            bucket_name: OCI bucket name (uses default if None)
            expiration_hours: URL expiration time in hours
            
        Returns:
            Presigned URL string or None if creation fails
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured")
            return None
            
        try:
            bucket_name = bucket_name or self.default_bucket
            if not bucket_name:
                logger.error("No OCI bucket specified")
                return None
                
            # OCI doesn't have direct presigned URL support like S3
            # Return the direct object URL (requires appropriate IAM policies)
            region = self.region
            namespace = self.namespace
            
            # Build direct access URL
            url = f"https://objectstorage.{region}.oraclecloud.com/n/{namespace}/b/{bucket_name}/o/{object_name}"
            
            logger.info(f"Generated OCI object URL: {url}")
            logger.warning("OCI direct URLs require proper IAM policies for access")
            
            return url
            
        except Exception as e:
            logger.error(f"Error creating OCI object URL: {e}")
            return None
            
    def get_oci_info(self) -> Dict[str, Any]:
        """
        Get OCI configuration information
        
        Returns:
            Dictionary with OCI info
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
        
    def sync_data_between_buckets(self, source_path: str, destination_path: str,
                                 source_bucket: str = None, dest_bucket: str = None) -> bool:
        """
        Sync data between OCI Object Storage buckets
        
        Args:
            source_path: Source object path
            destination_path: Destination object path
            source_bucket: Source bucket name (uses default if None)
            dest_bucket: Destination bucket name (uses default if None)
            
        Returns:
            True if sync successful, False otherwise
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured")
            return False
            
        try:
            source_bucket = source_bucket or self.default_bucket
            dest_bucket = dest_bucket or self.default_bucket
            
            if not source_bucket or not dest_bucket:
                logger.error("Source and destination buckets must be specified")
                return False
                
            # Get object from source
            get_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=source_bucket,
                object_name=source_path
            )
            
            # Put object to destination
            self.object_storage_client.put_object(
                namespace_name=self.namespace,
                bucket_name=dest_bucket,
                object_name=destination_path,
                put_object_body=get_response.data.content
            )
            
            logger.info(f"Synced object from {source_bucket}/{source_path} to {dest_bucket}/{destination_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error syncing between OCI buckets: {e}")
            return False