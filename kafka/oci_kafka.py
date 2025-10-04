"""
OCI Kafka Operations
Kafka operations with OCI Object Storage integration for state store and data export
"""

import logging
import json
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Union, Tuple, Generator
from datetime import datetime

from .kafka_schema_ops import KafkaSchemaOperations

logger = logging.getLogger(__name__)

try:
    import oci
    from oci.object_storage import ObjectStorageClient
    from oci.exceptions import ServiceError
    OCI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"OCI libraries not available: {e}")
    OCI_AVAILABLE = False


class OCIKafkaOperations(KafkaSchemaOperations):
    """
    Kafka operations with OCI Object Storage integration
    Combines Kafka functionality with OCI cloud storage capabilities for state management and data export
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OCI Kafka operations
        
        Args:
            config: Configuration dictionary containing both Kafka and OCI settings
                Kafka settings:
                - bootstrap_servers, security_protocol, sasl_*, ssl_*, schema_registry_*
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
            
    def collect_state_store(self, state_store_path: str) -> Dict[str, Any]:
        """
        Reads partition offset state from OCI Object Storage
        
        Args:
            state_store_path: OCI path to state store file (oci://bucket/object or just object if default_bucket set)
            
        Returns:
            Dictionary with partition offset states
            
        Raises:
            KafkaOperationError: If operation fails
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured for state store operations")
            return {}
            
        try:
            # Parse OCI path
            if state_store_path.startswith('oci://'):
                # Full OCI URL
                path_parts = state_store_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No OCI bucket specified and no default bucket configured")
                    return {}
                bucket_name = self.default_bucket
                object_name = state_store_path.lstrip('/')
                
            # Check if state store exists
            try:
                get_object_response = self.object_storage_client.get_object(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    object_name=object_name
                )
                
                state_data = json.loads(get_object_response.data.content.decode('utf-8'))
                logger.info(f"Loaded state store from oci://{bucket_name}/{object_name}")
                return state_data
                
            except ServiceError as e:
                if e.status == 404:
                    logger.info(f"State store not found at oci://{bucket_name}/{object_name}, returning empty state")
                    return {}
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Error collecting state store from OCI Object Storage: {e}")
            from .base_kafka import KafkaOperationError
            raise KafkaOperationError(f"Failed to collect state store: {e}")
            
    def save_state_store(self, state_store_path: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves partition offset state to OCI Object Storage
        
        Args:
            state_store_path: OCI path to state store file
            state_data: Dictionary with partition offset states
            
        Returns:
            True if save successful, False otherwise
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured for state store operations")
            return False
            
        try:
            # Parse OCI path
            if state_store_path.startswith('oci://'):
                # Full OCI URL
                path_parts = state_store_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No OCI bucket specified and no default bucket configured")
                    return False
                bucket_name = self.default_bucket
                object_name = state_store_path.lstrip('/')
                
            # Add metadata
            state_data['last_saved'] = datetime.now().isoformat()
            state_data['saved_by'] = 'oci_kafka_operations'
            
            # Save to OCI Object Storage
            json_data = json.dumps(state_data, indent=2)
            data_bytes = json_data.encode('utf-8')
            
            self.object_storage_client.put_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=data_bytes,
                content_type='application/json'
            )
            
            logger.info(f"Saved state store to oci://{bucket_name}/{object_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state store to OCI Object Storage: {e}")
            return False
            
    def load_schema_dictionary(self, schema_dict_path: str) -> Dict[str, Any]:
        """
        Loads schema definitions from OCI Object Storage schema dictionary
        
        Args:
            schema_dict_path: OCI path to schema dictionary file
            
        Returns:
            Dictionary with schema definitions
            
        Raises:
            Exception: If load operation fails
        """
        if not self.object_storage_client:
            raise Exception("OCI Object Storage not configured for schema dictionary operations")
            
        try:
            # Parse OCI path
            if schema_dict_path.startswith('oci://'):
                # Full OCI URL
                path_parts = schema_dict_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise Exception("No OCI bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_name = schema_dict_path.lstrip('/')
                
            # Load schema dictionary from OCI Object Storage
            get_object_response = self.object_storage_client.get_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            schema_dict = json.loads(get_object_response.data.content.decode('utf-8'))
            logger.info(f"Loaded schema dictionary from oci://{bucket_name}/{object_name}")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error loading schema dictionary from OCI Object Storage: {e}")
            raise
            
    def export_to_oci(self, data: Union[pd.DataFrame, List[Dict], str], 
                      oci_path: str, file_format: str = 'csv', compression: bool = False) -> bool:
        """
        Exports data to OCI Object Storage in various formats
        
        Args:
            data: Data to export (DataFrame, list of dicts, or string)
            oci_path: OCI path (oci://bucket/object or just object if default_bucket set)
            file_format: File format ('csv', 'parquet', 'json', 'txt')
            compression: Whether to compress the data
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.object_storage_client:
            logger.error("OCI Object Storage not configured for export operations")
            return False
            
        try:
            # Parse OCI path
            if oci_path.startswith('oci://'):
                # Full OCI URL
                path_parts = oci_path.replace('oci://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No OCI bucket specified and no default bucket configured")
                    return False
                bucket_name = self.default_bucket
                object_name = oci_path.lstrip('/')
                
            # Prepare data based on format
            if file_format.lower() == 'csv':
                if isinstance(data, pd.DataFrame):
                    data_str = data.to_csv(index=False)
                elif isinstance(data, list):
                    df = pd.DataFrame(data)
                    data_str = df.to_csv(index=False)
                else:
                    data_str = str(data)
                content_type = 'text/csv'
                
            elif file_format.lower() == 'parquet':
                if isinstance(data, pd.DataFrame):
                    parquet_buffer = io.BytesIO()
                    data.to_parquet(parquet_buffer, index=False)
                    data_bytes = parquet_buffer.getvalue()
                elif isinstance(data, list):
                    df = pd.DataFrame(data)
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False)
                    data_bytes = parquet_buffer.getvalue()
                else:
                    raise ValueError("Parquet format requires DataFrame or list of dictionaries")
                content_type = 'application/octet-stream'
                
            elif file_format.lower() == 'json':
                if isinstance(data, (pd.DataFrame, list, dict)):
                    if isinstance(data, pd.DataFrame):
                        data_str = data.to_json(orient='records', lines=True)
                    else:
                        data_str = json.dumps(data, indent=2)
                else:
                    data_str = str(data)
                content_type = 'application/json'
                
            elif file_format.lower() == 'txt':
                data_str = str(data)
                content_type = 'text/plain'
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            # Apply compression if requested
            if compression and file_format.lower() != 'parquet':
                data_bytes = self.compress_data(data_str)
                object_name += '.bz2' if not object_name.endswith('.bz2') else ''
                upload_data = data_bytes
            else:
                if file_format.lower() == 'parquet':
                    upload_data = data_bytes
                else:
                    upload_data = data_str.encode('utf-8')
                    
            # Upload to OCI Object Storage
            self.object_storage_client.put_object(
                namespace_name=self.namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=upload_data,
                content_type=content_type
            )
            
            data_size = len(data) if isinstance(data, (list, pd.DataFrame)) else len(str(data))
            logger.info(f"Exported data to oci://{bucket_name}/{object_name} in {file_format} format "
                       f"(size: {data_size}, compressed: {compression})")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to OCI Object Storage: {e}")
            return False
            
    def consume_messages_to_oci(self, topic_name: str, oci_export_path: str,
                               max_messages_per_partition: int = 1000,
                               file_format: str = 'txt', compression: bool = True) -> Dict[str, Any]:
        """
        Consumes Kafka messages and exports directly to OCI Object Storage
        
        Args:
            topic_name: Kafka topic name
            oci_export_path: OCI base path for export files
            max_messages_per_partition: Maximum messages per partition
            file_format: Export file format
            compression: Whether to compress exported files
            
        Returns:
            Dictionary with consumption results
        """
        try:
            # Load or initialize state store
            state_store_path = f"{oci_export_path}/state_store.json"
            state_store = self.collect_state_store(state_store_path)
            
            # Extend state store with new partitions
            state_store = self.extend_statestore_with_new_partitions(topic_name, state_store)
            
            # Get target offset ranges
            offset_ranges = self.get_target_offset_range(
                topic_name, state_store, max_messages_per_partition
            )
            
            if not offset_ranges:
                logger.info("No new messages to consume")
                return {
                    'success': True,
                    'message': 'No new messages to consume',
                    'partitions_processed': 0,
                    'total_messages': 0
                }
                
            results = {
                'success': True,
                'partitions_processed': 0,
                'total_messages': 0,
                'export_files': [],
                'errors': []
            }
            
            # Process each partition
            for partition_id, (start_offset, end_offset) in offset_ranges.items():
                try:
                    # Create partition-specific export path
                    partition_export_path = f"{oci_export_path}/partition_{partition_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
                    
                    # Consume messages from partition
                    messages = self._consume_partition_messages(
                        topic_name, partition_id, start_offset, end_offset
                    )
                    
                    if messages:
                        # Export messages to OCI
                        export_success = self.export_to_oci(
                            messages, partition_export_path, file_format, compression
                        )
                        
                        if export_success:
                            results['export_files'].append(partition_export_path)
                            results['total_messages'] += len(messages)
                            results['partitions_processed'] += 1
                            
                            # Update state store
                            if 'partitions' not in state_store:
                                state_store['partitions'] = {}
                            state_store['partitions'][str(partition_id)] = {
                                'last_offset': end_offset,
                                'last_updated': datetime.now().isoformat(),
                                'status': 'processed'
                            }
                        else:
                            results['errors'].append(f"Failed to export partition {partition_id}")
                            
                except Exception as e:
                    error_msg = f"Error processing partition {partition_id}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
            # Save updated state store
            if results['partitions_processed'] > 0:
                self.save_state_store(state_store_path, state_store)
                
            logger.info(f"Consumption completed: {results['partitions_processed']} partitions, "
                       f"{results['total_messages']} messages, {len(results['errors'])} errors")
            
            return results
            
        except Exception as e:
            logger.error(f"Error consuming messages to OCI: {e}")
            return {
                'success': False,
                'error': str(e),
                'partitions_processed': 0,
                'total_messages': 0
            }
            
    def _consume_partition_messages(self, topic_name: str, partition_id: int,
                                   start_offset: int, end_offset: int) -> List[str]:
        """
        Consumes messages from a specific partition and offset range
        
        Args:
            topic_name: Kafka topic name
            partition_id: Partition ID
            start_offset: Start offset
            end_offset: End offset
            
        Returns:
            List of formatted message strings
        """
        try:
            from kafka import TopicPartition
            
            topic_partition = TopicPartition(topic_name, partition_id)
            
            # Assign partition and seek to start offset
            self.consumer.assign([topic_partition])
            self.consumer.seek(topic_partition, start_offset)
            
            messages = []
            current_offset = start_offset
            
            # Create header record
            header = self.create_header_record(
                topic_name, partition_id, (start_offset, end_offset)
            )
            messages.append(header)
            
            # Consume messages
            while current_offset <= end_offset:
                message_batch = self.consumer.poll(timeout_ms=5000)
                
                if not message_batch:
                    break
                    
                for tp, batch_messages in message_batch.items():
                    for message in batch_messages:
                        if message.offset > end_offset:
                            break
                            
                        # Process message
                        try:
                            # Try to deserialize if schema available
                            schema_id = self._extract_schema_id_from_message(message)
                            
                            if schema_id and hasattr(self, 'deserialize_avro_message'):
                                deserialized = self.deserialize_avro_message(
                                    message.value.encode() if isinstance(message.value, str) else message.value,
                                    schema_id
                                )
                                if deserialized:
                                    formatted_message = self.format_message_as_pipe_delimited(
                                        deserialized, schema_id
                                    )
                                else:
                                    formatted_message = f"DESERIALIZATION_FAILED|{message.offset}|{message.value}"
                            else:
                                # Use raw message value
                                message_dict = {
                                    'offset': message.offset,
                                    'timestamp': message.timestamp,
                                    'key': message.key,
                                    'value': message.value
                                }
                                formatted_message = self.format_message_as_pipe_delimited(message_dict)
                                
                            messages.append(formatted_message)
                            current_offset = message.offset + 1
                            
                        except Exception as e:
                            logger.warning(f"Error processing message at offset {message.offset}: {e}")
                            error_message = f"ERROR|{message.offset}|Processing failed: {e}"
                            messages.append(error_message)
                            current_offset = message.offset + 1
                            
            logger.debug(f"Consumed {len(messages)-1} messages from partition {partition_id}")
            return messages
            
        except Exception as e:
            logger.error(f"Error consuming partition messages: {e}")
            return []
            
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