"""
GCP Kafka Operations
Kafka operations with Google Cloud Storage integration for state store and data export
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
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.auth.exceptions import DefaultCredentialsError
    GCP_AVAILABLE = True
except ImportError as e:
    logger.warning(f"GCP libraries not available: {e}")
    GCP_AVAILABLE = False


class GCPKafkaOperations(KafkaSchemaOperations):
    """
    Kafka operations with Google Cloud Storage integration
    Combines Kafka functionality with GCP cloud storage capabilities for state management and data export
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GCP Kafka operations
        
        Args:
            config: Configuration dictionary containing both Kafka and GCP settings
                Kafka settings:
                - bootstrap_servers, security_protocol, sasl_*, ssl_*, schema_registry_*
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
            
    def collect_state_store(self, state_store_path: str) -> Dict[str, Any]:
        """
        Reads partition offset state from Google Cloud Storage
        
        Args:
            state_store_path: GCS path to state store file (gs://bucket/object or just object if default_bucket set)
            
        Returns:
            Dictionary with partition offset states
            
        Raises:
            KafkaOperationError: If operation fails
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured for state store operations")
            return {}
            
        try:
            # Parse GCS path
            if state_store_path.startswith('gs://'):
                # Full GCS URL
                path_parts = state_store_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket_client:
                    logger.error("No GCS bucket specified and no default bucket configured")
                    return {}
                bucket = self.bucket_client
                blob_name = state_store_path.lstrip('/')
                
            # Check if state store exists
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                state_data = json.loads(blob.download_as_text())
                logger.info(f"Loaded state store from gs://{bucket.name}/{blob_name}")
                return state_data
            else:
                logger.info(f"State store not found at gs://{bucket.name}/{blob_name}, returning empty state")
                return {}
                
        except Exception as e:
            logger.error(f"Error collecting state store from GCS: {e}")
            from .base_kafka import KafkaOperationError
            raise KafkaOperationError(f"Failed to collect state store: {e}")
            
    def save_state_store(self, state_store_path: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves partition offset state to Google Cloud Storage
        
        Args:
            state_store_path: GCS path to state store file
            state_data: Dictionary with partition offset states
            
        Returns:
            True if save successful, False otherwise
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured for state store operations")
            return False
            
        try:
            # Parse GCS path
            if state_store_path.startswith('gs://'):
                # Full GCS URL
                path_parts = state_store_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket_client:
                    logger.error("No GCS bucket specified and no default bucket configured")
                    return False
                bucket = self.bucket_client
                blob_name = state_store_path.lstrip('/')
                
            # Add metadata
            state_data['last_saved'] = datetime.now().isoformat()
            state_data['saved_by'] = 'gcp_kafka_operations'
            
            # Save to GCS
            blob = bucket.blob(blob_name)
            json_data = json.dumps(state_data, indent=2)
            blob.upload_from_string(json_data, content_type='application/json')
            
            logger.info(f"Saved state store to gs://{bucket.name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state store to GCS: {e}")
            return False
            
    def load_schema_dictionary(self, schema_dict_path: str) -> Dict[str, Any]:
        """
        Loads schema definitions from Google Cloud Storage schema dictionary
        
        Args:
            schema_dict_path: GCS path to schema dictionary file
            
        Returns:
            Dictionary with schema definitions
            
        Raises:
            Exception: If load operation fails
        """
        if not self.storage_client:
            raise Exception("GCP Cloud Storage not configured for schema dictionary operations")
            
        try:
            # Parse GCS path
            if schema_dict_path.startswith('gs://'):
                # Full GCS URL
                path_parts = schema_dict_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket_client:
                    raise Exception("No GCS bucket specified and no default bucket configured")
                bucket = self.bucket_client
                blob_name = schema_dict_path.lstrip('/')
                
            # Load schema dictionary from GCS
            blob = bucket.blob(blob_name)
            if not blob.exists():
                raise Exception(f"Schema dictionary not found: gs://{bucket.name}/{blob_name}")
                
            schema_dict = json.loads(blob.download_as_text())
            logger.info(f"Loaded schema dictionary from gs://{bucket.name}/{blob_name}")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error loading schema dictionary from GCS: {e}")
            raise
            
    def export_to_gcs(self, data: Union[pd.DataFrame, List[Dict], str], 
                      gcs_path: str, file_format: str = 'csv', compression: bool = False) -> bool:
        """
        Exports data to Google Cloud Storage in various formats
        
        Args:
            data: Data to export (DataFrame, list of dicts, or string)
            gcs_path: GCS path (gs://bucket/object or just object if default_bucket set)
            file_format: File format ('csv', 'parquet', 'json', 'txt')
            compression: Whether to compress the data
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured for export operations")
            return False
            
        try:
            # Parse GCS path
            if gcs_path.startswith('gs://'):
                # Full GCS URL
                path_parts = gcs_path.replace('gs://', '').split('/', 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                bucket = self.storage_client.bucket(bucket_name)
            else:
                # Use default bucket
                if not self.default_bucket or not self.bucket_client:
                    logger.error("No GCS bucket specified and no default bucket configured")
                    return False
                bucket = self.bucket_client
                blob_name = gcs_path.lstrip('/')
                
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
                blob_name += '.bz2' if not blob_name.endswith('.bz2') else ''
                upload_data = data_bytes
            else:
                if file_format.lower() == 'parquet':
                    upload_data = data_bytes
                else:
                    upload_data = data_str
                    
            # Upload to GCS
            blob = bucket.blob(blob_name)
            
            if isinstance(upload_data, bytes):
                blob.upload_from_string(upload_data, content_type=content_type)
            else:
                blob.upload_from_string(upload_data, content_type=content_type)
                
            data_size = len(data) if isinstance(data, (list, pd.DataFrame)) else len(str(data))
            logger.info(f"Exported data to gs://{bucket.name}/{blob_name} in {file_format} format "
                       f"(size: {data_size}, compressed: {compression})")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to GCS: {e}")
            return False
            
    def consume_messages_to_gcs(self, topic_name: str, gcs_export_path: str,
                               max_messages_per_partition: int = 1000,
                               file_format: str = 'txt', compression: bool = True) -> Dict[str, Any]:
        """
        Consumes Kafka messages and exports directly to Google Cloud Storage
        
        Args:
            topic_name: Kafka topic name
            gcs_export_path: GCS base path for export files
            max_messages_per_partition: Maximum messages per partition
            file_format: Export file format
            compression: Whether to compress exported files
            
        Returns:
            Dictionary with consumption results
        """
        try:
            # Load or initialize state store
            state_store_path = f"{gcs_export_path}/state_store.json"
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
                    partition_export_path = f"{gcs_export_path}/partition_{partition_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
                    
                    # Consume messages from partition
                    messages = self._consume_partition_messages(
                        topic_name, partition_id, start_offset, end_offset
                    )
                    
                    if messages:
                        # Export messages to GCS
                        export_success = self.export_to_gcs(
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
            logger.error(f"Error consuming messages to GCS: {e}")
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
            
    def gcs_list_blobs(self, prefix: str, bucket_name: str = None, max_blobs: int = 1000) -> List[str]:
        """
        List blobs in GCS bucket with given prefix
        
        Args:
            prefix: GCS prefix to search under
            bucket_name: GCS bucket name (uses default if None)
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob names
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured")
            return []
            
        try:
            if bucket_name:
                bucket = self.storage_client.bucket(bucket_name)
            elif self.bucket_client:
                bucket = self.bucket_client
            else:
                logger.error("No GCS bucket specified")
                return []
                
            blobs = []
            
            for blob in bucket.list_blobs(prefix=prefix, max_results=max_blobs):
                blobs.append(blob.name)
                if len(blobs) >= max_blobs:
                    break
                    
            logger.info(f"Listed {len(blobs)} blobs with prefix '{prefix}' in bucket '{bucket.name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing GCS blobs: {e}")
            return []
            
    def create_signed_url(self, blob_name: str, bucket_name: str = None, 
                         expiration_hours: int = 24, method: str = 'GET') -> Optional[str]:
        """
        Create a signed URL for GCS blob access
        
        Args:
            blob_name: GCS blob name
            bucket_name: GCS bucket name (uses default if None)
            expiration_hours: URL expiration time in hours
            method: HTTP method ('GET', 'PUT', 'POST', 'DELETE')
            
        Returns:
            Signed URL string or None if creation fails
        """
        if not self.storage_client:
            logger.error("GCP Cloud Storage not configured")
            return None
            
        try:
            from datetime import timedelta
            
            if bucket_name:
                bucket = self.storage_client.bucket(bucket_name)
            elif self.bucket_client:
                bucket = self.bucket_client
            else:
                logger.error("No GCS bucket specified")
                return None
                
            blob = bucket.blob(blob_name)
            
            # Generate signed URL
            signed_url = blob.generate_signed_url(
                expiration=timedelta(hours=expiration_hours),
                method=method
            )
            
            logger.info(f"Generated signed URL for blob: {bucket.name}/{blob_name}")
            return signed_url
            
        except Exception as e:
            logger.error(f"Error creating signed URL: {e}")
            return None
            
    def get_gcp_info(self) -> Dict[str, Any]:
        """
        Get GCP configuration information
        
        Returns:
            Dictionary with GCP info
        """
        return {
            'gcp_configured': self.storage_client is not None,
            'default_bucket': self.default_bucket,
            'project_id': self.project_id,
            'has_credentials_path': self.credentials_path is not None,
            'has_credentials_object': self.credentials is not None
        }