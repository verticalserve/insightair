"""
Azure Kafka Operations
Kafka operations with Azure Blob Storage integration for state store and data export
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
    from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    from azure.core.exceptions import ResourceNotFoundError, AzureError
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Azure libraries not available: {e}")
    AZURE_AVAILABLE = False


class AzureKafkaOperations(KafkaSchemaOperations):
    """
    Kafka operations with Azure Blob Storage integration
    Combines Kafka functionality with Azure cloud storage capabilities for state management and data export
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure Kafka operations
        
        Args:
            config: Configuration dictionary containing both Kafka and Azure settings
                Kafka settings:
                - bootstrap_servers, security_protocol, sasl_*, ssl_*, schema_registry_*
                Azure settings:
                - account_name: Storage account name
                - account_key: Storage account key (optional)
                - connection_string: Storage connection string (optional)
                - sas_token: SAS token (optional)
                - credential: Azure credential object (optional)
                - account_url: Storage account URL (optional)
                - default_container: Default container for operations
        """
        super().__init__(config)
        
        # Azure configuration
        self.account_name = config.get('account_name')
        self.account_key = config.get('account_key')
        self.connection_string = config.get('connection_string')
        self.sas_token = config.get('sas_token')
        self.credential = config.get('credential')
        self.account_url = config.get('account_url')
        self.default_container = config.get('default_container')
        
        # Initialize Azure clients
        self.blob_service_client = None
        self.container_client = None
        
        if AZURE_AVAILABLE:
            try:
                self._initialize_azure_clients()
            except Exception as e:
                logger.error(f"Failed to initialize Azure clients: {e}")
                # Continue without Azure - operations will fall back gracefully
        else:
            logger.warning("Azure libraries not available, Blob Storage operations will not work")
            
    def _initialize_azure_clients(self):
        """
        Initialize Azure Blob Storage clients
        """
        try:
            # Initialize BlobServiceClient with different authentication methods
            if self.connection_string:
                # Use connection string
                self.blob_service_client = BlobServiceClient.from_connection_string(
                    conn_str=self.connection_string
                )
            elif self.account_url and self.credential:
                # Use account URL with credential
                self.blob_service_client = BlobServiceClient(
                    account_url=self.account_url,
                    credential=self.credential
                )
            elif self.account_name and self.account_key:
                # Use account name and key
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.account_key
                )
            elif self.account_name and self.sas_token:
                # Use account name and SAS token
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.sas_token
                )
            elif self.account_name:
                # Use default Azure credential
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=DefaultAzureCredential()
                )
            else:
                raise ValueError("No valid Azure authentication method provided")
                
            # Get default container client if specified
            if self.default_container:
                self.container_client = self.blob_service_client.get_container_client(
                    container=self.default_container
                )
                
                try:
                    # Test container access
                    self.container_client.get_container_properties()
                    logger.info(f"Verified access to default container: {self.default_container}")
                except ResourceNotFoundError:
                    logger.warning(f"Default Azure container '{self.default_container}' not found")
                except Exception as e:
                    logger.warning(f"Could not access default container: {e}")
                    
            logger.info("Azure Blob Storage clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error initializing Azure clients: {e}")
            raise
            
    def collect_state_store(self, state_store_path: str) -> Dict[str, Any]:
        """
        Reads partition offset state from Azure Blob Storage
        
        Args:
            state_store_path: Azure path to state store file (azure://container/blob or just blob if default_container set)
            
        Returns:
            Dictionary with partition offset states
            
        Raises:
            KafkaOperationError: If operation fails
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured for state store operations")
            return {}
            
        try:
            # Parse Azure path
            if state_store_path.startswith('azure://'):
                # Full Azure URL
                path_parts = state_store_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    logger.error("No Azure container specified and no default container configured")
                    return {}
                container_client = self.container_client
                blob_name = state_store_path.lstrip('/')
                
            # Check if state store exists
            blob_client = container_client.get_blob_client(blob=blob_name)
            
            if blob_client.exists():
                blob_data = blob_client.download_blob().readall().decode('utf-8')
                state_data = json.loads(blob_data)
                
                logger.info(f"Loaded state store from azure://{container_client.container_name}/{blob_name}")
                return state_data
            else:
                logger.info(f"State store not found at azure://{container_client.container_name}/{blob_name}, returning empty state")
                return {}
                
        except Exception as e:
            logger.error(f"Error collecting state store from Azure Blob Storage: {e}")
            from .base_kafka import KafkaOperationError
            raise KafkaOperationError(f"Failed to collect state store: {e}")
            
    def save_state_store(self, state_store_path: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves partition offset state to Azure Blob Storage
        
        Args:
            state_store_path: Azure path to state store file
            state_data: Dictionary with partition offset states
            
        Returns:
            True if save successful, False otherwise
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured for state store operations")
            return False
            
        try:
            # Parse Azure path
            if state_store_path.startswith('azure://'):
                # Full Azure URL
                path_parts = state_store_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    logger.error("No Azure container specified and no default container configured")
                    return False
                container_client = self.container_client
                blob_name = state_store_path.lstrip('/')
                
            # Add metadata
            state_data['last_saved'] = datetime.now().isoformat()
            state_data['saved_by'] = 'azure_kafka_operations'
            
            # Save to Azure Blob Storage
            blob_client = container_client.get_blob_client(blob=blob_name)
            json_data = json.dumps(state_data, indent=2)
            blob_client.upload_blob(json_data, overwrite=True, content_type='application/json')
            
            logger.info(f"Saved state store to azure://{container_client.container_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state store to Azure Blob Storage: {e}")
            return False
            
    def load_schema_dictionary(self, schema_dict_path: str) -> Dict[str, Any]:
        """
        Loads schema definitions from Azure Blob Storage schema dictionary
        
        Args:
            schema_dict_path: Azure path to schema dictionary file
            
        Returns:
            Dictionary with schema definitions
            
        Raises:
            Exception: If load operation fails
        """
        if not self.blob_service_client:
            raise Exception("Azure Blob Storage not configured for schema dictionary operations")
            
        try:
            # Parse Azure path
            if schema_dict_path.startswith('azure://'):
                # Full Azure URL
                path_parts = schema_dict_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    raise Exception("No Azure container specified and no default container configured")
                container_client = self.container_client
                blob_name = schema_dict_path.lstrip('/')
                
            # Load schema dictionary from Azure Blob Storage
            blob_client = container_client.get_blob_client(blob=blob_name)
            if not blob_client.exists():
                raise Exception(f"Schema dictionary not found: azure://{container_client.container_name}/{blob_name}")
                
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            schema_dict = json.loads(blob_data)
            
            logger.info(f"Loaded schema dictionary from azure://{container_client.container_name}/{blob_name}")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error loading schema dictionary from Azure Blob Storage: {e}")
            raise
            
    def export_to_azure(self, data: Union[pd.DataFrame, List[Dict], str], 
                        azure_path: str, file_format: str = 'csv', compression: bool = False) -> bool:
        """
        Exports data to Azure Blob Storage in various formats
        
        Args:
            data: Data to export (DataFrame, list of dicts, or string)
            azure_path: Azure path (azure://container/blob or just blob if default_container set)
            file_format: File format ('csv', 'parquet', 'json', 'txt')
            compression: Whether to compress the data
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured for export operations")
            return False
            
        try:
            # Parse Azure path
            if azure_path.startswith('azure://'):
                # Full Azure URL
                path_parts = azure_path.replace('azure://', '').split('/', 1)
                container_name = path_parts[0]
                blob_name = path_parts[1] if len(path_parts) > 1 else ''
                container_client = self.blob_service_client.get_container_client(container_name)
            else:
                # Use default container
                if not self.default_container or not self.container_client:
                    logger.error("No Azure container specified and no default container configured")
                    return False
                container_client = self.container_client
                blob_name = azure_path.lstrip('/')
                
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
                    
            # Upload to Azure Blob Storage
            blob_client = container_client.get_blob_client(blob=blob_name)
            blob_client.upload_blob(upload_data, overwrite=True, content_type=content_type)
            
            data_size = len(data) if isinstance(data, (list, pd.DataFrame)) else len(str(data))
            logger.info(f"Exported data to azure://{container_client.container_name}/{blob_name} in {file_format} format "
                       f"(size: {data_size}, compressed: {compression})")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to Azure Blob Storage: {e}")
            return False
            
    def consume_messages_to_azure(self, topic_name: str, azure_export_path: str,
                                 max_messages_per_partition: int = 1000,
                                 file_format: str = 'txt', compression: bool = True) -> Dict[str, Any]:
        """
        Consumes Kafka messages and exports directly to Azure Blob Storage
        
        Args:
            topic_name: Kafka topic name
            azure_export_path: Azure base path for export files
            max_messages_per_partition: Maximum messages per partition
            file_format: Export file format
            compression: Whether to compress exported files
            
        Returns:
            Dictionary with consumption results
        """
        try:
            # Load or initialize state store
            state_store_path = f"{azure_export_path}/state_store.json"
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
                    partition_export_path = f"{azure_export_path}/partition_{partition_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
                    
                    # Consume messages from partition
                    messages = self._consume_partition_messages(
                        topic_name, partition_id, start_offset, end_offset
                    )
                    
                    if messages:
                        # Export messages to Azure
                        export_success = self.export_to_azure(
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
            logger.error(f"Error consuming messages to Azure: {e}")
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
            
    def azure_list_blobs(self, prefix: str, container_name: str = None, max_blobs: int = 1000) -> List[str]:
        """
        List blobs in Azure container with given prefix
        
        Args:
            prefix: Azure prefix to search under
            container_name: Azure container name (uses default if None)
            max_blobs: Maximum number of blobs to return
            
        Returns:
            List of blob names
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return []
            
        try:
            if container_name:
                container_client = self.blob_service_client.get_container_client(container_name)
            elif self.container_client:
                container_client = self.container_client
            else:
                logger.error("No Azure container specified")
                return []
                
            blobs = []
            blob_list = container_client.list_blobs(
                name_starts_with=prefix if prefix else None,
                results_per_page=min(max_blobs, 5000)  # Azure limit
            )
            
            for blob in blob_list:
                blobs.append(blob.name)
                if len(blobs) >= max_blobs:
                    break
                    
            logger.info(f"Listed {len(blobs)} blobs with prefix '{prefix}' in container '{container_client.container_name}'")
            return blobs
            
        except Exception as e:
            logger.error(f"Error listing Azure blobs: {e}")
            return []
            
    def create_sas_url(self, blob_name: str, container_name: str = None, 
                      expiration_hours: int = 24, permissions: str = 'r') -> Optional[str]:
        """
        Create a SAS URL for Azure Blob Storage blob access
        
        Args:
            blob_name: Azure blob name
            container_name: Azure container name (uses default if None)
            expiration_hours: URL expiration time in hours
            permissions: SAS permissions ('r' for read, 'rw' for read/write)
            
        Returns:
            SAS URL string or None if creation fails
        """
        if not self.blob_service_client:
            logger.error("Azure Blob Storage not configured")
            return None
            
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            from datetime import datetime, timedelta
            
            container_name = container_name or self.default_container
            if not container_name:
                logger.error("No Azure container specified")
                return None
                
            if not self.account_key:
                logger.error("Account key required for SAS URL generation")
                return None
                
            # Set permissions
            sas_permissions = BlobSasPermissions(read=True)
            if 'w' in permissions.lower():
                sas_permissions.write = True
                
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=self.account_key,
                permission=sas_permissions,
                expiry=datetime.utcnow() + timedelta(hours=expiration_hours)
            )
            
            # Build SAS URL
            sas_url = f"https://{self.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            
            logger.info(f"Generated SAS URL for blob: {container_name}/{blob_name}")
            return sas_url
            
        except Exception as e:
            logger.error(f"Error creating SAS URL: {e}")
            return None
            
    def get_azure_info(self) -> Dict[str, Any]:
        """
        Get Azure configuration information
        
        Returns:
            Dictionary with Azure info
        """
        return {
            'azure_configured': self.blob_service_client is not None,
            'default_container': self.default_container,
            'account_name': self.account_name,
            'account_url': self.account_url,
            'has_account_key': self.account_key is not None,
            'has_sas_token': self.sas_token is not None,
            'has_connection_string': self.connection_string is not None
        }