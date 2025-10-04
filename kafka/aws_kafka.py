"""
AWS Kafka Operations
Kafka operations with AWS S3 integration for state store and data export
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
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError
    AWS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS libraries not available: {e}")
    AWS_AVAILABLE = False


class AWSKafkaOperations(KafkaSchemaOperations):
    """
    Kafka operations with AWS S3 integration
    Combines Kafka functionality with AWS cloud storage capabilities for state management and data export
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AWS Kafka operations
        
        Args:
            config: Configuration dictionary containing both Kafka and AWS settings
                Kafka settings:
                - bootstrap_servers, security_protocol, sasl_*, ssl_*, schema_registry_*
                AWS settings:
                - aws_access_key_id: AWS access key (optional, uses boto3 default chain)
                - aws_secret_access_key: AWS secret key (optional)
                - aws_session_token: AWS session token (optional)
                - region_name: AWS region (default: us-east-1)
                - default_bucket: Default S3 bucket for operations
                - bucket_region: S3 bucket region (optional)
        """
        super().__init__(config)
        
        # AWS configuration
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.aws_session_token = config.get('aws_session_token')
        self.region_name = config.get('region_name', 'us-east-1')
        self.default_bucket = config.get('default_bucket')
        self.bucket_region = config.get('bucket_region', self.region_name)
        
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
            
    def collect_state_store(self, state_store_path: str) -> Dict[str, Any]:
        """
        Reads partition offset state from S3
        
        Args:
            state_store_path: S3 path to state store file (s3://bucket/key or just key if default_bucket set)
            
        Returns:
            Dictionary with partition offset states
            
        Raises:
            KafkaOperationError: If operation fails
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured for state store operations")
            return {}
            
        try:
            # Parse S3 path
            if state_store_path.startswith('s3://'):
                # Full S3 URL
                path_parts = state_store_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No S3 bucket specified and no default bucket configured")
                    return {}
                bucket_name = self.default_bucket
                object_key = state_store_path.lstrip('/')
                
            # Check if state store exists
            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                state_data = json.loads(response['Body'].read().decode('utf-8'))
                
                logger.info(f"Loaded state store from s3://{bucket_name}/{object_key}")
                return state_data
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logger.info(f"State store not found at s3://{bucket_name}/{object_key}, returning empty state")
                    return {}
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Error collecting state store from S3: {e}")
            from .base_kafka import KafkaOperationError
            raise KafkaOperationError(f"Failed to collect state store: {e}")
            
    def save_state_store(self, state_store_path: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves partition offset state to S3
        
        Args:
            state_store_path: S3 path to state store file
            state_data: Dictionary with partition offset states
            
        Returns:
            True if save successful, False otherwise
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured for state store operations")
            return False
            
        try:
            # Parse S3 path
            if state_store_path.startswith('s3://'):
                # Full S3 URL
                path_parts = state_store_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No S3 bucket specified and no default bucket configured")
                    return False
                bucket_name = self.default_bucket
                object_key = state_store_path.lstrip('/')
                
            # Add metadata
            state_data['last_saved'] = datetime.now().isoformat()
            state_data['saved_by'] = 'aws_kafka_operations'
            
            # Save to S3
            json_data = json.dumps(state_data, indent=2)
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.info(f"Saved state store to s3://{bucket_name}/{object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state store to S3: {e}")
            return False
            
    def load_schema_dictionary(self, schema_dict_path: str) -> Dict[str, Any]:
        """
        Loads schema definitions from S3 schema dictionary
        
        Args:
            schema_dict_path: S3 path to schema dictionary file
            
        Returns:
            Dictionary with schema definitions
            
        Raises:
            Exception: If load operation fails
        """
        if not self.s3_client:
            raise Exception("AWS S3 not configured for schema dictionary operations")
            
        try:
            # Parse S3 path
            if schema_dict_path.startswith('s3://'):
                # Full S3 URL
                path_parts = schema_dict_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    raise Exception("No S3 bucket specified and no default bucket configured")
                bucket_name = self.default_bucket
                object_key = schema_dict_path.lstrip('/')
                
            # Load schema dictionary from S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            schema_dict = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info(f"Loaded schema dictionary from s3://{bucket_name}/{object_key}")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error loading schema dictionary from S3: {e}")
            raise
            
    def export_to_s3(self, data: Union[pd.DataFrame, List[Dict], str], 
                     s3_path: str, file_format: str = 'csv', compression: bool = False) -> bool:
        """
        Exports data to S3 in various formats
        
        Args:
            data: Data to export (DataFrame, list of dicts, or string)
            s3_path: S3 path (s3://bucket/key or just key if default_bucket set)
            file_format: File format ('csv', 'parquet', 'json', 'txt')
            compression: Whether to compress the data
            
        Returns:
            True if export successful, False otherwise
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured for export operations")
            return False
            
        try:
            # Parse S3 path
            if s3_path.startswith('s3://'):
                # Full S3 URL
                path_parts = s3_path.replace('s3://', '').split('/', 1)
                bucket_name = path_parts[0]
                object_key = path_parts[1] if len(path_parts) > 1 else ''
            else:
                # Use default bucket
                if not self.default_bucket:
                    logger.error("No S3 bucket specified and no default bucket configured")
                    return False
                bucket_name = self.default_bucket
                object_key = s3_path.lstrip('/')
                
            # Prepare data based on format
            if file_format.lower() == 'csv':
                if isinstance(data, pd.DataFrame):
                    data_bytes = data.to_csv(index=False).encode('utf-8')
                elif isinstance(data, list):
                    df = pd.DataFrame(data)
                    data_bytes = df.to_csv(index=False).encode('utf-8')
                else:
                    data_bytes = str(data).encode('utf-8')
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
                        json_data = data.to_json(orient='records', lines=True)
                    else:
                        json_data = json.dumps(data, indent=2)
                    data_bytes = json_data.encode('utf-8')
                else:
                    data_bytes = str(data).encode('utf-8')
                content_type = 'application/json'
                
            elif file_format.lower() == 'txt':
                data_bytes = str(data).encode('utf-8')
                content_type = 'text/plain'
                
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
                
            # Apply compression if requested
            if compression:
                data_bytes = self.compress_data(data_bytes.decode('utf-8'))
                object_key += '.bz2' if not object_key.endswith('.bz2') else ''
                
            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=data_bytes,
                ContentType=content_type
            )
            
            data_size = len(data) if isinstance(data, (list, pd.DataFrame)) else len(str(data))
            logger.info(f"Exported data to s3://{bucket_name}/{object_key} in {file_format} format "
                       f"(size: {data_size}, compressed: {compression})")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to S3: {e}")
            return False
            
    def consume_messages_to_s3(self, topic_name: str, s3_export_path: str,
                              max_messages_per_partition: int = 1000,
                              file_format: str = 'txt', compression: bool = True) -> Dict[str, Any]:
        """
        Consumes Kafka messages and exports directly to S3
        
        Args:
            topic_name: Kafka topic name
            s3_export_path: S3 base path for export files
            max_messages_per_partition: Maximum messages per partition
            file_format: Export file format
            compression: Whether to compress exported files
            
        Returns:
            Dictionary with consumption results
        """
        try:
            # Load or initialize state store
            state_store_path = f"{s3_export_path}/state_store.json"
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
                    partition_export_path = f"{s3_export_path}/partition_{partition_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
                    
                    # Consume messages from partition
                    messages = self._consume_partition_messages(
                        topic_name, partition_id, start_offset, end_offset
                    )
                    
                    if messages:
                        # Export messages to S3
                        export_success = self.export_to_s3(
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
            logger.error(f"Error consuming messages to S3: {e}")
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
            
    def s3_list_objects(self, prefix: str, bucket_name: str = None, max_objects: int = 1000) -> List[str]:
        """
        List objects in S3 bucket with given prefix
        
        Args:
            prefix: S3 prefix to search under
            bucket_name: S3 bucket name (uses default if None)
            max_objects: Maximum number of objects to return
            
        Returns:
            List of object keys
        """
        if not self.s3_client:
            logger.error("AWS S3 not configured")
            return []
            
        try:
            bucket_name = bucket_name or self.default_bucket
            if not bucket_name:
                logger.error("No S3 bucket specified")
                return []
                
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append(obj['Key'])
                        if len(objects) >= max_objects:
                            break
                            
                if len(objects) >= max_objects:
                    break
                    
            logger.info(f"Listed {len(objects)} objects with prefix '{prefix}' in bucket '{bucket_name}'")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []
            
    def get_aws_info(self) -> Dict[str, Any]:
        """
        Get AWS configuration information
        
        Returns:
            Dictionary with AWS info
        """
        return {
            'aws_configured': self.s3_client is not None,
            'default_bucket': self.default_bucket,
            'region_name': self.region_name,
            'bucket_region': self.bucket_region,
            'has_access_key': self.aws_access_key_id is not None,
            'has_session_token': self.aws_session_token is not None
        }