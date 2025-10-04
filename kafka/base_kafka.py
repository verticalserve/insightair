"""
Base Kafka Operations Class
Provides core Kafka operations for data streaming and consumption across cloud platforms
"""

import logging
import json
import re
import bz2
import io
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple, Generator
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError
    from kafka.admin import KafkaAdminClient, NewTopic
    import avro.schema
    import avro.io
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.avro import AvroConsumer
    KAFKA_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Kafka libraries not available: {e}")
    KAFKA_AVAILABLE = False


class TopicDoesNotExistError(Exception):
    """Exception raised when a Kafka topic does not exist"""
    pass


class MultipleSchemaUseInTopicError(Exception):
    """Exception raised when multiple schemas are used in a topic but not handled"""
    pass


class KafkaOperationError(Exception):
    """Exception raised for general Kafka operation errors"""
    pass


class BaseKafkaOperations(ABC):
    """
    Base class for Kafka operations across different cloud platforms
    Provides common functionality for Kafka topic management, consumption, and data processing
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base Kafka operations
        
        Args:
            config: Configuration dictionary containing Kafka and cloud settings
                - bootstrap_servers: Kafka broker list
                - security_protocol: Security protocol (PLAINTEXT, SSL, SASL_SSL)
                - ssl_cafile: Path to CA certificate file (for SSL)
                - ssl_certfile: Path to client certificate file (for SSL)
                - ssl_keyfile: Path to client key file (for SSL)
                - sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, etc.)
                - sasl_username: SASL username
                - sasl_password: SASL password
                - schema_registry_url: Schema registry URL
                - schema_registry_auth: Schema registry authentication
                - consumer_group_id: Consumer group ID
                - auto_offset_reset: Auto offset reset policy
                - max_poll_records: Maximum records per poll
                - session_timeout_ms: Session timeout in milliseconds
                - request_timeout_ms: Request timeout in milliseconds
        """
        self.config = config
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.security_protocol = config.get('security_protocol', 'PLAINTEXT')
        self.ssl_cafile = config.get('ssl_cafile')
        self.ssl_certfile = config.get('ssl_certfile')
        self.ssl_keyfile = config.get('ssl_keyfile')
        self.sasl_mechanism = config.get('sasl_mechanism')
        self.sasl_username = config.get('sasl_username')
        self.sasl_password = config.get('sasl_password')
        
        # Schema Registry settings
        self.schema_registry_url = config.get('schema_registry_url')
        self.schema_registry_auth = config.get('schema_registry_auth')
        
        # Consumer settings
        self.consumer_group_id = config.get('consumer_group_id', 'default_consumer_group')
        self.auto_offset_reset = config.get('auto_offset_reset', 'earliest')
        self.max_poll_records = config.get('max_poll_records', 500)
        self.session_timeout_ms = config.get('session_timeout_ms', 30000)
        self.request_timeout_ms = config.get('request_timeout_ms', 60000)
        
        # Initialize clients
        self.consumer = None
        self.producer = None
        self.admin_client = None
        self.schema_registry_client = None
        
        if KAFKA_AVAILABLE:
            try:
                self._initialize_kafka_clients()
            except Exception as e:
                logger.error(f"Failed to initialize Kafka clients: {e}")
                raise KafkaOperationError(f"Kafka client initialization failed: {e}")
        else:
            logger.warning("Kafka libraries not available")
            
    def _initialize_kafka_clients(self):
        """
        Initialize Kafka clients (consumer, producer, admin)
        """
        try:
            # Common configuration
            common_config = {
                'bootstrap_servers': self.bootstrap_servers,
                'security_protocol': self.security_protocol
            }
            
            # Add SSL configuration if specified
            if self.security_protocol in ['SSL', 'SASL_SSL']:
                if self.ssl_cafile:
                    common_config['ssl_cafile'] = self.ssl_cafile
                if self.ssl_certfile:
                    common_config['ssl_certfile'] = self.ssl_certfile
                if self.ssl_keyfile:
                    common_config['ssl_keyfile'] = self.ssl_keyfile
                    
            # Add SASL configuration if specified
            if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                if self.sasl_mechanism:
                    common_config['sasl_mechanism'] = self.sasl_mechanism
                if self.sasl_username:
                    common_config['sasl_plain_username'] = self.sasl_username
                if self.sasl_password:
                    common_config['sasl_plain_password'] = self.sasl_password
                    
            # Initialize admin client
            self.admin_client = KafkaAdminClient(**common_config)
            
            # Initialize consumer
            consumer_config = common_config.copy()
            consumer_config.update({
                'group_id': self.consumer_group_id,
                'auto_offset_reset': self.auto_offset_reset,
                'max_poll_records': self.max_poll_records,
                'session_timeout_ms': self.session_timeout_ms,
                'request_timeout_ms': self.request_timeout_ms,
                'value_deserializer': lambda x: x.decode('utf-8') if x else None,
                'enable_auto_commit': False
            })
            
            self.consumer = KafkaConsumer(**consumer_config)
            
            # Initialize producer
            producer_config = common_config.copy()
            producer_config.update({
                'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
                'acks': 'all',
                'retries': 3
            })
            
            self.producer = KafkaProducer(**producer_config)
            
            # Initialize schema registry client if URL provided
            if self.schema_registry_url:
                sr_config = {'url': self.schema_registry_url}
                if self.schema_registry_auth:
                    sr_config.update(self.schema_registry_auth)
                self.schema_registry_client = SchemaRegistryClient(sr_config)
                
            logger.info("Kafka clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka clients: {e}")
            raise
            
    def check_target_topic_existence(self, topic_name: str) -> bool:
        """
        Verifies if the target Kafka topic exists on the cluster
        
        Args:
            topic_name: Name of the topic to check
            
        Returns:
            True if topic exists, False otherwise
            
        Raises:
            KafkaOperationError: If operation fails
        """
        if not self.admin_client:
            raise KafkaOperationError("Kafka admin client not initialized")
            
        try:
            # Get cluster metadata
            metadata = self.admin_client.list_consumer_groups()
            
            # Use consumer to get topic metadata
            consumer_metadata = self.consumer.list_consumer_group_offsets(self.consumer_group_id)
            
            # Alternative approach: try to get topic metadata directly
            try:
                topic_metadata = self.consumer.partitions_for_topic(topic_name)
                exists = topic_metadata is not None and len(topic_metadata) > 0
                
                logger.info(f"Topic '{topic_name}' existence check: {exists}")
                return exists
                
            except Exception:
                # If direct check fails, try listing all topics
                all_topics = self.consumer.topics()
                exists = topic_name in all_topics
                
                logger.info(f"Topic '{topic_name}' existence check (via topic list): {exists}")
                return exists
                
        except Exception as e:
            logger.error(f"Error checking topic existence: {e}")
            raise KafkaOperationError(f"Failed to check topic existence: {e}")
            
    def get_topic_metadata(self, topic_name: str) -> Dict[str, Any]:
        """
        Retrieves topic partition metadata including watermark offsets and message counts
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary with topic metadata
            
        Raises:
            TopicDoesNotExistError: If topic doesn't exist
            KafkaOperationError: If operation fails
        """
        if not self.check_target_topic_existence(topic_name):
            raise TopicDoesNotExistError(f"Topic '{topic_name}' does not exist")
            
        try:
            # Get partition information
            partitions = self.consumer.partitions_for_topic(topic_name)
            if not partitions:
                raise TopicDoesNotExistError(f"No partitions found for topic '{topic_name}'")
                
            topic_metadata = {
                'topic_name': topic_name,
                'partition_count': len(partitions),
                'partitions': {}
            }
            
            # Get metadata for each partition
            for partition_id in partitions:
                topic_partition = TopicPartition(topic_name, partition_id)
                
                # Get low and high water marks
                low_offset, high_offset = self.consumer.get_watermark_offsets(topic_partition)
                
                # Calculate message count
                message_count = high_offset - low_offset
                
                partition_metadata = {
                    'partition_id': partition_id,
                    'low_water_mark': low_offset,
                    'high_water_mark': high_offset,
                    'message_count': message_count,
                    'last_updated': datetime.now().isoformat()
                }
                
                topic_metadata['partitions'][partition_id] = partition_metadata
                
            # Add summary statistics
            total_messages = sum(p['message_count'] for p in topic_metadata['partitions'].values())
            topic_metadata['total_message_count'] = total_messages
            topic_metadata['metadata_retrieved_at'] = datetime.now().isoformat()
            
            logger.info(f"Retrieved metadata for topic '{topic_name}': "
                       f"{len(partitions)} partitions, {total_messages} total messages")
            
            return topic_metadata
            
        except Exception as e:
            logger.error(f"Error getting topic metadata: {e}")
            raise KafkaOperationError(f"Failed to get topic metadata: {e}")
            
    @abstractmethod
    def collect_state_store(self, state_store_path: str) -> Dict[str, Any]:
        """
        Reads partition offset state from cloud storage
        
        Args:
            state_store_path: Path to state store file in cloud storage
            
        Returns:
            Dictionary with partition offset states
        """
        pass
        
    @abstractmethod
    def save_state_store(self, state_store_path: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves partition offset state to cloud storage
        
        Args:
            state_store_path: Path to state store file in cloud storage
            state_data: Dictionary with partition offset states
            
        Returns:
            True if save successful, False otherwise
        """
        pass
        
    def extend_statestore_with_new_partitions(self, topic_name: str, 
                                            state_store: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds new partitions to state store with default offset 0
        
        Args:
            topic_name: Name of the topic
            state_store: Current state store data
            
        Returns:
            Updated state store with new partitions
            
        Raises:
            KafkaOperationError: If operation fails
        """
        try:
            # Get current topic metadata
            topic_metadata = self.get_topic_metadata(topic_name)
            current_partitions = set(topic_metadata['partitions'].keys())
            
            # Get partitions from state store
            state_partitions = set()
            if 'partitions' in state_store:
                state_partitions = set(state_store['partitions'].keys())
                
            # Find new partitions
            new_partitions = current_partitions - state_partitions
            
            if new_partitions:
                logger.info(f"Found {len(new_partitions)} new partitions for topic '{topic_name}': {new_partitions}")
                
                # Initialize state store structure if not exists
                if 'partitions' not in state_store:
                    state_store['partitions'] = {}
                    
                # Add new partitions with default offset 0
                for partition_id in new_partitions:
                    state_store['partitions'][str(partition_id)] = {
                        'last_offset': 0,
                        'created_at': datetime.now().isoformat(),
                        'status': 'new'
                    }
                    
                # Update metadata
                state_store['topic_name'] = topic_name
                state_store['partition_count'] = len(current_partitions)
                state_store['last_updated'] = datetime.now().isoformat()
                
                logger.info(f"Added {len(new_partitions)} new partitions to state store")
                
            return state_store
            
        except Exception as e:
            logger.error(f"Error extending state store with new partitions: {e}")
            raise KafkaOperationError(f"Failed to extend state store: {e}")
            
    def get_target_offset_range(self, topic_name: str, state_store: Dict[str, Any],
                              max_messages_per_partition: int = 1000) -> Dict[int, Tuple[int, int]]:
        """
        Calculates start/end offset ranges for each partition based on state store and message count limits
        
        Args:
            topic_name: Name of the topic
            state_store: Current state store data
            max_messages_per_partition: Maximum messages to consume per partition
            
        Returns:
            Dictionary mapping partition ID to (start_offset, end_offset) tuples
            
        Raises:
            KafkaOperationError: If operation fails
        """
        try:
            # Get topic metadata
            topic_metadata = self.get_topic_metadata(topic_name)
            
            offset_ranges = {}
            
            for partition_id, partition_info in topic_metadata['partitions'].items():
                low_water_mark = partition_info['low_water_mark']
                high_water_mark = partition_info['high_water_mark']
                
                # Get last consumed offset from state store
                last_offset = 0
                if ('partitions' in state_store and 
                    str(partition_id) in state_store['partitions']):
                    last_offset = state_store['partitions'][str(partition_id)].get('last_offset', 0)
                    
                # Calculate start offset (next message after last consumed)
                start_offset = max(last_offset + 1, low_water_mark)
                
                # Calculate end offset (limited by max messages)
                max_end_offset = start_offset + max_messages_per_partition - 1
                end_offset = min(max_end_offset, high_water_mark - 1)
                
                # Ensure valid range
                if start_offset <= end_offset and start_offset < high_water_mark:
                    offset_ranges[partition_id] = (start_offset, end_offset)
                    logger.debug(f"Partition {partition_id}: offset range {start_offset}-{end_offset}")
                else:
                    logger.debug(f"Partition {partition_id}: no new messages to consume")
                    
            logger.info(f"Calculated offset ranges for {len(offset_ranges)} partitions")
            return offset_ranges
            
        except Exception as e:
            logger.error(f"Error calculating target offset ranges: {e}")
            raise KafkaOperationError(f"Failed to calculate offset ranges: {e}")
            
    def remove_control_chars(self, text: str) -> str:
        """
        Removes control characters from message content
        
        Args:
            text: Input text string
            
        Returns:
            Text with control characters removed
        """
        if not isinstance(text, str):
            return str(text) if text is not None else ""
            
        # Remove control characters (ASCII 0-31 except tab, newline, carriage return)
        control_chars = ''.join(chr(i) for i in range(32) if i not in [9, 10, 13])
        translator = str.maketrans('', '', control_chars)
        return text.translate(translator)
        
    def remove_non_printable_chars(self, text: str) -> str:
        """
        Filters non-printable characters
        
        Args:
            text: Input text string
            
        Returns:
            Text with non-printable characters removed
        """
        if not isinstance(text, str):
            return str(text) if text is not None else ""
            
        # Keep only printable ASCII characters and common whitespace
        printable_pattern = re.compile(r'[^\x20-\x7E\t\n\r]')
        return printable_pattern.sub('', text)
        
    def format_message_as_pipe_delimited(self, message: Dict[str, Any], 
                                       schema_id: Optional[int] = None) -> str:
        """
        Converts message to pipe-delimited format
        
        Args:
            message: Message dictionary
            schema_id: Schema ID for the message
            
        Returns:
            Pipe-delimited string representation
        """
        try:
            # Handle null message
            if message is None:
                return ""
                
            # Convert message values to strings and clean
            values = []
            for key, value in message.items():
                if value is None:
                    clean_value = "NULL"
                else:
                    str_value = str(value)
                    # Remove control and non-printable characters
                    clean_value = self.remove_non_printable_chars(
                        self.remove_control_chars(str_value)
                    )
                    # Replace pipe characters to avoid delimiter conflicts
                    clean_value = clean_value.replace('|', '¦')
                    
                values.append(clean_value)
                
            # Add schema ID if provided
            if schema_id is not None:
                values.append(str(schema_id))
                
            return '|'.join(values)
            
        except Exception as e:
            logger.error(f"Error formatting message as pipe-delimited: {e}")
            return str(message) if message else ""
            
    def parse_file_size_limit(self, size_str: str) -> int:
        """
        Parses file size limit string and converts to bytes
        
        Args:
            size_str: Size string like '100MB', '1GB', '500KB'
            
        Returns:
            Size in bytes
            
        Raises:
            ValueError: If size string format is invalid
        """
        if not size_str:
            return 0
            
        size_str = size_str.upper().strip()
        
        # Extract number and unit
        match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$', size_str)
        if not match:
            raise ValueError(f"Invalid size format: {size_str}")
            
        number = float(match.group(1))
        unit = match.group(2)
        
        # Convert to bytes
        multipliers = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 ** 2,
            'GB': 1024 ** 3,
            'TB': 1024 ** 4
        }
        
        multiplier = multipliers.get(unit, 1)
        return int(number * multiplier)
        
    def create_header_record(self, topic_name: str, partition_id: int, 
                           offset_range: Tuple[int, int], schema_id: Optional[int] = None,
                           additional_metadata: Dict[str, Any] = None) -> str:
        """
        Creates header record with metadata
        
        Args:
            topic_name: Kafka topic name
            partition_id: Partition ID
            offset_range: Tuple of (start_offset, end_offset)
            schema_id: Schema ID if applicable
            additional_metadata: Additional metadata to include
            
        Returns:
            Header record string
        """
        try:
            start_offset, end_offset = offset_range
            
            header_data = {
                'type': 'HEADER',
                'topic': topic_name,
                'partition': partition_id,
                'start_offset': start_offset,
                'end_offset': end_offset,
                'timestamp': datetime.now().isoformat(),
                'message_count': end_offset - start_offset + 1
            }
            
            if schema_id is not None:
                header_data['schema_id'] = schema_id
                
            if additional_metadata:
                header_data.update(additional_metadata)
                
            # Format as pipe-delimited
            return self.format_message_as_pipe_delimited(header_data)
            
        except Exception as e:
            logger.error(f"Error creating header record: {e}")
            return f"HEADER|{topic_name}|{partition_id}|{datetime.now().isoformat()}"
            
    def compress_data(self, data: str) -> bytes:
        """
        Compresses data using bz2 compression
        
        Args:
            data: String data to compress
            
        Returns:
            Compressed data as bytes
        """
        try:
            return bz2.compress(data.encode('utf-8'))
        except Exception as e:
            logger.error(f"Error compressing data: {e}")
            return data.encode('utf-8')
            
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information (without sensitive data)
        
        Returns:
            Dictionary with connection info
        """
        safe_config = self.config.copy()
        # Remove sensitive information
        sensitive_keys = ['sasl_password', 'ssl_keyfile', 'schema_registry_auth']
        for key in sensitive_keys:
            if key in safe_config:
                safe_config[key] = "***"
        return safe_config