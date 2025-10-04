"""
Kafka Schema Operations
Handles schema validation, testing, and Avro deserialization operations
"""

import logging
import json
from typing import Dict, List, Any, Optional, Set, Tuple
from abc import ABC, abstractmethod

from .base_kafka import BaseKafkaOperations, MultipleSchemaUseInTopicError, KafkaOperationError

logger = logging.getLogger(__name__)

try:
    from kafka import TopicPartition
    import avro.schema
    import avro.io
    from confluent_kafka.avro import AvroConsumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    KAFKA_AVRO_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Kafka Avro libraries not available: {e}")
    KAFKA_AVRO_AVAILABLE = False


class KafkaSchemaOperations(BaseKafkaOperations):
    """
    Extended Kafka operations with schema validation and Avro deserialization support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka schema operations
        
        Args:
            config: Configuration dictionary including schema-specific settings
                - schema_dict_path: Path to schema dictionary in cloud storage
                - allow_multiple_schemas: Whether to allow multiple schemas in topic
                - schema_validation_mode: Schema validation mode ('strict', 'lenient', 'none')
        """
        super().__init__(config)
        
        self.schema_dict_path = config.get('schema_dict_path')
        self.allow_multiple_schemas = config.get('allow_multiple_schemas', False)
        self.schema_validation_mode = config.get('schema_validation_mode', 'strict')
        
        # Schema cache
        self.schema_cache = {}
        self.topic_schema_cache = {}
        
        # Initialize Avro consumer if available
        self.avro_consumer = None
        if KAFKA_AVRO_AVAILABLE and self.schema_registry_client:
            try:
                self._initialize_avro_consumer()
            except Exception as e:
                logger.warning(f"Failed to initialize Avro consumer: {e}")
                
    def _initialize_avro_consumer(self):
        """
        Initialize Avro consumer for schema registry integration
        """
        try:
            if not self.schema_registry_url:
                logger.warning("Schema registry URL not provided, Avro consumer not initialized")
                return
                
            # Configure Avro consumer
            avro_config = {
                'bootstrap.servers': ','.join(self.bootstrap_servers),
                'group.id': self.consumer_group_id,
                'auto.offset.reset': self.auto_offset_reset,
                'schema.registry.url': self.schema_registry_url
            }
            
            # Add security configuration
            if self.security_protocol != 'PLAINTEXT':
                avro_config['security.protocol'] = self.security_protocol
                
            if self.security_protocol in ['SSL', 'SASL_SSL']:
                if self.ssl_cafile:
                    avro_config['ssl.ca.location'] = self.ssl_cafile
                if self.ssl_certfile:
                    avro_config['ssl.certificate.location'] = self.ssl_certfile
                if self.ssl_keyfile:
                    avro_config['ssl.key.location'] = self.ssl_keyfile
                    
            if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                if self.sasl_mechanism:
                    avro_config['sasl.mechanism'] = self.sasl_mechanism
                if self.sasl_username:
                    avro_config['sasl.username'] = self.sasl_username
                if self.sasl_password:
                    avro_config['sasl.password'] = self.sasl_password
                    
            # Add schema registry auth if provided
            if self.schema_registry_auth:
                for key, value in self.schema_registry_auth.items():
                    avro_config[f'schema.registry.{key}'] = value
                    
            self.avro_consumer = AvroConsumer(avro_config)
            logger.info("Avro consumer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Avro consumer: {e}")
            raise
            
    @abstractmethod
    def load_schema_dictionary(self, schema_dict_path: str) -> Dict[str, Any]:
        """
        Loads schema definitions from cloud storage schema dictionary
        
        Args:
            schema_dict_path: Path to schema dictionary file
            
        Returns:
            Dictionary with schema definitions
        """
        pass
        
    def get_schema_from_registry(self, schema_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves schema from schema registry by ID
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema definition dictionary or None if not found
        """
        if not self.schema_registry_client:
            logger.warning("Schema registry client not available")
            return None
            
        try:
            # Check cache first
            if schema_id in self.schema_cache:
                return self.schema_cache[schema_id]
                
            # Get schema from registry
            schema = self.schema_registry_client.get_schema(schema_id)
            
            # Parse schema
            schema_dict = {
                'id': schema_id,
                'version': getattr(schema, 'version', None),
                'schema': schema.schema_str,
                'schema_type': getattr(schema, 'schema_type', 'AVRO')
            }
            
            # Cache schema
            self.schema_cache[schema_id] = schema_dict
            
            logger.debug(f"Retrieved schema {schema_id} from registry")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error retrieving schema {schema_id} from registry: {e}")
            return None
            
    def validate_avro_schema(self, schema_str: str) -> bool:
        """
        Validates Avro schema string
        
        Args:
            schema_str: Avro schema as JSON string
            
        Returns:
            True if schema is valid, False otherwise
        """
        try:
            avro.schema.parse(schema_str)
            return True
        except Exception as e:
            logger.error(f"Invalid Avro schema: {e}")
            return False
            
    def get_schema_use_one_partition(self, topic_name: str, partition_id: int,
                                   sample_size: int = 100) -> Dict[str, Any]:
        """
        Tests schema usage on a single partition
        
        Args:
            topic_name: Name of the topic
            partition_id: Partition ID to test
            sample_size: Number of messages to sample
            
        Returns:
            Dictionary with schema usage information
            
        Raises:
            KafkaOperationError: If operation fails
        """
        try:
            # Create topic partition
            topic_partition = TopicPartition(topic_name, partition_id)
            
            # Get partition metadata
            low_offset, high_offset = self.consumer.get_watermark_offsets(topic_partition)
            
            if high_offset <= low_offset:
                return {
                    'partition_id': partition_id,
                    'message_count': 0,
                    'schemas_found': [],
                    'schema_usage': {}
                }
                
            # Calculate sample offsets
            total_messages = high_offset - low_offset
            step = max(1, total_messages // sample_size)
            sample_offsets = list(range(low_offset, high_offset, step))[:sample_size]
            
            # Assign partition and seek to start
            self.consumer.assign([topic_partition])
            
            schema_usage = {}
            messages_processed = 0
            
            for offset in sample_offsets:
                try:
                    # Seek to specific offset
                    self.consumer.seek(topic_partition, offset)
                    
                    # Poll for message
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if topic_partition in message_batch:
                        messages = message_batch[topic_partition]
                        
                        for message in messages:
                            if message.value:
                                # Try to extract schema ID from message
                                schema_id = self._extract_schema_id_from_message(message)
                                
                                if schema_id:
                                    if schema_id not in schema_usage:
                                        schema_usage[schema_id] = {
                                            'count': 0,
                                            'first_offset': offset,
                                            'last_offset': offset
                                        }
                                    schema_usage[schema_id]['count'] += 1
                                    schema_usage[schema_id]['last_offset'] = offset
                                    
                                messages_processed += 1
                                break  # Only process first message at each offset
                                
                except Exception as e:
                    logger.warning(f"Error processing message at offset {offset}: {e}")
                    continue
                    
            result = {
                'partition_id': partition_id,
                'message_count': total_messages,
                'messages_sampled': messages_processed,
                'sample_size': sample_size,
                'schemas_found': list(schema_usage.keys()),
                'schema_usage': schema_usage,
                'multiple_schemas_detected': len(schema_usage) > 1
            }
            
            logger.info(f"Schema usage test on partition {partition_id}: "
                       f"{len(schema_usage)} schemas found in {messages_processed} messages")
            
            return result
            
        except Exception as e:
            logger.error(f"Error testing schema usage on partition {partition_id}: {e}")
            raise KafkaOperationError(f"Schema usage test failed: {e}")
            
    def get_schema_use_all_partitions(self, topic_name: str, 
                                     sample_size_per_partition: int = 50) -> Dict[str, Any]:
        """
        Validates schema consistency across all partitions
        
        Args:
            topic_name: Name of the topic
            sample_size_per_partition: Number of messages to sample per partition
            
        Returns:
            Dictionary with schema usage across all partitions
            
        Raises:
            MultipleSchemaUseInTopicError: If multiple schemas found and not allowed
            KafkaOperationError: If operation fails
        """
        try:
            # Get topic metadata
            topic_metadata = self.get_topic_metadata(topic_name)
            partitions = list(topic_metadata['partitions'].keys())
            
            all_partition_results = {}
            all_schemas_found = set()
            total_messages_sampled = 0
            
            # Test each partition
            for partition_id in partitions:
                try:
                    partition_result = self.get_schema_use_one_partition(
                        topic_name, partition_id, sample_size_per_partition
                    )
                    
                    all_partition_results[partition_id] = partition_result
                    all_schemas_found.update(partition_result['schemas_found'])
                    total_messages_sampled += partition_result['messages_sampled']
                    
                except Exception as e:
                    logger.warning(f"Failed to test partition {partition_id}: {e}")
                    all_partition_results[partition_id] = {
                        'error': str(e),
                        'schemas_found': []
                    }
                    
            # Analyze results
            multiple_schemas_detected = len(all_schemas_found) > 1
            
            # Check if multiple schemas are allowed
            if multiple_schemas_detected and not self.allow_multiple_schemas:
                error_msg = (f"Multiple schemas detected in topic '{topic_name}': {all_schemas_found}. "
                           f"Set allow_multiple_schemas=True to handle multiple schemas.")
                raise MultipleSchemaUseInTopicError(error_msg)
                
            # Build comprehensive result
            result = {
                'topic_name': topic_name,
                'partition_count': len(partitions),
                'partitions_tested': len([p for p in all_partition_results.values() if 'error' not in p]),
                'total_messages_sampled': total_messages_sampled,
                'all_schemas_found': list(all_schemas_found),
                'multiple_schemas_detected': multiple_schemas_detected,
                'allow_multiple_schemas': self.allow_multiple_schemas,
                'partition_results': all_partition_results,
                'schema_distribution': self._analyze_schema_distribution(all_partition_results),
                'validation_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Schema validation across all partitions: "
                       f"{len(all_schemas_found)} unique schemas found, "
                       f"multiple schemas: {multiple_schemas_detected}")
            
            return result
            
        except MultipleSchemaUseInTopicError:
            raise
        except Exception as e:
            logger.error(f"Error validating schema usage across all partitions: {e}")
            raise KafkaOperationError(f"Schema validation failed: {e}")
            
    def _extract_schema_id_from_message(self, message) -> Optional[int]:
        """
        Extracts schema ID from Kafka message
        
        Args:
            message: Kafka message object
            
        Returns:
            Schema ID if found, None otherwise
        """
        try:
            # For Avro messages, schema ID is typically in the first 5 bytes
            # Byte 0: Magic byte (0x0)
            # Bytes 1-4: Schema ID (big endian)
            
            if hasattr(message, 'value') and message.value:
                if isinstance(message.value, bytes) and len(message.value) >= 5:
                    # Check for Confluent magic byte
                    if message.value[0] == 0:
                        # Extract schema ID from bytes 1-4
                        schema_id = int.from_bytes(message.value[1:5], byteorder='big')
                        return schema_id
                        
            # Alternative: check message headers for schema ID
            if hasattr(message, 'headers') and message.headers:
                for header_key, header_value in message.headers:
                    if header_key.lower() in ['schema-id', 'schema_id']:
                        try:
                            return int(header_value)
                        except (ValueError, TypeError):
                            continue
                            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting schema ID from message: {e}")
            return None
            
    def _analyze_schema_distribution(self, partition_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes schema distribution across partitions
        
        Args:
            partition_results: Results from all partition tests
            
        Returns:
            Schema distribution analysis
        """
        try:
            schema_stats = {}
            
            for partition_id, result in partition_results.items():
                if 'error' in result:
                    continue
                    
                for schema_id, usage in result.get('schema_usage', {}).items():
                    if schema_id not in schema_stats:
                        schema_stats[schema_id] = {
                            'total_count': 0,
                            'partitions_found': [],
                            'first_seen_offset': float('inf'),
                            'last_seen_offset': 0
                        }
                        
                    schema_stats[schema_id]['total_count'] += usage['count']
                    schema_stats[schema_id]['partitions_found'].append(partition_id)
                    schema_stats[schema_id]['first_seen_offset'] = min(
                        schema_stats[schema_id]['first_seen_offset'],
                        usage['first_offset']
                    )
                    schema_stats[schema_id]['last_seen_offset'] = max(
                        schema_stats[schema_id]['last_seen_offset'],
                        usage['last_offset']
                    )
                    
            # Calculate percentages
            total_messages = sum(stats['total_count'] for stats in schema_stats.values())
            
            for schema_id, stats in schema_stats.items():
                stats['percentage'] = (stats['total_count'] / total_messages * 100) if total_messages > 0 else 0
                stats['partition_count'] = len(stats['partitions_found'])
                
            return {
                'schema_count': len(schema_stats),
                'total_messages_analyzed': total_messages,
                'schema_statistics': schema_stats,
                'dominant_schema': max(schema_stats.items(), key=lambda x: x[1]['total_count'])[0] if schema_stats else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing schema distribution: {e}")
            return {'error': str(e)}
            
    def deserialize_avro_message(self, message_bytes: bytes, schema_id: int) -> Optional[Dict[str, Any]]:
        """
        Deserializes Avro message using schema ID
        
        Args:
            message_bytes: Raw message bytes
            schema_id: Schema ID for deserialization
            
        Returns:
            Deserialized message dictionary or None if failed
        """
        try:
            # Get schema from registry or cache
            schema_info = self.get_schema_from_registry(schema_id)
            if not schema_info:
                logger.error(f"Schema {schema_id} not found")
                return None
                
            # Parse Avro schema
            avro_schema = avro.schema.parse(schema_info['schema'])
            
            # Skip Confluent magic byte and schema ID (first 5 bytes)
            payload = message_bytes[5:] if len(message_bytes) > 5 else message_bytes
            
            # Deserialize message
            bytes_reader = io.BytesIO(payload)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(avro_schema)
            
            deserialized_data = reader.read(decoder)
            
            return deserialized_data
            
        except Exception as e:
            logger.error(f"Error deserializing Avro message with schema {schema_id}: {e}")
            return None
            
    def get_topic_schema_info(self, topic_name: str) -> Dict[str, Any]:
        """
        Gets comprehensive schema information for a topic
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary with topic schema information
        """
        try:
            # Check cache first
            if topic_name in self.topic_schema_cache:
                cached_info = self.topic_schema_cache[topic_name]
                # Check if cache is still valid (e.g., less than 1 hour old)
                from datetime import datetime, timedelta
                if datetime.fromisoformat(cached_info['cached_at']) > datetime.now() - timedelta(hours=1):
                    return cached_info
                    
            # Get schema usage across all partitions
            schema_usage = self.get_schema_use_all_partitions(topic_name)
            
            # Get detailed schema information
            schema_details = {}
            for schema_id in schema_usage['all_schemas_found']:
                schema_info = self.get_schema_from_registry(schema_id)
                if schema_info:
                    schema_details[schema_id] = schema_info
                    
            # Load schema dictionary if available
            schema_dict = {}
            if self.schema_dict_path:
                try:
                    schema_dict = self.load_schema_dictionary(self.schema_dict_path)
                except Exception as e:
                    logger.warning(f"Could not load schema dictionary: {e}")
                    
            result = {
                'topic_name': topic_name,
                'schema_usage': schema_usage,
                'schema_details': schema_details,
                'schema_dictionary': schema_dict,
                'cached_at': datetime.now().isoformat()
            }
            
            # Cache result
            self.topic_schema_cache[topic_name] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting topic schema info: {e}")
            raise KafkaOperationError(f"Failed to get topic schema info: {e}")
            
    def clear_schema_cache(self):
        """
        Clears schema and topic caches
        """
        self.schema_cache.clear()
        self.topic_schema_cache.clear()
        logger.info("Schema caches cleared")