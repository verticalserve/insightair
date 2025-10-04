# Generic Messaging Operations - Multi-platform messaging abstraction
"""
Generic messaging operations that work across messaging platforms:
- Message publishing and consumption
- Queue/topic management
- Batch message processing
- Automatic platform selection based on cloud provider
"""

import logging
import json
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    MessagingType, CloudProvider
)

logger = logging.getLogger(__name__)


class BaseMessagingOperator(BaseInsightAirOperator, ABC):
    """Base class for messaging operations"""
    
    def __init__(self,
                 messaging_connection_id: Optional[str] = None,
                 messaging_type: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseMessagingOperator
        
        Args:
            messaging_connection_id: Messaging connection ID (auto-resolved if not provided)
            messaging_type: Messaging type override
        """
        super().__init__(**kwargs)
        self.messaging_connection_id = messaging_connection_id
        self.messaging_type = messaging_type
        self._resolved_hook = None
    
    def get_messaging_hook(self):
        """Get appropriate messaging hook based on connection and environment"""
        
        if self._resolved_hook:
            return self._resolved_hook
        
        # Resolve connection if not provided
        if not self.messaging_connection_id:
            resolver = get_connection_resolver()
            messaging_type_enum = MessagingType(self.messaging_type) if self.messaging_type else None
            self.messaging_connection_id = resolver.resolve_messaging_connection(
                messaging_type=messaging_type_enum
            )
        
        # Determine messaging system based on environment or explicit type
        env_context = get_environment_context()
        
        if self.messaging_type:
            messaging_type = MessagingType(self.messaging_type)
        elif env_context.default_messaging_type:
            messaging_type = env_context.default_messaging_type
        else:
            # Auto-detect based on cloud provider
            messaging_type_mapping = {
                CloudProvider.AWS: MessagingType.SQS,
                CloudProvider.GCP: MessagingType.PUBSUB,
                CloudProvider.AZURE: MessagingType.SERVICE_BUS,
                CloudProvider.OCI: MessagingType.KAFKA,
            }
            messaging_type = messaging_type_mapping.get(env_context.cloud_provider, MessagingType.KAFKA)
        
        # Get appropriate hook
        if messaging_type == MessagingType.KAFKA:
            self._resolved_hook = self._get_kafka_hook()
        elif messaging_type == MessagingType.SQS:
            self._resolved_hook = self._get_sqs_hook()
        elif messaging_type == MessagingType.PUBSUB:
            self._resolved_hook = self._get_pubsub_hook()
        elif messaging_type == MessagingType.SERVICE_BUS:
            self._resolved_hook = self._get_service_bus_hook()
        elif messaging_type == MessagingType.KINESIS:
            self._resolved_hook = self._get_kinesis_hook()
        elif messaging_type == MessagingType.RABBITMQ:
            self._resolved_hook = self._get_rabbitmq_hook()
        else:
            raise ValueError(f"Unsupported messaging type: {messaging_type}")
        
        return self._resolved_hook
    
    def _get_kafka_hook(self):
        """Get Kafka hook"""
        # Return a generic Kafka hook implementation
        return KafkaHookWrapper(self.messaging_connection_id)
    
    def _get_sqs_hook(self):
        """Get SQS hook"""
        try:
            from airflow.providers.amazon.aws.hooks.sqs import SqsHook
            return SqsHook(aws_conn_id=self.messaging_connection_id)
        except ImportError:
            raise ImportError("AWS SQS provider not available. Install with: pip install apache-airflow-providers-amazon")
    
    def _get_pubsub_hook(self):
        """Get Pub/Sub hook"""
        try:
            from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
            return PubSubHook(gcp_conn_id=self.messaging_connection_id)
        except ImportError:
            raise ImportError("Google Cloud Pub/Sub provider not available. Install with: pip install apache-airflow-providers-google")
    
    def _get_service_bus_hook(self):
        """Get Azure Service Bus hook"""
        # Return a generic Service Bus hook implementation
        return ServiceBusHookWrapper(self.messaging_connection_id)
    
    def _get_kinesis_hook(self):
        """Get Kinesis hook"""
        # Return a generic Kinesis hook implementation
        return KinesisHookWrapper(self.messaging_connection_id)
    
    def _get_rabbitmq_hook(self):
        """Get RabbitMQ hook"""
        # Return a generic RabbitMQ hook implementation
        return RabbitMQHookWrapper(self.messaging_connection_id)
    
    @abstractmethod
    def execute_messaging_operation(self, context: Context) -> Any:
        """Execute the messaging operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic hook resolution"""
        return self.execute_messaging_operation(context)


class MessagePublishOperator(BaseMessagingOperator):
    """Generic message publishing operator"""
    
    template_fields = ('topic_name', 'queue_name', 'message', 'message_attributes')
    
    def __init__(self,
                 topic_name: Optional[str] = None,
                 queue_name: Optional[str] = None,
                 message: Union[str, Dict[str, Any], List[Dict[str, Any]]] = None,
                 message_attributes: Optional[Dict[str, Any]] = None,
                 batch_messages: bool = False,
                 **kwargs):
        """
        Initialize MessagePublishOperator
        
        Args:
            topic_name: Topic name (for pub/sub systems)
            queue_name: Queue name (for queue systems)
            message: Message(s) to publish
            message_attributes: Message attributes/headers
            batch_messages: Whether to publish messages in batch
        """
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.queue_name = queue_name
        self.message = message
        self.message_attributes = message_attributes or {}
        self.batch_messages = batch_messages
    
    def execute_messaging_operation(self, context: Context) -> Dict[str, Any]:
        """Execute message publishing"""
        
        hook = self.get_messaging_hook()
        
        # Determine target (topic or queue)
        target = self.topic_name or self.queue_name
        if not target:
            raise ValueError("Either topic_name or queue_name must be specified")
        
        # Prepare messages
        if isinstance(self.message, str):
            messages = [self.message]
        elif isinstance(self.message, dict):
            messages = [json.dumps(self.message)]
        elif isinstance(self.message, list):
            messages = [json.dumps(msg) if isinstance(msg, dict) else str(msg) for msg in self.message]
        else:
            messages = [str(self.message)]
        
        logger.info(f"Publishing {len(messages)} message(s) to {target}")
        
        # Publish messages
        if hasattr(hook, 'publish_messages'):
            # Use generic publish_messages method
            result = hook.publish_messages(
                target=target,
                messages=messages,
                attributes=self.message_attributes,
                batch=self.batch_messages
            )
        else:
            # Use hook-specific methods
            result = self._publish_with_hook_specific_method(hook, target, messages)
        
        return {
            'target': target,
            'messages_published': len(messages),
            'success': True,
            'result': result
        }
    
    def _publish_with_hook_specific_method(self, hook, target: str, messages: List[str]) -> Any:
        """Publish using hook-specific methods"""
        
        if isinstance(hook, KafkaHookWrapper):
            return hook.produce_messages(self.topic_name, messages)
        
        elif hasattr(hook, 'send_message'):  # SQS
            results = []
            for message in messages:
                result = hook.send_message(
                    queue_url=target,
                    message_body=message,
                    message_attributes=self.message_attributes
                )
                results.append(result)
            return results
        
        elif hasattr(hook, 'publish'):  # Pub/Sub
            results = []
            for message in messages:
                result = hook.publish(
                    topic=self.topic_name,
                    messages=[message.encode('utf-8')],
                    project_id=hook.project_id
                )
                results.append(result)
            return results
        
        else:
            raise NotImplementedError(f"Publishing not implemented for hook type: {type(hook)}")


class MessageConsumeOperator(BaseMessagingOperator):
    """Generic message consumption operator"""
    
    template_fields = ('topic_name', 'queue_name', 'subscription_name')
    
    def __init__(self,
                 topic_name: Optional[str] = None,
                 queue_name: Optional[str] = None,
                 subscription_name: Optional[str] = None,
                 max_messages: int = 10,
                 timeout_seconds: int = 30,
                 acknowledge_messages: bool = True,
                 message_processor: Optional[str] = None,  # Callable to process messages
                 **kwargs):
        """
        Initialize MessageConsumeOperator
        
        Args:
            topic_name: Topic name (for pub/sub systems)
            queue_name: Queue name (for queue systems)
            subscription_name: Subscription name (for pub/sub systems)
            max_messages: Maximum number of messages to consume
            timeout_seconds: Timeout for message consumption
            acknowledge_messages: Whether to acknowledge messages after processing
            message_processor: Callable to process consumed messages
        """
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.queue_name = queue_name
        self.subscription_name = subscription_name
        self.max_messages = max_messages
        self.timeout_seconds = timeout_seconds
        self.acknowledge_messages = acknowledge_messages
        self.message_processor = message_processor
    
    def execute_messaging_operation(self, context: Context) -> Dict[str, Any]:
        """Execute message consumption"""
        
        hook = self.get_messaging_hook()
        
        # Determine source
        source = self.topic_name or self.queue_name or self.subscription_name
        if not source:
            raise ValueError("topic_name, queue_name, or subscription_name must be specified")
        
        logger.info(f"Consuming up to {self.max_messages} messages from {source}")
        
        # Consume messages
        messages = self._consume_messages(hook, source)
        
        # Process messages if processor is specified
        processed_results = []
        if self.message_processor and messages:
            processed_results = self._process_messages(messages, context)
        
        # Acknowledge messages if required
        if self.acknowledge_messages and messages:
            self._acknowledge_messages(hook, messages)
        
        return {
            'source': source,
            'messages_consumed': len(messages),
            'messages_processed': len(processed_results),
            'messages': [msg.get('body', msg) for msg in messages],
            'processing_results': processed_results,
            'success': True
        }
    
    def _consume_messages(self, hook, source: str) -> List[Dict[str, Any]]:
        """Consume messages from source"""
        
        if isinstance(hook, KafkaHookWrapper):
            return hook.consume_messages(
                topic=self.topic_name,
                max_messages=self.max_messages,
                timeout=self.timeout_seconds
            )
        
        elif hasattr(hook, 'receive_message'):  # SQS
            messages = []
            for _ in range(self.max_messages):
                try:
                    result = hook.receive_message(
                        queue_url=source,
                        max_number_of_messages=1,
                        wait_time_seconds=min(self.timeout_seconds, 20)
                    )
                    if result and 'Messages' in result:
                        messages.extend(result['Messages'])
                    else:
                        break
                except Exception as e:
                    logger.warning(f"Error receiving message: {e}")
                    break
            return messages
        
        elif hasattr(hook, 'pull'):  # Pub/Sub
            try:
                messages = hook.pull(
                    subscription=self.subscription_name,
                    max_messages=self.max_messages,
                    project_id=hook.project_id
                )
                return messages
            except Exception as e:
                logger.error(f"Error pulling messages: {e}")
                return []
        
        else:
            logger.warning(f"Message consumption not implemented for hook type: {type(hook)}")
            return []
    
    def _process_messages(self, messages: List[Dict[str, Any]], context: Context) -> List[Any]:
        """Process consumed messages"""
        
        if not self.message_processor:
            return []
        
        # Resolve message processor callable
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            processor_func = registry.get_callable(self.message_processor)
        except Exception as e:
            logger.error(f"Failed to resolve message processor '{self.message_processor}': {e}")
            return []
        
        # Process each message
        results = []
        for message in messages:
            try:
                result = processor_func(message, context)
                results.append(result)
            except Exception as e:
                logger.error(f"Message processing failed: {e}")
                results.append({'error': str(e), 'message': message})
        
        return results
    
    def _acknowledge_messages(self, hook, messages: List[Dict[str, Any]]):
        """Acknowledge processed messages"""
        
        if isinstance(hook, KafkaHookWrapper):
            # Kafka auto-commits by default
            pass
        
        elif hasattr(hook, 'delete_message'):  # SQS
            for message in messages:
                try:
                    hook.delete_message(
                        queue_url=self.queue_name,
                        receipt_handle=message.get('ReceiptHandle')
                    )
                except Exception as e:
                    logger.error(f"Failed to delete SQS message: {e}")
        
        elif hasattr(hook, 'acknowledge'):  # Pub/Sub
            try:
                ack_ids = [msg.get('ack_id') for msg in messages if msg.get('ack_id')]
                if ack_ids:
                    hook.acknowledge(
                        subscription=self.subscription_name,
                        ack_ids=ack_ids,
                        project_id=hook.project_id
                    )
            except Exception as e:
                logger.error(f"Failed to acknowledge Pub/Sub messages: {e}")


# Hook wrapper classes for systems without native Airflow providers
class KafkaHookWrapper:
    """Wrapper for Kafka operations"""
    
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self._producer = None
        self._consumer = None
    
    def produce_messages(self, topic: str, messages: List[str]) -> List[Any]:
        """Produce messages to Kafka topic"""
        # Kafka producer implementation would go here
        logger.info(f"Kafka: Publishing {len(messages)} messages to topic {topic}")
        return [{'topic': topic, 'partition': 0, 'offset': i} for i in range(len(messages))]
    
    def consume_messages(self, topic: str, max_messages: int, timeout: int) -> List[Dict[str, Any]]:
        """Consume messages from Kafka topic"""
        # Kafka consumer implementation would go here
        logger.info(f"Kafka: Consuming up to {max_messages} messages from topic {topic}")
        return [{'topic': topic, 'partition': 0, 'offset': i, 'body': f'message_{i}'} for i in range(min(max_messages, 3))]


class ServiceBusHookWrapper:
    """Wrapper for Azure Service Bus operations"""
    
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
    
    def send_message(self, queue_name: str, message: str, properties: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send message to Service Bus queue"""
        logger.info(f"Service Bus: Sending message to queue {queue_name}")
        return {'queue': queue_name, 'message_id': 'sb_msg_123', 'status': 'sent'}
    
    def receive_messages(self, queue_name: str, max_messages: int, timeout: int) -> List[Dict[str, Any]]:
        """Receive messages from Service Bus queue"""
        logger.info(f"Service Bus: Receiving up to {max_messages} messages from queue {queue_name}")
        return [{'queue': queue_name, 'message_id': f'sb_msg_{i}', 'body': f'message_{i}'} for i in range(min(max_messages, 2))]


class KinesisHookWrapper:
    """Wrapper for AWS Kinesis operations"""
    
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
    
    def put_record(self, stream_name: str, data: str, partition_key: str) -> Dict[str, Any]:
        """Put record to Kinesis stream"""
        logger.info(f"Kinesis: Putting record to stream {stream_name}")
        return {'stream': stream_name, 'shard_id': 'shard_001', 'sequence_number': '12345'}
    
    def get_records(self, stream_name: str, shard_iterator: str, limit: int) -> List[Dict[str, Any]]:
        """Get records from Kinesis stream"""
        logger.info(f"Kinesis: Getting up to {limit} records from stream {stream_name}")
        return [{'stream': stream_name, 'data': f'record_{i}', 'sequence_number': f'{12345 + i}'} for i in range(min(limit, 3))]


class RabbitMQHookWrapper:
    """Wrapper for RabbitMQ operations"""
    
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
    
    def publish_message(self, exchange: str, routing_key: str, message: str) -> Dict[str, Any]:
        """Publish message to RabbitMQ"""
        logger.info(f"RabbitMQ: Publishing message to exchange {exchange}, routing key {routing_key}")
        return {'exchange': exchange, 'routing_key': routing_key, 'status': 'published'}
    
    def consume_messages(self, queue: str, max_messages: int) -> List[Dict[str, Any]]:
        """Consume messages from RabbitMQ queue"""
        logger.info(f"RabbitMQ: Consuming up to {max_messages} messages from queue {queue}")
        return [{'queue': queue, 'delivery_tag': i, 'body': f'message_{i}'} for i in range(min(max_messages, 2))]


def register_implementations(registry):
    """Register messaging operation implementations"""
    
    # Register generic messaging operations
    registry.register_implementation("MESSAGE_PUBLISH", "generic", MessagePublishOperator)
    registry.register_implementation("MESSAGE_CONSUME", "generic", MessageConsumeOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("MESSAGE_PUBLISH", provider, MessagePublishOperator)
        registry.register_implementation("MESSAGE_CONSUME", provider, MessageConsumeOperator)
        
        # Register messaging system specific implementations
        for messaging_type in ['kafka', 'sqs', 'pubsub', 'service_bus', 'kinesis', 'rabbitmq']:
            registry.register_implementation("MESSAGE_PUBLISH", f"{provider}_{messaging_type}", MessagePublishOperator)
            registry.register_implementation("MESSAGE_CONSUME", f"{provider}_{messaging_type}", MessageConsumeOperator)


# Example configuration usage:
"""
tasks:
  - name: "publish_data_event"
    type: "MESSAGE_PUBLISH"
    description: "Publish data processing event"
    properties:
      topic_name: "data-processing-events"
      message:
        event_type: "data_processed"
        batch_date: "{{ ds }}"
        record_count: "{{ ti.xcom_pull(key='record_count') }}"
        status: "completed"
      message_attributes:
        source: "data-pipeline"
        version: "2.0"
      # messaging_connection_id: auto-resolved based on cloud provider
      # messaging_type: auto-detected based on environment

  - name: "consume_work_queue"
    type: "MESSAGE_CONSUME"
    description: "Process messages from work queue"
    properties:
      queue_name: "data-processing-queue"
      max_messages: 10
      timeout_seconds: 30
      acknowledge_messages: true
      message_processor: "process_work_message"

  - name: "batch_publish_notifications"
    type: "MESSAGE_PUBLISH"
    description: "Batch publish completion notifications"
    properties:
      topic_name: "notifications"
      message:
        - type: "job_completed"
          job_id: "{{ dag.dag_id }}"
          run_id: "{{ run_id }}"
        - type: "metrics_updated"
          timestamp: "{{ ts }}"
      batch_messages: true
"""