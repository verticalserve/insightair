from azure.servicebus import ServiceBusClient, ServiceBusMessage
from .base import Notifier
from utils.logging import get_logger

log = get_logger(__name__)

class AzureNotifier(Notifier):
    """
    Azure Service Bus notifier for job status events.
    """

    def __init__(self, config: dict):
        """
        config should include:
          - connection_string: str
          - topic_name: str
          - message_attributes: dict (optional)
        """
        super().__init__(config)
        self.client = ServiceBusClient.from_connection_string(config["connection_string"])
        self.topic_name = config["topic_name"]

    def send(self, subject: str, message: str, attributes: dict):
        try:
            with self.client:
                sender = self.client.get_topic_sender(self.topic_name)
                with sender:
                    props = {k: v.get("StringValue", "") for k, v in attributes.items()}
                    msg = ServiceBusMessage(body=message, subject=subject, application_properties=props)
                    sender.send_messages(msg)
                    log.info(f"Published to Azure Service Bus topic {self.topic_name}")
        except Exception as e:
            log.error(f"Failed to publish to Azure Service Bus: {e}")
            raise
