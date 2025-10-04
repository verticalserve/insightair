from google.cloud import pubsub_v1
from .base import Notifier
from utils.logging import get_logger

log = get_logger(__name__)

class PubSubNotifier(Notifier):
    """
    GCP Pub/Sub notifier for job status events.
    """

    def __init__(self, config: dict):
        """
        config should include:
          - topic_path: str  (e.g. "projects/PROJECT/topics/TOPIC")
          - client_kwargs: dict (optional)
          - message_attributes: dict (optional)
        """
        super().__init__(config)
        client_kwargs = config.get("client_kwargs", {})
        self.publisher = pubsub_v1.PublisherClient(**client_kwargs)
        self.topic_path = config["topic_path"]

    def send(self, subject: str, message: str, attributes: dict):
        data = message.encode("utf-8")
        # Flatten attributes to simple str->str
        flat_attrs = {k: v.get("StringValue", "") for k, v in attributes.items()}
        try:
            future = self.publisher.publish(self.topic_path, data, **flat_attrs)
            msg_id = future.result()
            log.info(f"Published to Pub/Sub topic {self.topic_path}, message_id={msg_id}")
        except Exception as e:
            log.error(f"Failed to publish Pub/Sub message: {e}")
            raise
