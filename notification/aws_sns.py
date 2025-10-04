import boto3
from .base import Notifier
from utils.logging import get_logger

log = get_logger(__name__)

class SNSNotifier(Notifier):
    """
    AWS SNS notifier for job status events.
    """

    def __init__(self, config: dict):
        """
        config should include:
          - topic_arn: str
          - aws_conn_kwargs: dict (optional)
          - message_attributes: dict (optional)
        """
        super().__init__(config)
        aws_kwargs = config.get("aws_conn_kwargs", {})
        self.client = boto3.client("sns", **aws_kwargs)
        self.topic_arn = config["topic_arn"]

    def send(self, subject: str, message: str, attributes: dict):
        try:
            resp = self.client.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message,
                MessageAttributes=attributes
            )
            log.info(f"Published to SNS topic {self.topic_arn}, message_id={resp.get('MessageId')}")
        except Exception as e:
            log.error(f"Failed to publish SNS message: {e}")
            raise
