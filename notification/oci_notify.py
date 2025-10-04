import oci
from .base import Notifier
from utils.logging import get_logger

log = get_logger(__name__)

class OCINotifier(Notifier):
    """
    OCI Notifications Service notifier for job status events.
    """

    def __init__(self, config: dict):
        """
        config should include:
          - oci_config: dict or config-file path (optional)
          - topic_id: str
          - message_attributes: dict (optional)
        """
        super().__init__(config)
        oci_cfg = config.get("oci_config")
        if oci_cfg is None:
            cfg = oci.config.from_file()
        elif isinstance(oci_cfg, str):
            cfg = oci.config.from_file(oci_cfg)
        elif "config_file" in oci_cfg:
            cfg = oci.config.from_file(oci_cfg["config_file"], oci_cfg.get("profile", "DEFAULT"))
        else:
            cfg = oci_cfg

        self.client = oci.notification.NotificationClient(cfg)
        self.topic_id = config["topic_id"]

    def send(self, subject: str, message: str, attributes: dict):
        try:
            resp = self.client.publish_message(
                topic_id=self.topic_id,
                message=message,
                subject=subject
            )
            log.info(f"Published to OCI Notifications topic {self.topic_id}, response={resp.data}")
        except Exception as e:
            log.error(f"Failed to publish OCI notification: {e}")
            raise
