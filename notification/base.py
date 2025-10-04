from abc import ABC, abstractmethod

class Notifier(ABC):
    """
    Abstract base class for job status notifications.
    """

    STATUS_LABELS = {
        "start": "Started",
        "success": "Succeeded",
        "failure": "Failed",
        "disabled": "Disabled"
    }

    def __init__(self, config: dict):
        """
        config: dict of notifier-specific params, may include:
          - topic_arn / topic_id / topic_path / connection_string, etc.
          - message_attributes: dict of extra attributes
        """
        self.config = config

    def send_job_notification(self, job_name: str, status: str, **kwargs):
        """
        Formats and sends a notification about a job.
        """
        label = self.STATUS_LABELS.get(status.lower(), "Unknown")
        subject = f"{job_name} - {label}"
        lines = [f"Job *{job_name}* has *{label}*.", f"Status: {status}"]
        for k, v in kwargs.items():
            lines.append(f"{k}: {v}")
        message = "\n".join(lines)

        # Default attributes for filtering
        attributes = {
            "job_name": {"DataType": "String", "StringValue": job_name},
            "status":   {"DataType": "String", "StringValue": status}
        }
        # Merge additional attributes if provided
        attributes.update(self.config.get("message_attributes", {}))

        self.send(subject, message, attributes=attributes)

    @abstractmethod
    def send(self, subject: str, message: str, attributes: dict):
        """
        Send the notification with the given subject, body, and message attributes.
        """
        pass
