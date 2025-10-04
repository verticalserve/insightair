# Generic Notification Operations - Multi-cloud notification and alerting
"""
Generic notification operations that work across cloud environments:
- Email notifications with templates and attachments
- Slack, Teams, and other messaging platform integration
- SMS notifications via cloud services
- Push notifications and webhooks
- Multi-channel notification routing
"""

import logging
import smtplib
import json
import requests
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    CloudProvider
)

logger = logging.getLogger(__name__)


class BaseNotificationOperator(BaseInsightAirOperator, ABC):
    """Base class for notification operations"""
    
    def __init__(self,
                 notification_connection_id: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseNotificationOperator
        
        Args:
            notification_connection_id: Notification connection ID (auto-resolved if not provided)
        """
        super().__init__(**kwargs)
        self.notification_connection_id = notification_connection_id
    
    @abstractmethod
    def execute_notification_operation(self, context: Context) -> Any:
        """Execute the notification operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic configuration"""
        return self.execute_notification_operation(context)


class EmailNotificationOperator(BaseNotificationOperator):
    """Generic email notification operator"""
    
    template_fields = ('to_emails', 'subject', 'html_content', 'text_content', 'attachments')
    
    def __init__(self,
                 to_emails: Union[str, List[str]],
                 subject: str,
                 html_content: Optional[str] = None,
                 text_content: Optional[str] = None,
                 cc_emails: Optional[Union[str, List[str]]] = None,
                 bcc_emails: Optional[Union[str, List[str]]] = None,
                 attachments: Optional[List[str]] = None,
                 template_name: Optional[str] = None,
                 template_vars: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize EmailNotificationOperator
        
        Args:
            to_emails: Recipient email addresses
            subject: Email subject
            html_content: HTML email content
            text_content: Plain text email content
            cc_emails: CC email addresses
            bcc_emails: BCC email addresses
            attachments: List of file paths to attach
            template_name: Email template name
            template_vars: Variables for template rendering
        """
        super().__init__(**kwargs)
        self.to_emails = to_emails if isinstance(to_emails, list) else [to_emails]
        self.subject = subject
        self.html_content = html_content
        self.text_content = text_content
        self.cc_emails = cc_emails if isinstance(cc_emails, list) else ([cc_emails] if cc_emails else [])
        self.bcc_emails = bcc_emails if isinstance(bcc_emails, list) else ([bcc_emails] if bcc_emails else [])
        self.attachments = attachments or []
        self.template_name = template_name
        self.template_vars = template_vars or {}
    
    def execute_notification_operation(self, context: Context) -> Dict[str, Any]:
        """Execute email notification"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._execute_aws(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._execute_gcp(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._execute_azure(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._execute_oci(context)
        else:
            return self._execute_generic(context)
    
    def _execute_aws(self, context: Context) -> Dict[str, Any]:
        """Execute on AWS (may use SES)"""
        result = self._execute_generic(context)
        result['platform'] = 'aws'
        
        # AWS SES integration could be added here
        
        return result
    
    def _execute_gcp(self, context: Context) -> Dict[str, Any]:
        """Execute on GCP (may use SendGrid or Gmail API)"""
        result = self._execute_generic(context)
        result['platform'] = 'gcp'
        return result
    
    def _execute_azure(self, context: Context) -> Dict[str, Any]:
        """Execute on Azure (may use Communication Services)"""
        result = self._execute_generic(context)
        result['platform'] = 'azure'  
        return result
    
    def _execute_oci(self, context: Context) -> Dict[str, Any]:
        """Execute on OCI (may use Email Delivery)"""
        result = self._execute_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _execute_generic(self, context: Context) -> Dict[str, Any]:
        """Generic email sending via SMTP"""
        
        logger.info(f"Sending email to {len(self.to_emails)} recipient(s)")
        
        # Resolve connection if not provided
        if not self.notification_connection_id:
            resolver = get_connection_resolver()
            self.notification_connection_id = resolver.resolve_email_connection()
        
        # Get SMTP configuration
        try:
            connection = BaseHook.get_connection(self.notification_connection_id)
            extra = connection.extra_dejson if connection.extra else {}
        except:
            # Use default configuration
            connection = None
            extra = {}
        
        # Prepare email content
        if self.template_name:
            html_content, text_content = self._render_template(context)
        else:
            html_content = self.html_content
            text_content = self.text_content
        
        # Create email message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = self.subject
        msg['From'] = connection.login if connection else extra.get('from_email', 'noreply@example.com')
        msg['To'] = ', '.join(self.to_emails)
        
        if self.cc_emails:
            msg['Cc'] = ', '.join(self.cc_emails)
        
        # Add text content
        if text_content:
            part1 = MIMEText(text_content, 'plain')
            msg.attach(part1)
        
        # Add HTML content
        if html_content:
            part2 = MIMEText(html_content, 'html')
            msg.attach(part2)
        
        # Add attachments
        attachment_info = []
        for attachment_path in self.attachments:
            if os.path.exists(attachment_path):
                attachment_info.append(self._add_attachment(msg, attachment_path))
            else:
                logger.warning(f"Attachment not found: {attachment_path}")
        
        # Send email
        try:
            if connection:
                # Use connection configuration
                smtp_server = connection.host or 'localhost'
                smtp_port = connection.port or 587
                username = connection.login
                password = connection.password
                use_tls = extra.get('use_tls', True)
            else:
                # Use default configuration
                smtp_server = extra.get('smtp_server', 'localhost')
                smtp_port = extra.get('smtp_port', 587)
                username = extra.get('username')
                password = extra.get('password')
                use_tls = extra.get('use_tls', True)
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if use_tls:
                    server.starttls()
                
                if username and password:
                    server.login(username, password)
                
                # Send to all recipients
                all_recipients = self.to_emails + self.cc_emails + self.bcc_emails
                server.send_message(msg, to_addrs=all_recipients)
            
            logger.info("Email sent successfully")
            
            return {
                'notification_type': 'email',
                'recipients': len(all_recipients),
                'to_emails': self.to_emails,
                'cc_emails': self.cc_emails,
                'bcc_emails': self.bcc_emails,
                'subject': self.subject,
                'attachments': len(attachment_info),
                'attachment_info': attachment_info,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            raise
    
    def _render_template(self, context: Context) -> tuple:
        """Render email template with variables"""
        # This is a simplified template rendering
        # In practice, you'd use Jinja2 or similar templating engine
        
        template_vars = {
            **self.template_vars,
            **context,
            'dag_id': context.get('dag').dag_id if context.get('dag') else None,
            'task_id': context.get('task').task_id if context.get('task') else None,
            'execution_date': context.get('execution_date'),
            'run_id': context.get('run_id')
        }
        
        # Simple variable substitution
        html_content = self.html_content or ""
        text_content = self.text_content or ""
        
        for key, value in template_vars.items():
            if isinstance(value, str):
                html_content = html_content.replace(f"{{{{ {key} }}}}", value)
                text_content = text_content.replace(f"{{{{ {key} }}}}", value)
        
        return html_content, text_content
    
    def _add_attachment(self, msg: MIMEMultipart, file_path: str) -> Dict[str, Any]:
        """Add attachment to email"""
        
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        
        filename = os.path.basename(file_path)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {filename}'
        )
        
        msg.attach(part)
        
        file_size = os.path.getsize(file_path)
        
        return {
            'filename': filename,
            'size': file_size,
            'path': file_path
        }


class SlackNotificationOperator(BaseNotificationOperator):
    """Generic Slack notification operator"""
    
    template_fields = ('channel', 'message', 'blocks')
    
    def __init__(self,
                 channel: str,
                 message: Optional[str] = None,
                 blocks: Optional[List[Dict[str, Any]]] = None,
                 username: Optional[str] = None,
                 icon_emoji: Optional[str] = None,
                 attachments: Optional[List[Dict[str, Any]]] = None,
                 **kwargs):
        """
        Initialize SlackNotificationOperator
        
        Args:
            channel: Slack channel (with # or without)
            message: Simple text message
            blocks: Slack blocks for rich formatting
            username: Bot username override
            icon_emoji: Bot icon emoji override
            attachments: Message attachments (legacy)
        """
        super().__init__(**kwargs)
        self.channel = channel if channel.startswith('#') else f'#{channel}'
        self.message = message
        self.blocks = blocks
        self.username = username
        self.icon_emoji = icon_emoji
        self.attachments = attachments
    
    def execute_notification_operation(self, context: Context) -> Dict[str, Any]:
        """Execute Slack notification"""
        
        logger.info(f"Sending Slack message to {self.channel}")
        
        # Resolve connection if not provided
        if not self.notification_connection_id:
            resolver = get_connection_resolver()
            self.notification_connection_id = resolver.resolve_slack_connection()
        
        # Get Slack configuration
        try:
            connection = BaseHook.get_connection(self.notification_connection_id)
            webhook_url = connection.host or connection.password  # Webhook URL
            token = connection.login  # Bot token (if using API)
            extra = connection.extra_dejson if connection.extra else {}
        except:
            raise ValueError("Slack connection not configured")
        
        # Prepare message payload
        payload = {
            'channel': self.channel,
            'username': self.username or extra.get('username', 'Airflow Bot'),
            'icon_emoji': self.icon_emoji or extra.get('icon_emoji', ':robot_face:')
        }
        
        if self.message:
            payload['text'] = self.message
        
        if self.blocks:
            payload['blocks'] = self.blocks
        
        if self.attachments:
            payload['attachments'] = self.attachments
        
        # Send message
        try:
            if webhook_url:
                # Use webhook
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'}
                )
                response.raise_for_status()
            else:
                # Use Slack API (requires token)
                if not token:
                    raise ValueError("Slack token required for API calls")
                
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(
                    'https://slack.com/api/chat.postMessage',
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                
                result = response.json()
                if not result.get('ok'):
                    raise Exception(f"Slack API error: {result.get('error')}")
            
            logger.info("Slack message sent successfully")
            
            return {
                'notification_type': 'slack',
                'channel': self.channel,
                'message_length': len(self.message) if self.message else 0,
                'has_blocks': bool(self.blocks),
                'has_attachments': bool(self.attachments),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to send Slack message: {str(e)}")
            raise


class TeamsNotificationOperator(BaseNotificationOperator):
    """Generic Microsoft Teams notification operator"""
    
    template_fields = ('title', 'text', 'sections')
    
    def __init__(self,
                 title: str,
                 text: Optional[str] = None,
                 sections: Optional[List[Dict[str, Any]]] = None,
                 theme_color: str = "0078D4",
                 **kwargs):
        """
        Initialize TeamsNotificationOperator
        
        Args:
            title: Message title
            text: Message text
            sections: Message sections for rich formatting
            theme_color: Message theme color (hex)
        """
        super().__init__(**kwargs)
        self.title = title
        self.text = text
        self.sections = sections or []
        self.theme_color = theme_color
    
    def execute_notification_operation(self, context: Context) -> Dict[str, Any]:
        """Execute Teams notification"""
        
        logger.info("Sending Microsoft Teams message")
        
        # Resolve connection if not provided
        if not self.notification_connection_id:
            resolver = get_connection_resolver()
            self.notification_connection_id = resolver.resolve_teams_connection()
        
        # Get Teams webhook URL
        try:
            connection = BaseHook.get_connection(self.notification_connection_id)
            webhook_url = connection.host or connection.password
        except:
            raise ValueError("Teams webhook URL not configured")
        
        # Prepare Teams message payload
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": self.title,
            "themeColor": self.theme_color,
            "title": self.title
        }
        
        if self.text:
            payload["text"] = self.text
        
        if self.sections:
            payload["sections"] = self.sections
        
        # Send message
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            logger.info("Teams message sent successfully")
            
            return {
                'notification_type': 'teams',
                'title': self.title,
                'text_length': len(self.text) if self.text else 0,
                'sections_count': len(self.sections),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to send Teams message: {str(e)}")
            raise


class MultiChannelNotificationOperator(BaseNotificationOperator):
    """Send notifications to multiple channels simultaneously"""
    
    template_fields = ('notifications',)
    
    def __init__(self,
                 notifications: List[Dict[str, Any]],
                 fail_on_any_error: bool = False,
                 **kwargs):
        """
        Initialize MultiChannelNotificationOperator
        
        Args:
            notifications: List of notification configurations
            fail_on_any_error: Whether to fail if any notification fails
        """
        super().__init__(**kwargs)
        self.notifications = notifications
        self.fail_on_any_error = fail_on_any_error
    
    def execute_notification_operation(self, context: Context) -> Dict[str, Any]:
        """Execute multi-channel notifications"""
        
        logger.info(f"Sending notifications to {len(self.notifications)} channels")
        
        results = []
        successful_notifications = 0
        failed_notifications = 0
        
        for i, notification_config in enumerate(self.notifications):
            try:
                channel_type = notification_config.get('type', '').lower()
                
                if channel_type == 'email':
                    operator = EmailNotificationOperator(**notification_config.get('config', {}))
                elif channel_type == 'slack':
                    operator = SlackNotificationOperator(**notification_config.get('config', {}))
                elif channel_type == 'teams':
                    operator = TeamsNotificationOperator(**notification_config.get('config', {}))
                else:
                    raise ValueError(f"Unsupported notification type: {channel_type}")
                
                # Execute notification
                result = operator.execute_notification_operation(context)
                result['channel_index'] = i
                result['channel_name'] = notification_config.get('name', f'channel_{i}')
                
                results.append(result)
                successful_notifications += 1
                
                logger.info(f"Notification {i + 1} sent successfully ({channel_type})")
                
            except Exception as e:
                error_result = {
                    'channel_index': i,
                    'channel_name': notification_config.get('name', f'channel_{i}'),
                    'notification_type': notification_config.get('type', 'unknown'),
                    'success': False,
                    'error': str(e)
                }
                
                results.append(error_result)
                failed_notifications += 1
                
                logger.error(f"Notification {i + 1} failed: {str(e)}")
                
                if self.fail_on_any_error:
                    raise
        
        # Store detailed results in XCom
        context['task_instance'].xcom_push(key='notification_results', value=results)
        
        success = failed_notifications == 0 or not self.fail_on_any_error
        
        logger.info(f"Multi-channel notification completed: {successful_notifications}/{len(self.notifications)} successful")
        
        return {
            'notification_type': 'multi_channel',
            'total_channels': len(self.notifications),
            'successful_notifications': successful_notifications,
            'failed_notifications': failed_notifications,
            'success': success,
            'results': results
        }


def register_implementations(registry):
    """Register notification operation implementations"""
    
    # Register generic notification operations
    registry.register_implementation("EMAIL", "generic", EmailNotificationOperator)
    registry.register_implementation("EMAIL_NOTIFICATION", "generic", EmailNotificationOperator)
    registry.register_implementation("SLACK", "generic", SlackNotificationOperator)
    registry.register_implementation("SLACK_NOTIFICATION", "generic", SlackNotificationOperator)
    registry.register_implementation("TEAMS", "generic", TeamsNotificationOperator)
    registry.register_implementation("TEAMS_NOTIFICATION", "generic", TeamsNotificationOperator)
    registry.register_implementation("MULTI_NOTIFICATION", "generic", MultiChannelNotificationOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("EMAIL", provider, EmailNotificationOperator)
        registry.register_implementation("EMAIL_NOTIFICATION", provider, EmailNotificationOperator)
        registry.register_implementation("SLACK", provider, SlackNotificationOperator)
        registry.register_implementation("SLACK_NOTIFICATION", provider, SlackNotificationOperator)
        registry.register_implementation("TEAMS", provider, TeamsNotificationOperator)
        registry.register_implementation("TEAMS_NOTIFICATION", provider, TeamsNotificationOperator)
        registry.register_implementation("MULTI_NOTIFICATION", provider, MultiChannelNotificationOperator)


# Example configuration usage:
"""
tasks:
  - name: "send_completion_email"
    type: "EMAIL"
    description: "Send pipeline completion email"
    properties:
      to_emails: ["admin@company.com", "team@company.com"]
      subject: "Pipeline {{ dag.dag_id }} Completed Successfully"
      html_content: |
        <h2>Pipeline Completion Report</h2>
        <p>The pipeline <strong>{{ dag.dag_id }}</strong> has completed successfully.</p>
        <ul>
          <li>Execution Date: {{ execution_date }}</li>
          <li>Run ID: {{ run_id }}</li>
          <li>Total Records Processed: {{ ti.xcom_pull(key='record_count') }}</li>
        </ul>
      text_content: |
        Pipeline {{ dag.dag_id }} completed successfully.
        Execution Date: {{ execution_date }}
        Records Processed: {{ ti.xcom_pull(key='record_count') }}
      # notification_connection_id: auto-resolved

  - name: "send_slack_alert"
    type: "SLACK"
    description: "Send Slack notification"
    properties:
      channel: "#data-pipeline-alerts"
      message: "🎉 Pipeline {{ dag.dag_id }} completed successfully!"
      blocks:
        - type: "section"
          text:
            type: "mrkdwn"
            text: "*Pipeline Status:* ✅ Success\n*DAG:* {{ dag.dag_id }}\n*Records:* {{ ti.xcom_pull(key='record_count') }}"

  - name: "send_multi_channel_alert"
    type: "MULTI_NOTIFICATION"
    description: "Send notifications to multiple channels"
    properties:
      fail_on_any_error: false
      notifications:
        - name: "email_alert"
          type: "email"
          config:
            to_emails: ["admin@company.com"]
            subject: "Pipeline Alert"
            text_content: "Pipeline completed"
        - name: "slack_alert"
          type: "slack"
          config:
            channel: "#alerts"
            message: "Pipeline completed successfully"
        - name: "teams_alert"
          type: "teams"
          config:
            title: "Pipeline Status"
            text: "Pipeline completed successfully"
"""