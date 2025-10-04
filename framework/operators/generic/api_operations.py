# Generic API Operations - Multi-cloud HTTP/REST API integration
"""
Generic API operations that work across cloud environments:
- HTTP/REST API calls (GET, POST, PUT, DELETE, PATCH)
- API authentication (OAuth, Bearer, API Key, Basic Auth)
- Request/response handling with retries
- Multi-cloud API gateway integration
- Webhook operations
"""

import logging
import json
import requests
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
import time
from urllib.parse import urljoin

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    CloudProvider
)

logger = logging.getLogger(__name__)


class BaseAPIOperator(BaseInsightAirOperator, ABC):
    """Base class for API operations"""
    
    def __init__(self,
                 api_connection_id: Optional[str] = None,
                 base_url: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseAPIOperator
        
        Args:
            api_connection_id: API connection ID (auto-resolved if not provided)
            base_url: Base URL override
        """
        super().__init__(**kwargs)
        self.api_connection_id = api_connection_id
        self.base_url = base_url
        self._session = None
    
    def get_api_session(self):
        """Get configured requests session based on environment"""
        
        if self._session:
            return self._session
        
        # Resolve connection if not provided
        if not self.api_connection_id:
            resolver = get_connection_resolver()
            self.api_connection_id = resolver.resolve_api_connection()
        
        # Get connection details
        try:
            connection = BaseHook.get_connection(self.api_connection_id)
            
            # Create session with authentication
            session = requests.Session()
            
            # Extract connection parameters
            extra = connection.extra_dejson if connection.extra else {}
            
            # Set base URL
            if self.base_url:
                base_url = self.base_url
            elif connection.host:
                scheme = connection.schema or 'https'
                port = f":{connection.port}" if connection.port else ""
                base_url = f"{scheme}://{connection.host}{port}"
            else:
                base_url = extra.get('base_url', '')
            
            session.base_url = base_url
            
            # Configure authentication
            self._configure_authentication(session, connection, extra)
            
            # Configure session defaults
            self._configure_session_defaults(session, extra)
            
            self._session = session
            return self._session
            
        except Exception as e:
            logger.error(f"Failed to create API session: {e}")
            # Create basic session without connection
            session = requests.Session()
            session.base_url = self.base_url or ""
            self._session = session
            return self._session
    
    def _configure_authentication(self, session: requests.Session, connection, extra: Dict[str, Any]):
        """Configure session authentication based on connection type"""
        
        auth_type = extra.get('auth_type', 'none').lower()
        
        if auth_type == 'basic' and connection.login and connection.password:
            session.auth = (connection.login, connection.password)
        
        elif auth_type == 'bearer' and connection.password:
            session.headers.update({'Authorization': f'Bearer {connection.password}'})
        
        elif auth_type == 'api_key':
            api_key = connection.password or extra.get('api_key')
            key_header = extra.get('api_key_header', 'X-API-Key')
            if api_key:
                session.headers.update({key_header: api_key})
        
        elif auth_type == 'oauth2':
            token = extra.get('access_token') or connection.password
            if token:
                session.headers.update({'Authorization': f'Bearer {token}'})
        
        elif auth_type == 'custom':
            # Custom headers from extra
            custom_headers = extra.get('headers', {})
            session.headers.update(custom_headers)
    
    def _configure_session_defaults(self, session: requests.Session, extra: Dict[str, Any]):
        """Configure session defaults"""
        
        # Default headers
        default_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'InsightAir-Framework/2.0'
        }
        
        # Override with extra headers
        headers = {**default_headers, **extra.get('headers', {})}
        session.headers.update(headers)
        
        # Timeout configuration
        session.timeout = extra.get('timeout', 30)
        
        # SSL verification
        session.verify = extra.get('verify_ssl', True)
    
    @abstractmethod
    def execute_api_operation(self, context: Context) -> Any:
        """Execute the API operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic session configuration"""
        return self.execute_api_operation(context)


class HTTPRequestOperator(BaseAPIOperator):
    """Generic HTTP request operator"""
    
    template_fields = ('endpoint', 'data', 'params', 'headers')
    
    def __init__(self,
                 endpoint: str,
                 method: str = 'GET',
                 data: Optional[Union[Dict[str, Any], str]] = None,
                 params: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, Any]] = None,
                 expected_status_codes: Optional[List[int]] = None,
                 retry_count: int = 3,
                 retry_delay_seconds: float = 1.0,
                 response_format: str = 'json',  # json, text, raw
                 **kwargs):
        """
        Initialize HTTPRequestOperator
        
        Args:
            endpoint: API endpoint (relative to base_url)
            method: HTTP method (GET, POST, PUT, DELETE, PATCH)
            data: Request body data
            params: Query parameters
            headers: Additional headers
            expected_status_codes: List of expected HTTP status codes
            retry_count: Number of retries on failure
            retry_delay_seconds: Delay between retries
            response_format: How to parse response (json, text, raw)
        """
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data
        self.params = params or {}
        self.headers = headers or {}
        self.expected_status_codes = expected_status_codes or [200, 201, 202, 204]
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds
        self.response_format = response_format.lower()
    
    def execute_api_operation(self, context: Context) -> Dict[str, Any]:
        """Execute HTTP request"""
        
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
        """Execute on AWS (may use API Gateway integration)"""
        result = self._execute_generic(context)
        result['platform'] = 'aws'
        
        # AWS-specific enhancements could be added here
        # e.g., API Gateway throttling, AWS signature auth, etc.
        
        return result
    
    def _execute_gcp(self, context: Context) -> Dict[str, Any]:
        """Execute on GCP (may use Cloud Endpoints integration)"""
        result = self._execute_generic(context)
        result['platform'] = 'gcp'
        return result
    
    def _execute_azure(self, context: Context) -> Dict[str, Any]:
        """Execute on Azure (may use API Management integration)"""
        result = self._execute_generic(context)
        result['platform'] = 'azure'
        return result
    
    def _execute_oci(self, context: Context) -> Dict[str, Any]:
        """Execute on OCI (may use API Gateway integration)"""
        result = self._execute_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _execute_generic(self, context: Context) -> Dict[str, Any]:
        """Generic HTTP request execution"""
        
        session = self.get_api_session()
        
        # Build full URL
        if hasattr(session, 'base_url') and session.base_url:
            url = urljoin(session.base_url, self.endpoint)
        else:
            url = self.endpoint
        
        logger.info(f"Making {self.method} request to {url}")
        
        # Prepare request data
        request_data = self._prepare_request_data()
        request_headers = {**session.headers, **self.headers}
        
        # Execute request with retries
        last_exception = None
        for attempt in range(self.retry_count + 1):
            try:
                response = session.request(
                    method=self.method,
                    url=url,
                    json=request_data if isinstance(request_data, dict) else None,
                    data=request_data if isinstance(request_data, str) else None,
                    params=self.params,
                    headers=request_headers,
                    timeout=getattr(session, 'timeout', 30)
                )
                
                # Check status code
                if response.status_code in self.expected_status_codes:
                    # Success - parse response
                    result = self._parse_response(response)
                    
                    # Store in XCom
                    context['task_instance'].xcom_push(key='api_response', value=result)
                    context['task_instance'].xcom_push(key='status_code', value=response.status_code)
                    
                    return {
                        'url': url,
                        'method': self.method,
                        'status_code': response.status_code,
                        'success': True,
                        'response': result,
                        'attempts': attempt + 1
                    }
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    logger.warning(f"Request failed (attempt {attempt + 1}): {error_msg}")
                    last_exception = Exception(error_msg)
                    
            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {str(e)}")
                last_exception = e
            
            # Wait before retry (except on last attempt)
            if attempt < self.retry_count:
                time.sleep(self.retry_delay_seconds * (2 ** attempt))  # Exponential backoff
        
        # All retries failed
        logger.error(f"HTTP request failed after {self.retry_count + 1} attempts")
        raise Exception(f"HTTP request failed: {str(last_exception)}")
    
    def _prepare_request_data(self) -> Union[Dict[str, Any], str, None]:
        """Prepare request data based on content type"""
        
        if not self.data:
            return None
        
        if isinstance(self.data, dict):
            return self.data
        elif isinstance(self.data, str):
            try:
                # Try to parse as JSON
                return json.loads(self.data)
            except json.JSONDecodeError:
                # Return as string
                return self.data
        else:
            return str(self.data)
    
    def _parse_response(self, response: requests.Response) -> Any:
        """Parse response based on response_format"""
        
        if self.response_format == 'json':
            try:
                return response.json()
            except json.JSONDecodeError:
                logger.warning("Failed to parse response as JSON, falling back to text")
                return response.text
        elif self.response_format == 'text':
            return response.text
        else:  # raw
            return {
                'content': response.content.decode('utf-8', errors='ignore'),
                'headers': dict(response.headers),
                'status_code': response.status_code
            }


class WebhookOperator(BaseAPIOperator):
    """Generic webhook operation operator"""
    
    template_fields = ('webhook_url', 'payload', 'headers')
    
    def __init__(self,
                 webhook_url: str,
                 payload: Union[Dict[str, Any], str],
                 method: str = 'POST',
                 headers: Optional[Dict[str, Any]] = None,
                 secret_header: Optional[str] = None,
                 signature_header: Optional[str] = None,
                 **kwargs):
        """
        Initialize WebhookOperator
        
        Args:
            webhook_url: Webhook URL
            payload: Webhook payload
            method: HTTP method
            headers: Additional headers
            secret_header: Secret header name for authentication
            signature_header: Signature header name for HMAC verification
        """
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.payload = payload
        self.method = method.upper()
        self.headers = headers or {}
        self.secret_header = secret_header
        self.signature_header = signature_header
    
    def execute_api_operation(self, context: Context) -> Dict[str, Any]:
        """Execute webhook operation"""
        
        logger.info(f"Sending webhook to {self.webhook_url}")
        
        # Prepare payload
        if isinstance(self.payload, dict):
            payload_data = json.dumps(self.payload)
            content_type = 'application/json'
        else:
            payload_data = str(self.payload)
            content_type = 'text/plain'
        
        # Prepare headers
        request_headers = {
            'Content-Type': content_type,
            'User-Agent': 'InsightAir-Webhook/2.0',
            **self.headers
        }
        
        # Add authentication if configured
        if self.secret_header and self.api_connection_id:
            try:
                connection = BaseHook.get_connection(self.api_connection_id)
                if connection.password:
                    request_headers[self.secret_header] = connection.password
            except Exception as e:
                logger.warning(f"Failed to get webhook secret: {e}")
        
        # Add signature if configured
        if self.signature_header:
            signature = self._generate_signature(payload_data)
            if signature:
                request_headers[self.signature_header] = signature
        
        try:
            response = requests.request(
                method=self.method,
                url=self.webhook_url,
                data=payload_data,
                headers=request_headers,
                timeout=30
            )
            
            success = 200 <= response.status_code < 300
            
            result = {
                'webhook_url': self.webhook_url,
                'method': self.method,
                'status_code': response.status_code,
                'success': success,
                'response_text': response.text[:1000] if response.text else None  # Limit response size
            }
            
            # Store in XCom
            context['task_instance'].xcom_push(key='webhook_response', value=result)
            
            if success:
                logger.info(f"Webhook sent successfully: {response.status_code}")
            else:
                logger.warning(f"Webhook failed: {response.status_code} - {response.text}")
            
            return result
            
        except Exception as e:
            logger.error(f"Webhook request failed: {str(e)}")
            raise
    
    def _generate_signature(self, payload: str) -> Optional[str]:
        """Generate HMAC signature for webhook"""
        try:
            import hmac
            import hashlib
            
            if not self.api_connection_id:
                return None
            
            connection = BaseHook.get_connection(self.api_connection_id)
            secret = connection.password
            
            if not secret:
                return None
            
            signature = hmac.new(
                secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return f"sha256={signature}"
            
        except Exception as e:
            logger.warning(f"Failed to generate webhook signature: {e}")
            return None


class APIPollingOperator(BaseAPIOperator):
    """Generic API polling operator for checking status"""
    
    template_fields = ('endpoint', 'params')
    
    def __init__(self,
                 endpoint: str,
                 condition_path: str,  # JSONPath to check in response
                 expected_value: Any,
                 method: str = 'GET',
                 params: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, Any]] = None,
                 max_attempts: int = 10,
                 poll_interval_seconds: int = 30,
                 **kwargs):
        """
        Initialize APIPollingOperator
        
        Args:
            endpoint: API endpoint to poll
            condition_path: JSONPath to check in response (e.g., "status", "data.state")
            expected_value: Expected value to wait for
            method: HTTP method
            params: Query parameters
            headers: Additional headers
            max_attempts: Maximum polling attempts
            poll_interval_seconds: Seconds between polls
        """
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.condition_path = condition_path
        self.expected_value = expected_value
        self.method = method.upper()
        self.params = params or {}
        self.headers = headers or {}
        self.max_attempts = max_attempts
        self.poll_interval_seconds = poll_interval_seconds
    
    def execute_api_operation(self, context: Context) -> Dict[str, Any]:
        """Execute API polling operation"""
        
        session = self.get_api_session()
        
        # Build full URL
        if hasattr(session, 'base_url') and session.base_url:
            url = urljoin(session.base_url, self.endpoint)
        else:
            url = self.endpoint
        
        logger.info(f"Starting API polling for {url}")
        logger.info(f"Waiting for {self.condition_path} = {self.expected_value}")
        
        for attempt in range(self.max_attempts):
            try:
                response = session.request(
                    method=self.method,
                    url=url,
                    params=self.params,
                    headers={**session.headers, **self.headers},
                    timeout=getattr(session, 'timeout', 30)
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        current_value = self._get_nested_value(data, self.condition_path)
                        
                        logger.info(f"Poll attempt {attempt + 1}: {self.condition_path} = {current_value}")
                        
                        if current_value == self.expected_value:
                            logger.info(f"Condition met after {attempt + 1} attempts")
                            
                            # Store final response in XCom
                            context['task_instance'].xcom_push(key='final_response', value=data)
                            
                            return {
                                'url': url,
                                'attempts': attempt + 1,
                                'condition_met': True,
                                'final_value': current_value,
                                'success': True
                            }
                    
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        logger.warning(f"Failed to parse response or find condition path: {e}")
                
                else:
                    logger.warning(f"Poll attempt {attempt + 1} failed: HTTP {response.status_code}")
                
            except Exception as e:
                logger.warning(f"Poll attempt {attempt + 1} failed: {str(e)}")
            
            # Wait before next attempt (except on last attempt)
            if attempt < self.max_attempts - 1:
                logger.info(f"Waiting {self.poll_interval_seconds} seconds before next poll...")
                time.sleep(self.poll_interval_seconds)
        
        # Polling failed
        raise Exception(f"API polling failed after {self.max_attempts} attempts")
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dictionary using dot notation"""
        keys = path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                raise KeyError(f"Path '{path}' not found in response")
        
        return current


def register_implementations(registry):
    """Register API operation implementations"""
    
    # Register generic API operations
    registry.register_implementation("HTTP_REQUEST", "generic", HTTPRequestOperator)
    registry.register_implementation("API_CALL", "generic", HTTPRequestOperator)
    registry.register_implementation("WEBHOOK", "generic", WebhookOperator)
    registry.register_implementation("API_POLLING", "generic", APIPollingOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("HTTP_REQUEST", provider, HTTPRequestOperator)
        registry.register_implementation("API_CALL", provider, HTTPRequestOperator)
        registry.register_implementation("WEBHOOK", provider, WebhookOperator)
        registry.register_implementation("API_POLLING", provider, APIPollingOperator)


# Example configuration usage:
"""
tasks:
  - name: "call_external_api"
    type: "HTTP_REQUEST"
    description: "Call external REST API"
    properties:
      endpoint: "/api/v1/data"
      method: "GET"
      params:
        start_date: "{{ ds }}"
        end_date: "{{ next_ds }}"
      expected_status_codes: [200, 202]
      retry_count: 3
      response_format: "json"
      # api_connection_id: auto-resolved

  - name: "send_webhook_notification"
    type: "WEBHOOK"
    description: "Send webhook notification"
    properties:
      webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
      payload:
        text: "Pipeline {{ dag.dag_id }} completed successfully"
        username: "airflow-bot"
      method: "POST"
      headers:
        Content-Type: "application/json"

  - name: "poll_job_status"
    type: "API_POLLING"
    description: "Poll external job status"
    properties:
      endpoint: "/api/v1/jobs/{{ ti.xcom_pull(key='job_id') }}"
      condition_path: "status"
      expected_value: "completed"
      max_attempts: 20
      poll_interval_seconds: 30

  - name: "create_resource"
    type: "HTTP_REQUEST"
    description: "Create external resource via API"
    properties:
      endpoint: "/api/v1/resources"
      method: "POST"
      data:
        name: "resource_{{ ds_nodash }}"
        type: "data_pipeline"
        config:
          source: "{{ var.value.data_source }}"
      expected_status_codes: [201]
"""