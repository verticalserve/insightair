# API/HTTP Sensor Operators - Monitor API endpoints and HTTP services
"""
API and HTTP sensor operators for monitoring external services including:
- REST API endpoint monitoring
- HTTP status code checking
- Response content validation
- API authentication and headers
"""

import logging
import json
import re
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urljoin

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.providers.http.hooks.http import HttpHook

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class APISensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors REST API endpoints for specific conditions
    """
    
    template_fields = ('endpoint', 'data', 'headers')
    
    def __init__(self,
                 endpoint: str,
                 http_conn_id: str = 'http_default',
                 method: str = 'GET',
                 data: Optional[Union[Dict[str, Any], str]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 expected_status_code: Union[int, List[int]] = 200,
                 response_check: Optional[callable] = None,
                 response_filter: Optional[str] = None,
                 json_path: Optional[str] = None,
                 expected_value: Optional[Any] = None,
                 timeout: int = 20,
                 poke_interval: int = 60,
                 sensor_timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize APISensor
        
        Args:
            endpoint: API endpoint path
            http_conn_id: Airflow HTTP connection ID
            method: HTTP method (GET, POST, PUT, etc.)
            data: Request body data
            headers: HTTP headers
            expected_status_code: Expected HTTP status code(s)
            response_check: Custom function to validate response
            response_filter: JSONPath or regex pattern to filter response
            json_path: JSONPath to extract specific value from response
            expected_value: Expected value at json_path
            timeout: HTTP request timeout
            poke_interval: Time between checks in seconds
            sensor_timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=sensor_timeout, **kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.data = data
        self.headers = headers or {}
        self.expected_status_code = expected_status_code
        self.response_check = response_check
        self.response_filter = response_filter
        self.json_path = json_path
        self.expected_value = expected_value
        self.request_timeout = timeout
    
    def poke(self, context: Context) -> bool:
        """
        Check if API condition is met
        
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            # Make HTTP request
            hook = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
            
            response = hook.run(
                endpoint=self.endpoint,
                data=self._prepare_data(),
                headers=self.headers,
                timeout=self.request_timeout
            )
            
            # Check status code
            if not self._check_status_code(response.status_code, context):
                return False
            
            # Check response content
            if not self._check_response_content(response, context):
                return False
            
            logger.info(f"API condition satisfied for endpoint: {self.endpoint}")
            return True
            
        except Exception as e:
            logger.error(f"APISensor poke failed: {str(e)}")
            return False
    
    def _prepare_data(self) -> Optional[str]:
        """Prepare request data"""
        if self.data is None:
            return None
        
        if isinstance(self.data, dict):
            return json.dumps(self.data)
        
        return str(self.data)
    
    def _check_status_code(self, status_code: int, context: Context) -> bool:
        """Check if status code matches expected"""
        expected_codes = self.expected_status_code
        if isinstance(expected_codes, int):
            expected_codes = [expected_codes]
        
        # Store status code in XCom
        context['task_instance'].xcom_push(key='status_code', value=status_code)
        
        if status_code not in expected_codes:
            logger.debug(f"Status code {status_code} not in expected {expected_codes}")
            return False
        
        return True
    
    def _check_response_content(self, response, context: Context) -> bool:
        """Check response content against conditions"""
        try:
            response_text = response.text
            
            # Store raw response in XCom
            context['task_instance'].xcom_push(key='response_text', value=response_text)
            
            # Try to parse as JSON
            try:
                response_json = response.json()
                context['task_instance'].xcom_push(key='response_json', value=response_json)
            except:
                response_json = None
            
            # Custom response check function
            if self.response_check:
                if callable(self.response_check):
                    return self.response_check(response)
                else:
                    # Try to resolve callable from string
                    from ...core.callable_registry import get_callable_registry
                    registry = get_callable_registry()
                    check_func = registry.get_callable(self.response_check)
                    return check_func(response)
            
            # JSONPath filtering
            if self.json_path and response_json:
                extracted_value = self._extract_json_path(response_json, self.json_path)
                context['task_instance'].xcom_push(key='extracted_value', value=extracted_value)
                
                if self.expected_value is not None:
                    return extracted_value == self.expected_value
                else:
                    return extracted_value is not None
            
            # Response filter (regex or JSONPath)
            if self.response_filter:
                return self._apply_response_filter(response_text, response_json, context)
            
            # Default: just check if we got a response
            return True
            
        except Exception as e:
            logger.error(f"Error checking response content: {e}")
            return False
    
    def _extract_json_path(self, json_data: Dict[str, Any], json_path: str) -> Any:
        """Extract value using JSONPath-like syntax"""
        try:
            # Simple JSONPath implementation
            keys = json_path.strip('$.').split('.')
            current = json_data
            
            for key in keys:
                if '[' in key and ']' in key:
                    # Handle array indexing
                    array_key, index_part = key.split('[', 1)
                    index = int(index_part.rstrip(']'))
                    current = current[array_key][index]
                else:
                    current = current[key]
            
            return current
            
        except Exception as e:
            logger.debug(f"JSONPath extraction failed: {e}")
            return None
    
    def _apply_response_filter(self, response_text: str, response_json: Optional[Dict], context: Context) -> bool:
        """Apply response filter pattern"""
        try:
            # Try regex pattern
            if re.search(self.response_filter, response_text):
                logger.debug(f"Response filter pattern matched: {self.response_filter}")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Response filter failed: {e}")
            return False


class HTTPSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Simple HTTP sensor for basic endpoint monitoring
    """
    
    template_fields = ('url',)
    
    def __init__(self,
                 url: str,
                 method: str = 'GET',
                 headers: Optional[Dict[str, str]] = None,
                 auth: Optional[tuple] = None,
                 verify_ssl: bool = True,
                 expected_status_codes: Union[int, List[int]] = 200,
                 timeout: int = 20,
                 poke_interval: int = 60,
                 sensor_timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize HTTPSensor
        
        Args:
            url: Full URL to monitor
            method: HTTP method
            headers: HTTP headers
            auth: Authentication tuple (username, password)
            verify_ssl: Whether to verify SSL certificates
            expected_status_codes: Expected HTTP status codes
            timeout: Request timeout
            poke_interval: Time between checks in seconds
            sensor_timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=sensor_timeout, **kwargs)
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.auth = auth
        self.verify_ssl = verify_ssl
        self.expected_status_codes = expected_status_codes
        self.request_timeout = timeout
    
    def poke(self, context: Context) -> bool:
        """
        Check if HTTP endpoint is available
        
        Returns:
            True if endpoint is accessible, False otherwise
        """
        try:
            import requests
            
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                auth=self.auth,
                verify=self.verify_ssl,
                timeout=self.request_timeout
            )
            
            # Check status code
            expected_codes = self.expected_status_codes
            if isinstance(expected_codes, int):
                expected_codes = [expected_codes]
            
            # Store response info in XCom
            context['task_instance'].xcom_push(
                key='http_response',
                value={
                    'status_code': response.status_code,
                    'headers': dict(response.headers),
                    'url': self.url,
                    'response_time': response.elapsed.total_seconds()
                }
            )
            
            if response.status_code in expected_codes:
                logger.info(f"HTTP endpoint accessible: {self.url} (status: {response.status_code})")
                return True
            else:
                logger.debug(f"HTTP status {response.status_code} not in expected {expected_codes}")
                return False
            
        except Exception as e:
            logger.debug(f"HTTP request failed: {str(e)}")
            return False


class WebhookSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that waits for webhook data to be received
    """
    
    def __init__(self,
                 webhook_id: str,
                 expected_data: Optional[Dict[str, Any]] = None,
                 data_key: Optional[str] = None,
                 poke_interval: int = 30,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize WebhookSensor
        
        Args:
            webhook_id: Unique identifier for webhook
            expected_data: Expected webhook payload data
            data_key: Key to check in webhook data
            poke_interval: Time between checks in seconds
            timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.webhook_id = webhook_id
        self.expected_data = expected_data
        self.data_key = data_key
    
    def poke(self, context: Context) -> bool:
        """
        Check if webhook data has been received
        
        Returns:
            True if webhook data matches conditions, False otherwise
        """
        try:
            # Check for webhook data in XCom from external source
            webhook_data = context['task_instance'].xcom_pull(
                key=f'webhook_{self.webhook_id}',
                include_prior_dates=True
            )
            
            if not webhook_data:
                logger.debug(f"No webhook data found for ID: {self.webhook_id}")
                return False
            
            # Check expected data conditions
            if self.expected_data:
                for key, expected_value in self.expected_data.items():
                    if key not in webhook_data or webhook_data[key] != expected_value:
                        logger.debug(f"Webhook data mismatch for key '{key}'")
                        return False
            
            # Check specific data key
            if self.data_key and self.data_key not in webhook_data:
                logger.debug(f"Webhook data missing required key: {self.data_key}")
                return False
            
            logger.info(f"Webhook condition satisfied for ID: {self.webhook_id}")
            
            # Store webhook data in XCom
            context['task_instance'].xcom_push(key='webhook_data', value=webhook_data)
            
            return True
            
        except Exception as e:
            logger.error(f"WebhookSensor poke failed: {str(e)}")
            return False


# Example configuration usage:
"""
tasks:
  - name: "wait_for_api_ready"
    type: "API_SENSOR"
    description: "Wait for API service to be ready"
    properties:
      http_conn_id: "api_service"
      endpoint: "/health"
      method: "GET"
      expected_status_code: 200
      json_path: "$.status"
      expected_value: "healthy"
      poke_interval: 30
      timeout: 1800

  - name: "check_data_api"
    type: "API_SENSOR"
    description: "Check if new data is available via API"
    properties:
      http_conn_id: "data_api"
      endpoint: "/api/v1/data/status"
      method: "POST"
      headers:
        Content-Type: "application/json"
        Authorization: "Bearer {{ var.value.api_token }}"
      data:
        date: "{{ ds }}"
        source: "daily_batch"
      expected_status_code: [200, 201]
      json_path: "$.data_available"
      expected_value: true
      poke_interval: 300

  - name: "monitor_external_service"
    type: "HTTP_SENSOR"
    description: "Monitor external service availability"
    properties:
      url: "https://api.external-service.com/ping"
      method: "GET"
      expected_status_codes: [200, 204]
      timeout: 10
      poke_interval: 60

  - name: "wait_for_webhook"
    type: "WEBHOOK_SENSOR"
    description: "Wait for external system webhook"
    properties:
      webhook_id: "data_processing_complete"
      expected_data:
        status: "completed"
        batch_date: "{{ ds }}"
      poke_interval: 30
      timeout: 3600
"""