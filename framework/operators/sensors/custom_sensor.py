# Custom Sensor Operators - Flexible and extensible sensor patterns
"""
Custom sensor operators for specialized monitoring scenarios including:
- Python callable-based sensors
- Multi-condition sensors
- Time-based sensors
- External system integration sensors
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, Any, Optional, List, Union, Callable

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class PythonSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that uses Python callable for condition checking
    """
    
    template_fields = ('python_callable', 'op_kwargs')
    
    def __init__(self,
                 python_callable: Union[Callable, str],
                 op_args: Optional[List] = None,
                 op_kwargs: Optional[Dict[str, Any]] = None,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize PythonSensor
        
        Args:
            python_callable: Python function that returns boolean
            op_args: Arguments to pass to python_callable
            op_kwargs: Keyword arguments to pass to python_callable
            poke_interval: Time between checks in seconds
            timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
    
    def poke(self, context: Context) -> bool:
        """
        Execute Python callable to check condition
        
        Returns:
            Result of python_callable execution
        """
        try:
            # Resolve callable if string
            if isinstance(self.python_callable, str):
                callable_func = self._resolve_callable_from_string(self.python_callable)
            else:
                callable_func = self.python_callable
            
            if not callable(callable_func):
                raise ValueError(f"python_callable must be callable, got {type(callable_func)}")
            
            # Execute callable with context
            result = callable_func(context, *self.op_args, **self.op_kwargs)
            
            # Ensure result is boolean
            return bool(result)
            
        except Exception as e:
            logger.error(f"PythonSensor poke failed: {str(e)}")
            return False
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve Python callable from string reference"""
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            return registry.get_callable(callable_string)
        except Exception as e:
            logger.error(f"Failed to resolve callable '{callable_string}': {e}")
            raise ValueError(f"Cannot resolve callable: {callable_string}")


class CustomSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Flexible sensor that supports multiple condition types and combinations
    """
    
    template_fields = ('conditions',)
    
    def __init__(self,
                 conditions: Union[Dict[str, Any], List[Dict[str, Any]]],
                 condition_mode: str = 'all',  # 'all', 'any', 'custom'
                 custom_logic: Optional[Union[Callable, str]] = None,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize CustomSensor
        
        Args:
            conditions: Single condition or list of conditions to check
            condition_mode: How to combine multiple conditions ('all', 'any', 'custom')
            custom_logic: Custom function for combining condition results
            poke_interval: Time between checks in seconds
            timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        
        # Normalize conditions to list
        if isinstance(conditions, dict):
            self.conditions = [conditions]
        else:
            self.conditions = conditions
            
        self.condition_mode = condition_mode
        self.custom_logic = custom_logic
    
    def poke(self, context: Context) -> bool:
        """
        Check all conditions according to specified logic
        
        Returns:
            True if conditions are satisfied, False otherwise
        """
        try:
            condition_results = []
            
            # Evaluate each condition
            for i, condition in enumerate(self.conditions):
                result = self._evaluate_condition(condition, context)
                condition_results.append(result)
                
                logger.debug(f"Condition {i} result: {result}")
                
                # Short-circuit for 'any' mode
                if self.condition_mode == 'any' and result:
                    logger.info("CustomSensor condition satisfied (any mode)")
                    return True
                
                # Short-circuit for 'all' mode
                if self.condition_mode == 'all' and not result:
                    logger.debug("CustomSensor condition not satisfied (all mode)")
                    return False
            
            # Store condition results in XCom
            context['task_instance'].xcom_push(key='condition_results', value=condition_results)
            
            # Apply combination logic
            if self.condition_mode == 'all':
                return all(condition_results)
            elif self.condition_mode == 'any':
                return any(condition_results)
            elif self.condition_mode == 'custom' and self.custom_logic:
                return self._apply_custom_logic(condition_results, context)
            else:
                logger.warning(f"Unknown condition_mode: {self.condition_mode}")
                return False
            
        except Exception as e:
            logger.error(f"CustomSensor poke failed: {str(e)}")
            return False
    
    def _evaluate_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate a single condition"""
        condition_type = condition.get('type', 'python')
        
        if condition_type == 'python':
            return self._evaluate_python_condition(condition, context)
        elif condition_type == 'time':
            return self._evaluate_time_condition(condition, context)
        elif condition_type == 'xcom':
            return self._evaluate_xcom_condition(condition, context)
        elif condition_type == 'variable':
            return self._evaluate_variable_condition(condition, context)
        elif condition_type == 'file':
            return self._evaluate_file_condition(condition, context)
        else:
            logger.warning(f"Unknown condition type: {condition_type}")
            return False
    
    def _evaluate_python_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate Python callable condition"""
        callable_name = condition.get('callable')
        if not callable_name:
            return False
        
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            condition_callable = registry.get_callable(callable_name)
            
            args = condition.get('args', [])
            kwargs = condition.get('kwargs', {})
            kwargs.update({'context': context})
            
            result = condition_callable(*args, **kwargs)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Failed to evaluate Python condition: {e}")
            return False
    
    def _evaluate_time_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate time-based condition"""
        from datetime import datetime, time
        
        condition_time = condition.get('time')  # Format: "HH:MM"
        operator = condition.get('operator', 'after')  # 'before', 'after', 'between'
        weekdays = condition.get('weekdays')  # List of weekday numbers (0=Monday)
        
        try:
            now = datetime.now()
            current_time = now.time()
            current_weekday = now.weekday()
            
            # Check weekday condition
            if weekdays and current_weekday not in weekdays:
                return False
            
            # Check time condition
            if operator == 'after':
                target_time = time.fromisoformat(condition_time)
                return current_time >= target_time
            elif operator == 'before':
                target_time = time.fromisoformat(condition_time)
                return current_time <= target_time
            elif operator == 'between':
                start_time = time.fromisoformat(condition.get('start_time'))
                end_time = time.fromisoformat(condition.get('end_time'))
                return start_time <= current_time <= end_time
            else:
                logger.warning(f"Unknown time operator: {operator}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to evaluate time condition: {e}")
            return False
    
    def _evaluate_xcom_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate XCom-based condition"""
        task_id = condition.get('task_id')
        key = condition.get('key', 'return_value')
        operator = condition.get('operator', 'equals')
        expected_value = condition.get('value')
        
        try:
            xcom_value = context['task_instance'].xcom_pull(task_ids=task_id, key=key)
            
            if operator == 'equals':
                return xcom_value == expected_value
            elif operator == 'not_equals':
                return xcom_value != expected_value
            elif operator == 'greater_than':
                return xcom_value > expected_value
            elif operator == 'less_than':
                return xcom_value < expected_value
            elif operator == 'contains':
                return expected_value in str(xcom_value)
            elif operator == 'exists':
                return xcom_value is not None
            else:
                logger.warning(f"Unknown XCom operator: {operator}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to evaluate XCom condition: {e}")
            return False
    
    def _evaluate_variable_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate Airflow Variable condition"""
        variable_name = condition.get('variable')
        operator = condition.get('operator', 'equals')
        expected_value = condition.get('value')
        
        try:
            from airflow.models import Variable
            variable_value = Variable.get(variable_name, default_var=None)
            
            if operator == 'equals':
                return variable_value == expected_value
            elif operator == 'not_equals':
                return variable_value != expected_value
            elif operator == 'exists':
                return variable_value is not None
            elif operator == 'contains':
                return expected_value in str(variable_value)
            else:
                logger.warning(f"Unknown Variable operator: {operator}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to evaluate Variable condition: {e}")
            return False
    
    def _evaluate_file_condition(self, condition: Dict[str, Any], context: Context) -> bool:
        """Evaluate file-based condition"""
        file_path = condition.get('file_path')
        check_type = condition.get('check', 'exists')  # 'exists', 'size', 'age'
        
        try:
            from pathlib import Path
            path = Path(file_path)
            
            if check_type == 'exists':
                return path.exists()
            elif check_type == 'size':
                min_size = condition.get('min_size', 0)
                return path.exists() and path.stat().st_size >= min_size
            elif check_type == 'age':
                max_age_hours = condition.get('max_age_hours', 24)
                if not path.exists():
                    return False
                
                file_age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
                max_age = timedelta(hours=max_age_hours)
                return file_age <= max_age
            else:
                logger.warning(f"Unknown file check type: {check_type}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to evaluate file condition: {e}")
            return False
    
    def _apply_custom_logic(self, condition_results: List[bool], context: Context) -> bool:
        """Apply custom logic to combine condition results"""
        try:
            if isinstance(self.custom_logic, str):
                # Resolve callable from string
                from ...core.callable_registry import get_callable_registry
                registry = get_callable_registry()
                logic_func = registry.get_callable(self.custom_logic)
            else:
                logic_func = self.custom_logic
            
            if not callable(logic_func):
                raise ValueError(f"custom_logic must be callable, got {type(logic_func)}")
            
            return bool(logic_func(condition_results, context))
            
        except Exception as e:
            logger.error(f"Failed to apply custom logic: {e}")
            return False


class TimeSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that waits for specific time conditions
    """
    
    def __init__(self,
                 target_time: str,
                 mode: str = 'time',  # 'time', 'datetime', 'cron'
                 timezone: Optional[str] = None,
                 weekdays: Optional[List[int]] = None,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize TimeSensor
        
        Args:
            target_time: Target time to wait for (format depends on mode)
            mode: Time mode ('time' for HH:MM, 'datetime' for ISO format, 'cron' for cron expression)
            timezone: Timezone name (e.g., 'UTC', 'America/New_York')
            weekdays: List of weekdays to check (0=Monday, 6=Sunday)
            poke_interval: Time between checks in seconds
            timeout: Sensor timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.target_time = target_time
        self.mode = mode
        self.timezone = timezone
        self.weekdays = weekdays
    
    def poke(self, context: Context) -> bool:
        """
        Check if target time condition is met
        
        Returns:
            True if time condition is satisfied, False otherwise
        """
        try:
            now = self._get_current_time()
            
            # Check weekday condition
            if self.weekdays and now.weekday() not in self.weekdays:
                logger.debug(f"Current weekday {now.weekday()} not in {self.weekdays}")
                return False
            
            # Check time condition based on mode
            if self.mode == 'time':
                return self._check_time_condition(now)
            elif self.mode == 'datetime':
                return self._check_datetime_condition(now)
            elif self.mode == 'cron':
                return self._check_cron_condition(now)
            else:
                logger.warning(f"Unknown time mode: {self.mode}")
                return False
                
        except Exception as e:
            logger.error(f"TimeSensor poke failed: {str(e)}")
            return False
    
    def _get_current_time(self) -> datetime:
        """Get current time with timezone handling"""
        if self.timezone:
            import pytz
            tz = pytz.timezone(self.timezone)
            return datetime.now(tz)
        else:
            return datetime.now()
    
    def _check_time_condition(self, now: datetime) -> bool:
        """Check time-only condition (HH:MM format)"""
        target_time = time.fromisoformat(self.target_time)
        current_time = now.time()
        
        # Check if current time has passed target time
        return current_time >= target_time
    
    def _check_datetime_condition(self, now: datetime) -> bool:
        """Check datetime condition (ISO format)"""
        target_datetime = datetime.fromisoformat(self.target_time)
        
        # Handle timezone if specified
        if self.timezone and target_datetime.tzinfo is None:
            import pytz
            tz = pytz.timezone(self.timezone)
            target_datetime = tz.localize(target_datetime)
        
        return now >= target_datetime
    
    def _check_cron_condition(self, now: datetime) -> bool:
        """Check cron expression condition"""
        try:
            from croniter import croniter
            
            # Create croniter object
            cron = croniter(self.target_time, now)
            
            # Check if current time matches cron expression
            # This is a simplified check - in practice you might want more sophisticated logic
            next_time = cron.get_next(datetime)
            prev_time = cron.get_prev(datetime)
            
            # If we're very close to a cron match (within the poke interval)
            time_diff = abs((now - prev_time).total_seconds())
            return time_diff <= self.poke_interval
            
        except ImportError:
            logger.error("croniter package required for cron mode")
            return False
        except Exception as e:
            logger.error(f"Cron condition check failed: {e}")
            return False


# Example configuration usage:
"""
tasks:
  - name: "custom_condition_check"
    type: "CUSTOM_SENSOR"
    description: "Check multiple custom conditions"
    properties:
      conditions:
        - type: "python"
          callable: "check_data_ready"
          kwargs:
            source: "external_api"
        - type: "time"
          time: "09:00"
          operator: "after"
          weekdays: [0, 1, 2, 3, 4]  # Monday to Friday
        - type: "xcom"
          task_id: "data_validation"
          key: "quality_score"
          operator: "greater_than"
          value: 0.95
      condition_mode: "all"
      poke_interval: 120

  - name: "python_condition_sensor"
    type: "PYTHON_SENSOR"
    description: "Use Python function for condition"
    properties:
      python_callable: "check_external_dependency"
      op_kwargs:
        service_url: "https://api.service.com"
        expected_status: "ready"
      poke_interval: 60

  - name: "wait_for_business_hours"
    type: "TIME_SENSOR"
    description: "Wait for business hours"
    properties:
      target_time: "09:00"
      mode: "time"
      timezone: "America/New_York"
      weekdays: [0, 1, 2, 3, 4]
      poke_interval: 300

  - name: "wait_for_scheduled_time"
    type: "TIME_SENSOR"
    description: "Wait for specific datetime"
    properties:
      target_time: "2024-01-15T10:30:00"
      mode: "datetime"
      timezone: "UTC"
      poke_interval: 60
"""