# Branch Operators - Conditional workflow execution
"""
Branch operators for conditional workflow execution including:
- Python-based branching decisions
- Configuration-driven branching
- Multi-path conditional execution
- Skip patterns and conditional task execution
"""

import logging
from typing import Dict, Any, List, Union, Callable, Optional
from airflow.operators.python import BranchPythonOperator as AirflowBranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class BranchPythonOperator(BaseInsightAirOperator, AirflowBranchPythonOperator):
    """
    Enhanced BranchPythonOperator with configuration support and advanced branching patterns
    """
    
    template_fields = ('branch_conditions', 'default_branch', 'python_callable')
    
    def __init__(self,
                 python_callable: Union[Callable, str],
                 branch_conditions: Optional[Dict[str, Any]] = None,
                 default_branch: Optional[str] = None,
                 branch_map: Optional[Dict[str, str]] = None,
                 op_args: Optional[List] = None,
                 op_kwargs: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize BranchPythonOperator
        
        Args:
            python_callable: Function that returns branch task ID(s)
            branch_conditions: Configuration for branch conditions
            default_branch: Default branch if no conditions match
            branch_map: Mapping from condition results to task IDs
            op_args: Arguments to pass to python_callable
            op_kwargs: Keyword arguments to pass to python_callable
        """
        
        # Prepare arguments for parent classes
        branch_kwargs = kwargs.copy()
        
        # Handle callable resolution
        if isinstance(python_callable, str):
            resolved_callable = self._resolve_callable_from_string(python_callable)
        else:
            resolved_callable = python_callable
        
        super().__init__(
            python_callable=resolved_callable,
            op_args=op_args or [],
            op_kwargs=op_kwargs or {},
            **branch_kwargs
        )
        
        self.branch_conditions = branch_conditions or {}
        self.default_branch = default_branch
        self.branch_map = branch_map or {}
        self.original_callable = python_callable
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve Python callable from string reference"""
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            return registry.get_callable(callable_string)
        except Exception as e:
            logger.error(f"Failed to resolve callable '{callable_string}': {e}")
            raise ValueError(f"Cannot resolve callable: {callable_string}")


class BranchOperator(BaseInsightAirOperator):
    """
    Configuration-driven branch operator that supports multiple branching patterns
    """
    
    template_fields = ('conditions', 'branches', 'default_branch')
    
    def __init__(self,
                 conditions: Dict[str, Any],
                 branches: Dict[str, Union[str, List[str]]],
                 default_branch: Optional[Union[str, List[str]]] = None,
                 branch_mode: str = 'single',  # 'single', 'multiple', 'all_success'
                 **kwargs):
        """
        Initialize BranchOperator
        
        Args:
            conditions: Dictionary of conditions to evaluate
            branches: Mapping from condition names to task IDs
            default_branch: Default branch if no conditions match
            branch_mode: How to handle multiple matching conditions
        """
        super().__init__(**kwargs)
        self.conditions = conditions
        self.branches = branches
        self.default_branch = default_branch
        self.branch_mode = branch_mode
    
    def execute(self, context: Context) -> Union[str, List[str]]:
        """
        Execute branching logic based on conditions
        
        Returns:
            Task ID(s) to execute next
        """
        matching_branches = []
        
        # Evaluate each condition
        for condition_name, condition_config in self.conditions.items():
            if self._evaluate_condition(condition_config, context):
                branch_tasks = self.branches.get(condition_name)
                if branch_tasks:
                    if isinstance(branch_tasks, str):
                        matching_branches.append(branch_tasks)
                    else:
                        matching_branches.extend(branch_tasks)
                    
                    logger.info(f"Condition '{condition_name}' matched, branching to: {branch_tasks}")
                    
                    # If single mode, return first match
                    if self.branch_mode == 'single':
                        return branch_tasks
        
        # Handle multiple matches
        if matching_branches:
            if self.branch_mode == 'multiple':
                return list(set(matching_branches))  # Remove duplicates
            elif self.branch_mode == 'all_success':
                return matching_branches
            else:
                return matching_branches[0]  # First match
        
        # No conditions matched, use default
        if self.default_branch:
            logger.info(f"No conditions matched, using default branch: {self.default_branch}")
            return self.default_branch
        
        # No default, skip downstream
        logger.info("No conditions matched and no default branch specified")
        return []
    
    def _evaluate_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """
        Evaluate a single condition
        
        Args:
            condition_config: Configuration for the condition
            context: Airflow context
            
        Returns:
            True if condition is met, False otherwise
        """
        condition_type = condition_config.get('type', 'python')
        
        if condition_type == 'python':
            return self._evaluate_python_condition(condition_config, context)
        elif condition_type == 'xcom':
            return self._evaluate_xcom_condition(condition_config, context)
        elif condition_type == 'variable':
            return self._evaluate_variable_condition(condition_config, context)
        elif condition_type == 'expression':
            return self._evaluate_expression_condition(condition_config, context)
        elif condition_type == 'time':
            return self._evaluate_time_condition(condition_config, context)
        else:
            logger.warning(f"Unknown condition type: {condition_type}")
            return False
    
    def _evaluate_python_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """Evaluate Python callable condition"""
        
        callable_name = condition_config.get('callable')
        if not callable_name:
            return False
        
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            condition_callable = registry.get_callable(callable_name)
            
            args = condition_config.get('args', [])
            kwargs = condition_config.get('kwargs', {})
            kwargs.update({'context': context})
            
            result = condition_callable(*args, **kwargs)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Failed to evaluate Python condition: {e}")
            return False
    
    def _evaluate_xcom_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """Evaluate XCom-based condition"""
        
        task_id = condition_config.get('task_id')
        key = condition_config.get('key', 'return_value')
        operator = condition_config.get('operator', 'equals')
        expected_value = condition_config.get('value')
        
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
    
    def _evaluate_variable_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """Evaluate Airflow Variable condition"""
        
        variable_name = condition_config.get('variable')
        operator = condition_config.get('operator', 'equals')
        expected_value = condition_config.get('value')
        
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
    
    def _evaluate_expression_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """Evaluate expression-based condition"""
        
        expression = condition_config.get('expression')
        if not expression:
            return False
        
        try:
            # Create safe evaluation context
            eval_context = {
                'context': context,
                'ds': context.get('ds'),
                'execution_date': context.get('execution_date'),
                'dag_run': context.get('dag_run'),
                'task_instance': context.get('task_instance')
            }
            
            # Add custom variables if specified
            variables = condition_config.get('variables', {})
            eval_context.update(variables)
            
            # Evaluate expression
            result = eval(expression, {"__builtins__": {}}, eval_context)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Failed to evaluate expression condition: {e}")
            return False
    
    def _evaluate_time_condition(self, condition_config: Dict[str, Any], context: Context) -> bool:
        """Evaluate time-based condition"""
        
        from datetime import datetime, time
        
        condition_time = condition_config.get('time')  # Format: "HH:MM"
        operator = condition_config.get('operator', 'after')  # 'before', 'after', 'between'
        
        try:
            current_time = datetime.now().time()
            
            if operator == 'after':
                target_time = time.fromisoformat(condition_time)
                return current_time >= target_time
            elif operator == 'before':
                target_time = time.fromisoformat(condition_time)
                return current_time <= target_time
            elif operator == 'between':
                start_time = time.fromisoformat(condition_config.get('start_time'))
                end_time = time.fromisoformat(condition_config.get('end_time'))
                return start_time <= current_time <= end_time
            else:
                logger.warning(f"Unknown time operator: {operator}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to evaluate time condition: {e}")
            return False


class ConditionalOperator(BaseInsightAirOperator):
    """
    Operator that conditionally executes based on runtime conditions
    """
    
    def __init__(self,
                 condition: Dict[str, Any],
                 python_callable: Union[Callable, str],
                 skip_on_false: bool = True,
                 op_args: Optional[List] = None,
                 op_kwargs: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize ConditionalOperator
        
        Args:
            condition: Condition configuration to evaluate
            python_callable: Function to execute if condition is true
            skip_on_false: Whether to skip execution if condition is false
            op_args: Arguments for python_callable
            op_kwargs: Keyword arguments for python_callable
        """
        super().__init__(**kwargs)
        self.condition = condition
        self.python_callable = python_callable
        self.skip_on_false = skip_on_false
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
    
    def execute(self, context: Context) -> Any:
        """
        Execute conditionally based on condition evaluation
        """
        # Evaluate condition using BranchOperator logic
        branch_op = BranchOperator(
            task_id='temp_branch',
            conditions={'main': self.condition},
            branches={'main': 'execute'},
            default_branch='skip'
        )
        
        condition_result = branch_op._evaluate_condition(self.condition, context)
        
        if condition_result:
            logger.info("Condition satisfied, executing task")
            
            # Resolve callable if string
            if isinstance(self.python_callable, str):
                from ...core.callable_registry import get_callable_registry
                registry = get_callable_registry()
                callable_func = registry.get_callable(self.python_callable)
            else:
                callable_func = self.python_callable
            
            # Execute the callable
            return callable_func(context, *self.op_args, **self.op_kwargs)
        
        else:
            logger.info("Condition not satisfied")
            if self.skip_on_false:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("Condition not met, skipping task")
            else:
                return None


# Example configuration usage:
"""
tasks:
  # Simple Python-based branching
  - name: "branch_by_day"
    type: "BRANCH_PYTHON"
    description: "Branch based on day of week"
    properties:
      python_callable: "determine_processing_branch"
      op_kwargs:
        weekday_branch: "process_weekday"
        weekend_branch: "process_weekend"

  # Configuration-driven branching
  - name: "conditional_branch"
    type: "BRANCH"
    description: "Branch based on multiple conditions"  
    properties:
      conditions:
        high_volume:
          type: "xcom"
          task_id: "check_volume"
          key: "record_count"
          operator: "greater_than"
          value: 10000
        emergency_mode:
          type: "variable"
          variable: "emergency_processing"
          operator: "equals"
          value: "true"
        business_hours:
          type: "time"
          operator: "between"
          start_time: "09:00"
          end_time: "17:00"
      branches:
        high_volume: "high_volume_processing"
        emergency_mode: "emergency_processing"
        business_hours: "normal_processing"
      default_branch: "standard_processing"
      branch_mode: "single"

  # Conditional execution
  - name: "conditional_task"
    type: "CONDITIONAL"
    description: "Execute only if condition is met"
    properties:
      condition:
        type: "xcom"
        task_id: "data_check"
        key: "has_new_data"
        operator: "equals"
        value: true
      python_callable: "process_new_data"
      skip_on_false: true

  # Multiple path branching
  - name: "multi_branch"
    type: "BRANCH"
    description: "Branch to multiple paths based on conditions"
    properties:
      conditions:
        needs_validation:
          type: "python"
          callable: "check_data_quality"
        needs_transformation:
          type: "python" 
          callable: "check_transformation_needed"
        ready_for_load:
          type: "python"
          callable: "check_load_ready"
      branches:
        needs_validation: ["validate_data", "quality_check"]
        needs_transformation: ["transform_data", "format_data"]
        ready_for_load: "load_data"
      branch_mode: "multiple"
      default_branch: "error_handling"
"""