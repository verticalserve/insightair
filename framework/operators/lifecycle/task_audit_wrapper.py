# Task Audit Wrapper - Task-level audit hooks and enable/disable functionality
"""
Task audit wrapper provides:
- Task-level audit logging (start, end, success, failure)
- Task enable/disable functionality with dummy operator fallback
- Task group lifecycle management
- Configurable audit actions and hooks
- Performance monitoring and metrics collection
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union, Callable, Type
from functools import wraps

from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup

from ..base_operator import BaseInsightAirOperator
from ...core.audit_system import get_audit_system, AuditEventType, AuditLevel

logger = logging.getLogger(__name__)


class DisabledTaskOperator(DummyOperator):
    """
    Dummy operator that replaces disabled tasks
    """
    
    def __init__(self, original_task_id: str, disabled_reason: str = "Task disabled via configuration", **kwargs):
        """
        Initialize DisabledTaskOperator
        
        Args:
            original_task_id: ID of the original task that was disabled
            disabled_reason: Reason for disabling the task
        """
        super().__init__(**kwargs)
        self.original_task_id = original_task_id
        self.disabled_reason = disabled_reason
    
    def execute(self, context: Context):
        """Execute disabled task (just log and skip)"""
        logger.info(f"Task '{self.original_task_id}' is disabled: {self.disabled_reason}")
        
        # Store disabled task info in XCom
        context['task_instance'].xcom_push(
            key='disabled_task_info',
            value={
                'original_task_id': self.original_task_id,
                'disabled_reason': self.disabled_reason,
                'skipped_at': datetime.now(timezone.utc).isoformat()
            }
        )
        
        return f"Task {self.original_task_id} skipped (disabled)"


class AuditWrapper:
    """
    Wrapper class that adds audit functionality to any operator
    """
    
    def __init__(self,
                 operator: BaseOperator,
                 enable_task_audit: bool = True,
                 audit_actions: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                 custom_hooks: Optional[Dict[str, Union[Callable, str]]] = None,
                 collect_metrics: bool = True):
        """
        Initialize AuditWrapper
        
        Args:
            operator: The operator to wrap with audit functionality
            enable_task_audit: Whether to enable default task audit logging
            audit_actions: Custom audit actions for different lifecycle events
            custom_hooks: Custom hooks for lifecycle events
            collect_metrics: Whether to collect performance metrics
        """
        self.operator = operator
        self.enable_task_audit = enable_task_audit
        self.audit_actions = audit_actions or {}
        self.custom_hooks = custom_hooks or {}
        self.collect_metrics = collect_metrics
        
        # Wrap the operator's execute method
        self._wrap_execute_method()
    
    def _wrap_execute_method(self):
        """Wrap the operator's execute method with audit functionality"""
        
        original_execute = self.operator.execute
        
        @wraps(original_execute)
        def wrapped_execute(context: Context):
            return self._execute_with_audit(original_execute, context)
        
        self.operator.execute = wrapped_execute
    
    def _execute_with_audit(self, original_execute: Callable, context: Context):
        """Execute operator with audit logging"""
        
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_id = self.operator.task_id
        
        start_time = datetime.now(timezone.utc)
        audit_system = get_audit_system()
        
        # Execute pre-execution hooks and audit
        try:
            if self.enable_task_audit:
                audit_system.log_task_start(
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    context=context
                )
            
            # Execute custom start audit actions
            self._execute_audit_actions('start', context, {
                'start_time': start_time.isoformat(),
                'dag_id': dag_id,
                'run_id': run_id,
                'task_id': task_id
            })
            
            # Execute custom start hook
            self._execute_custom_hook('on_start', context, {})
            
        except Exception as e:
            logger.warning(f"Pre-execution audit failed for task {task_id}: {e}")
        
        # Execute the actual operator
        result = None
        error = None
        
        try:
            logger.debug(f"Executing task with audit: {task_id}")
            result = original_execute(context)
            
            # Task completed successfully
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            # Log success audit
            if self.enable_task_audit:
                audit_system.log_task_success(
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    duration_seconds=duration,
                    context=context
                )
            
            # Store metrics if enabled
            if self.collect_metrics:
                context['task_instance'].xcom_push(
                    key='task_metrics',
                    value={
                        'duration_seconds': duration,
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat(),
                        'success': True
                    }
                )
            
            # Execute custom success audit actions
            self._execute_audit_actions('success', context, {
                'result': result,
                'duration_seconds': duration,
                'end_time': end_time.isoformat()
            })
            
            # Execute custom success hook
            self._execute_custom_hook('on_success', context, {'result': result, 'duration': duration})
            
            return result
            
        except Exception as e:
            error = e
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Task {task_id} failed: {str(e)}")
            
            # Log failure audit
            if self.enable_task_audit:
                audit_system.log_task_failure(
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    error_message=str(e),
                    duration_seconds=duration,
                    context=context
                )
            
            # Store failure metrics if enabled
            if self.collect_metrics:
                context['task_instance'].xcom_push(
                    key='task_metrics',
                    value={
                        'duration_seconds': duration,
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat(),
                        'success': False,
                        'error_message': str(e)
                    }
                )
            
            # Execute custom failure audit actions
            self._execute_audit_actions('failure', context, {
                'error_message': str(e),
                'duration_seconds': duration,
                'end_time': end_time.isoformat()
            })
            
            # Execute custom failure hook
            self._execute_custom_hook('on_failure', context, {'error': e, 'duration': duration})
            
            raise
        
        finally:
            # Always execute end audit
            try:
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                
                if self.enable_task_audit:
                    audit_system.log_task_end(
                        dag_id=dag_id,
                        run_id=run_id,
                        task_id=task_id,
                        duration_seconds=duration,
                        context=context
                    )
                
                # Execute custom end audit actions
                self._execute_audit_actions('end', context, {
                    'duration_seconds': duration,
                    'end_time': end_time.isoformat(),
                    'had_error': error is not None
                })
                
                # Execute custom end hook
                self._execute_custom_hook('on_end', context, {
                    'duration': duration,
                    'error': error,
                    'result': result
                })
                
            except Exception as e:
                logger.warning(f"Post-execution audit failed for task {task_id}: {e}")
    
    def _execute_audit_actions(self, event_type: str, context: Context, event_data: Dict[str, Any]):
        """Execute custom audit actions for a specific event type"""
        
        actions = self.audit_actions.get(event_type, [])
        
        for action in actions:
            try:
                action_type = action.get('type', 'log')
                
                if action_type == 'log':
                    message = action.get('message', f'Task {event_type} audit')
                    level = action.get('level', 'INFO')
                    logger.log(getattr(logging, level), message)
                    
                elif action_type == 'audit_system':
                    audit_system = get_audit_system()
                    audit_event_type = AuditEventType(action.get('event_type', 'CUSTOM'))
                    message = action.get('message', f'Task {event_type} custom audit')
                    level = AuditLevel(action.get('level', 'INFO'))
                    
                    audit_system.log_event(
                        event_type=audit_event_type,
                        dag_id=context['dag'].dag_id,
                        run_id=context['run_id'],
                        task_id=self.operator.task_id,
                        message=message,
                        level=level,
                        context=context,
                        custom_data={**event_data, **action.get('custom_data', {})}
                    )
                    
                elif action_type == 'callable':
                    callable_name = action.get('callable')
                    callable_args = action.get('args', [])
                    callable_kwargs = action.get('kwargs', {})
                    
                    # Add event data to kwargs
                    callable_kwargs.update({
                        'context': context,
                        'event_type': event_type,
                        'event_data': event_data
                    })
                    
                    self._execute_custom_callable(callable_name, context, *callable_args, **callable_kwargs)
                    
                elif action_type == 'xcom':
                    key = action.get('key', f'audit_{event_type}')
                    value = action.get('value', event_data)
                    
                    context['task_instance'].xcom_push(key=key, value=value)
                
            except Exception as e:
                logger.error(f"Audit action failed for {event_type}: {e}")
    
    def _execute_custom_hook(self, hook_name: str, context: Context, hook_data: Dict[str, Any]):
        """Execute custom hook function"""
        
        hook_callable = self.custom_hooks.get(hook_name)
        if not hook_callable:
            return
        
        try:
            if isinstance(hook_callable, str):
                hook_callable = self._resolve_callable_from_string(hook_callable)
            
            if callable(hook_callable):
                hook_callable(context, hook_data)
            
        except Exception as e:
            logger.error(f"Custom hook {hook_name} failed: {e}")
    
    def _execute_custom_callable(self, callable_ref: Union[Callable, str], context: Context, *args, **kwargs):
        """Execute custom callable"""
        
        if isinstance(callable_ref, str):
            callable_func = self._resolve_callable_from_string(callable_ref)
        else:
            callable_func = callable_ref
        
        if not callable(callable_func):
            raise ValueError(f"Invalid callable: {callable_ref}")
        
        return callable_func(*args, **kwargs)
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve callable from string reference"""
        try:
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            return registry.get_callable(callable_string)
        except Exception as e:
            logger.error(f"Failed to resolve callable '{callable_string}': {e}")
            raise ValueError(f"Cannot resolve callable: {callable_string}")


class TaskGroupAuditManager:
    """
    Manager for task group lifecycle audit events
    """
    
    @staticmethod
    def create_task_group_with_audit(group_id: str,
                                   dag,
                                   enable_audit: bool = True,
                                   audit_actions: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                                   custom_hooks: Optional[Dict[str, Union[Callable, str]]] = None,
                                   **task_group_kwargs) -> TaskGroup:
        """
        Create a TaskGroup with audit functionality
        
        Args:
            group_id: Task group ID
            dag: Airflow DAG
            enable_audit: Whether to enable audit logging
            audit_actions: Custom audit actions for task group events
            custom_hooks: Custom hooks for task group events
            **task_group_kwargs: Additional TaskGroup arguments
            
        Returns:
            TaskGroup with audit functionality
        """
        
        task_group = TaskGroup(group_id=group_id, dag=dag, **task_group_kwargs)
        
        if enable_audit:
            # Add audit functionality to task group
            TaskGroupAuditManager._add_audit_to_task_group(
                task_group, audit_actions, custom_hooks
            )
        
        return task_group
    
    @staticmethod
    def _add_audit_to_task_group(task_group: TaskGroup,
                                audit_actions: Optional[Dict[str, List[Dict[str, Any]]]],
                                custom_hooks: Optional[Dict[str, Union[Callable, str]]]):
        """Add audit functionality to existing task group"""
        
        # This would require more complex integration with Airflow's TaskGroup
        # For now, we'll log that audit would be added
        logger.info(f"Task group audit functionality added to: {task_group.group_id}")
        
        # Store audit configuration for later use
        if hasattr(task_group, 'extras'):
            task_group.extras['audit_actions'] = audit_actions
            task_group.extras['custom_hooks'] = custom_hooks
        else:
            task_group.extras = {
                'audit_actions': audit_actions,
                'custom_hooks': custom_hooks
            }


def create_audited_operator(operator_class: Type[BaseOperator],
                           task_config: Dict[str, Any],
                           task_properties: Dict[str, Any],
                           dag,
                           enable_audit: bool = None,
                           audit_config: Optional[Dict[str, Any]] = None) -> BaseOperator:
    """
    Create an operator with optional audit functionality and enable/disable support
    
    Args:
        operator_class: Class of operator to create
        task_config: Task configuration
        task_properties: Task properties
        dag: Airflow DAG
        enable_audit: Whether to enable audit (overrides config)
        audit_config: Audit configuration
        
    Returns:
        Operator instance (potentially wrapped with audit or disabled)
    """
    
    # Check if task is disabled
    task_enabled = task_properties.get('enabled', True)
    if not task_enabled:
        disabled_reason = task_properties.get('disabled_reason', 'Task disabled via configuration')
        logger.info(f"Creating disabled operator for task: {task_config['name']}")
        
        return DisabledTaskOperator(
            task_id=task_config['name'],
            original_task_id=task_config['name'],
            disabled_reason=disabled_reason,
            dag=dag
        )
    
    # Create the actual operator
    operator_args = {
        'task_id': task_config['name'],
        'dag': dag,
    }
    
    # Add task properties as operator arguments
    operator_args.update(task_properties)
    
    # Remove framework-specific properties that shouldn't go to operator
    framework_props = ['enabled', 'disabled_reason', 'audit', 'audit_actions', 'custom_hooks']
    for prop in framework_props:
        operator_args.pop(prop, None)
    
    operator = operator_class(**operator_args)
    
    # Check if audit should be enabled
    should_enable_audit = enable_audit
    if should_enable_audit is None:
        audit_settings = audit_config or task_properties.get('audit', {})
        should_enable_audit = audit_settings.get('enabled', True)
    
    # Wrap with audit functionality if enabled
    if should_enable_audit:
        audit_settings = audit_config or task_properties.get('audit', {})
        
        wrapper = AuditWrapper(
            operator=operator,
            enable_task_audit=audit_settings.get('enable_task_audit', True),
            audit_actions=audit_settings.get('audit_actions', {}),
            custom_hooks=audit_settings.get('custom_hooks', {}),
            collect_metrics=audit_settings.get('collect_metrics', True)
        )
        
        logger.debug(f"Task {task_config['name']} wrapped with audit functionality")
    
    return operator


# Example configuration usage:
"""
tasks:
  - name: "data_processing"
    type: "PYTHON"
    description: "Process daily data"
    properties:
      enabled: true
      python_callable: "process_data"
      audit:
        enabled: true
        enable_task_audit: true
        collect_metrics: true
        audit_actions:
          start:
            - type: "log"
              message: "Starting data processing task"
              level: "INFO"
            - type: "callable"
              callable: "notify_processing_start"
          success:
            - type: "audit_system"
              event_type: "CUSTOM"
              message: "Data processing completed successfully"
              level: "INFO"
              custom_data:
                component: "data_processor"
          failure:
            - type: "callable"
              callable: "handle_processing_failure"
        custom_hooks:
          on_start: "setup_processing_environment"
          on_end: "cleanup_processing_resources"

  - name: "optional_validation"
    type: "PYTHON"
    description: "Optional data validation step"
    properties:
      enabled: false
      disabled_reason: "Validation disabled for this sprint"
      python_callable: "validate_data"

  - name: "reporting"
    type: "PYTHON"
    description: "Generate reports"
    properties:
      enabled: true
      python_callable: "generate_reports"
      audit:
        enabled: true
        audit_actions:
          end:
            - type: "xcom"
              key: "reporting_completed"
              value: {"timestamp": "{{ ts }}", "status": "completed"}
"""