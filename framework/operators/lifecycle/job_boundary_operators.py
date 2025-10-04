# Job Boundary Operators - START and END tasks for job lifecycle management
"""
Job boundary operators provide structured job lifecycle management including:
- START tasks for job initialization and audit logging
- END tasks for job finalization and cleanup
- Configurable audit actions and custom logic
- Job-level metadata and context management
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union, Callable

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.context import Context
from airflow.models import Variable

from ..base_operator import BaseInsightAirOperator
from ...core.audit_system import get_audit_system, AuditEventType, AuditLevel

logger = logging.getLogger(__name__)


class StartOperator(BaseInsightAirOperator, PythonOperator):
    """
    START task operator for job initialization and audit logging
    """
    
    template_fields = ('audit_actions', 'initialization_actions', 'job_metadata')
    
    def __init__(self,
                 audit_actions: Optional[List[Dict[str, Any]]] = None,
                 initialization_actions: Optional[List[Dict[str, Any]]] = None,
                 job_metadata: Optional[Dict[str, Any]] = None,
                 enable_default_audit: bool = True,
                 custom_start_callable: Optional[Union[Callable, str]] = None,
                 op_kwargs: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize StartOperator
        
        Args:
            audit_actions: List of audit actions to perform
            initialization_actions: List of initialization actions to perform
            job_metadata: Job-level metadata to store
            enable_default_audit: Whether to perform default audit logging
            custom_start_callable: Custom callable for additional start logic
            op_kwargs: Additional keyword arguments for callables
        """
        
        # Set python_callable to our start execution method
        super().__init__(
            python_callable=self._execute_start_logic,
            op_kwargs=op_kwargs or {},
            **kwargs
        )
        
        self.audit_actions = audit_actions or []
        self.initialization_actions = initialization_actions or []
        self.job_metadata = job_metadata or {}
        self.enable_default_audit = enable_default_audit
        self.custom_start_callable = custom_start_callable
        self.start_time = None
    
    def _execute_start_logic(self, context: Context, **op_kwargs) -> Dict[str, Any]:
        """Execute START task logic"""
        
        self.start_time = datetime.now(timezone.utc)
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        
        logger.info(f"Starting job: {dag_id} (run_id: {run_id})")
        
        results = {
            'job_start_time': self.start_time.isoformat(),
            'dag_id': dag_id,
            'run_id': run_id,
            'job_metadata': self.job_metadata,
            'audit_results': [],
            'initialization_results': []
        }
        
        try:
            # Perform default audit logging
            if self.enable_default_audit:
                audit_system = get_audit_system()
                audit_system.log_job_start(
                    dag_id=dag_id,
                    run_id=run_id,
                    context=context,
                    details=self.job_metadata
                )
                logger.info("Default job start audit logged")
            
            # Execute custom audit actions
            for audit_action in self.audit_actions:
                audit_result = self._execute_audit_action(audit_action, context, results)
                results['audit_results'].append(audit_result)
            
            # Execute initialization actions
            for init_action in self.initialization_actions:
                init_result = self._execute_initialization_action(init_action, context, results)
                results['initialization_results'].append(init_result)
            
            # Store job metadata in XCom
            context['task_instance'].xcom_push(key='job_metadata', value=self.job_metadata)
            context['task_instance'].xcom_push(key='job_start_time', value=self.start_time.isoformat())
            
            # Execute custom start callable if provided
            if self.custom_start_callable:
                custom_result = self._execute_custom_callable(self.custom_start_callable, context, results, **op_kwargs)
                results['custom_result'] = custom_result
            
            logger.info(f"Job start completed successfully: {dag_id}")
            return results
            
        except Exception as e:
            logger.error(f"Job start failed: {str(e)}")
            
            # Log failure audit event
            if self.enable_default_audit:
                audit_system = get_audit_system()
                audit_system.log_job_failure(
                    dag_id=dag_id,
                    run_id=run_id,
                    error_message=str(e),
                    context=context
                )
            
            raise
    
    def _execute_audit_action(self, audit_action: Dict[str, Any], context: Context, results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single audit action"""
        
        action_type = audit_action.get('type', 'log')
        action_result = {
            'action_type': action_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'error': None
        }
        
        try:
            if action_type == 'log':
                # Simple logging action
                message = audit_action.get('message', 'Job start audit action')
                level = audit_action.get('level', 'INFO')
                logger.log(getattr(logging, level), message)
                action_result['success'] = True
                action_result['message'] = message
                
            elif action_type == 'audit_system':
                # Use audit system for logging
                audit_system = get_audit_system()
                event_type = AuditEventType(audit_action.get('event_type', 'CUSTOM'))
                message = audit_action.get('message', 'Custom audit action')
                level = AuditLevel(audit_action.get('level', 'INFO'))
                
                audit_system.log_event(
                    event_type=event_type,
                    dag_id=context['dag'].dag_id,
                    run_id=context['run_id'],
                    task_id=self.task_id,
                    message=message,
                    level=level,
                    context=context,
                    custom_data=audit_action.get('custom_data')
                )
                action_result['success'] = True
                
            elif action_type == 'callable':
                # Execute custom callable
                callable_name = audit_action.get('callable')
                callable_args = audit_action.get('args', [])
                callable_kwargs = audit_action.get('kwargs', {})
                
                result = self._execute_custom_callable(callable_name, context, results, *callable_args, **callable_kwargs)
                action_result['success'] = True
                action_result['result'] = result
                
            elif action_type == 'xcom':
                # Store data in XCom
                key = audit_action.get('key', 'audit_data')
                value = audit_action.get('value', {})
                
                context['task_instance'].xcom_push(key=key, value=value)
                action_result['success'] = True
                action_result['xcom_key'] = key
                
            else:
                logger.warning(f"Unknown audit action type: {action_type}")
                action_result['error'] = f"Unknown action type: {action_type}"
            
        except Exception as e:
            logger.error(f"Audit action failed: {str(e)}")
            action_result['error'] = str(e)
        
        return action_result
    
    def _execute_initialization_action(self, init_action: Dict[str, Any], context: Context, results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single initialization action"""
        
        action_type = init_action.get('type', 'callable')
        action_result = {
            'action_type': action_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'error': None
        }
        
        try:
            if action_type == 'callable':
                # Execute custom callable
                callable_name = init_action.get('callable')
                callable_args = init_action.get('args', [])
                callable_kwargs = init_action.get('kwargs', {})
                
                result = self._execute_custom_callable(callable_name, context, results, *callable_args, **callable_kwargs)
                action_result['success'] = True
                action_result['result'] = result
                
            elif action_type == 'variable':
                # Set Airflow Variable
                var_key = init_action.get('key')
                var_value = init_action.get('value')
                
                Variable.set(var_key, var_value)
                action_result['success'] = True
                action_result['variable_key'] = var_key
                
            elif action_type == 'xcom':
                # Set XCom value
                key = init_action.get('key')
                value = init_action.get('value')
                
                context['task_instance'].xcom_push(key=key, value=value)
                action_result['success'] = True
                action_result['xcom_key'] = key
                
            else:
                logger.warning(f"Unknown initialization action type: {action_type}")
                action_result['error'] = f"Unknown action type: {action_type}"
            
        except Exception as e:
            logger.error(f"Initialization action failed: {str(e)}")
            action_result['error'] = str(e)
        
        return action_result
    
    def _execute_custom_callable(self, callable_ref: Union[Callable, str], context: Context, results: Dict[str, Any], *args, **kwargs) -> Any:
        """Execute custom callable"""
        
        if isinstance(callable_ref, str):
            # Resolve callable from string
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            callable_func = registry.get_callable(callable_ref)
        else:
            callable_func = callable_ref
        
        if not callable(callable_func):
            raise ValueError(f"Invalid callable: {callable_ref}")
        
        # Add context and results to kwargs
        kwargs.update({
            'context': context,
            'start_results': results
        })
        
        return callable_func(*args, **kwargs)


class EndOperator(BaseInsightAirOperator, PythonOperator):
    """
    END task operator for job finalization and audit logging
    """
    
    template_fields = ('audit_actions', 'finalization_actions')
    
    def __init__(self,
                 audit_actions: Optional[List[Dict[str, Any]]] = None,
                 finalization_actions: Optional[List[Dict[str, Any]]] = None,
                 enable_default_audit: bool = True,
                 calculate_job_duration: bool = True,
                 custom_end_callable: Optional[Union[Callable, str]] = None,
                 op_kwargs: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize EndOperator
        
        Args:
            audit_actions: List of audit actions to perform
            finalization_actions: List of finalization actions to perform
            enable_default_audit: Whether to perform default audit logging
            calculate_job_duration: Whether to calculate job duration
            custom_end_callable: Custom callable for additional end logic
            op_kwargs: Additional keyword arguments for callables
        """
        
        # Set python_callable to our end execution method
        super().__init__(
            python_callable=self._execute_end_logic,
            op_kwargs=op_kwargs or {},
            **kwargs
        )
        
        self.audit_actions = audit_actions or []
        self.finalization_actions = finalization_actions or []
        self.enable_default_audit = enable_default_audit
        self.calculate_job_duration = calculate_job_duration
        self.custom_end_callable = custom_end_callable
        self.end_time = None
    
    def _execute_end_logic(self, context: Context, **op_kwargs) -> Dict[str, Any]:
        """Execute END task logic"""
        
        self.end_time = datetime.now(timezone.utc)
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        
        logger.info(f"Ending job: {dag_id} (run_id: {run_id})")
        
        # Calculate job duration if enabled
        job_duration = None
        if self.calculate_job_duration:
            try:
                start_time_str = context['task_instance'].xcom_pull(key='job_start_time')
                if start_time_str:
                    start_time = datetime.fromisoformat(start_time_str)
                    job_duration = (self.end_time - start_time).total_seconds()
                    logger.info(f"Job duration: {job_duration} seconds")
            except Exception as e:
                logger.warning(f"Could not calculate job duration: {e}")
        
        results = {
            'job_end_time': self.end_time.isoformat(),
            'dag_id': dag_id,
            'run_id': run_id,
            'job_duration_seconds': job_duration,
            'audit_results': [],
            'finalization_results': []
        }
        
        try:
            # Perform default audit logging
            if self.enable_default_audit:
                audit_system = get_audit_system()
                audit_system.log_job_end(
                    dag_id=dag_id,
                    run_id=run_id,
                    duration_seconds=job_duration,
                    context=context
                )
                logger.info("Default job end audit logged")
            
            # Execute custom audit actions
            for audit_action in self.audit_actions:
                audit_result = self._execute_audit_action(audit_action, context, results)
                results['audit_results'].append(audit_result)
            
            # Execute finalization actions
            for final_action in self.finalization_actions:
                final_result = self._execute_finalization_action(final_action, context, results)
                results['finalization_results'].append(final_result)
            
            # Store job end metadata in XCom
            context['task_instance'].xcom_push(key='job_end_time', value=self.end_time.isoformat())
            if job_duration:
                context['task_instance'].xcom_push(key='job_duration_seconds', value=job_duration)
            
            # Execute custom end callable if provided
            if self.custom_end_callable:
                custom_result = self._execute_custom_callable(self.custom_end_callable, context, results, **op_kwargs)
                results['custom_result'] = custom_result
            
            # Log job success
            if self.enable_default_audit:
                audit_system = get_audit_system()
                audit_system.log_job_success(
                    dag_id=dag_id,
                    run_id=run_id,
                    duration_seconds=job_duration,
                    context=context
                )
            
            logger.info(f"Job end completed successfully: {dag_id}")
            return results
            
        except Exception as e:
            logger.error(f"Job end failed: {str(e)}")
            
            # Log failure audit event
            if self.enable_default_audit:
                audit_system = get_audit_system()
                audit_system.log_job_failure(
                    dag_id=dag_id,
                    run_id=run_id,
                    error_message=str(e),
                    duration_seconds=job_duration,
                    context=context
                )
            
            raise
    
    def _execute_audit_action(self, audit_action: Dict[str, Any], context: Context, results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single audit action (same logic as StartOperator)"""
        
        action_type = audit_action.get('type', 'log')
        action_result = {
            'action_type': action_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'error': None
        }
        
        try:
            if action_type == 'log':
                message = audit_action.get('message', 'Job end audit action')
                level = audit_action.get('level', 'INFO')
                logger.log(getattr(logging, level), message)
                action_result['success'] = True
                action_result['message'] = message
                
            elif action_type == 'audit_system':
                audit_system = get_audit_system()
                event_type = AuditEventType(audit_action.get('event_type', 'CUSTOM'))
                message = audit_action.get('message', 'Custom audit action')
                level = AuditLevel(audit_action.get('level', 'INFO'))
                
                audit_system.log_event(
                    event_type=event_type,
                    dag_id=context['dag'].dag_id,
                    run_id=context['run_id'],
                    task_id=self.task_id,
                    message=message,
                    level=level,
                    context=context,
                    custom_data=audit_action.get('custom_data')
                )
                action_result['success'] = True
                
            elif action_type == 'callable':
                callable_name = audit_action.get('callable')
                callable_args = audit_action.get('args', [])
                callable_kwargs = audit_action.get('kwargs', {})
                
                result = self._execute_custom_callable(callable_name, context, results, *callable_args, **callable_kwargs)
                action_result['success'] = True
                action_result['result'] = result
                
            elif action_type == 'xcom':
                key = audit_action.get('key', 'audit_data')
                value = audit_action.get('value', {})
                
                context['task_instance'].xcom_push(key=key, value=value)
                action_result['success'] = True
                action_result['xcom_key'] = key
                
            else:
                logger.warning(f"Unknown audit action type: {action_type}")
                action_result['error'] = f"Unknown action type: {action_type}"
            
        except Exception as e:
            logger.error(f"Audit action failed: {str(e)}")
            action_result['error'] = str(e)
        
        return action_result
    
    def _execute_finalization_action(self, final_action: Dict[str, Any], context: Context, results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single finalization action"""
        
        action_type = final_action.get('type', 'callable')
        action_result = {
            'action_type': action_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'error': None
        }
        
        try:
            if action_type == 'callable':
                callable_name = final_action.get('callable')
                callable_args = final_action.get('args', [])
                callable_kwargs = final_action.get('kwargs', {})
                
                result = self._execute_custom_callable(callable_name, context, results, *callable_args, **callable_kwargs)
                action_result['success'] = True
                action_result['result'] = result
                
            elif action_type == 'cleanup':
                # Cleanup action
                cleanup_type = final_action.get('cleanup_type', 'xcom')
                
                if cleanup_type == 'xcom':
                    # Clean up specific XCom keys
                    keys_to_clean = final_action.get('xcom_keys', [])
                    for key in keys_to_clean:
                        try:
                            # Note: XCom cleanup would need custom implementation
                            logger.info(f"Would clean up XCom key: {key}")
                        except Exception as e:
                            logger.warning(f"Failed to clean up XCom key {key}: {e}")
                
                action_result['success'] = True
                
            elif action_type == 'notification':
                # Send notification
                notification_type = final_action.get('notification_type', 'log')
                message = final_action.get('message', 'Job completed')
                
                if notification_type == 'log':
                    logger.info(f"Job completion notification: {message}")
                elif notification_type == 'email':
                    # Email notification would need implementation
                    logger.info(f"Would send email notification: {message}")
                
                action_result['success'] = True
                
            else:
                logger.warning(f"Unknown finalization action type: {action_type}")
                action_result['error'] = f"Unknown action type: {action_type}"
            
        except Exception as e:
            logger.error(f"Finalization action failed: {str(e)}")
            action_result['error'] = str(e)
        
        return action_result
    
    def _execute_custom_callable(self, callable_ref: Union[Callable, str], context: Context, results: Dict[str, Any], *args, **kwargs) -> Any:
        """Execute custom callable (same logic as StartOperator)"""
        
        if isinstance(callable_ref, str):
            from ...core.callable_registry import get_callable_registry
            registry = get_callable_registry()
            callable_func = registry.get_callable(callable_ref)
        else:
            callable_func = callable_ref
        
        if not callable(callable_func):
            raise ValueError(f"Invalid callable: {callable_ref}")
        
        kwargs.update({
            'context': context,
            'end_results': results
        })
        
        return callable_func(*args, **kwargs)


# Example configuration usage:
"""
tasks:
  - name: "job_start"
    type: "START"
    description: "Initialize job with audit logging"
    properties:
      job_metadata:
        environment: "production"
        data_source: "external_api"
        batch_date: "{{ ds }}"
      audit_actions:
        - type: "audit_system"
          event_type: "CUSTOM"
          message: "Job initialization started"
          level: "INFO"
          custom_data:
            component: "data_pipeline"
        - type: "callable"
          callable: "send_start_notification"
          kwargs:
            recipient: "ops-team@company.com"
      initialization_actions:
        - type: "variable"
          key: "current_job_run_id"
          value: "{{ run_id }}"
        - type: "callable"
          callable: "setup_job_environment"
          kwargs:
            env: "production"

  - name: "job_end"
    type: "END"
    description: "Finalize job with audit logging and cleanup"
    properties:
      calculate_job_duration: true
      audit_actions:
        - type: "audit_system"
          event_type: "CUSTOM"
          message: "Job completion audit"
          level: "INFO"
        - type: "callable"
          callable: "generate_job_report"
      finalization_actions:
        - type: "cleanup"
          cleanup_type: "xcom"
          xcom_keys: ["temp_data", "intermediate_results"]
        - type: "notification"
          notification_type: "email"
          message: "Data pipeline completed successfully"
        - type: "callable"
          callable: "archive_job_artifacts"
"""