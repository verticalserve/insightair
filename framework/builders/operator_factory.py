# Operator Factory - Create operators from configuration
"""
OperatorFactory provides a factory pattern for creating Airflow operators
from configuration with support for:
- Dynamic operator creation based on task type
- Configuration-driven operator initialization
- Custom operator registration and extension
- Runtime parameter injection
"""

import logging
from typing import Dict, Any, Type, Optional, Callable, List
from airflow.models.baseoperator import BaseOperator

from ..core.registry import OperatorRegistry
from ..core.exceptions import OperatorCreationError, ConfigurationError
from ..operators.base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class OperatorFactory:
    """
    Factory class for creating Airflow operators from configuration
    """
    
    def __init__(self, registry: Optional[OperatorRegistry] = None):
        """
        Initialize OperatorFactory
        
        Args:
            registry: Operator registry instance (creates new if None)
        """
        self.registry = registry or OperatorRegistry()
        logger.debug("OperatorFactory initialized")
    
    def create_operator(self,
                       task_config: Dict[str, Any],
                       task_properties: Dict[str, Any],
                       dag,
                       runtime_params: Optional[Dict[str, Any]] = None) -> BaseOperator:
        """
        Create an operator from task configuration
        
        Args:
            task_config: Task configuration from main config
            task_properties: Task properties (from separate file or merged)
            dag: Airflow DAG instance
            runtime_params: Runtime parameters for override
            
        Returns:
            Created Airflow operator instance
            
        Raises:
            OperatorCreationError: If operator cannot be created
        """
        try:
            task_name = task_config.get('name')
            task_type = task_config.get('type')
            
            if not task_name:
                raise ConfigurationError("Task configuration must have 'name' field")
            
            if not task_type:
                raise ConfigurationError(f"Task '{task_name}' must have 'type' field")
            
            # Apply runtime parameter overrides
            effective_properties = task_properties.copy()
            if runtime_params:
                effective_properties.update(runtime_params)
            
            # Check if task is disabled first
            if not effective_properties.get('enabled', True):
                operator = self._create_disabled_operator(task_config, effective_properties, dag)
            else:
                # Check if we have a specific creation method for this task type
                operator = self._create_operator_by_type(task_config, effective_properties, dag, runtime_params)
                
                if operator is None:
                    # Fall back to registry-based creation
                    operator_class = self.registry.get_operator_class(task_type)
                    
                    # Build operator arguments
                    operator_args = self._build_operator_args(
                        task_config, effective_properties, dag, runtime_params
                    )
                    
                    # Create operator instance
                    operator = operator_class(**operator_args)
                
                # Apply audit wrapping if enabled
                operator = self._apply_audit_wrapper(operator, task_config, effective_properties)
            
            # Set additional properties
            self._set_operator_properties(operator, task_config, effective_properties)
            
            logger.info(f"Created {task_type} operator for task '{task_name}'")
            return operator
            
        except Exception as e:
            logger.error(f"Failed to create operator for task '{task_name}': {str(e)}")
            raise OperatorCreationError(f"Operator creation failed: {str(e)}") from e
    
    def _create_operator_by_type(self,
                                task_config: Dict[str, Any],
                                task_properties: Dict[str, Any],
                                dag,
                                runtime_params: Optional[Dict[str, Any]] = None) -> Optional[BaseOperator]:
        """
        Create operator using specific creation method based on task type
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            runtime_params: Runtime parameters
            
        Returns:
            Created operator or None if no specific method exists
        """
        task_type = task_config.get('type', '').upper()
        
        # Apply runtime parameter overrides to properties
        effective_properties = task_properties.copy()
        if runtime_params:
            effective_properties.update(runtime_params)
        
        # Map task types to creation methods
        creation_methods = {
            # Basic operators
            'PYTHON': self.create_python_operator,
            'BASH': self.create_bash_operator,
            
            # Control flow operators
            'FOREACH': self.create_foreach_operator,
            'BRANCH': self.create_branch_operator,
            'BRANCH_PYTHON': self.create_branch_python_operator,
            'CONDITIONAL': self.create_conditional_operator,
            
            # Sensor operators
            'FILE_SENSOR': self.create_file_sensor,
            'S3_SENSOR': self.create_s3_sensor,
            'GCS_SENSOR': self.create_gcs_sensor,
            'DATABASE_SENSOR': self.create_database_sensor,
            'TABLE_SENSOR': self.create_table_sensor,
            'API_SENSOR': self.create_api_sensor,
            'HTTP_SENSOR': self.create_http_sensor,
            'WEBHOOK_SENSOR': self.create_webhook_sensor,
            'PYTHON_SENSOR': self.create_python_sensor,
            'CUSTOM_SENSOR': self.create_custom_sensor,
            'TIME_SENSOR': self.create_time_sensor,
            
            # Lifecycle operators
            'START': self.create_start_operator,
            'END': self.create_end_operator,
            
            # Generic multi-cloud operators
            'STORAGE_UPLOAD': self.create_generic_operator,
            'STORAGE_DOWNLOAD': self.create_generic_operator,
            'STORAGE_SYNC': self.create_generic_operator,
            'DATABASE_QUERY': self.create_generic_operator,
            'DATABASE_TABLE': self.create_generic_operator,
            'DATABASE_BULK_LOAD': self.create_generic_operator,
            'MESSAGE_PUBLISH': self.create_generic_operator,
            'MESSAGE_CONSUME': self.create_generic_operator,
            'TABLEAU_HYPER': self.create_generic_operator,
            'TABLEAU_PUBLISH': self.create_generic_operator,
            'TABLEAU_REFRESH': self.create_generic_operator,
            'SALESFORCE_EXTRACT': self.create_generic_operator,
            'SALESFORCE_LOAD': self.create_generic_operator,
            'SALESFORCE_QUERY': self.create_generic_operator,
            
            # API/HTTP operations
            'HTTP_REQUEST': self.create_generic_operator,
            'API_CALL': self.create_generic_operator,
            'WEBHOOK': self.create_generic_operator,
            'API_POLLING': self.create_generic_operator,
            
            # File transfer operations
            'FILE_TRANSFER': self.create_generic_operator,
            'SFTP_UPLOAD': self.create_generic_operator,
            'SFTP_DOWNLOAD': self.create_generic_operator,
            'FTP_UPLOAD': self.create_generic_operator,
            'FTP_DOWNLOAD': self.create_generic_operator,
            'DIRECTORY_SYNC': self.create_generic_operator,
            
            # Data quality operations
            'DATA_VALIDATION': self.create_generic_operator,
            'DATA_QUALITY': self.create_generic_operator,
            'DQ': self.create_generic_operator,
            'DATA_PROFILING': self.create_generic_operator,
            
            # Archive operations
            'FILE_ARCHIVE': self.create_generic_operator,
            'ARCHIVE': self.create_generic_operator,
            'COMPRESS': self.create_generic_operator,
            'DECOMPRESS': self.create_generic_operator,
            'BATCH_ARCHIVE': self.create_generic_operator,
            
            # Notification operations
            'EMAIL': self.create_generic_operator,
            'EMAIL_NOTIFICATION': self.create_generic_operator,
            'SLACK': self.create_generic_operator,
            'SLACK_NOTIFICATION': self.create_generic_operator,
            'TEAMS': self.create_generic_operator,
            'TEAMS_NOTIFICATION': self.create_generic_operator,
            'MULTI_NOTIFICATION': self.create_generic_operator,
        }
        
        creation_method = creation_methods.get(task_type)
        if creation_method:
            logger.debug(f"Using specific creation method for task type: {task_type}")
            return creation_method(task_config, effective_properties, dag)
        
        return None
    
    def _build_operator_args(self,
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            dag,
                            runtime_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build arguments for operator initialization
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            runtime_params: Runtime parameters
            
        Returns:
            Dictionary of operator initialization arguments
        """
        # Start with base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard Airflow operator arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add task-specific properties
        self._add_task_properties(args, task_properties, runtime_params)
        
        # Add custom InsightAir arguments for our operators
        if self._is_insightair_operator(task_config['type']):
            self._add_insightair_args(args, task_config, task_properties, runtime_params)
        
        return args
    
    def _add_standard_args(self,
                          args: Dict[str, Any],
                          task_config: Dict[str, Any],
                          task_properties: Dict[str, Any]):
        """Add standard Airflow operator arguments"""
        
        # Task description
        if 'description' in task_config:
            args['doc_md'] = task_config['description']
        
        # Task timeout
        if 'timeout_minutes' in task_config:
            from datetime import timedelta
            args['execution_timeout'] = timedelta(minutes=task_config['timeout_minutes'])
        elif 'timeout_minutes' in task_properties:
            from datetime import timedelta
            args['execution_timeout'] = timedelta(minutes=task_properties['timeout_minutes'])
        
        # Retry configuration
        if 'retry_count' in task_properties:
            args['retries'] = task_properties['retry_count']
        
        if 'retry_delay_seconds' in task_properties:
            from datetime import timedelta
            args['retry_delay'] = timedelta(seconds=task_properties['retry_delay_seconds'])
        
        # Email configuration
        if 'email_on_failure' in task_properties:
            args['email_on_failure'] = task_properties['email_on_failure']
        
        if 'email_on_retry' in task_properties:
            args['email_on_retry'] = task_properties['email_on_retry']
        
        # Pool configuration
        if 'pool' in task_properties:
            args['pool'] = task_properties['pool']
        
        if 'pool_slots' in task_properties:
            args['pool_slots'] = task_properties['pool_slots']
        
        # Priority weight
        if 'priority_weight' in task_properties:
            args['priority_weight'] = task_properties['priority_weight']
        
        # Queue
        if 'queue' in task_properties:
            args['queue'] = task_properties['queue']
    
    def _add_task_properties(self,
                            args: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            runtime_params: Optional[Dict[str, Any]] = None):
        """Add task-specific properties to operator arguments"""
        
        # Apply runtime parameter overrides
        effective_properties = task_properties.copy()
        if runtime_params:
            effective_properties.update(runtime_params)
        
        # Add all properties to args (operator will filter what it needs)
        args.update(effective_properties)
    
    def _add_insightair_args(self,
                            args: Dict[str, Any],
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            runtime_params: Optional[Dict[str, Any]] = None):
        """Add InsightAir-specific arguments"""
        
        # Add InsightAir-specific configuration
        insightair_config = {
            'task_config': task_config,
            'task_properties': task_properties,
            'runtime_params': runtime_params or {},
            'framework_version': '2.0.0'
        }
        
        args['insightair_config'] = insightair_config
    
    def _is_insightair_operator(self, task_type: str) -> bool:
        """Check if task type is an InsightAir custom operator"""
        try:
            operator_class = self.registry.get_operator_class(task_type)
            return issubclass(operator_class, BaseInsightAirOperator)
        except:
            return False
    
    def _set_operator_properties(self,
                                operator: BaseOperator,
                                task_config: Dict[str, Any],
                                task_properties: Dict[str, Any]):
        """Set additional properties on created operator"""
        
        # Set documentation
        if 'description' in task_config and not hasattr(operator, 'doc_md'):
            operator.doc_md = task_config['description']
        
        # Set custom properties for InsightAir operators
        if isinstance(operator, BaseInsightAirOperator):
            operator.set_framework_properties(task_config, task_properties)
    
    def register_operator(self,
                         task_type: str,
                         operator_class: Type[BaseOperator],
                         override: bool = False):
        """
        Register a custom operator
        
        Args:
            task_type: Task type identifier
            operator_class: Operator class to register
            override: Whether to override existing registration
        """
        self.registry.register_operator(task_type, operator_class, override)
        logger.info(f"Registered operator {operator_class.__name__} for type '{task_type}'")
    
    def get_supported_types(self) -> Dict[str, Type[BaseOperator]]:
        """
        Get all supported task types and their operator classes
        
        Returns:
            Dictionary mapping task types to operator classes
        """
        return self.registry.get_all_operators()
    
    def create_python_operator(self,
                              task_config: Dict[str, Any],
                              task_properties: Dict[str, Any],
                              dag,
                              python_callable: Optional[Callable] = None) -> BaseOperator:
        """
        Create a PythonOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            python_callable: Python function to execute
            
        Returns:
            PythonOperator instance
        """
        from airflow.operators.python import PythonOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add Python-specific arguments
        if python_callable:
            args['python_callable'] = python_callable
        elif 'python_callable' in task_properties:
            # This would need to be resolved from a string/module path
            args['python_callable'] = task_properties['python_callable']
        
        # Add op_args and op_kwargs
        if 'op_args' in task_properties:
            args['op_args'] = task_properties['op_args']
        
        if 'op_kwargs' in task_properties:
            args['op_kwargs'] = task_properties['op_kwargs']
        else:
            # Pass all task properties as op_kwargs
            args['op_kwargs'] = task_properties
        
        return PythonOperator(**args)
    
    def create_bash_operator(self,
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            dag) -> BaseOperator:
        """
        Create a BashOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            BashOperator instance
        """
        from airflow.operators.bash import BashOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add Bash-specific arguments
        if 'bash_command' in task_properties:
            args['bash_command'] = task_properties['bash_command']
        elif 'command' in task_properties:
            args['bash_command'] = task_properties['command']
        else:
            raise ConfigurationError("BashOperator requires 'bash_command' or 'command' property")
        
        # Add optional Bash arguments
        if 'env' in task_properties:
            args['env'] = task_properties['env']
        
        if 'cwd' in task_properties:
            args['cwd'] = task_properties['cwd']
        
        return BashOperator(**args)
    
    def create_foreach_operator(self,
                               task_config: Dict[str, Any],
                               task_properties: Dict[str, Any],
                               dag) -> BaseOperator:
        """
        Create a ForEachOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            ForEachOperator instance
        """
        from ..operators.control_flow.foreach_operator import ForEachOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add ForEach-specific arguments
        if 'items_source' not in task_properties:
            raise ConfigurationError("ForEachOperator requires 'items_source' property")
        args['items_source'] = task_properties['items_source']
        
        if 'task_template' not in task_properties:
            raise ConfigurationError("ForEachOperator requires 'task_template' property")
        args['task_template'] = task_properties['task_template']
        
        # Optional ForEach arguments
        if 'task_group_id' in task_properties:
            args['task_group_id'] = task_properties['task_group_id']
            
        if 'max_parallel_tasks' in task_properties:
            args['max_parallel_tasks'] = task_properties['max_parallel_tasks']
            
        if 'fail_on_empty' in task_properties:
            args['fail_on_empty'] = task_properties['fail_on_empty']
        
        return ForEachOperator(**args)
    
    def create_branch_operator(self,
                              task_config: Dict[str, Any],
                              task_properties: Dict[str, Any],
                              dag) -> BaseOperator:
        """
        Create a BranchOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            BranchOperator instance
        """
        from ..operators.control_flow.branch_operator import BranchOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add Branch-specific arguments
        if 'conditions' not in task_properties:
            raise ConfigurationError("BranchOperator requires 'conditions' property")
        args['conditions'] = task_properties['conditions']
        
        if 'branches' not in task_properties:
            raise ConfigurationError("BranchOperator requires 'branches' property")
        args['branches'] = task_properties['branches']
        
        # Optional Branch arguments
        if 'default_branch' in task_properties:
            args['default_branch'] = task_properties['default_branch']
            
        if 'branch_mode' in task_properties:
            args['branch_mode'] = task_properties['branch_mode']
        
        return BranchOperator(**args)
    
    def create_branch_python_operator(self,
                                     task_config: Dict[str, Any],
                                     task_properties: Dict[str, Any],
                                     dag) -> BaseOperator:
        """
        Create a BranchPythonOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            BranchPythonOperator instance
        """
        from ..operators.control_flow.branch_operator import BranchPythonOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add BranchPython-specific arguments
        if 'python_callable' not in task_properties:
            raise ConfigurationError("BranchPythonOperator requires 'python_callable' property")
        args['python_callable'] = task_properties['python_callable']
        
        # Optional BranchPython arguments
        if 'branch_conditions' in task_properties:
            args['branch_conditions'] = task_properties['branch_conditions']
            
        if 'default_branch' in task_properties:
            args['default_branch'] = task_properties['default_branch']
            
        if 'branch_map' in task_properties:
            args['branch_map'] = task_properties['branch_map']
            
        if 'op_args' in task_properties:
            args['op_args'] = task_properties['op_args']
            
        if 'op_kwargs' in task_properties:
            args['op_kwargs'] = task_properties['op_kwargs']
        
        return BranchPythonOperator(**args)
    
    def create_conditional_operator(self,
                                   task_config: Dict[str, Any],
                                   task_properties: Dict[str, Any],
                                   dag) -> BaseOperator:
        """
        Create a ConditionalOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            ConditionalOperator instance
        """
        from ..operators.control_flow.branch_operator import ConditionalOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add Conditional-specific arguments
        if 'condition' not in task_properties:
            raise ConfigurationError("ConditionalOperator requires 'condition' property")
        args['condition'] = task_properties['condition']
        
        if 'python_callable' not in task_properties:
            raise ConfigurationError("ConditionalOperator requires 'python_callable' property")
        args['python_callable'] = task_properties['python_callable']
        
        # Optional Conditional arguments
        if 'skip_on_false' in task_properties:
            args['skip_on_false'] = task_properties['skip_on_false']
            
        if 'op_args' in task_properties:
            args['op_args'] = task_properties['op_args']
            
        if 'op_kwargs' in task_properties:
            args['op_kwargs'] = task_properties['op_kwargs']
        
        return ConditionalOperator(**args)
    
    def create_file_sensor(self,
                          task_config: Dict[str, Any],
                          task_properties: Dict[str, Any],
                          dag) -> BaseOperator:
        """
        Create a FileSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            FileSensor instance
        """
        from ..operators.sensors.file_sensor import FileSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add FileSensor-specific arguments
        if 'file_path' in task_properties:
            args['file_path'] = task_properties['file_path']
        if 'file_pattern' in task_properties:
            args['file_pattern'] = task_properties['file_pattern']
        if 'directory_path' in task_properties:
            args['directory_path'] = task_properties['directory_path']
            
        # Optional FileSensor arguments
        if 'min_file_size' in task_properties:
            args['min_file_size'] = task_properties['min_file_size']
        if 'max_age_hours' in task_properties:
            args['max_age_hours'] = task_properties['max_age_hours']
        if 'check_content' in task_properties:
            args['check_content'] = task_properties['check_content']
        if 'content_pattern' in task_properties:
            args['content_pattern'] = task_properties['content_pattern']
        if 'recursive' in task_properties:
            args['recursive'] = task_properties['recursive']
        if 'poke_interval' in task_properties:
            args['poke_interval'] = task_properties['poke_interval']
        if 'timeout' in task_properties:
            args['timeout'] = task_properties['timeout']
        
        return FileSensor(**args)
    
    def create_s3_sensor(self,
                        task_config: Dict[str, Any],
                        task_properties: Dict[str, Any],
                        dag) -> BaseOperator:
        """
        Create an S3Sensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            S3Sensor instance
        """
        from ..operators.sensors.file_sensor import S3Sensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add S3Sensor-specific arguments
        if 'bucket_name' not in task_properties:
            raise ConfigurationError("S3Sensor requires 'bucket_name' property")
        args['bucket_name'] = task_properties['bucket_name']
        
        if 'object_key' in task_properties:
            args['object_key'] = task_properties['object_key']
        if 'prefix' in task_properties:
            args['prefix'] = task_properties['prefix']
            
        # Optional S3Sensor arguments
        if 'aws_conn_id' in task_properties:
            args['aws_conn_id'] = task_properties['aws_conn_id']
        if 'min_object_size' in task_properties:
            args['min_object_size'] = task_properties['min_object_size']
        if 'max_age_hours' in task_properties:
            args['max_age_hours'] = task_properties['max_age_hours']
        if 'check_content_type' in task_properties:
            args['check_content_type'] = task_properties['check_content_type']
        if 'verify_content' in task_properties:
            args['verify_content'] = task_properties['verify_content']
        if 'poke_interval' in task_properties:
            args['poke_interval'] = task_properties['poke_interval']
        if 'timeout' in task_properties:
            args['timeout'] = task_properties['timeout']
        
        return S3Sensor(**args)
    
    def create_gcs_sensor(self,
                         task_config: Dict[str, Any],
                         task_properties: Dict[str, Any],
                         dag) -> BaseOperator:
        """
        Create a GCSSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            GCSSensor instance
        """
        from ..operators.sensors.file_sensor import GCSSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add GCSSensor-specific arguments
        if 'bucket_name' not in task_properties:
            raise ConfigurationError("GCSSensor requires 'bucket_name' property")
        args['bucket_name'] = task_properties['bucket_name']
        
        if 'object_name' in task_properties:
            args['object_name'] = task_properties['object_name']
        if 'prefix' in task_properties:
            args['prefix'] = task_properties['prefix']
            
        # Optional GCSSensor arguments
        if 'gcp_conn_id' in task_properties:
            args['gcp_conn_id'] = task_properties['gcp_conn_id']
        if 'min_object_size' in task_properties:
            args['min_object_size'] = task_properties['min_object_size']
        if 'poke_interval' in task_properties:
            args['poke_interval'] = task_properties['poke_interval']
        if 'timeout' in task_properties:
            args['timeout'] = task_properties['timeout']
        
        return GCSSensor(**args)
    
    def create_database_sensor(self,
                              task_config: Dict[str, Any],
                              task_properties: Dict[str, Any],
                              dag) -> BaseOperator:
        """
        Create a DatabaseSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            DatabaseSensor instance
        """
        from ..operators.sensors.database_sensor import DatabaseSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add DatabaseSensor-specific arguments
        if 'sql' not in task_properties:
            raise ConfigurationError("DatabaseSensor requires 'sql' property")
        args['sql'] = task_properties['sql']
        
        if 'conn_id' not in task_properties:
            raise ConfigurationError("DatabaseSensor requires 'conn_id' property")
        args['conn_id'] = task_properties['conn_id']
        
        # Optional DatabaseSensor arguments
        optional_args = [
            'expected_result', 'comparison_operator', 'min_row_count', 'max_row_count',
            'hook_params', 'poke_interval', 'timeout'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return DatabaseSensor(**args)
    
    def create_table_sensor(self,
                           task_config: Dict[str, Any],
                           task_properties: Dict[str, Any],
                           dag) -> BaseOperator:
        """
        Create a TableSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            TableSensor instance
        """
        from ..operators.sensors.database_sensor import TableSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add TableSensor-specific arguments
        if 'table_name' not in task_properties:
            raise ConfigurationError("TableSensor requires 'table_name' property")
        args['table_name'] = task_properties['table_name']
        
        if 'conn_id' not in task_properties:
            raise ConfigurationError("TableSensor requires 'conn_id' property")
        args['conn_id'] = task_properties['conn_id']
        
        # Optional TableSensor arguments
        optional_args = [
            'schema', 'check_existence', 'min_rows', 'max_age_hours',
            'timestamp_column', 'where_clause', 'poke_interval', 'timeout'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return TableSensor(**args)
    
    def create_api_sensor(self,
                         task_config: Dict[str, Any],
                         task_properties: Dict[str, Any],
                         dag) -> BaseOperator:
        """
        Create an APISensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            APISensor instance
        """
        from ..operators.sensors.api_sensor import APISensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add APISensor-specific arguments
        if 'endpoint' not in task_properties:
            raise ConfigurationError("APISensor requires 'endpoint' property")
        args['endpoint'] = task_properties['endpoint']
        
        # Optional APISensor arguments
        optional_args = [
            'http_conn_id', 'method', 'data', 'headers', 'expected_status_code',
            'response_check', 'response_filter', 'json_path', 'expected_value',
            'timeout', 'poke_interval', 'sensor_timeout'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return APISensor(**args)
    
    def create_http_sensor(self,
                          task_config: Dict[str, Any],
                          task_properties: Dict[str, Any],
                          dag) -> BaseOperator:
        """
        Create an HTTPSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            HTTPSensor instance
        """
        from ..operators.sensors.api_sensor import HTTPSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add HTTPSensor-specific arguments
        if 'url' not in task_properties:
            raise ConfigurationError("HTTPSensor requires 'url' property")
        args['url'] = task_properties['url']
        
        # Optional HTTPSensor arguments
        optional_args = [
            'method', 'headers', 'auth', 'verify_ssl', 'expected_status_codes',
            'timeout', 'poke_interval', 'sensor_timeout'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return HTTPSensor(**args)
    
    def create_webhook_sensor(self,
                             task_config: Dict[str, Any],
                             task_properties: Dict[str, Any],
                             dag) -> BaseOperator:
        """
        Create a WebhookSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            WebhookSensor instance
        """
        from ..operators.sensors.api_sensor import WebhookSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add WebhookSensor-specific arguments
        if 'webhook_id' not in task_properties:
            raise ConfigurationError("WebhookSensor requires 'webhook_id' property")
        args['webhook_id'] = task_properties['webhook_id']
        
        # Optional WebhookSensor arguments
        optional_args = ['expected_data', 'data_key', 'poke_interval', 'timeout']
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return WebhookSensor(**args)
    
    def create_python_sensor(self,
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            dag) -> BaseOperator:
        """
        Create a PythonSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            PythonSensor instance
        """
        from ..operators.sensors.custom_sensor import PythonSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add PythonSensor-specific arguments
        if 'python_callable' not in task_properties:
            raise ConfigurationError("PythonSensor requires 'python_callable' property")
        args['python_callable'] = task_properties['python_callable']
        
        # Optional PythonSensor arguments
        optional_args = ['op_args', 'op_kwargs', 'poke_interval', 'timeout']
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return PythonSensor(**args)
    
    def create_custom_sensor(self,
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any],
                            dag) -> BaseOperator:
        """
        Create a CustomSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            CustomSensor instance
        """
        from ..operators.sensors.custom_sensor import CustomSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add CustomSensor-specific arguments
        if 'conditions' not in task_properties:
            raise ConfigurationError("CustomSensor requires 'conditions' property")
        args['conditions'] = task_properties['conditions']
        
        # Optional CustomSensor arguments
        optional_args = ['condition_mode', 'custom_logic', 'poke_interval', 'timeout']
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return CustomSensor(**args)
    
    def create_time_sensor(self,
                          task_config: Dict[str, Any],
                          task_properties: Dict[str, Any],
                          dag) -> BaseOperator:
        """
        Create a TimeSensor with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            TimeSensor instance
        """
        from ..operators.sensors.custom_sensor import TimeSensor
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add TimeSensor-specific arguments
        if 'target_time' not in task_properties:
            raise ConfigurationError("TimeSensor requires 'target_time' property")
        args['target_time'] = task_properties['target_time']
        
        # Optional TimeSensor arguments
        optional_args = ['mode', 'timezone', 'weekdays', 'poke_interval', 'timeout']
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return TimeSensor(**args)
    
    def _create_disabled_operator(self,
                                 task_config: Dict[str, Any],
                                 task_properties: Dict[str, Any],
                                 dag) -> BaseOperator:
        """
        Create a disabled operator (DummyOperator replacement)
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            DisabledTaskOperator instance
        """
        from ..operators.lifecycle.task_audit_wrapper import DisabledTaskOperator
        
        disabled_reason = task_properties.get('disabled_reason', 'Task disabled via configuration')
        
        return DisabledTaskOperator(
            task_id=task_config['name'],
            original_task_id=task_config['name'],
            disabled_reason=disabled_reason,
            dag=dag
        )
    
    def _apply_audit_wrapper(self,
                            operator: BaseOperator,
                            task_config: Dict[str, Any],
                            task_properties: Dict[str, Any]) -> BaseOperator:
        """
        Apply audit wrapper to operator if audit is enabled
        
        Args:
            operator: Operator to wrap
            task_config: Task configuration
            task_properties: Task properties
            
        Returns:
            Operator (potentially wrapped with audit functionality)
        """
        
        # Check if audit should be enabled
        audit_settings = task_properties.get('audit', {})
        should_enable_audit = audit_settings.get('enabled', True)
        
        # Skip audit for certain operator types that handle their own audit
        skip_audit_types = ['START', 'END', 'DisabledTaskOperator']
        if operator.__class__.__name__ in skip_audit_types:
            should_enable_audit = False
        
        if should_enable_audit:
            from ..operators.lifecycle.task_audit_wrapper import AuditWrapper
            
            wrapper = AuditWrapper(
                operator=operator,
                enable_task_audit=audit_settings.get('enable_task_audit', True),
                audit_actions=audit_settings.get('audit_actions', {}),
                custom_hooks=audit_settings.get('custom_hooks', {}),
                collect_metrics=audit_settings.get('collect_metrics', True)
            )
            
            logger.debug(f"Task {task_config['name']} wrapped with audit functionality")
        
        return operator
    
    def create_start_operator(self,
                             task_config: Dict[str, Any],
                             task_properties: Dict[str, Any],
                             dag) -> BaseOperator:
        """
        Create a StartOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            StartOperator instance
        """
        from ..operators.lifecycle.job_boundary_operators import StartOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add StartOperator-specific arguments
        optional_args = [
            'audit_actions', 'initialization_actions', 'job_metadata',
            'enable_default_audit', 'custom_start_callable', 'op_kwargs'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return StartOperator(**args)
    
    def create_end_operator(self,
                           task_config: Dict[str, Any],
                           task_properties: Dict[str, Any],
                           dag) -> BaseOperator:
        """
        Create an EndOperator with configuration support
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            EndOperator instance
        """
        from ..operators.lifecycle.job_boundary_operators import EndOperator
        
        # Build base arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add EndOperator-specific arguments
        optional_args = [
            'audit_actions', 'finalization_actions', 'enable_default_audit',
            'calculate_job_duration', 'custom_end_callable', 'op_kwargs'
        ]
        for arg in optional_args:
            if arg in task_properties:
                args[arg] = task_properties[arg]
        
        return EndOperator(**args)
    
    def create_generic_operator(self,
                               task_config: Dict[str, Any],
                               task_properties: Dict[str, Any],
                               dag) -> BaseOperator:
        """
        Create a generic operator using the generic task type system
        
        Args:
            task_config: Task configuration
            task_properties: Task properties
            dag: Airflow DAG
            
        Returns:
            Generic operator instance selected based on environment
        """
        from ...core.generic_task_system import (
            get_task_type_registry, get_environment_context
        )
        
        task_type = task_config.get('type')
        
        # Get the appropriate implementation class
        registry = get_task_type_registry()
        env_context = get_environment_context()
        
        try:
            implementation_class = registry.get_implementation(
                task_type=task_type,
                env_context=env_context,
                task_properties=task_properties
            )
        except Exception as e:
            logger.error(f"Failed to get implementation for generic task type {task_type}: {e}")
            raise ConfigurationError(f"Generic operator creation failed for {task_type}: {str(e)}")
        
        # Build operator arguments
        args = {
            'task_id': task_config['name'],
            'dag': dag,
        }
        
        # Add standard arguments
        self._add_standard_args(args, task_config, task_properties)
        
        # Add all task properties (the generic operators will filter what they need)
        # Remove framework-specific properties that shouldn't go to operator
        filtered_properties = task_properties.copy()
        framework_props = ['enabled', 'disabled_reason', 'audit', 'audit_actions', 'custom_hooks']
        for prop in framework_props:
            filtered_properties.pop(prop, None)
        
        args.update(filtered_properties)
        
        # Create operator instance
        operator = implementation_class(**args)
        
        logger.info(f"Created generic {task_type} operator using {implementation_class.__name__} for environment {env_context.cloud_provider.value}")
        
        return operator