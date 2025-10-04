# InsightAir Framework - Main framework orchestration
"""
InsightAirFramework is the main entry point for the configuration-driven
Airflow DAG framework. It orchestrates all components to provide:
- Simple DAG creation from configuration files
- Runtime parameter override support
- Custom operator registration and management
- Task group and complex workflow support
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, Type
from airflow import DAG
from airflow.models.baseoperator import BaseOperator

from .registry import OperatorRegistry
from .context import WorkflowContext
from .exceptions import FrameworkError
from ..config.manager import ConfigManager
from ..builders.dag_builder import DAGBuilder
from ..builders.operator_factory import OperatorFactory
from ..validators.config_validator import ConfigValidator

logger = logging.getLogger(__name__)


class InsightAirFramework:
    """
    Main framework class that provides a simple interface for creating
    configuration-driven Airflow DAGs with advanced features
    """
    
    def __init__(self,
                 config_dir: Optional[Union[str, Path]] = None,
                 enable_validation: bool = True,
                 enable_runtime_override: bool = True,
                 auto_register_operators: bool = True):
        """
        Initialize InsightAir Framework
        
        Args:
            config_dir: Base directory for configuration files
            enable_validation: Whether to validate configurations
            enable_runtime_override: Whether to support runtime parameter overrides
            auto_register_operators: Whether to automatically register built-in operators
        """
        self.config_dir = Path(config_dir) if config_dir else Path.cwd()
        self.enable_validation = enable_validation
        self.enable_runtime_override = enable_runtime_override
        
        # Initialize core components
        self.registry = OperatorRegistry()
        self.config_manager = ConfigManager(
            config_dir=self.config_dir,
            enable_validation=enable_validation,
            enable_runtime_override=enable_runtime_override
        )
        self.operator_factory = OperatorFactory(self.registry)
        self.dag_builder = DAGBuilder(self.config_manager, self.operator_factory)
        
        # Register built-in operators
        if auto_register_operators:
            self._register_builtin_operators()
        
        logger.info(f"InsightAir Framework initialized with config_dir: {self.config_dir}")
    
    def build_dag_from_config(self,
                             config_file: Union[str, Path],
                             properties_file: Optional[Union[str, Path]] = None,
                             runtime_params: Optional[Dict[str, Any]] = None,
                             dag_run_conf: Optional[Dict[str, Any]] = None) -> DAG:
        """
        Build a complete Airflow DAG from configuration files
        
        Args:
            config_file: Path to main configuration file (task chain definition)
            properties_file: Path to properties file (flat parameters)
            runtime_params: Runtime parameters for override
            dag_run_conf: DAG run configuration from Airflow
            
        Returns:
            Complete Airflow DAG ready for execution
            
        Example:
            ```python
            framework = InsightAirFramework()
            dag = framework.build_dag_from_config('config.yaml', 'properties.yaml')
            ```
        """
        try:
            logger.info(f"Building DAG from config: {config_file}")
            
            dag = self.dag_builder.build_dag(
                config_file=str(config_file),
                properties_file=str(properties_file) if properties_file else None,
                runtime_params=runtime_params,
                dag_run_conf=dag_run_conf
            )
            
            logger.info(f"Successfully built DAG '{dag.dag_id}' with {len(dag.tasks)} tasks")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to build DAG from configuration: {str(e)}")
            raise FrameworkError(f"DAG creation failed: {str(e)}") from e
    
    def build_dag_from_dict(self, config_dict: Dict[str, Any]) -> DAG:
        """
        Build a DAG directly from configuration dictionary
        
        Args:
            config_dict: Complete configuration dictionary
            
        Returns:
            Complete Airflow DAG
            
        Example:
            ```python
            config = {
                'properties': {'workflow_name': 'test_dag'},
                'tasks': [
                    {'name': 'start', 'type': 'START'},
                    {'name': 'end', 'type': 'END', 'parents': ['start']}
                ]
            }
            dag = framework.build_dag_from_dict(config)
            ```
        """
        try:
            return self.dag_builder.build_dag_from_dict(config_dict)
        except Exception as e:
            logger.error(f"Failed to build DAG from dictionary: {str(e)}")
            raise FrameworkError(f"DAG creation from dict failed: {str(e)}") from e
    
    def create_workflow_context(self,
                               config_file: Union[str, Path],
                               properties_file: Optional[Union[str, Path]] = None,
                               runtime_params: Optional[Dict[str, Any]] = None) -> WorkflowContext:
        """
        Create a workflow context for advanced DAG manipulation
        
        Args:
            config_file: Path to main configuration file
            properties_file: Path to properties file
            runtime_params: Runtime parameters
            
        Returns:
            WorkflowContext instance for advanced operations
        """
        config = self.config_manager.load_configuration(
            config_file=config_file,
            properties_file=properties_file,
            runtime_params=runtime_params
        )
        
        return WorkflowContext(
            config=config,
            framework=self,
            config_dir=self.config_dir
        )
    
    def register_operator(self,
                         task_type: str,
                         operator_class: Type[BaseOperator],
                         override: bool = False):
        """
        Register a custom operator for use in configurations
        
        Args:
            task_type: Task type identifier (used in config files)
            operator_class: Airflow operator class
            override: Whether to override existing registration
            
        Example:
            ```python
            @framework.register_operator('CUSTOM_TASK')
            class CustomOperator(BaseOperator):
                def execute(self, context):
                    # Implementation
                    pass
            ```
        """
        self.registry.register_operator(task_type, operator_class, override)
        logger.info(f"Registered custom operator {operator_class.__name__} for type '{task_type}'")
    
    def register_operator_decorator(self, task_type: str, override: bool = False):
        """
        Decorator for registering operators
        
        Args:
            task_type: Task type identifier
            override: Whether to override existing registration
            
        Returns:
            Decorator function
            
        Example:
            ```python
            @framework.register_operator_decorator('MY_TASK')
            class MyOperator(BaseOperator):
                pass
            ```
        """
        def decorator(operator_class: Type[BaseOperator]):
            self.register_operator(task_type, operator_class, override)
            return operator_class
        return decorator
    
    def get_supported_task_types(self) -> Dict[str, Type[BaseOperator]]:
        """
        Get all supported task types and their operator classes
        
        Returns:
            Dictionary mapping task types to operator classes
        """
        return self.registry.get_all_operators()
    
    def validate_configuration(self,
                              config_file: Union[str, Path],
                              properties_file: Optional[Union[str, Path]] = None) -> bool:
        """
        Validate configuration files without building DAG
        
        Args:
            config_file: Path to main configuration file
            properties_file: Path to properties file
            
        Returns:
            True if configuration is valid
            
        Raises:
            FrameworkError: If validation fails
        """
        try:
            config = self.config_manager.load_configuration(
                config_file=config_file,
                properties_file=properties_file
            )
            
            # Additional validation can be added here
            validator = ConfigValidator()
            validator.validate_configuration(config)
            
            logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            raise FrameworkError(f"Validation failed: {str(e)}") from e
    
    def create_task(self,
                   task_config: Dict[str, Any],
                   task_properties: Dict[str, Any],
                   dag: DAG) -> BaseOperator:
        """
        Create a single task from configuration
        
        Args:
            task_config: Task configuration dictionary
            task_properties: Task properties dictionary
            dag: Target DAG instance
            
        Returns:
            Created operator instance
        """
        return self.operator_factory.create_operator(
            task_config=task_config,
            task_properties=task_properties,
            dag=dag
        )
    
    def get_framework_info(self) -> Dict[str, Any]:
        """
        Get information about the framework instance
        
        Returns:
            Framework information dictionary
        """
        return {
            'version': '2.0.0',
            'config_dir': str(self.config_dir),
            'validation_enabled': self.enable_validation,
            'runtime_override_enabled': self.enable_runtime_override,
            'registered_operators': len(self.registry.get_all_operators()),
            'supported_task_types': list(self.registry.get_all_operators().keys())
        }
    
    def _register_builtin_operators(self):
        """Register built-in InsightAir operators"""
        try:
            # Import and register built-in operators
            from airflow.operators.dummy import DummyOperator
            from airflow.operators.python import PythonOperator
            from airflow.operators.bash import BashOperator
            from airflow.operators.email import EmailOperator
            
            # Register standard Airflow operators
            self.registry.register_operator('START', DummyOperator)
            self.registry.register_operator('END', DummyOperator)
            self.registry.register_operator('DUMMY', DummyOperator)
            self.registry.register_operator('PYTHON', PythonOperator)
            self.registry.register_operator('BASH', BashOperator)
            self.registry.register_operator('EMAIL', EmailOperator)
            
            # Register InsightAir custom operators
            try:
                from ..operators.database.db_operator import DatabaseOperator
                from ..operators.database.db_to_text import DatabaseToTextOperator
                from ..operators.quality.dq_operator import DataQualityOperator
                from ..operators.storage.archive_operator import ArchiveOperator
                
                self.registry.register_operator('DB', DatabaseOperator)
                self.registry.register_operator('DB_TO_TEXT', DatabaseToTextOperator)
                self.registry.register_operator('DQ', DataQualityOperator)
                self.registry.register_operator('ARCHIVE', ArchiveOperator)
                
            except ImportError as e:
                logger.warning(f"Some InsightAir operators not available: {e}")
            
            logger.debug("Built-in operators registered successfully")
            
        except Exception as e:
            logger.warning(f"Failed to register some built-in operators: {e}")
    
    def set_config_dir(self, config_dir: Union[str, Path]):
        """
        Update the configuration directory
        
        Args:
            config_dir: New configuration directory path
        """
        self.config_dir = Path(config_dir)
        self.config_manager.config_dir = self.config_dir
        logger.info(f"Configuration directory updated to: {self.config_dir}")
    
    def clear_caches(self):
        """Clear all internal caches"""
        self.config_manager.clear_cache()
        logger.info("Framework caches cleared")


# Convenience function for quick DAG creation
def create_dag_from_config(config_file: Union[str, Path],
                          properties_file: Optional[Union[str, Path]] = None,
                          config_dir: Optional[Union[str, Path]] = None,
                          **kwargs) -> DAG:
    """
    Convenience function to quickly create a DAG from configuration
    
    Args:
        config_file: Path to configuration file
        properties_file: Path to properties file
        config_dir: Configuration directory
        **kwargs: Additional arguments for framework initialization
        
    Returns:
        Constructed Airflow DAG
        
    Example:
        ```python
        # In your DAG file:
        from insightair_framework import create_dag_from_config
        
        dag = create_dag_from_config(
            config_file='config.yaml',
            properties_file='properties.yaml'
        )
        ```
    """
    framework = InsightAirFramework(config_dir=config_dir, **kwargs)
    return framework.build_dag_from_config(config_file, properties_file)