# DAG Builder - Build complete DAGs from configuration
"""
DAGBuilder constructs complete Airflow DAGs from configuration with support for:
- Task groups and nested workflows
- Dynamic task creation from configuration
- Dependency management and validation
- Runtime parameter integration
- Custom operators and standard Airflow operators
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
from airflow import DAG
from airflow.models.baseoperator import BaseOperator

from .task_builder import TaskBuilder
from .group_builder import TaskGroupBuilder
from .dependency_builder import DependencyBuilder
from .operator_factory import OperatorFactory
from ..config.manager import ConfigManager
from ..core.exceptions import DAGBuildError, ConfigurationError
from ..validators.dependency_validator import DependencyValidator

logger = logging.getLogger(__name__)


class DAGBuilder:
    """
    Builder class for constructing complete Airflow DAGs from configuration
    """
    
    def __init__(self,
                 config_manager: Optional[ConfigManager] = None,
                 operator_factory: Optional[OperatorFactory] = None):
        """
        Initialize DAGBuilder
        
        Args:
            config_manager: Configuration manager instance
            operator_factory: Operator factory instance
        """
        self.config_manager = config_manager or ConfigManager()
        self.operator_factory = operator_factory or OperatorFactory()
        self.task_builder = TaskBuilder(self.operator_factory)
        self.group_builder = TaskGroupBuilder(self.task_builder)
        self.dependency_builder = DependencyBuilder()
        self.dependency_validator = DependencyValidator()
        
        logger.debug("DAGBuilder initialized")
    
    def build_dag(self,
                  config_file: str,
                  properties_file: Optional[str] = None,
                  runtime_params: Optional[Dict[str, Any]] = None,
                  dag_run_conf: Optional[Dict[str, Any]] = None) -> DAG:
        """
        Build a complete DAG from configuration
        
        Args:
            config_file: Path to main configuration file
            properties_file: Path to properties file (optional)
            runtime_params: Runtime parameters for override
            dag_run_conf: DAG run configuration from Airflow
            
        Returns:
            Constructed Airflow DAG
            
        Raises:
            DAGBuildError: If DAG construction fails
        """
        try:
            logger.info(f"Building DAG from configuration: {config_file}")
            
            # Load configuration
            config = self.config_manager.load_configuration(
                config_file=config_file,
                properties_file=properties_file,
                runtime_params=runtime_params,
                dag_run_conf=dag_run_conf
            )
            
            # Create DAG instance
            dag = self._create_dag_instance(config)
            
            # Build tasks and task groups
            tasks_and_groups = self._build_tasks_and_groups(config, dag)
            
            # Set up dependencies
            self._setup_dependencies(config, tasks_and_groups)
            
            # Validate DAG structure
            self._validate_dag(dag, config)
            
            logger.info(f"Successfully built DAG '{dag.dag_id}' with {len(tasks_and_groups)} tasks/groups")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to build DAG from {config_file}: {str(e)}")
            raise DAGBuildError(f"DAG construction failed: {str(e)}") from e
    
    def _create_dag_instance(self, config: Dict[str, Any]) -> DAG:
        """
        Create DAG instance from configuration
        
        Args:
            config: Complete configuration dictionary
            
        Returns:
            Airflow DAG instance
        """
        # Extract properties
        properties = config.get('properties', {})
        
        # DAG basic information
        dag_id = properties.get('workflow_name', 'unnamed_workflow')
        description = properties.get('workflow_description', f'InsightAir workflow: {dag_id}')
        
        # Schedule configuration
        schedule_interval = properties.get('dag_schedule_interval', None)
        start_date_str = properties.get('dag_start_date', '2024-01-01')
        
        # Parse start date
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid start_date format '{start_date_str}', using default")
            start_date = datetime(2024, 1, 1)
        
        # DAG configuration
        catchup = properties.get('dag_catchup', False)
        max_active_runs = properties.get('dag_max_active_runs', 1)
        max_active_tasks = properties.get('dag_max_active_tasks', 16)
        
        # Default args
        default_args = self._build_default_args(properties)
        
        # Tags
        tags_str = properties.get('dag_tags', '')
        tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
        
        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            description=description,
            schedule_interval=schedule_interval,
            start_date=start_date,
            catchup=catchup,
            max_active_runs=max_active_runs,
            max_active_tasks=max_active_tasks,
            default_args=default_args,
            tags=tags,
            doc_md=self._generate_dag_documentation(config)
        )
        
        logger.debug(f"Created DAG instance: {dag_id}")
        return dag
    
    def _build_default_args(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Build default_args for DAG"""
        
        default_args = {
            'owner': properties.get('dag_owner', 'insightair'),
            'depends_on_past': properties.get('dag_depends_on_past', False),
            'retries': properties.get('dag_retries', 1),
            'email_on_failure': properties.get('email_on_failure', True),
            'email_on_retry': properties.get('email_on_retry', False),
        }
        
        # Retry delay
        retry_delay_mins = properties.get('dag_retry_delay_minutes', 5)
        default_args['retry_delay'] = timedelta(minutes=retry_delay_mins)
        
        # SLA
        sla_minutes = properties.get('sla_minutes')
        if sla_minutes:
            default_args['sla'] = timedelta(minutes=sla_minutes)
        
        # Email addresses
        ops_emails = properties.get('ops_emails', '')
        if ops_emails:
            emails = [email.strip() for email in ops_emails.split(',')]
            default_args['email'] = emails
        
        return default_args
    
    def _build_tasks_and_groups(self, config: Dict[str, Any], dag: DAG) -> Dict[str, Union[BaseOperator, Any]]:
        """
        Build all tasks and task groups from configuration
        
        Args:
            config: Configuration dictionary
            dag: DAG instance
            
        Returns:
            Dictionary mapping task/group names to their instances
        """
        tasks_and_groups = {}
        tasks_config = config.get('tasks', [])
        
        for task_config in tasks_config:
            task_name = task_config.get('name')
            task_type = task_config.get('type')
            
            if not task_name or not task_type:
                logger.warning(f"Skipping invalid task configuration: {task_config}")
                continue
            
            try:
                if task_type == 'TASK_GROUP':
                    # Build task group
                    task_group = self.group_builder.build_task_group(
                        task_config, dag, config.get('properties', {})
                    )
                    tasks_and_groups[task_name] = task_group
                    
                    # Add individual tasks from the group to the registry
                    group_tasks = self.group_builder.get_group_tasks(task_group)
                    tasks_and_groups.update(group_tasks)
                    
                else:
                    # Build individual task
                    task = self.task_builder.build_task(
                        task_config, dag, config.get('properties', {})
                    )
                    tasks_and_groups[task_name] = task
                    
            except Exception as e:
                logger.error(f"Failed to build task/group '{task_name}': {str(e)}")
                raise DAGBuildError(f"Task building failed for '{task_name}': {str(e)}") from e
        
        return tasks_and_groups
    
    def _setup_dependencies(self, 
                           config: Dict[str, Any], 
                           tasks_and_groups: Dict[str, Union[BaseOperator, Any]]):
        """
        Set up task dependencies from configuration
        
        Args:
            config: Configuration dictionary
            tasks_and_groups: Dictionary of created tasks and groups
        """
        try:
            # Build dependencies from configuration
            self.dependency_builder.build_dependencies(
                config.get('tasks', []), 
                tasks_and_groups
            )
            
            logger.debug(f"Set up dependencies for {len(tasks_and_groups)} tasks/groups")
            
        except Exception as e:
            logger.error(f"Failed to set up dependencies: {str(e)}")
            raise DAGBuildError(f"Dependency setup failed: {str(e)}") from e
    
    def _validate_dag(self, dag: DAG, config: Dict[str, Any]):
        """
        Validate constructed DAG
        
        Args:
            dag: Constructed DAG to validate
            config: Configuration used to build DAG
        """
        try:
            # Validate DAG structure
            if not dag.tasks:
                raise DAGBuildError("DAG has no tasks")
            
            # Validate dependencies
            self.dependency_validator.validate_dag_dependencies(dag)
            
            # Validate configuration compliance
            tasks_config = config.get('tasks', [])
            configured_tasks = {task.get('name') for task in tasks_config if task.get('name')}
            dag_task_ids = {task.task_id for task in dag.tasks}
            
            # Check for missing tasks (allowing for group prefixes)
            missing_tasks = configured_tasks - dag_task_ids
            # Filter out task groups (they don't appear as individual tasks)
            missing_tasks = {
                task for task in missing_tasks 
                if not any(task_config.get('type') == 'TASK_GROUP' and task_config.get('name') == task 
                          for task_config in tasks_config)
            }
            
            if missing_tasks:
                logger.warning(f"Some configured tasks not found in DAG: {missing_tasks}")
            
            logger.debug(f"DAG validation passed for '{dag.dag_id}'")
            
        except Exception as e:
            logger.error(f"DAG validation failed: {str(e)}")
            raise DAGBuildError(f"DAG validation failed: {str(e)}") from e
    
    def _generate_dag_documentation(self, config: Dict[str, Any]) -> str:
        """
        Generate documentation for the DAG
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Markdown documentation string
        """
        properties = config.get('properties', {})
        
        doc = f"""
# {properties.get('workflow_name', 'InsightAir Workflow')}

## Description
{properties.get('workflow_description', 'Configuration-driven Airflow workflow')}

## Configuration
- **Version**: {properties.get('workflow_version', '1.0.0')}
- **Category**: {properties.get('workflow_category', 'data_processing')}
- **Priority**: {properties.get('workflow_priority', 'P3')}
- **Environment**: {properties.get('workflow_data_group', 'production')}

## Schedule
- **Schedule Interval**: {properties.get('dag_schedule_interval', 'None')}
- **Start Date**: {properties.get('dag_start_date', '2024-01-01')}
- **Catchup**: {properties.get('dag_catchup', False)}

## Tasks
"""
        
        # Add task information
        tasks_config = config.get('tasks', [])
        for task_config in tasks_config:
            task_name = task_config.get('name', 'unnamed')
            task_type = task_config.get('type', 'unknown')
            task_desc = task_config.get('description', 'No description')
            
            doc += f"- **{task_name}** ({task_type}): {task_desc}\n"
        
        doc += f"""
## Runtime Parameters
This DAG supports runtime parameter overrides via:
- DAG Run Configuration (highest priority)
- Airflow Variables
- Environment Variables

Generated by InsightAir Framework v2.0.0
"""
        
        return doc
    
    def build_dag_from_dict(self, config_dict: Dict[str, Any]) -> DAG:
        """
        Build DAG directly from configuration dictionary
        
        Args:
            config_dict: Complete configuration dictionary
            
        Returns:
            Constructed Airflow DAG
        """
        try:
            # Create DAG instance
            dag = self._create_dag_instance(config_dict)
            
            # Build tasks and task groups
            tasks_and_groups = self._build_tasks_and_groups(config_dict, dag)
            
            # Set up dependencies
            self._setup_dependencies(config_dict, tasks_and_groups)
            
            # Validate DAG structure
            self._validate_dag(dag, config_dict)
            
            logger.info(f"Successfully built DAG from dictionary with {len(tasks_and_groups)} tasks/groups")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to build DAG from dictionary: {str(e)}")
            raise DAGBuildError(f"DAG construction from dict failed: {str(e)}") from e