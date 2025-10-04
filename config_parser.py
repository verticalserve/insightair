"""
Configuration Parser for InsightAir Workflow Framework
Parses environment.yaml, config.yaml and task yaml files for dynamic DAG generation
"""

import yaml
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import importlib
import logging

logger = logging.getLogger(__name__)


class ConfigParser:
    """
    Parses configuration files and provides dynamic module import capabilities
    """
    
    def __init__(self, config_bucket: str = None, config_path: str = None):
        self.config_bucket = config_bucket
        self.config_path = config_path
        self.environment_config = None
        self.workflow_config = None
        self.properties_config = None
        self.task_configs = {}
        
    def load_environment_config(self, env_file_path: str) -> Dict[str, Any]:
        """
        Load environment configuration from environment.yaml
        
        Args:
            env_file_path: Path to environment.yaml file
            
        Returns:
            Dictionary containing environment configuration
        """
        try:
            with open(env_file_path, 'r') as file:
                self.environment_config = yaml.safe_load(file)
            logger.info(f"Loaded environment config from {env_file_path}")
            return self.environment_config
        except Exception as e:
            logger.error(f"Error loading environment config: {e}")
            raise
            
    def load_workflow_config(self, config_file_path: str) -> Dict[str, Any]:
        """
        Load workflow configuration from config.yaml
        
        Args:
            config_file_path: Path to config.yaml file
            
        Returns:
            Dictionary containing workflow configuration
        """
        try:
            with open(config_file_path, 'r') as file:
                self.workflow_config = yaml.safe_load(file)
            logger.info(f"Loaded workflow config from {config_file_path}")
            return self.workflow_config
        except Exception as e:
            logger.error(f"Error loading workflow config: {e}")
            raise
            
    def load_properties_config(self, properties_file_path: str) -> Dict[str, Any]:
        """
        Load properties configuration from properties.yaml
        
        Args:
            properties_file_path: Path to properties.yaml file
            
        Returns:
            Dictionary containing properties configuration
        """
        try:
            with open(properties_file_path, 'r') as file:
                self.properties_config = yaml.safe_load(file)
            logger.info(f"Loaded properties config from {properties_file_path}")
            return self.properties_config
        except Exception as e:
            logger.error(f"Error loading properties config: {e}")
            raise
            
    def load_task_config(self, task_name: str, task_file_path: str) -> Dict[str, Any]:
        """
        Load task-specific configuration from task yaml files
        
        Args:
            task_name: Name of the task
            task_file_path: Path to task yaml file
            
        Returns:
            Dictionary containing task configuration
        """
        try:
            with open(task_file_path, 'r') as file:
                task_config = yaml.safe_load(file)
            self.task_configs[task_name] = task_config
            logger.info(f"Loaded task config for {task_name} from {task_file_path}")
            return task_config
        except Exception as e:
            logger.error(f"Error loading task config for {task_name}: {e}")
            raise
            
    def get_data_group_config(self, data_group_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific data group
        
        Args:
            data_group_name: Name of the data group
            
        Returns:
            Dictionary containing data group configuration
        """
        if not self.environment_config:
            raise ValueError("Environment config not loaded")
            
        data_groups = self.environment_config.get('data_groups', {})
        if data_group_name not in data_groups:
            raise ValueError(f"Data group '{data_group_name}' not found in environment config")
            
        return data_groups[data_group_name]
        
    def get_cloud_platform(self, data_group_name: str) -> str:
        """
        Determine cloud platform based on data group configuration
        
        Args:
            data_group_name: Name of the data group
            
        Returns:
            Cloud platform identifier (aws, gcp, azure, oci)
        """
        data_group_config = self.get_data_group_config(data_group_name)
        
        # Check for AWS indicators
        if any(key in data_group_config for key in ['role_id', 'role_name']) or \
           any('s3://' in str(value) for value in data_group_config.values()):
            return 'aws'
            
        # Check for GCP indicators
        if 'project_id' in data_group_config and 'service_account' in data_group_config:
            return 'gcp'
            
        # Check for Azure indicators
        if any('azure' in str(value).lower() for value in data_group_config.values()):
            return 'azure'
            
        # Check for OCI indicators
        if any('oci' in str(value).lower() for value in data_group_config.values()):
            return 'oci'
            
        return 'aws'  # Default to AWS
        
    def substitute_variables(self, text: str, context: Dict[str, Any]) -> str:
        """
        Substitute variables in text using context
        
        Args:
            text: Text containing variables in format $variable_name
            context: Dictionary containing variable values
            
        Returns:
            Text with variables substituted
        """
        if not isinstance(text, str):
            return text
            
        result = text
        for key, value in context.items():
            if isinstance(value, str):
                result = result.replace(f'${key}', value)
                
        return result
        
    def build_task_context(self, task_name: str, data_group_name: str) -> Dict[str, Any]:
        """
        Build context for a specific task combining all configurations
        
        Args:
            task_name: Name of the task
            data_group_name: Name of the data group
            
        Returns:
            Dictionary containing complete task context
        """
        context = {}
        
        # Add environment config
        if self.environment_config:
            context.update(self.environment_config)
            
        # Add data group specific config
        data_group_config = self.get_data_group_config(data_group_name)
        context.update(data_group_config)
        
        # Add workflow config
        if self.workflow_config:
            context.update(self.workflow_config)
            
        # Add properties config
        if self.properties_config:
            context.update(self.properties_config)
            
        # Add task specific config
        if task_name in self.task_configs:
            context.update(self.task_configs[task_name])
            
        return context


class DynamicImporter:
    """
    Handles dynamic module imports based on cloud platform and service requirements
    """
    
    def __init__(self):
        self.loaded_modules = {}
        
    def get_cloud_module(self, cloud_platform: str, service_type: str):
        """
        Dynamically import cloud-specific modules
        
        Args:
            cloud_platform: Platform identifier (aws, gcp, azure, oci)
            service_type: Service type (storage, notification, database, etc.)
            
        Returns:
            Imported module
        """
        module_key = f"{cloud_platform}_{service_type}"
        
        if module_key in self.loaded_modules:
            return self.loaded_modules[module_key]
            
        try:
            module_path = f"{service_type}.{cloud_platform}"
            module = importlib.import_module(module_path)
            self.loaded_modules[module_key] = module
            logger.info(f"Dynamically loaded module: {module_path}")
            return module
        except ImportError as e:
            logger.warning(f"Could not import {module_path}: {e}")
            # Fall back to base module if available
            try:
                base_module_path = f"{service_type}.base"
                base_module = importlib.import_module(base_module_path)
                self.loaded_modules[module_key] = base_module
                logger.info(f"Loaded base module: {base_module_path}")
                return base_module
            except ImportError:
                logger.error(f"Could not import base module {base_module_path}")
                raise
                
    def get_operator_class(self, task_type: str, cloud_platform: str = None):
        """
        Get appropriate operator class based on task type and cloud platform
        
        Args:
            task_type: Type of task (DB_TO_TEXT, START, END, etc.)
            cloud_platform: Cloud platform identifier
            
        Returns:
            Operator class
        """
        from airflow.operators.python import PythonOperator
        from airflow.operators.dummy import DummyOperator
        
        # Map task types to operators
        operator_mapping = {
            'START': DummyOperator,
            'END': DummyOperator,
            'DB_TO_TEXT': PythonOperator,
            'FILE_TRANSFER': PythonOperator,
            'DATA_QUALITY': PythonOperator,
            'NOTIFICATION': PythonOperator
        }
        
        return operator_mapping.get(task_type, PythonOperator)
        
    def get_service_class(self, service_type: str, cloud_platform: str):
        """
        Get cloud-specific service class
        
        Args:
            service_type: Type of service (storage, notification, etc.)
            cloud_platform: Cloud platform identifier
            
        Returns:
            Service class
        """
        module = self.get_cloud_module(cloud_platform, service_type)
        
        # Common class name patterns
        class_patterns = [
            f"{cloud_platform.upper()}{service_type.capitalize()}",
            f"{service_type.capitalize()}{cloud_platform.upper()}",
            f"{service_type.capitalize()}"
        ]
        
        for pattern in class_patterns:
            if hasattr(module, pattern):
                return getattr(module, pattern)
                
        # Fallback to first available class
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and not attr_name.startswith('_'):
                return attr
                
        raise ImportError(f"Could not find appropriate class in {service_type}.{cloud_platform}")