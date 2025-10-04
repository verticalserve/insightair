"""
Legacy Framework Compatibility Module
Provides compatibility with existing workflow_framework imports
"""

from .dag_builder import DAGBuilder
from .config_parser import ConfigParser, DynamicImporter
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

logger = logging.getLogger(__name__)


class Framework:
    """
    Legacy Framework class for backward compatibility
    """
    
    def __init__(self, workflow_name: str, config_path: str):
        self.workflow_name = workflow_name
        self.config_path = config_path
        self.dag_builder = DAGBuilder(workflow_name, config_path)
        self.dynamic_importer = DynamicImporter()
        
    def build_task(self, task_type: str, task_name: str, **kwargs):
        """
        Build a task for backward compatibility with existing DAGs
        
        Args:
            task_type: Type of the task (START, END, DB_TO_TEXT, etc.)
            task_name: Name of the task
            **kwargs: Additional arguments
            
        Returns:
            Task operator
        """
        if task_type in ['START', 'END']:
            return DummyOperator(
                task_id=task_name,
                **kwargs
            )
        else:
            # Create a generic Python operator
            def generic_task(**context):
                logger.info(f"Executing {task_type} task: {task_name}")
                return f"Task {task_name} of type {task_type} completed"
            
            return PythonOperator(
                task_id=task_name,
                python_callable=generic_task,
                **kwargs
            )


# Create a default framework instance for compatibility
framework = None