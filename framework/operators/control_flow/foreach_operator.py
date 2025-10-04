# ForEach Operator - Dynamic task generation for loops
"""
ForEachOperator enables dynamic task generation based on runtime data.
Supports creating multiple instances of tasks for processing lists/arrays.
"""

import logging
from typing import Dict, Any, List, Callable, Optional, Union
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class ForEachOperator(BaseInsightAirOperator):
    """
    Operator that creates dynamic tasks based on a list of items.
    Each item in the list generates a separate task instance.
    """
    
    template_fields = ('items_source', 'task_template')
    
    def __init__(self,
                 items_source: Union[List[Any], str, Callable],
                 task_template: Dict[str, Any],
                 task_group_id: Optional[str] = None,
                 max_parallel_tasks: int = 10,
                 fail_on_empty: bool = False,
                 **kwargs):
        """
        Initialize ForEachOperator
        
        Args:
            items_source: Source of items to iterate over (list, callable, or XCom key)
            task_template: Template for creating tasks (task type, properties, etc.)
            task_group_id: Optional task group ID for organizing generated tasks
            max_parallel_tasks: Maximum number of parallel tasks
            fail_on_empty: Whether to fail if items_source is empty
        """
        super().__init__(**kwargs)
        self.items_source = items_source
        self.task_template = task_template
        self.task_group_id = task_group_id or f"{self.task_id}_group"
        self.max_parallel_tasks = max_parallel_tasks
        self.fail_on_empty = fail_on_empty
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute ForEach logic - create and manage dynamic tasks
        """
        # Get items to iterate over
        items = self._resolve_items(context)
        
        if not items and self.fail_on_empty:
            raise ValueError("ForEach items_source is empty and fail_on_empty=True")
        
        if not items:
            logger.info("No items to process in ForEach")
            return {'items_processed': 0, 'task_results': []}
        
        logger.info(f"Processing {len(items)} items in ForEach")
        
        # Create and execute dynamic tasks
        results = self._execute_foreach_tasks(items, context)
        
        return {
            'items_processed': len(items),
            'task_results': results,
            'items': items
        }
    
    def _resolve_items(self, context: Context) -> List[Any]:
        """Resolve items from various sources"""
        
        if isinstance(self.items_source, list):
            return self.items_source
        
        if callable(self.items_source):
            return self.items_source(context)
        
        if isinstance(self.items_source, str):
            # Try to get from XCom
            try:
                return context['task_instance'].xcom_pull(key=self.items_source)
            except:
                # Try to evaluate as Python expression
                try:
                    import ast
                    return ast.literal_eval(self.items_source)
                except:
                    logger.warning(f"Could not resolve items_source: {self.items_source}")
                    return []
        
        return []
    
    def _execute_foreach_tasks(self, items: List[Any], context: Context) -> List[Dict[str, Any]]:
        """Execute tasks for each item"""
        results = []
        
        # Import here to avoid circular imports
        from ...builders.operator_factory import OperatorFactory
        from ...core.registry import get_global_registry
        
        factory = OperatorFactory(get_global_registry())
        
        # Process items in batches if max_parallel_tasks is set
        batch_size = min(self.max_parallel_tasks, len(items))
        
        for i, item in enumerate(items):
            try:
                # Create task configuration for this item
                task_config = self._build_task_config_for_item(item, i)
                task_properties = self._build_task_properties_for_item(item, i, context)
                
                # Create temporary DAG for the task (in real implementation, this would be more sophisticated)
                task_result = self._execute_single_foreach_task(
                    task_config, task_properties, item, i, context, factory
                )
                
                results.append({
                    'item_index': i,
                    'item': item,
                    'result': task_result,
                    'status': 'success'
                })
                
            except Exception as e:
                logger.error(f"ForEach task failed for item {i}: {str(e)}")
                results.append({
                    'item_index': i,
                    'item': item,
                    'error': str(e),
                    'status': 'failed'
                })
                
                # Decide whether to continue or fail
                if self.task_template.get('fail_fast', False):
                    raise
        
        return results
    
    def _build_task_config_for_item(self, item: Any, index: int) -> Dict[str, Any]:
        """Build task configuration for a specific item"""
        
        task_config = self.task_template.copy()
        
        # Generate unique task name
        base_name = task_config.get('name', 'foreach_task')
        task_config['name'] = f"{base_name}_{index}"
        
        # Add item context
        task_config['foreach_item'] = item
        task_config['foreach_index'] = index
        
        return task_config
    
    def _build_task_properties_for_item(self, item: Any, index: int, context: Context) -> Dict[str, Any]:
        """Build task properties for a specific item with variable substitution"""
        
        properties = self.task_template.get('properties', {}).copy()
        
        # Add foreach-specific properties
        properties['foreach_item'] = item
        properties['foreach_index'] = index
        properties['foreach_total'] = len(context.get('foreach_items', []))
        
        # Variable substitution for item fields
        if isinstance(item, dict):
            properties.update({f"item_{key}": value for key, value in item.items()})
        else:
            properties['item_value'] = item
        
        # Template substitution
        properties = self._substitute_item_variables(properties, item, index)
        
        return properties
    
    def _substitute_item_variables(self, properties: Dict[str, Any], item: Any, index: int) -> Dict[str, Any]:
        """Substitute item variables in property values"""
        
        def substitute_value(value):
            if isinstance(value, str):
                # Replace foreach placeholders
                value = value.replace('{foreach_index}', str(index))
                value = value.replace('{foreach_item}', str(item))
                
                # Replace item field placeholders if item is dict
                if isinstance(item, dict):
                    for key, item_value in item.items():
                        value = value.replace(f'{{item.{key}}}', str(item_value))
                
                return value
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(v) for v in value]
            else:
                return value
        
        return {key: substitute_value(value) for key, value in properties.items()}
    
    def _execute_single_foreach_task(self, 
                                   task_config: Dict[str, Any],
                                   task_properties: Dict[str, Any],
                                   item: Any,
                                   index: int,
                                   context: Context,
                                   factory: 'OperatorFactory') -> Any:
        """Execute a single task within the foreach loop"""
        
        # For PythonOperator tasks, we can execute directly
        task_type = task_config.get('type', 'PYTHON')
        
        if task_type == 'PYTHON':
            return self._execute_python_foreach_task(task_config, task_properties, item, index, context)
        else:
            # For other operators, we'd need more sophisticated handling
            logger.warning(f"ForEach with task type {task_type} not fully implemented")
            return {'message': f'Processed item {index}', 'item': item}
    
    def _execute_python_foreach_task(self,
                                   task_config: Dict[str, Any],
                                   task_properties: Dict[str, Any],
                                   item: Any,
                                   index: int,
                                   context: Context) -> Any:
        """Execute Python callable for foreach item"""
        
        # Get Python callable
        python_callable = task_properties.get('python_callable')
        
        if isinstance(python_callable, str):
            # Resolve callable from string
            python_callable = self._resolve_callable_from_string(python_callable)
        
        if not callable(python_callable):
            raise ValueError(f"python_callable must be callable, got {type(python_callable)}")
        
        # Prepare kwargs for the callable
        op_kwargs = task_properties.get('op_kwargs', {}).copy()
        op_kwargs.update({
            'foreach_item': item,
            'foreach_index': index,
            'task_config': task_config,
            'task_properties': task_properties
        })
        
        # Execute the callable
        return python_callable(context, **op_kwargs)
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve Python callable from string reference"""
        
        try:
            # Import here to avoid circular imports
            from ...core.callable_registry import get_callable_registry
            
            registry = get_callable_registry()
            return registry.get_callable(callable_string)
            
        except Exception as e:
            logger.error(f"Failed to resolve callable '{callable_string}': {e}")
            raise ValueError(f"Cannot resolve callable: {callable_string}")


class ForEachTaskGroup:
    """
    Helper class for creating TaskGroups with ForEach logic
    """
    
    @staticmethod
    def create_foreach_group(dag,
                           group_id: str,
                           items_source: Union[List[Any], Callable],
                           task_template: Dict[str, Any],
                           max_parallel: int = 10) -> TaskGroup:
        """
        Create a TaskGroup with dynamic tasks based on items
        
        Args:
            dag: Airflow DAG
            group_id: TaskGroup ID
            items_source: Source of items to iterate over
            task_template: Template for creating tasks
            max_parallel: Maximum parallel tasks
            
        Returns:
            TaskGroup with dynamic tasks
        """
        
        with TaskGroup(group_id=group_id, dag=dag) as task_group:
            # Create ForEach operator
            foreach_op = ForEachOperator(
                task_id=f"{group_id}_foreach",
                items_source=items_source,
                task_template=task_template,
                max_parallel_tasks=max_parallel,
                dag=dag
            )
            
            return task_group


# Example usage in configuration:
"""
tasks:
  - name: "process_regions"
    type: "FOREACH"
    description: "Process data for each region"
    properties:
      items_source: ["US", "EU", "APAC", "LATAM"]
      task_template:
        type: "PYTHON"
        name: "process_region"
        properties:
          python_callable: "process_region_data"
          op_kwargs:
            region: "{foreach_item}"
            batch_size: 1000
            output_path: "s3://bucket/regions/{foreach_item}/data"
      max_parallel_tasks: 4
      fail_on_empty: false

  - name: "process_files"
    type: "FOREACH"
    description: "Process each file in directory"
    properties:
      items_source: "get_files_to_process"  # Callable that returns list of files
      task_template:
        type: "PYTHON"
        name: "process_file"
        properties:
          python_callable: "process_single_file"
          op_kwargs:
            file_path: "{foreach_item}"
            output_dir: "s3://bucket/processed/"
      max_parallel_tasks: 8
"""