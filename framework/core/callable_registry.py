# Python Callable Registry - Manage custom business logic functions
"""
CallableRegistry provides centralized management of Python callable functions
for use in PythonOperators, branching logic, and custom business logic.
"""

import logging
import importlib
from typing import Dict, Callable, Optional, Any, Type
from pathlib import Path

from .exceptions import CallableRegistrationError, CallableNotFoundError

logger = logging.getLogger(__name__)


class CallableRegistry:
    """
    Registry for managing Python callable functions used across the framework
    """
    
    def __init__(self):
        """Initialize CallableRegistry"""
        self._callables: Dict[str, Callable] = {}
        self._callable_metadata: Dict[str, Dict[str, Any]] = {}
        logger.debug("CallableRegistry initialized")
    
    def register_callable(self,
                         name: str,
                         callable_func: Callable,
                         description: Optional[str] = None,
                         category: Optional[str] = None,
                         override: bool = False):
        """
        Register a callable function
        
        Args:
            name: Unique identifier for the callable
            callable_func: Python callable function
            description: Description of what the function does
            category: Category for organizing functions
            override: Whether to override existing registration
            
        Raises:
            CallableRegistrationError: If registration fails
        """
        if not callable(callable_func):
            raise CallableRegistrationError(f"Object {callable_func} is not callable")
        
        if name in self._callables and not override:
            existing_func = self._callables[name]
            raise CallableRegistrationError(
                f"Callable '{name}' already registered with {existing_func}. "
                f"Use override=True to replace."
            )
        
        # Register callable
        self._callables[name] = callable_func
        
        # Store metadata
        self._callable_metadata[name] = {
            'function_name': getattr(callable_func, '__name__', 'unknown'),
            'module': getattr(callable_func, '__module__', 'unknown'),
            'description': description or f"Callable function: {name}",
            'category': category or 'custom',
            'doc': getattr(callable_func, '__doc__', None)
        }
        
        action = "Overrode" if name in self._callables else "Registered"
        logger.info(f"{action} callable '{name}' -> {callable_func}")
    
    def get_callable(self, name: str) -> Callable:
        """
        Get callable function by name
        
        Args:
            name: Callable identifier
            
        Returns:
            Callable function
            
        Raises:
            CallableNotFoundError: If callable is not registered
        """
        if name not in self._callables:
            # Try to resolve from string path
            resolved_callable = self._resolve_callable_from_path(name)
            if resolved_callable:
                # Auto-register resolved callable
                self.register_callable(name, resolved_callable, 
                                     description=f"Auto-resolved from {name}")
                return resolved_callable
            
            available_callables = list(self._callables.keys())
            raise CallableNotFoundError(
                f"Callable '{name}' not found. Available callables: {available_callables}"
            )
        
        return self._callables[name]
    
    def _resolve_callable_from_path(self, callable_path: str) -> Optional[Callable]:
        """
        Resolve callable from module.function path string
        
        Args:
            callable_path: String like "module.submodule.function_name"
            
        Returns:
            Resolved callable or None if not found
        """
        try:
            # Split module path and function name
            if '.' not in callable_path:
                return None
            
            module_path, func_name = callable_path.rsplit('.', 1)
            
            # Import module
            module = importlib.import_module(module_path)
            
            # Get function from module
            if hasattr(module, func_name):
                func = getattr(module, func_name)
                if callable(func):
                    logger.debug(f"Resolved callable from path: {callable_path}")
                    return func
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to resolve callable from path '{callable_path}': {e}")
            return None
    
    def is_registered(self, name: str) -> bool:
        """
        Check if a callable is registered
        
        Args:
            name: Callable name to check
            
        Returns:
            True if callable is registered
        """
        return name in self._callables
    
    def get_all_callables(self) -> Dict[str, Callable]:
        """
        Get all registered callables
        
        Returns:
            Dictionary mapping names to callables
        """
        return self._callables.copy()
    
    def get_callable_metadata(self, name: str) -> Dict[str, Any]:
        """
        Get metadata for a callable
        
        Args:
            name: Callable name
            
        Returns:
            Metadata dictionary
            
        Raises:
            CallableNotFoundError: If callable is not registered
        """
        if name not in self._callable_metadata:
            raise CallableNotFoundError(f"Callable '{name}' not found")
        
        return self._callable_metadata[name].copy()
    
    def get_all_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        Get metadata for all registered callables
        
        Returns:
            Dictionary mapping names to their metadata
        """
        return self._callable_metadata.copy()
    
    def unregister_callable(self, name: str) -> bool:
        """
        Unregister a callable
        
        Args:
            name: Callable name to unregister
            
        Returns:
            True if callable was unregistered, False if not found
        """
        if name in self._callables:
            del self._callables[name]
            del self._callable_metadata[name]
            logger.info(f"Unregistered callable '{name}'")
            return True
        return False
    
    def get_callables_by_category(self, category: str) -> Dict[str, Callable]:
        """
        Get callables filtered by category
        
        Args:
            category: Category to filter by
            
        Returns:
            Dictionary of callables in the specified category
        """
        filtered_callables = {}
        
        for name, callable_func in self._callables.items():
            metadata = self._callable_metadata.get(name, {})
            if metadata.get('category') == category:
                filtered_callables[name] = callable_func
        
        return filtered_callables
    
    def list_callables(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all callables with their metadata
        
        Args:
            category: Optional category filter
            
        Returns:
            List of callable information dictionaries
        """
        callables_info = []
        
        for name, callable_func in self._callables.items():
            metadata = self._callable_metadata.get(name, {})
            
            # Filter by category if specified
            if category and metadata.get('category') != category:
                continue
            
            info = {
                'name': name,
                'function_name': metadata.get('function_name', 'unknown'),
                'module': metadata.get('module', 'unknown'),
                'description': metadata.get('description', 'No description'),
                'category': metadata.get('category', 'unknown'),
                'doc': metadata.get('doc')
            }
            callables_info.append(info)
        
        # Sort by name
        return sorted(callables_info, key=lambda x: x['name'])
    
    def register_callable_decorator(self, name: str, 
                                   category: Optional[str] = None,
                                   description: Optional[str] = None,
                                   override: bool = False):
        """
        Decorator for registering callable functions
        
        Args:
            name: Callable identifier
            category: Category for organization
            description: Function description
            override: Whether to override existing registration
            
        Returns:
            Decorator function
            
        Example:
            @registry.register_callable_decorator('my_function', category='data_processing')
            def my_function(context, **kwargs):
                return "Hello World"
        """
        def decorator(func: Callable):
            self.register_callable(
                name=name,
                callable_func=func,
                description=description,
                category=category,
                override=override
            )
            return func
        return decorator
    
    def register_callables_from_module(self,
                                      module,
                                      prefix: Optional[str] = None,
                                      category: Optional[str] = None,
                                      filter_func: Optional[Callable] = None):
        """
        Register all callable functions from a module
        
        Args:
            module: Python module containing callable functions
            prefix: Optional prefix for callable names
            category: Category for all callables from this module
            filter_func: Optional function to filter which callables to register
        """
        import inspect
        
        registered_count = 0
        
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            # Skip private functions
            if name.startswith('_'):
                continue
            
            # Apply filter if provided
            if filter_func and not filter_func(name, obj):
                continue
            
            # Generate callable name
            callable_name = name
            if prefix:
                callable_name = f"{prefix}.{name}"
            
            try:
                self.register_callable(
                    name=callable_name,
                    callable_func=obj,
                    description=f"Auto-registered from {module.__name__}.{name}",
                    category=category or 'auto_registered'
                )
                registered_count += 1
            except CallableRegistrationError as e:
                logger.warning(f"Failed to register {name}: {e}")
        
        logger.info(f"Auto-registered {registered_count} callables from {module.__name__}")
    
    def clear_registry(self):
        """Clear all registered callables"""
        self._callables.clear()
        self._callable_metadata.clear()
        logger.info("Callable registry cleared")
    
    def register_builtin_callables(self):
        """Register built-in callable functions"""
        
        # Data processing functions
        @self.register_callable_decorator('get_current_date', category='datetime')
        def get_current_date(context, **kwargs):
            """Get current date in YYYY-MM-DD format"""
            from datetime import datetime
            return datetime.now().strftime('%Y-%m-%d')
        
        @self.register_callable_decorator('get_execution_date', category='datetime')
        def get_execution_date(context, **kwargs):
            """Get execution date from context"""
            return context.get('ds', '')
        
        @self.register_callable_decorator('log_message', category='utility')
        def log_message(context, message="", level="INFO", **kwargs):
            """Log a message with specified level"""
            logger.log(getattr(logging, level, logging.INFO), message)
            return message
        
        @self.register_callable_decorator('check_file_exists', category='file_ops')
        def check_file_exists(context, file_path, **kwargs):
            """Check if file exists"""
            return Path(file_path).exists()
        
        @self.register_callable_decorator('get_file_size', category='file_ops')
        def get_file_size(context, file_path, **kwargs):
            """Get file size in bytes"""
            path = Path(file_path)
            return path.stat().st_size if path.exists() else 0
        
        # Branching helper functions
        @self.register_callable_decorator('branch_by_weekday', category='branching')
        def branch_by_weekday(context, weekday_task="weekday", weekend_task="weekend", **kwargs):
            """Branch based on day of week"""
            from datetime import datetime
            weekday = datetime.now().weekday()  # 0=Monday, 6=Sunday
            return weekend_task if weekday >= 5 else weekday_task
        
        @self.register_callable_decorator('branch_by_hour', category='branching')
        def branch_by_hour(context, business_hours_task="business", 
                          after_hours_task="after_hours", start_hour=9, end_hour=17, **kwargs):
            """Branch based on hour of day"""
            from datetime import datetime
            current_hour = datetime.now().hour
            return business_hours_task if start_hour <= current_hour < end_hour else after_hours_task
        
        logger.info("Built-in callables registered")


# Global registry instance
_global_callable_registry = None


def get_callable_registry() -> CallableRegistry:
    """
    Get the global callable registry instance
    
    Returns:
        Global CallableRegistry instance
    """
    global _global_callable_registry
    if _global_callable_registry is None:
        _global_callable_registry = CallableRegistry()
        _global_callable_registry.register_builtin_callables()
    return _global_callable_registry


def register_callable_globally(name: str,
                               callable_func: Callable,
                               **kwargs):
    """
    Register a callable in the global registry
    
    Args:
        name: Callable identifier
        callable_func: Callable function
        **kwargs: Additional registration arguments
    """
    global_registry = get_callable_registry()
    global_registry.register_callable(name, callable_func, **kwargs)


def callable_decorator(name: str, **kwargs):
    """
    Decorator for registering callables globally
    
    Args:
        name: Callable identifier
        **kwargs: Additional registration arguments
        
    Example:
        @callable_decorator('my_function', category='data_processing')
        def my_function(context, **kwargs):
            return "Hello World"
    """
    def decorator(func: Callable):
        register_callable_globally(name, func, **kwargs)
        return func
    return decorator


# Example usage:
"""
# Register callable functions for use in configurations
from framework.core.callable_registry import callable_decorator

@callable_decorator('process_sales_data', category='business_logic')
def process_sales_data(context, region, date_range, **kwargs):
    # Business logic for processing sales data
    return f"Processed sales data for {region} from {date_range}"

@callable_decorator('validate_data_quality', category='quality')
def validate_data_quality(context, dataset, min_records=1000, **kwargs):
    # Data quality validation logic
    record_count = len(dataset)
    return record_count >= min_records

@callable_decorator('determine_processing_branch', category='branching')
def determine_processing_branch(context, **kwargs):
    # Branching logic based on conditions
    volume = context['task_instance'].xcom_pull(key='record_count')
    return 'high_volume_processing' if volume > 10000 else 'standard_processing'

# Configuration usage:
tasks:
  - name: "process_data"
    type: "PYTHON"
    properties:
      python_callable: "process_sales_data"
      op_kwargs:
        region: "{{ params.region }}"
        date_range: "{{ ds }}"

  - name: "quality_check"
    type: "PYTHON"
    properties:
      python_callable: "validate_data_quality"
      op_kwargs:
        dataset: "{{ ti.xcom_pull(task_ids='load_data') }}"
        min_records: 500

  - name: "branch_processing"
    type: "BRANCH_PYTHON"
    properties:
      python_callable: "determine_processing_branch"
"""