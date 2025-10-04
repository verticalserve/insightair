# Operator Registry - Manage operator registration and lookup
"""
OperatorRegistry provides centralized management of operator classes
with support for:
- Dynamic operator registration
- Type-safe operator lookup
- Custom operator support
- Built-in operator management
"""

import logging
from typing import Dict, Type, Optional, List
from airflow.models.baseoperator import BaseOperator

from .exceptions import OperatorRegistrationError, OperatorNotFoundError

logger = logging.getLogger(__name__)


class OperatorRegistry:
    """
    Registry for managing operator classes and their task type mappings
    """
    
    def __init__(self):
        """Initialize OperatorRegistry"""
        self._operators: Dict[str, Type[BaseOperator]] = {}
        self._operator_metadata: Dict[str, Dict[str, str]] = {}
        logger.debug("OperatorRegistry initialized")
    
    def register_operator(self,
                         task_type: str,
                         operator_class: Type[BaseOperator],
                         override: bool = False,
                         description: Optional[str] = None,
                         category: Optional[str] = None):
        """
        Register an operator class for a task type
        
        Args:
            task_type: Task type identifier (e.g., 'DB_TO_TEXT')
            operator_class: Airflow operator class
            override: Whether to override existing registration
            description: Description of the operator
            category: Category for grouping operators
            
        Raises:
            OperatorRegistrationError: If registration fails
        """
        if not issubclass(operator_class, BaseOperator):
            raise OperatorRegistrationError(
                f"Operator class {operator_class.__name__} must inherit from BaseOperator"
            )
        
        if task_type in self._operators and not override:
            existing_class = self._operators[task_type]
            raise OperatorRegistrationError(
                f"Task type '{task_type}' already registered with {existing_class.__name__}. "
                f"Use override=True to replace."
            )
        
        # Register operator
        self._operators[task_type] = operator_class
        
        # Store metadata
        self._operator_metadata[task_type] = {
            'class_name': operator_class.__name__,
            'module': operator_class.__module__,
            'description': description or f"{operator_class.__name__} operator",
            'category': category or 'custom'
        }
        
        action = "Overrode" if task_type in self._operators else "Registered"
        logger.info(f"{action} operator {operator_class.__name__} for task type '{task_type}'")
    
    def get_operator_class(self, task_type: str) -> Type[BaseOperator]:
        """
        Get operator class for a task type
        
        Args:
            task_type: Task type identifier
            
        Returns:
            Operator class
            
        Raises:
            OperatorNotFoundError: If task type is not registered
        """
        if task_type not in self._operators:
            available_types = list(self._operators.keys())
            raise OperatorNotFoundError(
                f"Task type '{task_type}' not found. Available types: {available_types}"
            )
        
        return self._operators[task_type]
    
    def is_registered(self, task_type: str) -> bool:
        """
        Check if a task type is registered
        
        Args:
            task_type: Task type to check
            
        Returns:
            True if task type is registered
        """
        return task_type in self._operators
    
    def get_all_operators(self) -> Dict[str, Type[BaseOperator]]:
        """
        Get all registered operators
        
        Returns:
            Dictionary mapping task types to operator classes
        """
        return self._operators.copy()
    
    def get_operator_metadata(self, task_type: str) -> Dict[str, str]:
        """
        Get metadata for an operator
        
        Args:
            task_type: Task type identifier
            
        Returns:
            Operator metadata dictionary
            
        Raises:
            OperatorNotFoundError: If task type is not registered
        """
        if task_type not in self._operator_metadata:
            raise OperatorNotFoundError(f"Task type '{task_type}' not found")
        
        return self._operator_metadata[task_type].copy()
    
    def get_all_metadata(self) -> Dict[str, Dict[str, str]]:
        """
        Get metadata for all registered operators
        
        Returns:
            Dictionary mapping task types to their metadata
        """
        return self._operator_metadata.copy()
    
    def unregister_operator(self, task_type: str) -> bool:
        """
        Unregister an operator
        
        Args:
            task_type: Task type to unregister
            
        Returns:
            True if operator was unregistered, False if not found
        """
        if task_type in self._operators:
            del self._operators[task_type]
            del self._operator_metadata[task_type]
            logger.info(f"Unregistered operator for task type '{task_type}'")
            return True
        return False
    
    def get_operators_by_category(self, category: str) -> Dict[str, Type[BaseOperator]]:
        """
        Get operators filtered by category
        
        Args:
            category: Category to filter by
            
        Returns:
            Dictionary of operators in the specified category
        """
        filtered_operators = {}
        
        for task_type, operator_class in self._operators.items():
            metadata = self._operator_metadata.get(task_type, {})
            if metadata.get('category') == category:
                filtered_operators[task_type] = operator_class
        
        return filtered_operators
    
    def get_available_categories(self) -> List[str]:
        """
        Get all available operator categories
        
        Returns:
            List of unique categories
        """
        categories = set()
        for metadata in self._operator_metadata.values():
            categories.add(metadata.get('category', 'unknown'))
        
        return sorted(list(categories))
    
    def list_operators(self, category: Optional[str] = None) -> List[Dict[str, str]]:
        """
        List all operators with their metadata
        
        Args:
            category: Optional category filter
            
        Returns:
            List of operator information dictionaries
        """
        operators_info = []
        
        for task_type, operator_class in self._operators.items():
            metadata = self._operator_metadata.get(task_type, {})
            
            # Filter by category if specified
            if category and metadata.get('category') != category:
                continue
            
            info = {
                'task_type': task_type,
                'class_name': operator_class.__name__,
                'module': operator_class.__module__,
                'description': metadata.get('description', 'No description'),
                'category': metadata.get('category', 'unknown')
            }
            operators_info.append(info)
        
        # Sort by task type
        return sorted(operators_info, key=lambda x: x['task_type'])
    
    def validate_operator_class(self, operator_class: Type[BaseOperator]) -> bool:
        """
        Validate that an operator class is suitable for registration
        
        Args:
            operator_class: Operator class to validate
            
        Returns:
            True if valid
            
        Raises:
            OperatorRegistrationError: If validation fails
        """
        # Check inheritance
        if not issubclass(operator_class, BaseOperator):
            raise OperatorRegistrationError(
                f"Class {operator_class.__name__} must inherit from BaseOperator"
            )
        
        # Check for execute method
        if not hasattr(operator_class, 'execute'):
            raise OperatorRegistrationError(
                f"Class {operator_class.__name__} must implement execute method"
            )
        
        # Check execute method is callable
        if not callable(getattr(operator_class, 'execute')):
            raise OperatorRegistrationError(
                f"execute method in {operator_class.__name__} must be callable"
            )
        
        return True
    
    def register_operators_from_module(self,
                                      module,
                                      prefix: Optional[str] = None,
                                      category: Optional[str] = None):
        """
        Register all operator classes from a module
        
        Args:
            module: Python module containing operator classes
            prefix: Optional prefix for task types
            category: Category for all operators from this module
        """
        import inspect
        
        registered_count = 0
        
        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Skip if not an operator
            if not issubclass(obj, BaseOperator):
                continue
            
            # Skip BaseOperator itself
            if obj is BaseOperator:
                continue
            
            # Generate task type from class name
            task_type = name.upper().replace('OPERATOR', '')
            if prefix:
                task_type = f"{prefix}_{task_type}"
            
            try:
                self.register_operator(
                    task_type=task_type,
                    operator_class=obj,
                    description=f"Auto-registered from {module.__name__}",
                    category=category or 'auto_registered'
                )
                registered_count += 1
            except OperatorRegistrationError as e:
                logger.warning(f"Failed to register {name}: {e}")
        
        logger.info(f"Auto-registered {registered_count} operators from {module.__name__}")
    
    def get_registry_stats(self) -> Dict[str, int]:
        """
        Get statistics about the registry
        
        Returns:
            Dictionary with registry statistics
        """
        categories = {}
        for metadata in self._operator_metadata.values():
            category = metadata.get('category', 'unknown')
            categories[category] = categories.get(category, 0) + 1
        
        return {
            'total_operators': len(self._operators),
            'categories': len(categories),
            'category_breakdown': categories
        }
    
    def clear_registry(self):
        """Clear all registered operators"""
        self._operators.clear()
        self._operator_metadata.clear()
        logger.info("Operator registry cleared")


# Global registry instance
_global_registry = None


def get_global_registry() -> OperatorRegistry:
    """
    Get the global operator registry instance
    
    Returns:
        Global OperatorRegistry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = OperatorRegistry()
    return _global_registry


def register_operator_globally(task_type: str,
                              operator_class: Type[BaseOperator],
                              **kwargs):
    """
    Register an operator in the global registry
    
    Args:
        task_type: Task type identifier
        operator_class: Operator class
        **kwargs: Additional registration arguments
    """
    global_registry = get_global_registry()
    global_registry.register_operator(task_type, operator_class, **kwargs)