# Framework - Configuration-Driven Airflow DAG Framework
"""
Framework provides a comprehensive configuration-driven approach 
to building Airflow DAGs with support for:

- Task groups and nested workflows
- Runtime parameter overrides  
- Custom operators and integrations
- Configuration validation and schema enforcement
- Template processing and variable substitution
"""

__version__ = "2.0.0"
__author__ = "InsightAir Team"

# Core Framework Components
from .core.framework import InsightAirFramework
from .core.context import WorkflowContext
from .core.registry import OperatorRegistry

# Configuration Management
from .config.manager import ConfigManager
from .config.loader import ConfigLoader

# Builders
from .builders.dag_builder import DAGBuilder
from .builders.task_builder import TaskBuilder
from .builders.group_builder import TaskGroupBuilder

# Base Classes for Extension
from .operators.base_operator import BaseInsightAirOperator
from .parsers.base_parser import BaseParser

__all__ = [
    # Core
    'InsightAirFramework',
    'WorkflowContext', 
    'OperatorRegistry',
    
    # Configuration
    'ConfigManager',
    'ConfigLoader',
    
    # Builders
    'DAGBuilder',
    'TaskBuilder',
    'TaskGroupBuilder',
    
    # Base Classes
    'BaseInsightAirOperator',
    'BaseParser'
]