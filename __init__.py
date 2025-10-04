"""
InsightAir Workflow Framework
Configuration-driven Airflow framework supporting multi-cloud platforms
"""

__version__ = "1.0.0"

from .config_parser import ConfigParser, DynamicImporter
from .dag_builder import DAGBuilder, create_dag_from_config

__all__ = ['ConfigParser', 'DynamicImporter', 'DAGBuilder', 'create_dag_from_config']