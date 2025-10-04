# Simple DAG Example - InsightAir Framework
"""
Example of a simple DAG using the InsightAir Framework.
This demonstrates the basic usage pattern for configuration-driven DAG creation.
"""

from pathlib import Path
from framework import create_dag_from_config

# Define the configuration directory (relative to this file)
CONFIG_DIR = Path(__file__).parent

# Create DAG from configuration
dag = create_dag_from_config(
    config_file=CONFIG_DIR / 'config.yaml',
    properties_file=CONFIG_DIR / 'properties.yaml',
    config_dir=CONFIG_DIR
)

# The DAG is now ready for Airflow to discover and execute