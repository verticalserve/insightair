"""
Example Dynamic DAG using the InsightAir Configuration-Driven Framework
This demonstrates how to create DAGs dynamically from configuration files
"""

from pathlib import Path
from datetime import datetime, timedelta
from dag_builder import create_dag_from_config

# Configuration paths
workflow_name = 'dynamic-config-driven-workflow'
config_dir = Path(__file__).parent / 'example_dags' / 'load'
environment_file = Path(__file__).parent / 'example_dags' / 'environment.yaml'

# Default arguments for the DAG
default_args = {
    'owner': 'insightair-framework',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30)
}

# Create the DAG dynamically from configuration
dag = create_dag_from_config(
    workflow_name=workflow_name,
    config_path=str(config_dir),
    environment_file=str(environment_file),
    default_args=default_args,
    schedule_interval=None
)

# The DAG is automatically registered with Airflow
globals()[workflow_name.replace('-', '_')] = dag