# Audit and Lifecycle Management Example DAG
"""
Example DAG demonstrating comprehensive audit and lifecycle management features:
- Job START and END boundary tasks with audit actions
- Task-level audit logging and metrics collection
- Task enable/disable functionality
- Task group lifecycle management
- Custom audit actions and hooks
- Configurable audit destinations
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.utils.task_group import TaskGroup

# Import InsightAir framework
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from framework.core.framework import InsightAirFramework
from framework.core.audit_system import configure_audit_system
from framework.operators.lifecycle.task_audit_wrapper import TaskGroupAuditManager

# Get configuration files
config_dir = Path(__file__).parent
config_file = config_dir / "config.yaml"
properties_file = config_dir / "properties.yaml"

# Initialize framework
framework = InsightAirFramework()

# Build DAG from configuration
dag = framework.build_dag_from_config(
    config_file=config_file,
    properties_file=properties_file
)

# Configure global audit system
audit_config = {
    'enabled': True,
    'level': 'INFO',
    'destinations': [
        {
            'type': 'logger',
            'logger_name': 'insightair.audit.lifecycle_example',
            'enabled': True
        },
        {
            'type': 'xcom',
            'key_prefix': 'audit_event',
            'enabled': True
        }
    ]
}

configure_audit_system(audit_config)

# The DAG is automatically created by the framework
# Additional manual configuration can be done here if needed

if __name__ == "__main__":
    # For testing purposes
    print(f"DAG created: {dag.dag_id}")
    print(f"Number of tasks: {len(dag.tasks)}")
    for task in dag.tasks:
        print(f"  - {task.task_id} ({task.__class__.__name__})")
        
        # Show audit configuration for tasks
        if hasattr(task, 'audit'):
            print(f"    Audit enabled: {task.audit.get('enabled', False)}")
        
        # Show if task is disabled
        if hasattr(task, 'disabled_reason'):
            print(f"    Disabled: {task.disabled_reason}")