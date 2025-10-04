# Framework Examples

This directory contains example implementations demonstrating various features and usage patterns of the Framework.

## Examples Overview

### 1. Simple DAG (`simple_dag/`)
**Purpose**: Basic framework usage with standard Airflow operators
**Features**:
- Simple linear task chain
- Flat properties configuration
- Python and Email operators
- Runtime parameter support

**Files**:
- `simple_dag.py` - DAG definition using framework
- `config.yaml` - Task chain configuration
- `properties.yaml` - Flat properties for all parameters

**Usage**:
```python
from framework import create_dag_from_config
dag = create_dag_from_config('config.yaml', 'properties.yaml')
```

### 2. Complex Workflow (`complex_workflow/`)
**Purpose**: Advanced features with task groups and nested workflows
**Features**:
- Task groups and nested groups
- Custom operators
- Parallel processing
- Conditional branching

### 3. Data Pipeline (`data_pipeline/`)
**Purpose**: Real-world data processing pipeline
**Features**:
- Database to text conversion
- Data quality checks
- Archive operations
- Error handling and notifications

### 4. ML Pipeline (`ml_pipeline/`)
**Purpose**: Machine learning workflow example
**Features**:
- Model training and validation
- Feature engineering
- Model deployment
- Performance monitoring

## Running Examples

### Prerequisites
1. Airflow environment set up
2. InsightAir Framework installed
3. Required dependencies for specific examples

### Basic Usage
1. Copy example folder to your Airflow DAGs directory
2. Modify properties.yaml for your environment
3. Airflow will automatically discover the DAG

### Runtime Parameter Override
```bash
# Via Airflow CLI
airflow dags trigger simple_example_dag \
  --conf '{"python_process_count": 200, "sla_minutes": 45}'

# Via Airflow Variables
airflow variables set python_process_count 200
airflow variables set email_to "custom@company.com"
```

## Configuration Patterns

### 1. Flat Properties Structure
```yaml
# Easy runtime override
database_host: "db.example.com"
database_port: 5432
batch_size: 1000
timeout_minutes: 30
```

### 2. Task Chain Definition
```yaml
tasks:
  - name: "extract_data"
    type: "DB_TO_TEXT"
    parents: ["validate_source"]
    
  - name: "quality_check"
    type: "DQ"
    parents: ["extract_data"]
```

### 3. Task Groups
```yaml
tasks:
  - name: "processing_group"
    type: "TASK_GROUP"
    tasks:
      - name: "extract"
        type: "DB_TO_TEXT"
      - name: "transform"
        type: "PYTHON"
        parents: ["extract"]
```

### 4. Runtime Override Support
```yaml
# Properties support variable substitution
database_host: "${DB_HOST:-localhost}"
batch_size: "${BATCH_SIZE:-1000}"
timeout_minutes: "${TIMEOUT_MINUTES:-30}"
```

## Customization Examples

### 1. Custom Operator Registration
```python
from framework import InsightAirFramework
from airflow.models.baseoperator import BaseOperator

class CustomOperator(BaseOperator):
    def execute(self, context):
        # Implementation
        pass

framework = InsightAirFramework()
framework.register_operator('CUSTOM_TASK', CustomOperator)
```

### 2. Advanced DAG Builder
```python
from framework import InsightAirFramework

framework = InsightAirFramework()

# Create workflow context for advanced operations
context = framework.create_workflow_context('config.yaml', 'properties.yaml')

# Build DAG with runtime parameters
dag = framework.build_dag_from_config(
    'config.yaml',
    'properties.yaml',
    runtime_params={'batch_size': 2000}
)
```

### 3. Configuration Validation
```python
from framework import InsightAirFramework

framework = InsightAirFramework()

# Validate configuration without building DAG
is_valid = framework.validate_configuration('config.yaml', 'properties.yaml')
```

## Best Practices Demonstrated

### 1. **Configuration Organization**
- Separate task chain logic from properties
- Use flat properties for runtime override
- Group related configurations logically

### 2. **Task Design**
- Clear task names and descriptions
- Proper dependency definition
- Error handling and retry configuration

### 3. **Runtime Flexibility**
- Support for environment-specific overrides
- Parameter validation and defaults
- Dynamic configuration based on runtime context

### 4. **Documentation**
- Comprehensive task descriptions
- Configuration comments and examples
- Usage instructions and patterns

## Troubleshooting

### Common Issues

1. **Configuration File Not Found**
   - Ensure paths are correct relative to DAG file
   - Check file permissions and accessibility

2. **Operator Not Found**
   - Verify operator is registered in framework
   - Check task type spelling in configuration

3. **Runtime Override Not Working**
   - Ensure property names match exactly
   - Check override priority order
   - Validate variable substitution syntax

4. **Task Group Issues**
   - Verify nested task dependencies
   - Check group name uniqueness
   - Validate task group configuration structure

### Debugging Tips

1. **Enable Debug Logging**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Validate Configuration**
   ```python
   framework.validate_configuration('config.yaml', 'properties.yaml')
   ```

3. **Check Framework Info**
   ```python
   info = framework.get_framework_info()
   print(info)
   ```

These examples provide a comprehensive starting point for using the Framework in various scenarios and demonstrate best practices for configuration-driven DAG development.