# Enhanced Load Workflow - load_new

## Overview
This directory demonstrates the enhanced InsightAir configuration approach with improved structure, validation, and maintainability.

## Key Improvements

### 1. Unified Configuration (`config.yaml`)
- **Single file** contains all configuration instead of multiple property files
- **Hierarchical structure** with clear sections (metadata, properties, tasks)
- **Environment variable substitution** using `${VAR}` syntax
- **Environment-specific overrides** for dev/staging/production

### 2. Enhanced DAG Structure (`policy_history_text_processing.py`)
- **Customizable task chains** - clearly defined task flow section
- **Comprehensive documentation** - each task has detailed doc_md
- **Dynamic task support** - framework for creating tasks programmatically
- **Better error handling** - enhanced callbacks and monitoring

### 3. Schema Validation (`schema.yaml`)
- **Configuration validation** using JSON Schema
- **Type safety** with enforced data types and constraints  
- **Documentation** embedded in schema definitions
- **Enum validation** for controlled vocabularies

## File Structure
```
load_new/
├── config.yaml                      # Main configuration file
├── policy_history_text_processing.py # Enhanced DAG implementation
├── schema.yaml                      # Configuration schema
└── README.md                        # This documentation
```

## Configuration Structure

### Metadata Section
```yaml
metadata:
  name: "workflow-identifier"
  version: "2.0.0" 
  description: "Workflow description"
  category: "data_processing"
  priority: "P3"
  data_group: "production"
```

### Properties Section
```yaml
properties:
  source:
    database: "source_db"
    connection_id: "postgres_conn"
  target:
    bucket: "${STAGE_BUCKET}"
    path: "output/path"
  processing:
    engine: "glue"
    timeout_minutes: 45
```

### Tasks Section
```yaml
tasks:
  - name: "task_name"
    type: "DB_TO_TEXT"
    description: "Task description"
    parents: ["parent_task"]
    properties:
      # Task-specific configuration
```

## Customizing Task Chains

The DAG file provides a clear section for customizing task flows:

```python
# ===== CUSTOMIZABLE TASK CHAIN =====
# Users can modify this section to add/remove/reorder tasks

# Create tasks
start = framework_instance.build_task('START', 'start')
process_data = framework_instance.build_task('DB_TO_TEXT', 'process_data')
end = framework_instance.build_task('END', 'end')

# ===== TASK FLOW DEFINITION =====
# Define dependencies
start >> process_data >> end

# Alternative flows:
# Parallel: start >> [task1, task2] >> merge >> end
# Conditional: start >> branch >> [path1, path2] >> end
```

## Adding Custom Operators

To add a custom operator to the task chain:

1. **Register the operator** in the framework
2. **Add task configuration** to config.yaml
3. **Insert into task chain** in the DAG file

Example:
```python
# In DAG file
custom_task = framework_instance.build_task('CUSTOM_TYPE', 'custom_task')
process_data >> custom_task >> quality_check
```

## Environment-Specific Configuration

Override properties for different environments:

```yaml
environments:
  development:
    properties:
      target:
        bucket: "dev-bucket"
      notifications:
        sla_minutes: 60
        
  production:
    properties:
      target:
        bucket: "prod-bucket"
      notifications:
        sla_minutes: 30
```

## Benefits of New Approach

1. **Reduced File Proliferation**: Single config file vs multiple property files
2. **Better Maintainability**: Clear structure and documentation
3. **Environment Flexibility**: Easy environment-specific overrides
4. **Validation**: Schema-based configuration validation
5. **Customization**: Clear extension points for custom logic
6. **Documentation**: Embedded documentation and examples

## Migration from Old Format

To migrate from the old format:

1. **Consolidate** multiple property files into single config.yaml
2. **Restructure** flat properties into hierarchical sections
3. **Add metadata** section with workflow information
4. **Update DAG** to use enhanced structure
5. **Add validation** using schema.yaml

## Usage

1. **Copy this directory** as a template for new workflows
2. **Modify config.yaml** with your specific configuration
3. **Customize task chain** in the DAG file
4. **Validate configuration** against schema.yaml
5. **Deploy** to Airflow

This enhanced approach provides a solid foundation for scalable, maintainable data workflows while preserving the flexibility to customize task chains as needed.