# Hybrid Configuration Approach - load_hybrid

## Overview
This directory demonstrates the **optimal hybrid configuration approach** that balances the benefits of both centralized and distributed configuration management.

## Why Hybrid Configuration?

### Problems with Single Config File
- ❌ **Runtime Override Complexity**: Nested properties difficult to override at runtime
- ❌ **Large File Maintenance**: Complex queries and templates make config unwieldy  
- ❌ **Version Control**: Large diffs when changing small task properties
- ❌ **Team Collaboration**: Multiple developers editing same large file

### Problems with Too Many Files
- ❌ **File Proliferation**: Too many small files become hard to manage
- ❌ **Configuration Scatter**: Related settings spread across many files
- ❌ **Dependency Tracking**: Hard to see overall workflow configuration

### Hybrid Solution Benefits
- ✅ **Runtime Override Support**: Flat properties.yaml for easy parameter overrides
- ✅ **Complex Content Separation**: Large queries/scripts in dedicated task files
- ✅ **Maintainability**: Task operators load properties on-demand
- ✅ **Team Productivity**: Clear separation of concerns
- ✅ **Performance**: Properties loaded only when tasks execute

## File Structure
```
load_hybrid/
├── config.yaml                 # Lightweight main config (task chain + metadata)
├── properties.yaml             # Flat global properties (runtime overrideable)
├── validate_source.yaml        # Task-specific properties (loaded on-demand)
├── extract_policy_data.yaml    # Complex queries & templates (200+ lines)
├── quality_check.yaml          # Complex validation rules (150+ lines)  
├── create_metadata.yaml        # Complex Python script (300+ lines)
├── archive_source.yaml         # Archive configuration
├── send_notification.yaml      # Complex HTML email template (400+ lines)
├── policy_history_hybrid.py    # Enhanced DAG with runtime support
└── README.md                   # This documentation
```

## Configuration Architecture

### 1. Main Config (`config.yaml`)
**Purpose**: Workflow metadata and task chain definition
```yaml
name: "workflow-name"
properties_file: "properties.yaml"  # Reference to global properties
tasks:
  - name: "extract_data"
    type: "DB_TO_TEXT"
    properties_file: "extract_data.yaml"  # Task-specific properties
```

### 2. Global Properties (`properties.yaml`) 
**Purpose**: Flat properties for runtime parameter overrides
```yaml
# Easy to override via Airflow Variables or DAG run configuration
source_database: "policy_db"
batch_size: 1000
timeout_minutes: 45
target_bucket: "data-lake-prod"
```

### 3. Task Property Files (`task_name.yaml`)
**Purpose**: Complex task-specific configuration
- Large SQL queries (100+ lines)
- Complex templates and scripts
- Detailed task configurations
- Loaded by task operators at runtime

## Runtime Parameter Override System

### Override Priority (Highest to Lowest):
1. **DAG Run Configuration** (UI/API)
2. **Airflow Variables**  
3. **Environment Variables**
4. **Properties File Values**
5. **Default Values**

### Override Examples:

#### 1. Via Airflow UI (DAG Run Configuration):
```json
{
  "batch_size": 2000,
  "target_bucket": "data-lake-staging", 
  "enable_quality_checks": false,
  "timeout_minutes": 60
}
```

#### 2. Via Airflow Variables:
```bash
# Set via Airflow UI or CLI
airflow variables set batch_size 2000
airflow variables set target_bucket data-lake-staging
```

#### 3. Via Environment Variables:
```bash  
export BATCH_SIZE=2000
export TARGET_BUCKET=data-lake-staging
```

#### 4. Via API/CLI Trigger:
```bash
airflow dags trigger workflow-name \
  --conf '{"batch_size": 2000, "environment": "staging"}'
```

## Task Property Loading Pattern

### In Task Operators:
```python
class DBToTextOperator(BaseOperator):
    def execute(self, context):
        # Load task properties at runtime
        task_props = self.load_task_properties('extract_policy_data.yaml')
        
        # Apply runtime overrides
        batch_size = get_runtime_property('batch_size', 
                                        task_props.get('batch_size', 1000), 
                                        context)
        
        # Load complex query from task file
        query = task_props['extraction_query']
        
        # Execute with runtime parameters
        self.execute_extraction(query, batch_size)
```

## Benefits by Use Case

### 1. Development Teams
- **Query Developers**: Edit complex SQL in dedicated files
- **Template Designers**: Maintain HTML email templates separately  
- **DevOps Engineers**: Override runtime parameters without code changes
- **Data Engineers**: Focus on task chain logic in main config

### 2. Runtime Operations
- **Environment Promotion**: Override buckets, connections via variables
- **Performance Tuning**: Adjust batch sizes, timeouts without redeploys
- **Emergency Response**: Disable quality checks, change notifications
- **A/B Testing**: Route to different processing engines

### 3. Maintenance & Debugging
- **Issue Isolation**: Problems isolated to specific property files
- **Version Control**: Cleaner diffs for specific changes
- **Testing**: Mock individual task properties for unit tests
- **Documentation**: Task-specific docs embedded in property files

## Migration from Existing Approaches

### From Single Large Config:
1. **Extract global properties** to `properties.yaml`
2. **Move complex content** (queries, templates) to task files
3. **Keep task chain** in main `config.yaml`
4. **Update DAG** to support runtime overrides

### From Multiple Small Files:
1. **Consolidate related properties** into task-specific files
2. **Create global properties** file for runtime overrides  
3. **Maintain task chain** visibility in main config
4. **Add runtime override support**

## Best Practices

### ✅ Do:
- Keep **workflow metadata** and **task chains** in main config
- Put **runtime-overrideable properties** in flat global properties file
- Put **complex content** (queries, scripts, templates) in task files
- Use **consistent naming** between files and task names
- **Document runtime parameters** in task descriptions
- **Validate configurations** before deployment

### ❌ Don't:
- Put complex queries in main config file
- Put runtime parameters in nested structures
- Create too many granular property files
- Hardcode environment-specific values
- Skip documentation of override mechanisms

## Task Chain Customization

The hybrid approach maintains easy task chain customization:

```python
# ===== CUSTOMIZABLE TASK CHAIN =====
# Clear task definition section
start = framework_instance.build_task('START', 'start')
extract = framework_instance.build_task('DB_TO_TEXT', 'extract_policy_data')
validate = framework_instance.build_task('DQ', 'quality_check')
end = framework_instance.build_task('END', 'end')

# ===== TASK FLOW DEFINITION =====
start >> extract >> validate >> end

# Alternative flows:
# Parallel: start >> [task1, task2] >> merge >> end
# Conditional: start >> branch >> [path1, path2] >> end
```

## Summary

The hybrid configuration approach provides:

1. **Runtime Flexibility**: Easy parameter overrides via multiple mechanisms
2. **Maintainability**: Complex content separated into focused files
3. **Performance**: Properties loaded on-demand by task operators
4. **Team Productivity**: Clear separation of concerns for different roles
5. **Operational Excellence**: Runtime control without code changes

This approach is **recommended for production workloads** where runtime flexibility and maintainability are critical requirements.