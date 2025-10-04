# Flat Task Properties for Dynamic Parameter Replacement

## Overview
All task property files now use **flat structure** for maximum runtime parameter replacement flexibility while maintaining complex content like SQL queries and templates.

## Why Flat Task Properties?

### Problems with Nested Properties
```yaml
# NESTED - Hard to override at runtime
database:
  connection:
    host: "db.example.com"
    port: 5432
processing:
  batch:
    size: 1000
    timeout: 300
```
❌ **Runtime Override Complexity**: `processing.batch.size` difficult to replace  
❌ **Template Substitution**: Nested paths hard to reference  
❌ **Variable Mapping**: Complex mapping from flat variables to nested config

### Benefits of Flat Properties
```yaml
# FLAT - Easy to override at runtime
database_connection_host: "db.example.com"
database_connection_port: 5432
processing_batch_size: 1000
processing_batch_timeout: 300
```
✅ **Simple Runtime Override**: Direct 1:1 mapping with Airflow Variables  
✅ **Easy Template Substitution**: Direct `${variable_name}` replacement  
✅ **Clear Parameter Names**: Self-documenting parameter purposes

## Flat Structure Patterns

### 1. **Prefix-Based Grouping**
```yaml
# Source Configuration
source_database: "${source_database}"
source_schema: "${source_schema}"
source_table: "policy_history"
source_connection_id: "${source_connection_id}"

# Target Configuration  
target_bucket: "${target_bucket}"
target_path: "${target_path}"
target_format: "text"
target_compression: "gzip"

# Processing Configuration
processing_batch_size: "${batch_size}"
processing_timeout_minutes: "${timeout_minutes}"
processing_parallel_enabled: "${enable_parallel_processing}"
processing_max_parallel: "${max_parallel_tasks}"
```

### 2. **Feature-Based Grouping**
```yaml
# Quality Check Parameters
quality_enable_checks: "${quality_enable_checks}"
quality_min_records: "${quality_min_record_count}"
quality_max_null_percentage: "${quality_max_null_percentage}"

# Error Handling Parameters
error_continue_on_error: false
error_max_error_records: 100
error_log_errors: true
error_retry_count: 3

# Performance Parameters
performance_parallel_enabled: true
performance_max_concurrent_checks: 3
performance_timeout_minutes: 15
```

### 3. **Complex Content Preservation**
```yaml
# Flat parameters for runtime override
query_source_schema: "${source_schema}"
query_source_table: "policy_history"
query_date_column: "created_date"
query_max_age_hours: "${max_age_hours}"

# Complex query template - maintained for readability
validation_query_template: |
  SELECT 
    COUNT(*) as record_count,
    MAX({query_date_column}) as latest_date,
    MIN({query_date_column}) as earliest_date
  FROM {query_source_schema}.{query_source_table}
  WHERE {query_date_column} >= CURRENT_DATE - INTERVAL '{query_max_age_hours} hours'
```

## Runtime Parameter Replacement Examples

### 1. **Via Airflow Variables**
```bash
# Set variables for task-specific overrides
airflow variables set processing_batch_size 2000
airflow variables set quality_min_records 500
airflow variables set error_retry_count 5
airflow variables set performance_timeout_minutes 30
```

### 2. **Via DAG Run Configuration**
```json
{
  "processing_batch_size": 2000,
  "quality_enable_checks": false,
  "error_continue_on_error": true,
  "performance_parallel_enabled": false,
  "source_table": "policy_history_staging",
  "target_bucket": "staging-data-lake"
}
```

### 3. **Via Environment Variables**
```bash
export PROCESSING_BATCH_SIZE=2000
export QUALITY_MIN_RECORDS=500
export ERROR_RETRY_COUNT=5
export PERFORMANCE_TIMEOUT_MINUTES=30
```

### 4. **Task Operator Implementation**
```python
class DBToTextOperator(BaseOperator):
    def execute(self, context):
        # Load flat task properties
        task_props = self.load_task_properties('extract_policy_data.yaml')
        
        # Direct property access - no nested navigation
        batch_size = get_runtime_property('processing_batch_size', 
                                        task_props.get('processing_batch_size', 1000), 
                                        context)
        
        timeout_mins = get_runtime_property('processing_timeout_minutes',
                                          task_props.get('processing_timeout_minutes', 45),
                                          context)
        
        parallel_enabled = get_runtime_property('processing_parallel_enabled',
                                              task_props.get('processing_parallel_enabled', True),
                                              context)
        
        # Use properties directly
        self.execute_processing(batch_size, timeout_mins, parallel_enabled)
```

## Parameter Override Priority

### Override Hierarchy (Highest to Lowest):
1. **DAG Run Configuration** (UI/API)
2. **Airflow Variables**
3. **Environment Variables** 
4. **Task Properties File**
5. **Global Properties File**
6. **Default Values**

### Implementation Example:
```python
def get_runtime_property(key, task_default, context=None):
    """Get property with full override hierarchy"""
    
    # 1. DAG Run Configuration (highest priority)
    if context and 'dag_run' in context and context['dag_run'].conf:
        if key in context['dag_run'].conf:
            return context['dag_run'].conf[key]
    
    # 2. Airflow Variables
    try:
        var_value = Variable.get(key, default_var=None)
        if var_value is not None:
            return var_value
    except:
        pass
    
    # 3. Environment Variables
    env_value = os.getenv(key.upper(), None)
    if env_value is not None:
        return env_value
    
    # 4. Task Properties File (provided as task_default)
    return task_default
```

## Flat Property Naming Conventions

### 1. **Use Descriptive Prefixes**
```yaml
# Good - Clear purpose and scope
database_connection_host: "db.example.com"
processing_batch_size: 1000
quality_min_records: 100
email_smtp_host: "smtp.company.com"

# Bad - Ambiguous or too generic
host: "db.example.com"
size: 1000
min: 100
server: "smtp.company.com"
```

### 2. **Consistent Naming Patterns**
```yaml
# Pattern: {category}_{subcategory}_{property}
source_database_host: "source-db.example.com"
source_database_port: 5432
source_database_name: "policy_db"

target_storage_bucket: "data-lake-prod"
target_storage_path: "processed/policies"
target_storage_compression: "gzip"

processing_engine_type: "glue"
processing_engine_timeout: 45
processing_engine_retry_count: 3
```

### 3. **Boolean and Numeric Conventions**
```yaml
# Boolean: Use clear enable/disable, true/false
processing_parallel_enabled: true
quality_checks_enabled: "${quality_enable_checks}"
error_logging_enabled: true

# Numeric: Include units in name
timeout_minutes: 45
timeout_seconds: 300
max_size_mb: 1024
retry_count: 3
batch_size: 1000
```

## Complex Content Handling

### SQL Queries and Templates
```yaml
# Flat parameters for runtime replacement
query_source_schema: "${source_schema}"
query_date_filter: "created_date >= '{{ ds }}'"
query_status_filter: "policy_status IN ('ACTIVE', 'PENDING_RENEWAL')"

# Complex query with parameter placeholders
extraction_query_template: |
  SELECT 
    p.policy_number,
    p.company_name,
    p.created_date
  FROM {query_source_schema}.policy_history p
  WHERE {query_date_filter}
    AND {query_status_filter}
  ORDER BY p.created_date
```

### Email Templates
```yaml
# Flat styling parameters
template_header_color: "#2E86C1"
template_success_color: "#27AE60"
template_error_color: "#E74C3C"

# Complex HTML template with parameter placeholders
html_template: |
  <div style="background-color: {template_header_color};">
    <h2>Workflow Complete</h2>
  </div>
  <div class="status {template_success_color}">
    Processing completed successfully
  </div>
```

### Python Scripts
```yaml
# Flat configuration parameters
script_timeout_minutes: 10
script_memory_limit_mb: 2048
script_log_level: "INFO"

# Complex script with parameter placeholders
script_content: |
  import logging
  logging.basicConfig(level="{script_log_level}")
  
  def process_data():
      timeout = {script_timeout_minutes} * 60
      memory_limit = {script_memory_limit_mb} * 1024 * 1024
      # ... script implementation
```

## Migration from Nested to Flat

### Step 1: Identify Nested Properties
```yaml
# Before - Nested structure
source:
  database: "policy_db"
  connection:
    host: "db.example.com"
    port: 5432
processing:
  batch:
    size: 1000
    timeout: 300
  parallel:
    enabled: true
    max_workers: 5
```

### Step 2: Flatten with Descriptive Names
```yaml
# After - Flat structure
source_database: "policy_db"
source_connection_host: "db.example.com"
source_connection_port: 5432
processing_batch_size: 1000
processing_batch_timeout: 300
processing_parallel_enabled: true
processing_parallel_max_workers: 5
```

### Step 3: Update Task Operators
```python
# Before - Nested access
database = config['source']['database']
batch_size = config['processing']['batch']['size']

# After - Direct access
database = config.get('source_database')
batch_size = config.get('processing_batch_size', 1000)
```

## Best Practices

### ✅ Do:
- **Use descriptive prefixes** to group related properties
- **Include units** in parameter names (minutes, seconds, mb, count)
- **Use consistent naming patterns** across all task files
- **Preserve complex content** (queries, templates, scripts) 
- **Document parameter purposes** with comments
- **Test runtime overrides** before deployment

### ❌ Don't:
- **Use generic names** like `host`, `port`, `size`, `timeout`
- **Create overly long names** (>50 characters)
- **Mix naming conventions** within the same file
- **Nest any parameters** that need runtime override
- **Hardcode values** that should be configurable

## Summary

Flat task properties provide:

1. **Maximum Runtime Flexibility**: Every parameter can be overridden
2. **Simple Override Mechanism**: Direct 1:1 mapping with variables
3. **Clear Parameter Names**: Self-documenting configuration
4. **Complex Content Preservation**: SQL queries and templates maintained
5. **Consistent Patterns**: Predictable naming across all tasks

This approach combines the **runtime flexibility** of flat properties with the **maintainability** of keeping complex content in dedicated task files.