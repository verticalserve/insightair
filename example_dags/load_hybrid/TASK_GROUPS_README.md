# Airflow Task Groups Support in InsightAir Framework

## Overview
This document explains how to use Airflow Task Groups in the InsightAir hybrid configuration approach for better workflow organization and UI visualization.

## Task Group Benefits

### 1. **Visual Organization**
- Groups related tasks together in Airflow UI
- Collapsible/expandable groups for better readability
- Hierarchical view of complex workflows

### 2. **Logical Separation**
- Separate concerns (validation, processing, quality, notifications)
- Easier debugging and monitoring
- Clear data flow visualization

### 3. **Parallel Execution**
- Multiple task groups can run in parallel
- Better resource utilization
- Reduced overall workflow runtime

### 4. **Nested Organization**
- Support for nested task groups (subgroups)
- Multi-level workflow organization
- Complex pipeline management

## Configuration Structure

### Basic Task Group Definition
```yaml
tasks:
  - name: "data_validation_group"
    type: "TASK_GROUP"
    description: "Data validation and preparation tasks"
    tooltip: "Validates source data and prepares for processing" 
    prefix_group_id: true
    tasks:
      - name: "validate_source"
        type: "DB"
        properties_file: "validate_source.yaml"
      - name: "check_dependencies"
        type: "SCRIPT"
        properties_file: "check_dependencies.yaml"
        parents: ["validate_source"]
    parents: ["start"]
```

### Nested Task Groups
```yaml
tasks:
  - name: "data_processing_group"
    type: "TASK_GROUP"
    description: "Main data processing pipeline"
    tasks:
      # Nested subgroup
      - name: "extraction_subgroup"
        type: "TASK_GROUP"
        description: "Data extraction operations"
        tasks:
          - name: "extract_policy_data"
            type: "DB_TO_TEXT"
            properties_file: "extract_policy_data.yaml"
          - name: "extract_metadata"
            type: "DB"
            properties_file: "extract_metadata.yaml"
```

## Task Group Configuration Options

### Standard Properties
- **name**: Unique identifier for the task group
- **type**: Must be "TASK_GROUP"
- **description**: Human-readable description
- **tooltip**: UI tooltip text
- **prefix_group_id**: Whether to prefix task IDs with group name
- **ui_color**: Background color in UI
- **ui_fgcolor**: Foreground color in UI
- **tasks**: Array of tasks within the group
- **parents**: Dependencies on other tasks/groups

### Advanced Properties
```yaml
task_group_defaults:
  prefix_group_id: true
  tooltip: ""
  ui_color: "#f0f0f0"
  ui_fgcolor: "#000000"
```

## Task Group Patterns

### 1. **Sequential Processing Groups**
```yaml
# Group 1 → Group 2 → Group 3
- name: "validation_group"
  type: "TASK_GROUP"
  parents: ["start"]
  
- name: "processing_group" 
  type: "TASK_GROUP"
  parents: ["validation_group"]
  
- name: "notification_group"
  type: "TASK_GROUP"
  parents: ["processing_group"]
```

### 2. **Parallel Processing Groups**
```yaml
# Multiple groups running in parallel
- name: "quality_group"
  type: "TASK_GROUP"
  parents: ["processing_group"]
  
- name: "archive_group"
  type: "TASK_GROUP"
  parents: ["processing_group"]  # Same parent = parallel execution
  
- name: "notification_group"
  type: "TASK_GROUP"
  parents: ["quality_group", "archive_group"]  # Wait for both
```

### 3. **Nested Groups (Multi-level)**
```yaml
- name: "main_processing_group"
  type: "TASK_GROUP"
  tasks:
    - name: "extraction_subgroup"
      type: "TASK_GROUP"
      tasks:
        - name: "extract_data"
          type: "DB_TO_TEXT"
        - name: "extract_metadata"
          type: "DB"
    
    - name: "transformation_subgroup"
      type: "TASK_GROUP"
      tasks:
        - name: "apply_rules"
          type: "SCRIPT"
      parents: ["extraction_subgroup"]
```

### 4. **Conditional Groups**
```yaml
# Groups that execute based on conditions
- name: "emergency_processing_group"
  type: "CONDITIONAL_TASK_GROUP"
  condition: "{{ dag_run.conf.get('emergency_mode', False) }}"
  tasks:
    - name: "priority_extraction"
      type: "DB_TO_TEXT"
```

### 5. **Dynamic Groups**
```yaml
# Groups created at runtime
task_group_patterns:
  dynamic_groups:
    - name: "process_regions"
      type: "DYNAMIC_TASK_GROUP"
      generator_script: "generate_region_tasks.py"
      task_template:
        type: "DB_TO_TEXT"
        properties_template: "process_region_template.yaml"
```

## Implementation in DAG Code

### Task Group Creation Function
```python
def create_task_group(group_config, parent_dag=None):
    """Create a TaskGroup from configuration"""
    group_name = group_config['name']
    description = group_config.get('description', f'Task group: {group_name}')
    tooltip = group_config.get('tooltip', description)
    prefix_group_id = group_config.get('prefix_group_id', True)
    
    task_group = TaskGroup(
        group_id=group_name,
        tooltip=tooltip,
        prefix_group_id=prefix_group_id,
        ui_color=group_config.get('ui_color', '#f0f0f0'),
        ui_fgcolor=group_config.get('ui_fgcolor', '#000000'),
        dag=dag
    )
    
    return task_group
```

### Task Creation Within Groups
```python
def create_tasks_in_group(tasks_config, task_group):
    """Create tasks within a task group"""
    group_tasks = {}
    
    for task_config in tasks_config:
        task_name = task_config['name']
        task_type = task_config.get('type', 'DUMMY')
        
        if task_type == 'TASK_GROUP':
            # Handle nested groups
            nested_group = create_task_group(task_config, dag)
            nested_tasks = create_tasks_in_group(task_config.get('tasks', []), nested_group)
            group_tasks[task_name] = nested_group
            group_tasks.update(nested_tasks)
        else:
            # Create regular task within the group
            with task_group:
                task = framework_instance.build_task(task_type, task_name)
                group_tasks[task_name] = task
    
    return group_tasks
```

## Best Practices

### ✅ Do:
- **Group Related Tasks**: Keep logically related tasks together
- **Use Descriptive Names**: Clear group names and descriptions
- **Parallel When Possible**: Use parallel groups for independent operations
- **Limit Nesting Depth**: Maximum 2-3 levels of nesting
- **Consistent Naming**: Use consistent naming conventions
- **Document Groups**: Add comprehensive documentation to groups

### ❌ Don't:
- **Over-group**: Don't create groups for single tasks
- **Deep Nesting**: Avoid more than 3 levels of nesting
- **Cross-group Dependencies**: Minimize dependencies between tasks in different groups
- **Large Groups**: Keep groups focused (5-10 tasks max)
- **Inconsistent Patterns**: Use consistent grouping patterns

## Runtime Control

### Enable/Disable Task Groups
```python
# In DAG parameters
params={
    'enable_task_groups': True,  # Can be overridden at runtime
    'emergency_mode': False,     # Activates emergency processing group
    'regions': ['US', 'EU'],     # For dynamic region groups
}
```

### Override via DAG Run Configuration
```json
{
  "enable_task_groups": false,
  "emergency_mode": true,
  "regions": ["US", "EU", "APAC"],
  "group_parallelism": 3
}
```

## UI Benefits

### Before Task Groups
```
start → validate_source → check_deps → validate_schema → extract_data → 
transform_data → quality_check → compliance_check → archive → notify → end
```
*Linear view with 10+ tasks in a row*

### After Task Groups
```
start → [Validation Group] → [Processing Group] → [QA Group] → [Management Group] → end
```
*Organized view with collapsible groups*

### Expanded View
```
start → 
[Validation Group]
  ├── validate_source
  ├── check_dependencies  
  └── validate_schema
→ [Processing Group]
  ├── [Extraction Subgroup]
  │   ├── extract_policy_data
  │   └── extract_metadata
  └── [Transformation Subgroup]
      ├── apply_business_rules
      └── format_output
→ end
```

## File Organization

### Task Group Files
```
load_hybrid/
├── config_with_groups.yaml          # Main config with task groups
├── dag_with_task_groups.py          # DAG implementation
├── properties.yaml                  # Global properties
├── validate_source.yaml             # Validation group task
├── check_dependencies.yaml          # Validation group task  
├── validate_schema.yaml             # Validation group task
├── validation_summary.yaml          # Validation group task
├── extract_policy_data.yaml         # Processing group task
└── TASK_GROUPS_README.md            # This documentation
```

## Migration Strategy

### From Flat Task Structure
1. **Identify Related Tasks**: Group tasks by function (validation, processing, etc.)
2. **Update Configuration**: Wrap related tasks in task group definitions
3. **Test Dependencies**: Ensure task dependencies work correctly with groups
4. **Update Documentation**: Document the new group structure

### Example Migration
```yaml
# Before (flat structure)
tasks:
  - name: "validate_source"
    type: "DB"
  - name: "check_dependencies"
    type: "SCRIPT"
    parents: ["validate_source"]

# After (with groups)
tasks:
  - name: "validation_group"
    type: "TASK_GROUP"
    tasks:
      - name: "validate_source"
        type: "DB"
      - name: "check_dependencies"
        type: "SCRIPT"
        parents: ["validate_source"]
```

Task Groups provide powerful workflow organization capabilities while maintaining the flexibility and runtime override support of the hybrid configuration approach.