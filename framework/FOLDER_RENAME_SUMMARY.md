# Folder Rename Summary

## Change Made
Renamed `insightair_framework/` folder to `framework/` for cleaner namespace.

## Files Updated

### 1. Core Framework Files
- `framework/__init__.py` - Updated package documentation
- `framework/README.md` - Updated folder structure references and import examples
- `framework/examples/README.md` - Updated all import references and framework name
- `framework/examples/simple_dag/simple_dag.py` - Updated import statement

### 2. Import Statement Changes

**Before:**
```python
from insightair_framework import create_dag_from_config
from insightair_framework import InsightAirFramework
from insightair_framework.core import registry
```

**After:**
```python
from framework import create_dag_from_config
from framework import InsightAirFramework
from framework.core import registry
```

## Updated Folder Structure

```
framework/
├── README.md                    # Framework documentation
├── __init__.py                  # Framework entry point
│
├── config/                      # Configuration Management
├── parsers/                     # Configuration Parsers
├── operators/                   # Custom Operators
├── builders/                    # DAG Construction
├── core/                        # Framework Core
├── utils/                       # Utilities
├── validators/                  # Configuration validation
├── templates/                   # Template processing
└── examples/                    # Example implementations
    └── simple_dag/             # Complete working example
```

## Usage Examples (Updated)

### Simple Usage
```python
from framework import create_dag_from_config

dag = create_dag_from_config(
    config_file='config.yaml',
    properties_file='properties.yaml'
)
```

### Advanced Usage
```python
from framework import InsightAirFramework

framework = InsightAirFramework()
dag = framework.build_dag_from_config('config.yaml', 'properties.yaml')
```

### Custom Operator Registration
```python
from framework import InsightAirFramework

framework = InsightAirFramework()

@framework.register_operator_decorator('CUSTOM_TASK')
class CustomOperator(BaseOperator):
    def execute(self, context):
        # Implementation
        pass
```

## Impact
- **Cleaner imports**: `from framework` instead of `from insightair_framework`
- **Shorter namespace**: Easier to type and remember
- **No functional changes**: All functionality remains identical
- **Backward compatibility**: Old imports will break - this is a breaking change

## Next Steps
All framework functionality remains the same. Users just need to update their import statements from `insightair_framework` to `framework`.