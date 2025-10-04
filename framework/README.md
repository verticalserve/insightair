# Framework Architecture

## Overview
The Framework provides a comprehensive configuration-driven approach to building Airflow DAGs with support for task groups, runtime parameter overrides, and complex workflow patterns.

## Folder Structure

```
framework/
├── README.md                    # This documentation
├── __init__.py                  # Framework initialization
│
├── config/                      # Configuration Management
│   ├── __init__.py
│   ├── manager.py              # Configuration manager
│   ├── loader.py               # Config file loaders
│   ├── merger.py               # Config merging logic
│   ├── resolver.py             # Variable resolution and substitution
│   └── schema/                 # Configuration schemas
│       ├── __init__.py
│       ├── dag_schema.py       # DAG configuration schema
│       ├── task_schema.py      # Task configuration schema
│       └── group_schema.py     # Task group schema
│
├── parsers/                     # Configuration Parsers
│   ├── __init__.py
│   ├── base_parser.py          # Base parser interface
│   ├── yaml_parser.py          # YAML configuration parser
│   ├── json_parser.py          # JSON configuration parser
│   ├── env_parser.py           # Environment variable parser
│   ├── variable_parser.py      # Airflow variable parser
│   └── runtime_parser.py       # Runtime parameter parser
│
├── operators/                   # Custom Operators
│   ├── __init__.py
│   ├── base_operator.py        # Base InsightAir operator
│   ├── database/               # Database operators
│   │   ├── __init__.py
│   │   ├── db_operator.py      # Generic database operations
│   │   ├── db_to_text.py       # Database to text conversion
│   │   └── db_validator.py     # Database validation
│   ├── processing/             # Processing operators
│   │   ├── __init__.py
│   │   ├── glue_operator.py    # AWS Glue operations
│   │   ├── emr_operator.py     # EMR operations
│   │   └── ray_operator.py     # Ray processing
│   ├── quality/                # Data quality operators
│   │   ├── __init__.py
│   │   ├── dq_operator.py      # Data quality checks
│   │   └── validator.py        # Data validation
│   ├── notification/           # Notification operators
│   │   ├── __init__.py
│   │   ├── email_operator.py   # Enhanced email operator
│   │   ├── slack_operator.py   # Slack notifications
│   │   └── teams_operator.py   # Microsoft Teams
│   └── storage/                # Storage operators
│       ├── __init__.py
│       ├── s3_operator.py      # S3 operations
│       ├── gcs_operator.py     # Google Cloud Storage
│       └── archive_operator.py # Archival operations
│
├── builders/                    # Task and DAG Builders
│   ├── __init__.py
│   ├── dag_builder.py          # DAG construction from config
│   ├── task_builder.py         # Task creation and configuration
│   ├── group_builder.py        # Task group construction
│   ├── operator_factory.py     # Operator factory pattern
│   └── dependency_builder.py   # Dependency management
│
├── core/                        # Framework Core Components
│   ├── __init__.py
│   ├── framework.py            # Main framework class
│   ├── context.py              # Workflow context management
│   ├── registry.py             # Operator registry
│   ├── lifecycle.py            # DAG lifecycle management
│   └── exceptions.py           # Custom exceptions
│
├── utils/                       # Utility Functions
│   ├── __init__.py
│   ├── string_utils.py         # String manipulation utilities
│   ├── date_utils.py           # Date/time utilities
│   ├── file_utils.py           # File system utilities
│   ├── logging_utils.py        # Logging configuration
│   └── runtime_utils.py        # Runtime parameter utilities
│
├── validators/                  # Configuration Validators
│   ├── __init__.py
│   ├── config_validator.py     # Configuration validation
│   ├── schema_validator.py     # Schema validation
│   ├── dependency_validator.py # Task dependency validation
│   └── runtime_validator.py    # Runtime parameter validation
│
├── templates/                   # Template Management
│   ├── __init__.py
│   ├── template_engine.py      # Template processing engine
│   ├── jinja_engine.py         # Jinja2 template engine
│   └── builtin_templates/      # Built-in templates
│       ├── email_templates.py  # Email templates
│       ├── sql_templates.py    # SQL query templates
│       └── script_templates.py # Script templates
│
└── examples/                    # Example Implementations
    ├── __init__.py
    ├── simple_dag/             # Simple DAG example
    ├── complex_workflow/       # Complex workflow with groups
    ├── data_pipeline/          # Data processing pipeline
    └── ml_pipeline/            # Machine learning pipeline
```

## Component Responsibilities

### 1. **Config Management (`config/`)**
- **manager.py**: Central configuration management
- **loader.py**: Load configurations from various sources
- **merger.py**: Merge configurations from multiple files
- **resolver.py**: Resolve variables and substitutions
- **schema/**: Define and validate configuration schemas

### 2. **Parsers (`parsers/`)**
- **yaml_parser.py**: Parse YAML configuration files
- **env_parser.py**: Parse environment variables
- **variable_parser.py**: Parse Airflow variables
- **runtime_parser.py**: Parse runtime parameters from DAG runs

### 3. **Operators (`operators/`)**
- **Custom operators** organized by functionality
- **Built-in integrations** with AWS, GCP, Azure services
- **Enhanced functionality** beyond standard Airflow operators
- **Configuration-driven** operator behavior

### 4. **Builders (`builders/`)**
- **dag_builder.py**: Construct complete DAGs from configuration
- **task_builder.py**: Create individual tasks
- **group_builder.py**: Build task groups and nested structures
- **operator_factory.py**: Factory pattern for operator creation

### 5. **Core (`core/`)**
- **framework.py**: Main framework orchestration
- **context.py**: Manage workflow execution context
- **registry.py**: Register and manage available operators
- **lifecycle.py**: Handle DAG lifecycle events

### 6. **Utilities (`utils/`)**
- **Common utilities** used across the framework
- **Runtime parameter** handling
- **Logging and monitoring** support
- **File and string** manipulation

### 7. **Validators (`validators/`)**
- **Configuration validation** before DAG creation
- **Schema compliance** checking
- **Dependency validation** for task chains
- **Runtime parameter** validation

### 8. **Templates (`templates/`)**
- **Template processing** for SQL, scripts, emails
- **Built-in templates** for common patterns
- **Custom template** support

## Architecture Principles

### 1. **Separation of Concerns**
- Configuration parsing separate from execution
- Operator logic separate from DAG construction
- Validation separate from runtime execution

### 2. **Extensibility**
- Plugin-based operator registration
- Custom parser support
- Template engine extensibility

### 3. **Configuration-Driven**
- All behavior controlled via configuration
- Runtime parameter override support
- Environment-specific configurations

### 4. **Type Safety**
- Strong typing with Python type hints
- Schema validation for configurations
- Runtime type checking

### 5. **Error Handling**
- Comprehensive error reporting
- Graceful degradation
- Detailed logging and diagnostics

## Usage Patterns

### 1. **Simple DAG Creation**
```python
from framework import InsightAirFramework

framework = InsightAirFramework()
dag = framework.build_dag_from_config('config.yaml')
```

### 2. **Custom Operator Registration**
```python
from framework.core import registry

@registry.register_operator('CUSTOM_TYPE')
class CustomOperator(BaseOperator):
    # Implementation
```

### 3. **Runtime Parameter Override**
```python
framework = InsightAirFramework()
dag = framework.build_dag_from_config(
    'config.yaml',
    runtime_params={'batch_size': 2000}
)
```

This architecture provides a **scalable, maintainable, and extensible** foundation for configuration-driven Airflow DAG creation.