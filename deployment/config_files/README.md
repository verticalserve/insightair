# InsightAir Configuration Files

## Overview
This directory contains centralized configuration files for the InsightAir framework deployment.

## File Structure
```
config_files/
├── environment.yaml          # Main environment configuration
├── cluster_config.yaml       # Cluster-specific settings
├── connections.yaml          # Airflow connection definitions
└── README.md                 # This documentation
```

## Environment Configuration

### Environment Variables
The `environment.yaml` file supports environment variable substitution using `${VAR_NAME:-default}` syntax:

- `AIRFLOW_ENV`: Environment name (development/staging/production)
- `CLOUD_PROVIDER`: Cloud provider (aws/gcp/azure)
- `CLOUD_REGION`: Cloud region
- `*_BUCKET`: Storage bucket names
- `*_DB_HOST`: Database hostnames
- `LOG_LEVEL`: Logging level
- `SECRET_BACKEND`: Secret management backend

### Accessing Configuration in Code

```python
from workflow_framework.config import Config

# Load configuration
config = Config('workflow_name', config_path)
config.load_configs()

# Access nested values
raw_bucket = config.get_env()['storage']['raw_bucket']
db_host = config.get_data_group()['database_host']
timeout = config.get_env()['engines']['glue']['timeout_minutes']

# Safe access with defaults
max_workers = config.get_env().get('engines', {}).get('ray', {}).get('max_workers', 2)
```

### Data Groups
Data groups provide environment-specific configurations:
- `development`: Development environment settings
- `staging`: Staging environment settings  
- `production`: Production environment settings

### Best Practices

1. **Environment Variables**: Use environment variables for sensitive data and environment-specific values
2. **Defaults**: Always provide sensible defaults using `${VAR:-default}` syntax
3. **Validation**: Validate configuration on startup
4. **Documentation**: Document all configuration options
5. **Versioning**: Version your configuration files

### Security Considerations

- Never commit secrets directly to configuration files
- Use environment variables or secret management systems
- Encrypt sensitive configuration data
- Implement proper access controls

### Deployment

1. **Development**: Place in `/home/ec2-user/airflow/config_files/`
2. **Production**: Place in `/usr/local/airflow/config_files/`
3. **Kubernetes**: Mount as ConfigMap
4. **Docker**: Mount as volume

Example deployment:
```bash
# Copy configuration files
sudo mkdir -p /usr/local/airflow/config_files
sudo cp environment.yaml /usr/local/airflow/config_files/
sudo chown -R airflow:airflow /usr/local/airflow/config_files
```