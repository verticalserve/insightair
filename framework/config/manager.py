# Configuration Manager - Central configuration management
"""
ConfigManager provides centralized configuration management with support for:
- Multiple configuration sources (files, environment, runtime)
- Variable resolution and substitution
- Configuration merging and inheritance
- Schema validation and type checking
"""

import os
import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path

from .loader import ConfigLoader
from .merger import ConfigMerger
from .resolver import VariableResolver
from ..validators.config_validator import ConfigValidator
from ..core.exceptions import ConfigurationError, ValidationError

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Central configuration management class that orchestrates:
    - Configuration loading from multiple sources
    - Variable resolution and substitution  
    - Configuration validation
    - Runtime parameter override support
    """
    
    def __init__(self, 
                 config_dir: Optional[Union[str, Path]] = None,
                 enable_validation: bool = True,
                 enable_runtime_override: bool = True):
        """
        Initialize ConfigManager
        
        Args:
            config_dir: Base directory for configuration files
            enable_validation: Whether to validate configurations
            enable_runtime_override: Whether to support runtime overrides
        """
        self.config_dir = Path(config_dir) if config_dir else Path.cwd()
        self.enable_validation = enable_validation
        self.enable_runtime_override = enable_runtime_override
        
        # Initialize components
        self.loader = ConfigLoader()
        self.merger = ConfigMerger()
        self.resolver = VariableResolver()
        self.validator = ConfigValidator() if enable_validation else None
        
        # Configuration cache
        self._config_cache: Dict[str, Dict[str, Any]] = {}
        self._resolved_cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"ConfigManager initialized with config_dir: {self.config_dir}")
    
    def load_configuration(self, 
                          config_file: Union[str, Path],
                          properties_file: Optional[Union[str, Path]] = None,
                          runtime_params: Optional[Dict[str, Any]] = None,
                          dag_run_conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Load and process complete configuration from multiple sources
        
        Args:
            config_file: Main configuration file (DAG structure)
            properties_file: Properties file (flat parameters)
            runtime_params: Runtime parameters from various sources
            dag_run_conf: DAG run configuration from Airflow
            
        Returns:
            Complete processed configuration dictionary
        """
        try:
            # Create cache key
            cache_key = self._create_cache_key(config_file, properties_file, runtime_params)
            
            # Check cache first
            if cache_key in self._resolved_cache:
                logger.debug(f"Returning cached configuration for {cache_key}")
                return self._resolved_cache[cache_key]
            
            # Load base configurations
            config = self._load_base_configurations(config_file, properties_file)
            
            # Apply runtime overrides
            if self.enable_runtime_override:
                config = self._apply_runtime_overrides(config, runtime_params, dag_run_conf)
            
            # Resolve variables and substitutions
            config = self.resolver.resolve_variables(config)
            
            # Validate configuration
            if self.validator:
                self.validator.validate_configuration(config)
            
            # Cache resolved configuration
            self._resolved_cache[cache_key] = config
            
            logger.info(f"Configuration loaded successfully for {config_file}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_file}: {str(e)}")
            raise ConfigurationError(f"Configuration loading failed: {str(e)}") from e
    
    def _load_base_configurations(self, 
                                 config_file: Union[str, Path],
                                 properties_file: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
        """Load base configurations from files"""
        
        # Resolve file paths
        config_path = self._resolve_config_path(config_file)
        properties_path = self._resolve_config_path(properties_file) if properties_file else None
        
        # Load main configuration
        main_config = self.loader.load_config_file(config_path)
        
        # Load properties if specified
        if properties_path and properties_path.exists():
            properties = self.loader.load_config_file(properties_path)
            # Merge properties into main config
            main_config = self.merger.merge_configurations(main_config, {'properties': properties})
        elif 'properties_file' in main_config:
            # Load properties file referenced in main config
            prop_path = self._resolve_config_path(main_config['properties_file'])
            if prop_path.exists():
                properties = self.loader.load_config_file(prop_path)
                main_config = self.merger.merge_configurations(main_config, {'properties': properties})
        
        return main_config
    
    def _apply_runtime_overrides(self,
                                config: Dict[str, Any],
                                runtime_params: Optional[Dict[str, Any]] = None,
                                dag_run_conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Apply runtime parameter overrides with priority order"""
        
        if not (runtime_params or dag_run_conf):
            return config
        
        # Priority order (highest to lowest):
        # 1. DAG run configuration
        # 2. Runtime parameters
        # 3. Airflow variables
        # 4. Environment variables
        # 5. Existing configuration
        
        override_sources = []
        
        # Add environment variables
        env_overrides = self._get_environment_overrides()
        if env_overrides:
            override_sources.append(('environment', env_overrides))
        
        # Add Airflow variables
        airflow_overrides = self._get_airflow_variable_overrides()
        if airflow_overrides:
            override_sources.append(('airflow_variables', airflow_overrides))
        
        # Add runtime parameters
        if runtime_params:
            override_sources.append(('runtime_params', runtime_params))
        
        # Add DAG run configuration (highest priority)
        if dag_run_conf:
            override_sources.append(('dag_run_conf', dag_run_conf))
        
        # Apply overrides in order
        for source_name, overrides in override_sources:
            logger.debug(f"Applying {len(overrides)} overrides from {source_name}")
            config = self._apply_overrides_to_config(config, overrides)
        
        return config
    
    def _apply_overrides_to_config(self, 
                                  config: Dict[str, Any], 
                                  overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Apply overrides to configuration with flat property support"""
        
        # Apply to global properties if they exist
        if 'properties' in config and isinstance(config['properties'], dict):
            for key, value in overrides.items():
                if key in config['properties']:
                    logger.debug(f"Overriding properties.{key}: {config['properties'][key]} -> {value}")
                    config['properties'][key] = value
        
        # Apply to top-level configuration
        for key, value in overrides.items():
            if key in config:
                logger.debug(f"Overriding {key}: {config[key]} -> {value}")
                config[key] = value
        
        return config
    
    def _get_environment_overrides(self) -> Dict[str, Any]:
        """Get configuration overrides from environment variables"""
        overrides = {}
        
        # Look for environment variables that match configuration keys
        for key, value in os.environ.items():
            # Convert UPPER_CASE env vars to lower_case config keys
            config_key = key.lower()
            
            # Try to convert to appropriate type
            converted_value = self._convert_env_value(value)
            overrides[config_key] = converted_value
        
        return overrides
    
    def _get_airflow_variable_overrides(self) -> Dict[str, Any]:
        """Get configuration overrides from Airflow Variables"""
        overrides = {}
        
        try:
            from airflow.models import Variable
            
            # Get all variables (this is expensive, consider caching)
            # In practice, you might want to look for specific prefixes
            # or maintain a list of configuration keys to check
            
            # For now, return empty dict - implement based on your needs
            return overrides
            
        except ImportError:
            logger.warning("Airflow not available, skipping variable overrides")
            return overrides
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type"""
        # Handle boolean values
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Handle numeric values
        if value.isdigit():
            return int(value)
        
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value
    
    def _resolve_config_path(self, config_file: Union[str, Path]) -> Path:
        """Resolve configuration file path"""
        path = Path(config_file)
        
        if path.is_absolute():
            return path
        else:
            return self.config_dir / path
    
    def _create_cache_key(self, 
                         config_file: Union[str, Path],
                         properties_file: Optional[Union[str, Path]] = None,
                         runtime_params: Optional[Dict[str, Any]] = None) -> str:
        """Create cache key for configuration"""
        key_parts = [str(config_file)]
        
        if properties_file:
            key_parts.append(str(properties_file))
        
        if runtime_params:
            # Sort keys for consistent cache keys
            params_str = str(sorted(runtime_params.items()))
            key_parts.append(params_str)
        
        return "|".join(key_parts)
    
    def clear_cache(self):
        """Clear configuration cache"""
        self._config_cache.clear()
        self._resolved_cache.clear()
        logger.info("Configuration cache cleared")
    
    def get_task_properties(self, 
                           task_name: str,
                           properties_file: Union[str, Path],
                           runtime_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Load task-specific properties with runtime override support
        
        Args:
            task_name: Name of the task
            properties_file: Task properties file
            runtime_params: Runtime parameters for override
            
        Returns:
            Task properties dictionary with overrides applied
        """
        try:
            # Load task properties
            properties_path = self._resolve_config_path(properties_file)
            task_props = self.loader.load_config_file(properties_path)
            
            # Apply runtime overrides
            if runtime_params and self.enable_runtime_override:
                task_props = self._apply_overrides_to_config(task_props, runtime_params)
            
            # Resolve variables
            task_props = self.resolver.resolve_variables(task_props)
            
            logger.debug(f"Loaded properties for task {task_name} from {properties_file}")
            return task_props
            
        except Exception as e:
            logger.error(f"Failed to load task properties for {task_name}: {str(e)}")
            raise ConfigurationError(f"Task properties loading failed: {str(e)}") from e