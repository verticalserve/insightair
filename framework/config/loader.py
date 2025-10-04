# Configuration Loader - Load configurations from various sources
"""
ConfigLoader handles loading configuration data from different file formats
and sources with support for:
- YAML and JSON configuration files
- Environment variable substitution
- File validation and error handling
- Caching for performance
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Union
import yaml

from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Configuration loader that supports multiple file formats and sources
    """
    
    def __init__(self):
        """Initialize ConfigLoader"""
        self._file_cache: Dict[str, Dict[str, Any]] = {}
        logger.debug("ConfigLoader initialized")
    
    def load_config_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration from a file
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            ConfigurationError: If file cannot be loaded or parsed
        """
        path = Path(file_path)
        
        # Check cache first
        cache_key = str(path.absolute())
        if cache_key in self._file_cache:
            logger.debug(f"Returning cached configuration for {path}")
            return self._file_cache[cache_key]
        
        # Validate file exists
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")
        
        if not path.is_file():
            raise ConfigurationError(f"Path is not a file: {path}")
        
        # Determine file type and load
        try:
            if path.suffix.lower() in ['.yml', '.yaml']:
                config = self._load_yaml_file(path)
            elif path.suffix.lower() == '.json':
                config = self._load_json_file(path)
            else:
                # Try YAML first, then JSON
                try:
                    config = self._load_yaml_file(path)
                except Exception:
                    config = self._load_json_file(path)
            
            # Cache the result
            self._file_cache[cache_key] = config
            
            logger.info(f"Successfully loaded configuration from {path}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {path}: {str(e)}")
            raise ConfigurationError(f"Failed to load {path}: {str(e)}") from e
    
    def _load_yaml_file(self, path: Path) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with path.open('r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
            
            if content is None:
                return {}
            
            if not isinstance(content, dict):
                raise ConfigurationError(f"YAML file must contain a dictionary at root level: {path}")
            
            return content
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML syntax in {path}: {str(e)}") from e
    
    def _load_json_file(self, path: Path) -> Dict[str, Any]:
        """Load JSON configuration file"""
        try:
            with path.open('r', encoding='utf-8') as f:
                content = json.load(f)
            
            if not isinstance(content, dict):
                raise ConfigurationError(f"JSON file must contain an object at root level: {path}")
            
            return content
            
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON syntax in {path}: {str(e)}") from e
    
    def load_config_string(self, config_string: str, format: str = 'yaml') -> Dict[str, Any]:
        """
        Load configuration from a string
        
        Args:
            config_string: Configuration content as string
            format: Format of the string ('yaml' or 'json')
            
        Returns:
            Configuration dictionary
        """
        try:
            if format.lower() == 'yaml':
                content = yaml.safe_load(config_string)
            elif format.lower() == 'json':
                content = json.loads(config_string)
            else:
                raise ConfigurationError(f"Unsupported format: {format}")
            
            if content is None:
                return {}
            
            if not isinstance(content, dict):
                raise ConfigurationError(f"Configuration must be a dictionary")
            
            return content
            
        except Exception as e:
            raise ConfigurationError(f"Failed to parse {format} string: {str(e)}") from e
    
    def reload_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Reload configuration file, bypassing cache
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        path = Path(file_path)
        cache_key = str(path.absolute())
        
        # Remove from cache if present
        if cache_key in self._file_cache:
            del self._file_cache[cache_key]
        
        # Load fresh copy
        return self.load_config_file(path)
    
    def clear_cache(self):
        """Clear all cached configurations"""
        self._file_cache.clear()
        logger.info("Configuration file cache cleared")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached configurations"""
        return {
            'cached_files': list(self._file_cache.keys()),
            'cache_size': len(self._file_cache)
        }
    
    def validate_config_structure(self, config: Dict[str, Any]) -> bool:
        """
        Validate basic configuration structure
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            True if structure is valid
            
        Raises:
            ConfigurationError: If structure is invalid
        """
        # Check for required top-level keys
        if 'tasks' not in config:
            raise ConfigurationError("Configuration must contain 'tasks' section")
        
        if not isinstance(config['tasks'], list):
            raise ConfigurationError("'tasks' must be a list")
        
        # Validate each task has required fields
        for i, task in enumerate(config['tasks']):
            if not isinstance(task, dict):
                raise ConfigurationError(f"Task {i} must be a dictionary")
            
            if 'name' not in task:
                raise ConfigurationError(f"Task {i} must have a 'name' field")
            
            if 'type' not in task:
                raise ConfigurationError(f"Task {i} ({task.get('name', 'unnamed')}) must have a 'type' field")
        
        logger.debug("Configuration structure validation passed")
        return True