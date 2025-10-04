# Variable Resolver - Handle variable substitution and resolution
"""
VariableResolver handles variable substitution and resolution in configurations
with support for:
- Environment variable substitution (${VAR_NAME:-default})
- Airflow variable resolution
- Cross-reference resolution within configuration
- Template processing and string interpolation
"""

import os
import re
import logging
from typing import Dict, Any, Union, Optional, List
from string import Template

from ..core.exceptions import ConfigurationError, VariableResolutionError

logger = logging.getLogger(__name__)


class VariableResolver:
    """
    Variable resolver that handles substitution and resolution of variables
    in configuration dictionaries with support for multiple variable sources
    """
    
    # Pattern for ${VAR_NAME:-default_value} syntax
    ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')
    
    # Pattern for {variable_name} syntax (template substitution)
    TEMPLATE_PATTERN = re.compile(r'\{([^}]+)\}')
    
    def __init__(self):
        """Initialize VariableResolver"""
        self._resolved_cache: Dict[str, Any] = {}
        logger.debug("VariableResolver initialized")
    
    def resolve_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve all variables in configuration dictionary
        
        Args:
            config: Configuration dictionary with variables to resolve
            
        Returns:
            Configuration dictionary with variables resolved
        """
        try:
            # Clear cache for fresh resolution
            self._resolved_cache.clear()
            
            # Deep copy and resolve
            resolved_config = self._deep_resolve(config)
            
            logger.info("Variable resolution completed successfully")
            return resolved_config
            
        except Exception as e:
            logger.error(f"Variable resolution failed: {str(e)}")
            raise VariableResolutionError(f"Failed to resolve variables: {str(e)}") from e
    
    def _deep_resolve(self, obj: Any, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        Recursively resolve variables in nested structures
        
        Args:
            obj: Object to resolve (dict, list, string, or other)
            context: Context dictionary for variable resolution
            
        Returns:
            Object with variables resolved
        """
        if context is None:
            context = {}
        
        if isinstance(obj, dict):
            return {key: self._deep_resolve(value, context) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_resolve(item, context) for item in obj]
        elif isinstance(obj, str):
            return self._resolve_string(obj, context)
        else:
            return obj
    
    def _resolve_string(self, text: str, context: Dict[str, Any]) -> str:
        """
        Resolve variables in a string
        
        Args:
            text: String with variables to resolve
            context: Context dictionary for resolution
            
        Returns:
            String with variables resolved
        """
        if not isinstance(text, str):
            return text
        
        # First pass: resolve environment variables (${VAR:-default})
        resolved_text = self._resolve_environment_variables(text)
        
        # Second pass: resolve Airflow variables
        resolved_text = self._resolve_airflow_variables(resolved_text)
        
        # Third pass: resolve template variables ({var})
        resolved_text = self._resolve_template_variables(resolved_text, context)
        
        return resolved_text
    
    def _resolve_environment_variables(self, text: str) -> str:
        """
        Resolve environment variables in ${VAR_NAME:-default} format
        
        Args:
            text: String containing environment variables
            
        Returns:
            String with environment variables resolved
        """
        def replace_env_var(match):
            var_expr = match.group(1)
            
            # Handle default value syntax (VAR_NAME:-default)
            if ':-' in var_expr:
                var_name, default_value = var_expr.split(':-', 1)
                var_name = var_name.strip()
                default_value = default_value.strip()
            else:
                var_name = var_expr.strip()
                default_value = ''
            
            # Get environment variable value
            env_value = os.getenv(var_name, default_value)
            
            logger.debug(f"Resolved environment variable {var_name} = {env_value}")
            return env_value
        
        return self.ENV_VAR_PATTERN.sub(replace_env_var, text)
    
    def _resolve_airflow_variables(self, text: str) -> str:
        """
        Resolve Airflow variables
        
        Args:
            text: String containing Airflow variable references
            
        Returns:
            String with Airflow variables resolved
        """
        # Pattern for Airflow variable syntax: {{Variable.get('var_name', 'default')}}
        airflow_var_pattern = re.compile(r"{{Variable\.get\('([^']+)'(?:,\s*'([^']*)')?\)}}")
        
        def replace_airflow_var(match):
            var_name = match.group(1)
            default_value = match.group(2) or ''
            
            try:
                from airflow.models import Variable
                value = Variable.get(var_name, default_var=default_value)
                logger.debug(f"Resolved Airflow variable {var_name} = {value}")
                return value
            except ImportError:
                logger.warning(f"Airflow not available, using default for {var_name}")
                return default_value
            except Exception as e:
                logger.warning(f"Failed to resolve Airflow variable {var_name}: {e}")
                return default_value
        
        return airflow_var_pattern.sub(replace_airflow_var, text)
    
    def _resolve_template_variables(self, text: str, context: Dict[str, Any]) -> str:
        """
        Resolve template variables in {variable_name} format
        
        Args:
            text: String containing template variables
            context: Context dictionary for variable lookup
            
        Returns:
            String with template variables resolved
        """
        if not self.TEMPLATE_PATTERN.search(text):
            return text
        
        try:
            # Use Python's Template class for safe substitution
            template = Template(text)
            
            # Create substitution context
            substitution_context = self._build_substitution_context(context)
            
            # Perform substitution
            resolved_text = template.safe_substitute(substitution_context)
            
            logger.debug(f"Template substitution: {text} -> {resolved_text}")
            return resolved_text
            
        except Exception as e:
            logger.warning(f"Template substitution failed for '{text}': {e}")
            return text
    
    def _build_substitution_context(self, context: Dict[str, Any]) -> Dict[str, str]:
        """
        Build substitution context from configuration
        
        Args:
            context: Configuration context
            
        Returns:
            Dictionary suitable for template substitution
        """
        substitution_context = {}
        
        # Add context variables
        for key, value in context.items():
            if isinstance(value, (str, int, float, bool)):
                substitution_context[key] = str(value)
        
        # Add common Airflow template variables
        substitution_context.update({
            'ds': '{{ ds }}',  # Execution date as YYYY-MM-DD
            'ts': '{{ ts }}',  # Execution timestamp
            'dag_run_start_date': '{{ dag_run.start_date }}',
            'execution_date': '{{ ds }}'
        })
        
        return substitution_context
    
    def resolve_task_properties(self, 
                               properties: Dict[str, Any],
                               global_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Resolve variables in task properties with global context
        
        Args:
            properties: Task properties dictionary
            global_context: Global configuration context
            
        Returns:
            Task properties with variables resolved
        """
        # Merge global context with task properties for resolution
        context = {}
        if global_context:
            context.update(global_context)
        context.update(properties)
        
        # Resolve variables
        resolved_properties = self._deep_resolve(properties, context)
        
        logger.debug("Task properties variables resolved")
        return resolved_properties
    
    def validate_variable_references(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate that all variable references can be resolved
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of unresolved variable references
        """
        unresolved_vars = []
        
        def check_string(text: str, path: str = ""):
            if not isinstance(text, str):
                return
            
            # Check environment variables
            env_matches = self.ENV_VAR_PATTERN.findall(text)
            for var_expr in env_matches:
                var_name = var_expr.split(':-')[0].strip()
                if var_name not in os.environ:
                    unresolved_vars.append(f"{path}: ${{{var_name}}}")
            
            # Check template variables
            template_matches = self.TEMPLATE_PATTERN.findall(text)
            for var_name in template_matches:
                # This would need context to validate properly
                pass
        
        def walk_config(obj: Any, path: str = ""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    walk_config(value, f"{path}.{key}" if path else key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    walk_config(item, f"{path}[{i}]")
            elif isinstance(obj, str):
                check_string(obj, path)
        
        walk_config(config)
        return unresolved_vars
    
    def get_variable_references(self, text: str) -> Dict[str, List[str]]:
        """
        Extract all variable references from a string
        
        Args:
            text: String to analyze
            
        Returns:
            Dictionary with variable types and their references
        """
        references = {
            'environment': [],
            'template': [],
            'airflow': []
        }
        
        # Environment variables
        env_matches = self.ENV_VAR_PATTERN.findall(text)
        for var_expr in env_matches:
            var_name = var_expr.split(':-')[0].strip()
            references['environment'].append(var_name)
        
        # Template variables
        template_matches = self.TEMPLATE_PATTERN.findall(text)
        references['template'].extend(template_matches)
        
        # Airflow variables (if any patterns exist)
        airflow_var_pattern = re.compile(r"{{Variable\.get\('([^']+)'")
        airflow_matches = airflow_var_pattern.findall(text)
        references['airflow'].extend(airflow_matches)
        
        return references