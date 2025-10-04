"""
Engine Operations Module
Provides distributed processing engine operations for various platforms including AWS Glue, EMR, and Ray
"""

# Import base classes
from .base_engine import (
    BaseEngineOperations,
    EngineConfigError,
    EngineOperationError,
    EngineJobError
)

# Import engine implementations
from .glue_engine import GlueEngineOperations
from .emr_engine import EMREngineOperations
from .ray_engine import RayEngineOperations

__version__ = "1.0.0"

__all__ = [
    # Base classes
    "BaseEngineOperations",
    "EngineConfigError",
    "EngineOperationError",
    "EngineJobError",
    
    # Engine implementations
    "GlueEngineOperations",
    "EMREngineOperations",
    "RayEngineOperations",
]

# Engine platform mapping for dynamic instantiation
ENGINE_PLATFORMS = {
    'glue': GlueEngineOperations,
    'emr': EMREngineOperations,
    'ray': RayEngineOperations,
}


def create_engine_operations(engine_platform: str, context) -> BaseEngineOperations:
    """
    Factory function to create appropriate engine operations instance
    
    Args:
        engine_platform: Engine platform identifier ('glue', 'emr', 'ray')
        context: Workflow context for the engine
        
    Returns:
        Appropriate engine operations instance
        
    Raises:
        ValueError: If engine platform is not supported
    """
    platform = engine_platform.lower()
    
    if platform not in ENGINE_PLATFORMS:
        raise ValueError(f"Unsupported engine platform: {engine_platform}. "
                        f"Supported platforms: {', '.join(ENGINE_PLATFORMS.keys())}")
    
    return ENGINE_PLATFORMS[platform](context)


def get_supported_engines() -> List[str]:
    """
    Get list of supported engine platforms
    
    Returns:
        List of supported engine platform names
    """
    return list(ENGINE_PLATFORMS.keys())


def get_engine_info() -> Dict[str, Dict[str, str]]:
    """
    Get information about all supported engines
    
    Returns:
        Dictionary with engine information
    """
    return {
        'glue': {
            'name': 'AWS Glue',
            'description': 'Serverless ETL service for data preparation and transformation',
            'use_cases': ['ETL jobs', 'Data cataloging', 'Serverless data processing'],
            'supported_languages': ['Python', 'Scala'],
            'compute_type': 'Serverless'
        },
        'emr': {
            'name': 'AWS EMR',
            'description': 'Managed cluster platform for big data frameworks',
            'use_cases': ['Large-scale data processing', 'Machine learning', 'Data analytics'],
            'supported_languages': ['Python', 'Scala', 'Java', 'R'],
            'compute_type': 'Managed clusters'
        },
        'ray': {
            'name': 'Ray',
            'description': 'Distributed computing framework for ML and data processing',
            'use_cases': ['Distributed ML training', 'Hyperparameter tuning', 'Distributed data processing'],
            'supported_languages': ['Python'],
            'compute_type': 'Distributed clusters'
        }
    }