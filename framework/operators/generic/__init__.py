# Generic Operators
"""
Generic operators that automatically select appropriate cloud provider 
and service-specific implementations based on deployment environment.
"""

__all__ = [
    'database_operations',
    'storage_operations', 
    'messaging_operations',
    'tableau_operations',
    'salesforce_operations'
]