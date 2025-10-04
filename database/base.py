"""
Base Database Operations Class
Provides common database functionality for all database implementations
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
import pandas as pd

logger = logging.getLogger(__name__)


class BaseDatabaseOperations(ABC):
    """
    Abstract base class for database operations
    Defines common interface for all database implementations
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize database connection parameters
        
        Args:
            connection_params: Dictionary containing connection parameters
        """
        self.connection_params = connection_params
        self.connection = None
        self.cursor = None
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the database
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
        
    @abstractmethod
    def disconnect(self):
        """
        Close database connection
        """
        pass
        
    @abstractmethod
    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing query results
        """
        pass
        
    @abstractmethod
    def execute_command(self, command: str, params: Optional[List[Any]] = None) -> int:
        """
        Execute a non-SELECT command (INSERT, UPDATE, DELETE, etc.)
        
        Args:
            command: SQL command string
            params: Optional command parameters
            
        Returns:
            Number of affected rows
        """
        pass
        
    def validate_connection(self) -> bool:
        """
        Validate if the database connection is active
        
        Returns:
            True if connection is active, False otherwise
        """
        if not self.connection:
            return False
            
        try:
            # Try a simple query to test connection
            result = self.execute_query("SELECT 1")
            return len(result) > 0
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
            
    def format_params(self, params: Optional[List[Any]]) -> str:
        """
        Format parameters for SQL queries
        
        Args:
            params: List of parameters
            
        Returns:
            Formatted parameter string
        """
        if not params:
            return ""
            
        formatted_params = []
        for param in params:
            if isinstance(param, str):
                formatted_params.append(f"'{param}'")
            elif param is None:
                formatted_params.append("NULL")
            else:
                formatted_params.append(str(param))
                
        return ", ".join(formatted_params)
        
    def build_partition_location(self, bucket: str, folder: str, file: str, partition_str: str) -> str:
        """
        Build S3 location string for partition
        
        Args:
            bucket: S3 bucket name
            folder: Folder path
            file: File path
            partition_str: Partition string
            
        Returns:
            Complete S3 location URL
        """
        # Remove s3:// prefix if present
        bucket = bucket.replace('s3://', '')
        
        # Ensure proper path formatting
        parts = [folder.strip('/'), file.strip('/'), partition_str.strip('/')]
        path = '/'.join(part for part in parts if part)
        
        return f"s3://{bucket}/{path}"
        
    def parse_partition_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        """
        Parse partition parameters from input dictionary
        
        Args:
            params: Dictionary containing partition parameters
            
        Returns:
            Dictionary with parsed partition parameters
        """
        required_params = ['schema', 'table', 'partition_sql', 'bucket', 'folder', 'file', 'partition_str']
        parsed_params = {}
        
        for param in required_params:
            if param not in params:
                raise ValueError(f"Missing required partition parameter: {param}")
            parsed_params[param] = str(params[param])
            
        return parsed_params
        
    def log_operation(self, operation: str, details: str):
        """
        Log database operation
        
        Args:
            operation: Type of operation
            details: Operation details
        """
        logger.info(f"Database Operation - {operation}: {details}")
        
    def __enter__(self):
        """
        Context manager entry
        """
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit
        """
        self.disconnect()
        
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information (without sensitive data)
        
        Returns:
            Dictionary with connection info
        """
        safe_params = self.connection_params.copy()
        # Remove sensitive information
        sensitive_keys = ['password', 'secret', 'token', 'key']
        for key in list(safe_params.keys()):
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                safe_params[key] = "***"
        return safe_params