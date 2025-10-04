"""
Base File Watcher Class
Provides common functionality for file monitoring across different cloud platforms
"""

import os
import time
import glob
import logging
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TimeOutError(Exception):
    """Custom exception for file watching timeouts"""
    pass


class BaseFileWatcher(ABC):
    """
    Abstract base class for file watching operations
    Provides common functionality for all file watcher implementations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base file watcher
        
        Args:
            config: Configuration dictionary containing watcher settings
        """
        self.config = config
        self.default_poll_interval = 30  # seconds
        self.min_poll_interval = 5  # seconds
        self.max_poll_interval = 300  # seconds
        
    def parse_wait_time(self, wait_time: str) -> int:
        """
        Parse wait time string and convert to seconds
        Supports 's' (seconds), 'm' (minutes), 'h' (hours)
        
        Args:
            wait_time: Time string like '30s', '5m', '2h'
            
        Returns:
            Time in seconds
            
        Raises:
            ValueError: If wait time format is invalid
        """
        if not wait_time:
            return 0
            
        # Remove whitespace and convert to lowercase
        wait_time = wait_time.strip().lower()
        
        # Extract number and unit
        match = re.match(r'^(\d+)([smh]?)$', wait_time)
        if not match:
            raise ValueError(f"Invalid wait time format: {wait_time}. Use format like '30s', '5m', '2h'")
            
        number = int(match.group(1))
        unit = match.group(2) or 's'  # Default to seconds
        
        # Convert to seconds
        multipliers = {'s': 1, 'm': 60, 'h': 3600}
        seconds = number * multipliers[unit]
        
        logger.debug(f"Parsed wait time '{wait_time}' to {seconds} seconds")
        return seconds
        
    def calculate_poll_interval(self, max_wait_seconds: int) -> int:
        """
        Calculate appropriate polling interval based on max wait time
        
        Args:
            max_wait_seconds: Maximum wait time in seconds
            
        Returns:
            Polling interval in seconds
        """
        if max_wait_seconds <= 120:  # 2 minutes
            interval = self.min_poll_interval
        elif max_wait_seconds <= 600:  # 10 minutes
            interval = 15
        elif max_wait_seconds <= 1800:  # 30 minutes
            interval = 30
        elif max_wait_seconds <= 3600:  # 1 hour
            interval = 60
        else:
            interval = 120  # 2 minutes for longer waits
        
        # Ensure interval is within bounds
        interval = max(self.min_poll_interval, min(self.max_poll_interval, interval))
        
        logger.debug(f"Calculated poll interval: {interval} seconds for max wait: {max_wait_seconds} seconds")
        return interval
        
    def create_search_pattern(self, file_pattern: str, is_rna_wizard: bool = False) -> str:
        """
        Create search pattern from file pattern
        Replaces pattern placeholders with wildcards
        
        Args:
            file_pattern: Original file pattern
            is_rna_wizard: Whether this is for RNA wizard files
            
        Returns:
            Search pattern with wildcards
        """
        if not file_pattern:
            return ""
            
        search_pattern = file_pattern
        
        # Special handling for RNA wizard files
        if is_rna_wizard and 'wizard_rna' in file_pattern.lower():
            # Replace specific RNA patterns
            search_pattern = re.sub(r'wizard_rna/[^/]+', 'wizard_rna/*', search_pattern)
            logger.debug(f"RNA wizard pattern created: {search_pattern}")
        else:
            # General pattern replacement - replace known patterns with *
            pattern_replacements = [
                (r'\{[^}]+\}', '*'),  # Replace {pattern} with *
                (r'\[[^\]]+\]', '*'),  # Replace [pattern] with *
                (r'YYYY', '*'),        # Replace date patterns
                (r'MM', '*'),
                (r'DD', '*'),
                (r'HH', '*'),
                (r'yyyy', '*'),
                (r'mm', '*'),
                (r'dd', '*'),
                (r'hh', '*'),
            ]
            
            for pattern, replacement in pattern_replacements:
                search_pattern = re.sub(pattern, replacement, search_pattern)
                
        logger.debug(f"Search pattern created: '{file_pattern}' -> '{search_pattern}'")
        return search_pattern
        
    def validate_file_count(self, found_files: List[str], expected_count: Optional[int] = None) -> bool:
        """
        Validate if found files match expected count
        
        Args:
            found_files: List of found files
            expected_count: Expected number of files (None means any count > 0)
            
        Returns:
            True if file count is valid, False otherwise
        """
        if not found_files:
            logger.debug("No files found")
            return False
            
        if expected_count is None:
            # Any files found is considered valid
            is_valid = len(found_files) > 0
        else:
            # Check exact count
            is_valid = len(found_files) == expected_count
            
        logger.debug(f"File count validation: found {len(found_files)} files, "
                    f"expected {expected_count}, valid: {is_valid}")
        return is_valid
        
    def log_waiting_status(self, elapsed_seconds: int, max_wait_seconds: int, 
                          poll_interval: int, files_checked: int = 0):
        """
        Log current waiting status
        
        Args:
            elapsed_seconds: Time elapsed since start
            max_wait_seconds: Maximum wait time
            poll_interval: Current polling interval
            files_checked: Number of files checked so far
        """
        remaining_seconds = max_wait_seconds - elapsed_seconds
        remaining_minutes = remaining_seconds // 60
        
        if remaining_minutes > 0:
            logger.info(f"File watching - Elapsed: {elapsed_seconds}s, "
                       f"Remaining: {remaining_minutes}m {remaining_seconds % 60}s, "
                       f"Files checked: {files_checked}")
        else:
            logger.info(f"File watching - Elapsed: {elapsed_seconds}s, "
                       f"Remaining: {remaining_seconds}s, "
                       f"Files checked: {files_checked}")
                       
    def check_local_file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in the local file system
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            exists = os.path.exists(file_path) and os.path.isfile(file_path)
            logger.debug(f"Local file check: '{file_path}' exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking local file '{file_path}': {e}")
            return False
            
    def search_local_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files in local file system using glob pattern
        
        Args:
            pattern: Glob pattern to search for
            
        Returns:
            List of matching file paths
        """
        try:
            files = glob.glob(pattern)
            files = [f for f in files if os.path.isfile(f)]  # Filter out directories
            logger.debug(f"Local pattern search '{pattern}' found {len(files)} files")
            return files
        except Exception as e:
            logger.error(f"Error searching local files with pattern '{pattern}': {e}")
            return []
            
    @abstractmethod
    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a specific file exists in the storage system
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file exists, False otherwise
        """
        pass
        
    @abstractmethod
    def search_files_by_pattern(self, pattern: str) -> List[str]:
        """
        Search for files using a pattern
        
        Args:
            pattern: Pattern to search for
            
        Returns:
            List of matching file paths
        """
        pass
        
    @abstractmethod
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        pass
        
    def wait_for_file(self, file_path: str, max_wait: str, 
                     poll_interval: Optional[int] = None) -> bool:
        """
        Wait for a specific file to appear
        
        Args:
            file_path: Path to the file to wait for
            max_wait: Maximum wait time (e.g., '5m', '1h', '30s')
            poll_interval: Polling interval in seconds (auto-calculated if None)
            
        Returns:
            True if file found, False if timeout
            
        Raises:
            TimeOutError: If file not found within max_wait time
        """
        max_wait_seconds = self.parse_wait_time(max_wait)
        if poll_interval is None:
            poll_interval = self.calculate_poll_interval(max_wait_seconds)
            
        start_time = time.time()
        files_checked = 0
        
        logger.info(f"Starting file watch for: '{file_path}', max wait: {max_wait} "
                   f"({max_wait_seconds}s), poll interval: {poll_interval}s")
        
        while True:
            elapsed_seconds = int(time.time() - start_time)
            files_checked += 1
            
            # Check if file exists
            if self.check_file_exists(file_path):
                logger.info(f"File found: '{file_path}' after {elapsed_seconds} seconds")
                return True
                
            # Check if timeout reached
            if elapsed_seconds >= max_wait_seconds:
                error_msg = (f"Timeout waiting for file: '{file_path}'. "
                           f"Waited {elapsed_seconds} seconds, checked {files_checked} times")
                logger.error(error_msg)
                raise TimeOutError(error_msg)
                
            # Log status periodically
            if files_checked % 10 == 0 or elapsed_seconds % 300 == 0:  # Every 10 checks or 5 minutes
                self.log_waiting_status(elapsed_seconds, max_wait_seconds, poll_interval, files_checked)
                
            # Wait before next check
            time.sleep(poll_interval)
            
    def wait_for_files_by_pattern(self, pattern: str, max_wait: str, 
                                 expected_count: Optional[int] = None,
                                 poll_interval: Optional[int] = None,
                                 is_rna_wizard: bool = False) -> List[str]:
        """
        Wait for files matching a pattern to appear
        
        Args:
            pattern: File pattern to search for
            max_wait: Maximum wait time (e.g., '5m', '1h', '30s')
            expected_count: Expected number of files (None means any count > 0)
            poll_interval: Polling interval in seconds (auto-calculated if None)
            is_rna_wizard: Whether this is for RNA wizard files
            
        Returns:
            List of found files
            
        Raises:
            TimeOutError: If files not found within max_wait time
        """
        max_wait_seconds = self.parse_wait_time(max_wait)
        if poll_interval is None:
            poll_interval = self.calculate_poll_interval(max_wait_seconds)
            
        search_pattern = self.create_search_pattern(pattern, is_rna_wizard)
        start_time = time.time()
        checks_performed = 0
        
        logger.info(f"Starting pattern file watch for: '{pattern}' -> '{search_pattern}', "
                   f"max wait: {max_wait} ({max_wait_seconds}s), "
                   f"expected count: {expected_count}, poll interval: {poll_interval}s")
        
        while True:
            elapsed_seconds = int(time.time() - start_time)
            checks_performed += 1
            
            # Search for files matching pattern
            found_files = self.search_files_by_pattern(search_pattern)
            
            # Validate file count
            if self.validate_file_count(found_files, expected_count):
                logger.info(f"Files found matching pattern: '{pattern}' after {elapsed_seconds} seconds. "
                           f"Found {len(found_files)} files: {found_files}")
                return found_files
                
            # Check if timeout reached
            if elapsed_seconds >= max_wait_seconds:
                error_msg = (f"Timeout waiting for files matching pattern: '{pattern}'. "
                           f"Waited {elapsed_seconds} seconds, performed {checks_performed} checks. "
                           f"Found {len(found_files)} files but expected {expected_count}")
                logger.error(error_msg)
                raise TimeOutError(error_msg)
                
            # Log status periodically
            if checks_performed % 10 == 0 or elapsed_seconds % 300 == 0:  # Every 10 checks or 5 minutes
                self.log_waiting_status(elapsed_seconds, max_wait_seconds, poll_interval, 
                                      len(found_files))
                logger.debug(f"Currently found files: {found_files}")
                
            # Wait before next check
            time.sleep(poll_interval)
            
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information (without sensitive data)
        
        Returns:
            Dictionary with connection info
        """
        safe_config = self.config.copy()
        # Remove sensitive information
        sensitive_keys = ['password', 'secret', 'token', 'key', 'credential']
        for key in list(safe_config.keys()):
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                safe_config[key] = "***"
        return safe_config