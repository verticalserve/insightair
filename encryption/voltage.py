"""
Voltage Encryption Operations
Provides encryption and decryption operations using Voltage SecureStateless library
Supports both individual text encryption and pandas DataFrame column operations
"""

import logging
import re
import pandas as pd
from typing import Dict, Any, Optional, Union, List
import numpy as np

logger = logging.getLogger(__name__)

try:
    # Import Voltage SecureStateless library
    import vusswlib
    from vusswlib import VwProtectString, VwAccessString
    VOLTAGE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Voltage library not available: {e}")
    VOLTAGE_AVAILABLE = False
    # Create mock classes for development/testing
    class VwProtectString:
        def __init__(self, *args, **kwargs):
            pass
        def protect(self, text, format_name):
            return f"ENCRYPTED_{text}_FORMAT_{format_name}"
    
    class VwAccessString:
        def __init__(self, *args, **kwargs):
            pass
        def access(self, encrypted_text, format_name):
            # Simple mock decryption
            if encrypted_text.startswith("ENCRYPTED_") and "_FORMAT_" in encrypted_text:
                parts = encrypted_text.split("_FORMAT_")
                return parts[0].replace("ENCRYPTED_", "")
            return encrypted_text


class VoltageEncryptionError(Exception):
    """Custom exception for Voltage encryption operations"""
    pass


class VoltageOperations:
    """
    Voltage encryption and decryption operations class
    Handles individual text and DataFrame column encryption/decryption
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize Voltage operations
        
        Args:
            config: Configuration dictionary containing Voltage settings
                - identity_file: Path to Voltage identity file
                - server_url: Voltage server URL
                - username: Username for authentication
                - password: Password for authentication
                - default_format: Default field format for encryption
        """
        self.config = config or {}
        self.identity_file = self.config.get('identity_file')
        self.server_url = self.config.get('server_url')
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.default_format = self.config.get('default_format', 'FPE_SSN')
        
        # Initialize Voltage protect and access objects
        self.protector = None
        self.accessor = None
        
        if VOLTAGE_AVAILABLE:
            try:
                self._initialize_voltage()
            except Exception as e:
                logger.error(f"Failed to initialize Voltage: {e}")
                raise VoltageEncryptionError(f"Voltage initialization failed: {e}")
        else:
            logger.warning("Using mock Voltage implementation for development")
            self.protector = VwProtectString()
            self.accessor = VwAccessString()
            
    def _initialize_voltage(self):
        """
        Initialize Voltage SecureStateless objects
        """
        try:
            # Initialize VwProtectString for encryption
            if self.identity_file:
                self.protector = VwProtectString(self.identity_file)
            elif self.server_url and self.username and self.password:
                self.protector = VwProtectString(
                    server_url=self.server_url,
                    username=self.username,
                    password=self.password
                )
            else:
                # Use default configuration
                self.protector = VwProtectString()
                
            # Initialize VwAccessString for decryption
            if self.identity_file:
                self.accessor = VwAccessString(self.identity_file)
            elif self.server_url and self.username and self.password:
                self.accessor = VwAccessString(
                    server_url=self.server_url,
                    username=self.username,
                    password=self.password
                )
            else:
                # Use default configuration
                self.accessor = VwAccessString()
                
            logger.info("Voltage SecureStateless initialized successfully")
            
        except Exception as e:
            logger.error(f"Voltage initialization error: {e}")
            raise
            
    def isEncryptable(self, text: Union[str, Any]) -> bool:
        """
        Validates whether text can be encrypted by checking if it contains 
        more than one alphanumeric character
        
        Args:
            text: Text to validate for encryption
            
        Returns:
            True if text is encryptable, False otherwise
        """
        # Handle None, NaN, or empty values
        if text is None or text == '' or pd.isna(text):
            return False
            
        # Convert to string if not already
        text_str = str(text).strip()
        
        # Check if empty after stripping
        if not text_str:
            return False
            
        # Count alphanumeric characters
        alphanumeric_count = len(re.findall(r'[a-zA-Z0-9]', text_str))
        
        # Must have more than one alphanumeric character
        is_encryptable = alphanumeric_count > 1
        
        logger.debug(f"Text '{text_str}' encryptability check: {is_encryptable} "
                    f"(alphanumeric count: {alphanumeric_count})")
        
        return is_encryptable
        
    def encrypt(self, txt: Union[str, Any], field_format: str = None) -> str:
        """
        Encrypts a single text string using Voltage encryption with a specified field format
        Only encrypts if the text passes the encryptability check
        
        Args:
            txt: Text to encrypt
            field_format: Voltage field format for encryption (e.g., 'FPE_SSN', 'FPE_CREDIT_CARD')
            
        Returns:
            Encrypted text if encryptable, original text otherwise
            
        Raises:
            VoltageEncryptionError: If encryption fails
        """
        # Use default format if not specified
        if field_format is None:
            field_format = self.default_format
            
        # Handle None, NaN, or empty values
        if txt is None or txt == '' or pd.isna(txt):
            logger.debug("Skipping encryption for None/empty value")
            return txt
            
        # Convert to string
        text_str = str(txt)
        
        # Check if text is encryptable
        if not self.isEncryptable(text_str):
            logger.debug(f"Text '{text_str}' is not encryptable, returning original")
            return text_str
            
        try:
            # Perform encryption using Voltage
            encrypted_text = self.protector.protect(text_str, field_format)
            
            logger.debug(f"Successfully encrypted text with format '{field_format}'")
            return encrypted_text
            
        except Exception as e:
            error_msg = f"Encryption failed for text with format '{field_format}': {e}"
            logger.error(error_msg)
            raise VoltageEncryptionError(error_msg)
            
    def decrypt(self, encrypted_txt: Union[str, Any], field_format: str = None) -> str:
        """
        Decrypts a single encrypted text string using Voltage decryption
        
        Args:
            encrypted_txt: Encrypted text to decrypt
            field_format: Voltage field format used for encryption
            
        Returns:
            Decrypted text
            
        Raises:
            VoltageEncryptionError: If decryption fails
        """
        # Use default format if not specified
        if field_format is None:
            field_format = self.default_format
            
        # Handle None, NaN, or empty values
        if encrypted_txt is None or encrypted_txt == '' or pd.isna(encrypted_txt):
            logger.debug("Skipping decryption for None/empty value")
            return encrypted_txt
            
        # Convert to string
        encrypted_str = str(encrypted_txt)
        
        try:
            # Perform decryption using Voltage
            decrypted_text = self.accessor.access(encrypted_str, field_format)
            
            logger.debug(f"Successfully decrypted text with format '{field_format}'")
            return decrypted_text
            
        except Exception as e:
            error_msg = f"Decryption failed for text with format '{field_format}': {e}"
            logger.error(error_msg)
            raise VoltageEncryptionError(error_msg)
            
    def voltage_encr_df(self, encr_columns: Dict[str, str], df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs Voltage encryption on specified columns in a pandas DataFrame
        Takes a dictionary of column names and their formats, then applies encryption 
        to each specified column
        
        Args:
            encr_columns: Dictionary mapping column names to their encryption formats
                         Example: {'ssn': 'FPE_SSN', 'credit_card': 'FPE_CREDIT_CARD'}
            df: pandas DataFrame to encrypt
            
        Returns:
            DataFrame with specified columns encrypted
            
        Raises:
            VoltageEncryptionError: If DataFrame operations fail
        """
        if df is None or df.empty:
            logger.warning("DataFrame is None or empty, returning as-is")
            return df
            
        if not encr_columns:
            logger.warning("No encryption columns specified, returning original DataFrame")
            return df.copy()
            
        try:
            # Create a copy of the DataFrame to avoid modifying the original
            result_df = df.copy()
            
            encryption_stats = {
                'total_columns': len(encr_columns),
                'successful_columns': 0,
                'skipped_columns': 0,
                'failed_columns': 0,
                'total_rows_processed': 0,
                'rows_encrypted': 0
            }
            
            # Process each column specified for encryption
            for column_name, field_format in encr_columns.items():
                try:
                    # Check if column exists in DataFrame
                    if column_name not in result_df.columns:
                        logger.warning(f"Column '{column_name}' not found in DataFrame, skipping")
                        encryption_stats['skipped_columns'] += 1
                        continue
                        
                    logger.info(f"Encrypting column '{column_name}' with format '{field_format}'")
                    
                    # Get the column data
                    column_data = result_df[column_name]
                    encryption_stats['total_rows_processed'] += len(column_data)
                    
                    # Apply encryption to each value in the column
                    encrypted_values = []
                    rows_encrypted_in_column = 0
                    
                    for value in column_data:
                        try:
                            encrypted_value = self.encrypt(value, field_format)
                            encrypted_values.append(encrypted_value)
                            
                            # Count if value was actually encrypted (changed)
                            if str(encrypted_value) != str(value):
                                rows_encrypted_in_column += 1
                                
                        except Exception as e:
                            logger.error(f"Failed to encrypt value in column '{column_name}': {e}")
                            # Keep original value if encryption fails
                            encrypted_values.append(value)
                            
                    # Update the column with encrypted values
                    result_df[column_name] = encrypted_values
                    
                    encryption_stats['successful_columns'] += 1
                    encryption_stats['rows_encrypted'] += rows_encrypted_in_column
                    
                    logger.info(f"Column '{column_name}' encryption completed. "
                               f"Rows encrypted: {rows_encrypted_in_column}/{len(column_data)}")
                    
                except Exception as e:
                    error_msg = f"Failed to encrypt column '{column_name}': {e}"
                    logger.error(error_msg)
                    encryption_stats['failed_columns'] += 1
                    # Continue with other columns
                    
            # Log encryption summary
            logger.info(f"DataFrame encryption completed. Stats: {encryption_stats}")
            
            return result_df
            
        except Exception as e:
            error_msg = f"DataFrame encryption operation failed: {e}"
            logger.error(error_msg)
            raise VoltageEncryptionError(error_msg)
            
    def voltage_decrypt_df(self, encrypted_columns: Dict[str, str], df: pd.DataFrame) -> pd.DataFrame:
        """
        Decrypts specified columns in a pandas DataFrame
        Takes a dictionary of encrypted column names and their formats, 
        then applies decryption to each specified column
        
        Args:
            encrypted_columns: Dictionary mapping column names to their encryption formats
                              Example: {'ssn': 'FPE_SSN', 'credit_card': 'FPE_CREDIT_CARD'}
            df: pandas DataFrame with encrypted columns to decrypt
            
        Returns:
            DataFrame with specified columns decrypted
            
        Raises:
            VoltageEncryptionError: If DataFrame operations fail
        """
        if df is None or df.empty:
            logger.warning("DataFrame is None or empty, returning as-is")
            return df
            
        if not encrypted_columns:
            logger.warning("No decryption columns specified, returning original DataFrame")
            return df.copy()
            
        try:
            # Create a copy of the DataFrame to avoid modifying the original
            result_df = df.copy()
            
            decryption_stats = {
                'total_columns': len(encrypted_columns),
                'successful_columns': 0,
                'skipped_columns': 0,
                'failed_columns': 0,
                'total_rows_processed': 0,
                'rows_decrypted': 0
            }
            
            # Process each column specified for decryption
            for column_name, field_format in encrypted_columns.items():
                try:
                    # Check if column exists in DataFrame
                    if column_name not in result_df.columns:
                        logger.warning(f"Column '{column_name}' not found in DataFrame, skipping")
                        decryption_stats['skipped_columns'] += 1
                        continue
                        
                    logger.info(f"Decrypting column '{column_name}' with format '{field_format}'")
                    
                    # Get the column data
                    column_data = result_df[column_name]
                    decryption_stats['total_rows_processed'] += len(column_data)
                    
                    # Apply decryption to each value in the column
                    decrypted_values = []
                    rows_decrypted_in_column = 0
                    
                    for value in column_data:
                        try:
                            decrypted_value = self.decrypt(value, field_format)
                            decrypted_values.append(decrypted_value)
                            
                            # Count if value was actually decrypted (changed)
                            if str(decrypted_value) != str(value):
                                rows_decrypted_in_column += 1
                                
                        except Exception as e:
                            logger.error(f"Failed to decrypt value in column '{column_name}': {e}")
                            # Keep original value if decryption fails
                            decrypted_values.append(value)
                            
                    # Update the column with decrypted values
                    result_df[column_name] = decrypted_values
                    
                    decryption_stats['successful_columns'] += 1
                    decryption_stats['rows_decrypted'] += rows_decrypted_in_column
                    
                    logger.info(f"Column '{column_name}' decryption completed. "
                               f"Rows decrypted: {rows_decrypted_in_column}/{len(column_data)}")
                    
                except Exception as e:
                    error_msg = f"Failed to decrypt column '{column_name}': {e}"
                    logger.error(error_msg)
                    decryption_stats['failed_columns'] += 1
                    # Continue with other columns
                    
            # Log decryption summary
            logger.info(f"DataFrame decryption completed. Stats: {decryption_stats}")
            
            return result_df
            
        except Exception as e:
            error_msg = f"DataFrame decryption operation failed: {e}"
            logger.error(error_msg)
            raise VoltageEncryptionError(error_msg)
            
    def validate_field_format(self, field_format: str) -> bool:
        """
        Validate if the specified field format is supported
        
        Args:
            field_format: Field format to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        # Common Voltage field formats
        valid_formats = [
            'FPE_SSN',
            'FPE_CREDIT_CARD',
            'FPE_PHONE',
            'FPE_EMAIL',
            'FPE_NAME',
            'FPE_ADDRESS',
            'FPE_DATE',
            'FPE_NUMBER',
            'FPE_ALPHANUMERIC',
            'DATATYPE_PRESERVING'
        ]
        
        is_valid = field_format in valid_formats
        if not is_valid:
            logger.warning(f"Field format '{field_format}' may not be supported. "
                          f"Valid formats: {valid_formats}")
        
        return is_valid
        
    def get_encryption_statistics(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
        """
        Get statistics about encryptable data in specified DataFrame columns
        
        Args:
            df: DataFrame to analyze
            columns: List of column names to analyze
            
        Returns:
            Dictionary with encryption statistics
        """
        stats = {
            'total_columns': len(columns),
            'column_stats': {}
        }
        
        for column in columns:
            if column not in df.columns:
                stats['column_stats'][column] = {'error': 'Column not found'}
                continue
                
            column_data = df[column]
            encryptable_count = sum(1 for value in column_data if self.isEncryptable(value))
            
            stats['column_stats'][column] = {
                'total_rows': len(column_data),
                'encryptable_rows': encryptable_count,
                'non_encryptable_rows': len(column_data) - encryptable_count,
                'encryptable_percentage': (encryptable_count / len(column_data)) * 100 if len(column_data) > 0 else 0
            }
            
        return stats
        
    def test_encryption_decryption(self, test_data: List[str], field_format: str = None) -> Dict[str, Any]:
        """
        Test encryption and decryption operations with sample data
        
        Args:
            test_data: List of test strings
            field_format: Field format to use for testing
            
        Returns:
            Dictionary with test results
        """
        if field_format is None:
            field_format = self.default_format
            
        results = {
            'field_format': field_format,
            'total_tests': len(test_data),
            'successful_tests': 0,
            'failed_tests': 0,
            'test_details': []
        }
        
        for i, test_string in enumerate(test_data):
            test_result = {
                'test_index': i,
                'original': test_string,
                'encryptable': self.isEncryptable(test_string),
                'encrypted': None,
                'decrypted': None,
                'success': False,
                'error': None
            }
            
            try:
                # Test encryption
                encrypted = self.encrypt(test_string, field_format)
                test_result['encrypted'] = encrypted
                
                # Test decryption
                decrypted = self.decrypt(encrypted, field_format)
                test_result['decrypted'] = decrypted
                
                # Check if round-trip was successful
                if test_result['encryptable']:
                    test_result['success'] = (str(decrypted) == str(test_string))
                else:
                    test_result['success'] = (str(encrypted) == str(test_string))
                
                if test_result['success']:
                    results['successful_tests'] += 1
                else:
                    results['failed_tests'] += 1
                    
            except Exception as e:
                test_result['error'] = str(e)
                results['failed_tests'] += 1
                
            results['test_details'].append(test_result)
            
        return results