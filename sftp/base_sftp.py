"""
Base SFTP Operations Class
Provides core SFTP operations for streaming data from cloud object stores to SFTP locations
"""

import logging
import os
import io
import tempfile
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple, Generator
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

try:
    import paramiko
    from paramiko import SSHClient, SFTPClient, AutoAddPolicy
    from paramiko.ssh_exception import AuthenticationException, SSHException
    PARAMIKO_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Paramiko library not available: {e}")
    PARAMIKO_AVAILABLE = False


class SFTPConnectionError(Exception):
    """Exception raised for SFTP connection errors"""
    pass


class SFTPTransferError(Exception):
    """Exception raised for SFTP transfer errors"""
    pass


class SFTPOperationError(Exception):
    """Exception raised for general SFTP operation errors"""
    pass


class BaseSFTPOperations(ABC):
    """
    Base class for SFTP operations across different cloud platforms
    Provides common functionality for SFTP connections and file transfers
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base SFTP operations
        
        Args:
            config: Configuration dictionary containing SFTP and cloud settings
                SFTP settings:
                - hostname: SFTP server hostname
                - port: SFTP server port (default: 22)
                - username: SFTP username
                - password: SFTP password (optional)
                - private_key_path: Path to private key file (optional)
                - private_key: Private key string content (optional)
                - passphrase: Private key passphrase (optional)
                - host_key_policy: Host key policy ('auto_add', 'reject', 'warning')
                - timeout: Connection timeout in seconds (default: 30)
                - compress: Enable compression (default: False)
                - buffer_size: Transfer buffer size in bytes (default: 32768)
                - max_concurrent_transfers: Maximum concurrent transfers (default: 5)
        """
        self.config = config
        
        # SFTP connection settings
        self.hostname = config.get('hostname')
        self.port = config.get('port', 22)
        self.username = config.get('username')
        self.password = config.get('password')
        self.private_key_path = config.get('private_key_path')
        self.private_key = config.get('private_key')
        self.passphrase = config.get('passphrase')
        self.host_key_policy = config.get('host_key_policy', 'auto_add')
        self.timeout = config.get('timeout', 30)
        self.compress = config.get('compress', False)
        
        # Transfer settings
        self.buffer_size = config.get('buffer_size', 32768)
        self.max_concurrent_transfers = config.get('max_concurrent_transfers', 5)
        
        # Connection objects
        self.ssh_client = None
        self.sftp_client = None
        
        if not PARAMIKO_AVAILABLE:
            logger.warning("Paramiko not available, SFTP operations will not work")
        elif not self.hostname or not self.username:
            logger.warning("SFTP hostname and username are required")
        else:
            try:
                self._initialize_sftp_connection()
            except Exception as e:
                logger.error(f"Failed to initialize SFTP connection: {e}")
                
    def _initialize_sftp_connection(self):
        """
        Initialize SFTP connection
        """
        if not PARAMIKO_AVAILABLE:
            raise SFTPConnectionError("Paramiko library not available")
            
        try:
            # Create SSH client
            self.ssh_client = SSHClient()
            
            # Set host key policy
            if self.host_key_policy == 'auto_add':
                self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            elif self.host_key_policy == 'reject':
                self.ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
            elif self.host_key_policy == 'warning':
                self.ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy())
            else:
                self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
                
            # Prepare authentication parameters
            auth_params = {
                'hostname': self.hostname,
                'port': self.port,
                'username': self.username,
                'timeout': self.timeout,
                'compress': self.compress
            }
            
            # Add authentication method
            if self.private_key_path:
                # Use private key file
                try:
                    if self.private_key_path.endswith('.rsa') or 'rsa' in self.private_key_path.lower():
                        pkey = paramiko.RSAKey.from_private_key_file(
                            self.private_key_path, password=self.passphrase
                        )
                    elif self.private_key_path.endswith('.dsa') or 'dsa' in self.private_key_path.lower():
                        pkey = paramiko.DSSKey.from_private_key_file(
                            self.private_key_path, password=self.passphrase
                        )
                    elif self.private_key_path.endswith('.ecdsa') or 'ecdsa' in self.private_key_path.lower():
                        pkey = paramiko.ECDSAKey.from_private_key_file(
                            self.private_key_path, password=self.passphrase
                        )
                    else:
                        # Try RSA first, then others
                        try:
                            pkey = paramiko.RSAKey.from_private_key_file(
                                self.private_key_path, password=self.passphrase
                            )
                        except:
                            try:
                                pkey = paramiko.Ed25519Key.from_private_key_file(
                                    self.private_key_path, password=self.passphrase
                                )
                            except:
                                pkey = paramiko.ECDSAKey.from_private_key_file(
                                    self.private_key_path, password=self.passphrase
                                )
                    auth_params['pkey'] = pkey
                except Exception as e:
                    logger.error(f"Failed to load private key: {e}")
                    if self.password:
                        auth_params['password'] = self.password
                    else:
                        raise SFTPConnectionError(f"Failed to load private key and no password provided: {e}")
                        
            elif self.private_key:
                # Use private key string
                try:
                    key_file = io.StringIO(self.private_key)
                    try:
                        pkey = paramiko.RSAKey.from_private_key(key_file, password=self.passphrase)
                    except:
                        key_file.seek(0)
                        try:
                            pkey = paramiko.Ed25519Key.from_private_key(key_file, password=self.passphrase)
                        except:
                            key_file.seek(0)
                            pkey = paramiko.ECDSAKey.from_private_key(key_file, password=self.passphrase)
                    auth_params['pkey'] = pkey
                except Exception as e:
                    logger.error(f"Failed to parse private key: {e}")
                    if self.password:
                        auth_params['password'] = self.password
                    else:
                        raise SFTPConnectionError(f"Failed to parse private key and no password provided: {e}")
                        
            elif self.password:
                # Use password authentication
                auth_params['password'] = self.password
            else:
                raise SFTPConnectionError("No authentication method provided (password or private key required)")
                
            # Connect to SSH server
            self.ssh_client.connect(**auth_params)
            
            # Create SFTP client
            self.sftp_client = self.ssh_client.open_sftp()
            
            logger.info(f"SFTP connection established to {self.hostname}:{self.port}")
            
        except AuthenticationException as e:
            logger.error(f"SFTP authentication failed: {e}")
            raise SFTPConnectionError(f"Authentication failed: {e}")
        except SSHException as e:
            logger.error(f"SSH connection error: {e}")
            raise SFTPConnectionError(f"SSH error: {e}")
        except Exception as e:
            logger.error(f"Failed to establish SFTP connection: {e}")
            raise SFTPConnectionError(f"Connection failed: {e}")
            
    def test_connection(self) -> bool:
        """
        Test SFTP connection
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.sftp_client:
                self._initialize_sftp_connection()
                
            # Test connection by listing current directory
            self.sftp_client.listdir('.')
            logger.info("SFTP connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"SFTP connection test failed: {e}")
            return False
            
    def ensure_connection(self):
        """
        Ensure SFTP connection is active, reconnect if necessary
        
        Raises:
            SFTPConnectionError: If connection cannot be established
        """
        try:
            if not self.sftp_client or not self.ssh_client or not self.ssh_client.get_transport().is_active():
                logger.info("SFTP connection not active, reconnecting...")
                self.close_connection()
                self._initialize_sftp_connection()
        except Exception as e:
            logger.error(f"Failed to ensure SFTP connection: {e}")
            raise SFTPConnectionError(f"Connection check failed: {e}")
            
    def close_connection(self):
        """
        Close SFTP and SSH connections
        """
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
                
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
                
            logger.info("SFTP connection closed")
            
        except Exception as e:
            logger.warning(f"Error closing SFTP connection: {e}")
            
    def create_directory(self, remote_path: str, recursive: bool = True) -> bool:
        """
        Create directory on SFTP server
        
        Args:
            remote_path: Remote directory path
            recursive: Create parent directories if they don't exist
            
        Returns:
            True if directory created or already exists, False otherwise
        """
        try:
            self.ensure_connection()
            
            if recursive:
                # Create directories recursively
                path_parts = remote_path.strip('/').split('/')
                current_path = ''
                
                for part in path_parts:
                    current_path = f"{current_path}/{part}" if current_path else part
                    
                    try:
                        self.sftp_client.stat(current_path)
                        # Directory exists
                    except FileNotFoundError:
                        # Directory doesn't exist, create it
                        try:
                            self.sftp_client.mkdir(current_path)
                            logger.debug(f"Created SFTP directory: {current_path}")
                        except Exception as e:
                            logger.error(f"Failed to create directory {current_path}: {e}")
                            return False
            else:
                # Create single directory
                try:
                    self.sftp_client.mkdir(remote_path)
                    logger.debug(f"Created SFTP directory: {remote_path}")
                except FileExistsError:
                    # Directory already exists
                    pass
                    
            return True
            
        except Exception as e:
            logger.error(f"Error creating SFTP directory {remote_path}: {e}")
            return False
            
    def file_exists(self, remote_path: str) -> bool:
        """
        Check if file exists on SFTP server
        
        Args:
            remote_path: Remote file path
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.ensure_connection()
            self.sftp_client.stat(remote_path)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Error checking if SFTP file exists {remote_path}: {e}")
            return False
            
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """
        Get file information from SFTP server
        
        Args:
            remote_path: Remote file path
            
        Returns:
            Dictionary with file information or None if file doesn't exist
        """
        try:
            self.ensure_connection()
            stat = self.sftp_client.stat(remote_path)
            
            return {
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_directory': paramiko.SFTPAttributes._flag_bits.get(stat.st_mode & 0o170000) == 'directory'
            }
            
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Error getting SFTP file info for {remote_path}: {e}")
            return None
            
    def list_directory(self, remote_path: str = '.') -> List[str]:
        """
        List files and directories on SFTP server
        
        Args:
            remote_path: Remote directory path
            
        Returns:
            List of file/directory names
        """
        try:
            self.ensure_connection()
            return self.sftp_client.listdir(remote_path)
        except Exception as e:
            logger.error(f"Error listing SFTP directory {remote_path}: {e}")
            return []
            
    def calculate_file_hash(self, file_data: bytes, algorithm: str = 'md5') -> str:
        """
        Calculate hash of file data
        
        Args:
            file_data: File content as bytes
            algorithm: Hash algorithm ('md5', 'sha1', 'sha256')
            
        Returns:
            Hash string
        """
        try:
            if algorithm == 'md5':
                return hashlib.md5(file_data).hexdigest()
            elif algorithm == 'sha1':
                return hashlib.sha1(file_data).hexdigest()
            elif algorithm == 'sha256':
                return hashlib.sha256(file_data).hexdigest()
            else:
                return hashlib.md5(file_data).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating file hash: {e}")
            return ""
            
    def transfer_from_stream(self, data_stream: Union[io.IOBase, bytes], 
                           remote_path: str, file_size: Optional[int] = None) -> bool:
        """
        Transfer data from stream to SFTP server
        
        Args:
            data_stream: Data stream or bytes to transfer
            remote_path: Remote file path
            file_size: Expected file size for progress tracking
            
        Returns:
            True if transfer successful, False otherwise
        """
        try:
            self.ensure_connection()
            
            # Ensure remote directory exists
            remote_dir = '/'.join(remote_path.split('/')[:-1])
            if remote_dir:
                self.create_directory(remote_dir)
                
            # Handle different input types
            if isinstance(data_stream, bytes):
                data_stream = io.BytesIO(data_stream)
                
            # Transfer file
            with self.sftp_client.open(remote_path, 'wb') as remote_file:
                bytes_transferred = 0
                
                while True:
                    chunk = data_stream.read(self.buffer_size)
                    if not chunk:
                        break
                        
                    remote_file.write(chunk)
                    bytes_transferred += len(chunk)
                    
                    # Log progress for large files
                    if file_size and bytes_transferred % (1024 * 1024) == 0:  # Every MB
                        progress = (bytes_transferred / file_size) * 100
                        logger.debug(f"Transfer progress: {progress:.1f}% ({bytes_transferred}/{file_size} bytes)")
                        
            logger.info(f"Successfully transferred {bytes_transferred} bytes to {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error transferring data to SFTP {remote_path}: {e}")
            return False
            
    @abstractmethod
    def stream_from_cloud_storage(self, cloud_path: str, remote_path: str, 
                                 chunk_size: int = None) -> Dict[str, Any]:
        """
        Stream data from cloud storage to SFTP server
        
        Args:
            cloud_path: Path to file in cloud storage
            remote_path: Remote SFTP path
            chunk_size: Size of chunks to stream (optional)
            
        Returns:
            Dictionary with transfer results
        """
        pass
        
    @abstractmethod
    def batch_stream_from_cloud_storage(self, transfer_list: List[Dict[str, str]], 
                                       max_concurrent: int = None) -> Dict[str, Any]:
        """
        Batch stream multiple files from cloud storage to SFTP server
        
        Args:
            transfer_list: List of transfer specifications
            max_concurrent: Maximum concurrent transfers
            
        Returns:
            Dictionary with batch transfer results
        """
        pass
        
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information
        
        Returns:
            Dictionary with connection info (without sensitive data)
        """
        return {
            'hostname': self.hostname,
            'port': self.port,
            'username': self.username,
            'connected': self.sftp_client is not None and self.ssh_client is not None,
            'transport_active': (self.ssh_client.get_transport().is_active() 
                               if self.ssh_client and self.ssh_client.get_transport() else False),
            'buffer_size': self.buffer_size,
            'max_concurrent_transfers': self.max_concurrent_transfers
        }
        
    def __enter__(self):
        """Context manager entry"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_connection()
        
    def __del__(self):
        """Destructor to ensure connections are closed"""
        try:
            self.close_connection()
        except:
            pass