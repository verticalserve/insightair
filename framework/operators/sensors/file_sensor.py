# File Sensor Operators - Monitor file system conditions
"""
File sensor operators for monitoring file system conditions including:
- Local file system monitoring
- S3 object monitoring
- GCS object monitoring
- File pattern matching and validation
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union, Callable
from pathlib import Path

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.configuration import conf

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class FileSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors local file system for file existence and conditions
    """
    
    template_fields = ('file_path', 'file_pattern', 'directory_path')
    
    def __init__(self,
                 file_path: Optional[str] = None,
                 file_pattern: Optional[str] = None,
                 directory_path: Optional[str] = None,
                 min_file_size: int = 0,
                 max_age_hours: Optional[int] = None,
                 check_content: bool = False,
                 content_pattern: Optional[str] = None,
                 recursive: bool = False,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize FileSensor
        
        Args:
            file_path: Specific file path to monitor
            file_pattern: Pattern to match files (e.g., "*.csv")
            directory_path: Directory to monitor
            min_file_size: Minimum file size in bytes
            max_age_hours: Maximum file age in hours
            check_content: Whether to check file content
            content_pattern: Pattern to find in file content
            recursive: Whether to search recursively in subdirectories
            poke_interval: Time between checks in seconds
            timeout: Timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.file_path = file_path
        self.file_pattern = file_pattern
        self.directory_path = directory_path
        self.min_file_size = min_file_size
        self.max_age_hours = max_age_hours
        self.check_content = check_content
        self.content_pattern = content_pattern
        self.recursive = recursive
        
        # Validation
        if not any([file_path, file_pattern, directory_path]):
            raise ValueError("Must specify either file_path, file_pattern, or directory_path")
    
    def poke(self, context: Context) -> bool:
        """
        Check if file condition is met
        
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            if self.file_path:
                return self._check_specific_file(self.file_path, context)
            elif self.file_pattern or self.directory_path:
                return self._check_file_pattern(context)
            
            return False
            
        except Exception as e:
            logger.error(f"FileSensor poke failed: {str(e)}")
            return False
    
    def _check_specific_file(self, file_path: str, context: Context) -> bool:
        """Check if a specific file meets all conditions"""
        
        path = Path(file_path)
        
        # Check existence
        if not path.exists():
            logger.debug(f"File does not exist: {file_path}")
            return False
        
        if not path.is_file():
            logger.debug(f"Path is not a file: {file_path}")
            return False
        
        # Check file size
        if self.min_file_size > 0:
            file_size = path.stat().st_size
            if file_size < self.min_file_size:
                logger.debug(f"File size {file_size} < minimum {self.min_file_size}: {file_path}")
                return False
        
        # Check file age
        if self.max_age_hours:
            file_age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
            max_age = timedelta(hours=self.max_age_hours)
            if file_age > max_age:
                logger.debug(f"File age {file_age} > maximum {max_age}: {file_path}")
                return False
        
        # Check content if required
        if self.check_content:
            if not self._check_file_content(path):
                return False
        
        logger.info(f"File condition satisfied: {file_path}")
        
        # Store file info in XCom
        context['task_instance'].xcom_push(
            key='file_info',
            value={
                'path': str(path),
                'size': path.stat().st_size,
                'modified': datetime.fromtimestamp(path.stat().st_mtime).isoformat()
            }
        )
        
        return True
    
    def _check_file_pattern(self, context: Context) -> bool:
        """Check if files matching pattern meet conditions"""
        
        directory = Path(self.directory_path) if self.directory_path else Path.cwd()
        
        if not directory.exists():
            logger.debug(f"Directory does not exist: {directory}")
            return False
        
        # Find matching files
        if self.file_pattern:
            if self.recursive:
                matching_files = list(directory.rglob(self.file_pattern))
            else:
                matching_files = list(directory.glob(self.file_pattern))
        else:
            # All files in directory
            if self.recursive:
                matching_files = [f for f in directory.rglob('*') if f.is_file()]
            else:
                matching_files = [f for f in directory.glob('*') if f.is_file()]
        
        if not matching_files:
            logger.debug(f"No matching files found for pattern: {self.file_pattern}")
            return False
        
        # Check each file
        valid_files = []
        for file_path in matching_files:
            if self._check_specific_file(str(file_path), context):
                valid_files.append({
                    'path': str(file_path),
                    'size': file_path.stat().st_size,
                    'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                })
        
        if valid_files:
            logger.info(f"Found {len(valid_files)} files matching conditions")
            
            # Store matching files in XCom
            context['task_instance'].xcom_push(
                key='matching_files',
                value=valid_files
            )
            
            return True
        
        return False
    
    def _check_file_content(self, file_path: Path) -> bool:
        """Check file content against pattern"""
        
        if not self.content_pattern:
            return True
        
        try:
            with file_path.open('r', encoding='utf-8') as f:
                content = f.read()
                
            import re
            if re.search(self.content_pattern, content):
                logger.debug(f"Content pattern found in file: {file_path}")
                return True
            else:
                logger.debug(f"Content pattern not found in file: {file_path}")
                return False
                
        except Exception as e:
            logger.warning(f"Failed to check file content for {file_path}: {e}")
            return False


class S3Sensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors S3 objects for existence and conditions
    """
    
    template_fields = ('bucket_name', 'object_key', 'prefix')
    
    def __init__(self,
                 bucket_name: str,
                 object_key: Optional[str] = None,
                 prefix: Optional[str] = None,
                 aws_conn_id: str = 'aws_default',
                 min_object_size: int = 0,
                 max_age_hours: Optional[int] = None,
                 check_content_type: Optional[str] = None,
                 verify_content: bool = False,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize S3Sensor
        
        Args:
            bucket_name: S3 bucket name
            object_key: Specific object key to monitor
            prefix: Prefix to match objects
            aws_conn_id: Airflow AWS connection ID
            min_object_size: Minimum object size in bytes
            max_age_hours: Maximum object age in hours
            check_content_type: Expected content type
            verify_content: Whether to verify object content
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.min_object_size = min_object_size
        self.max_age_hours = max_age_hours
        self.check_content_type = check_content_type
        self.verify_content = verify_content
        
        if not any([object_key, prefix]):
            raise ValueError("Must specify either object_key or prefix")
    
    def poke(self, context: Context) -> bool:
        """Check if S3 object condition is met"""
        
        try:
            import boto3
            from airflow.hooks.base import BaseHook
            
            # Get AWS connection
            connection = BaseHook.get_connection(self.aws_conn_id)
            
            # Create S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=connection.login,
                aws_secret_access_key=connection.password,
                region_name=connection.extra_dejson.get('region_name', 'us-east-1')
            )
            
            if self.object_key:
                return self._check_specific_object(s3_client, context)
            else:
                return self._check_objects_with_prefix(s3_client, context)
                
        except Exception as e:
            logger.error(f"S3Sensor poke failed: {str(e)}")
            return False
    
    def _check_specific_object(self, s3_client, context: Context) -> bool:
        """Check if specific S3 object meets conditions"""
        
        try:
            # Check if object exists
            response = s3_client.head_object(Bucket=self.bucket_name, Key=self.object_key)
            
            # Check object size
            object_size = response.get('ContentLength', 0)
            if object_size < self.min_object_size:
                logger.debug(f"Object size {object_size} < minimum {self.min_object_size}")
                return False
            
            # Check object age
            if self.max_age_hours:
                last_modified = response.get('LastModified')
                if last_modified:
                    age = datetime.now(last_modified.tzinfo) - last_modified
                    max_age = timedelta(hours=self.max_age_hours)
                    if age > max_age:
                        logger.debug(f"Object age {age} > maximum {max_age}")
                        return False
            
            # Check content type
            if self.check_content_type:
                content_type = response.get('ContentType', '')
                if content_type != self.check_content_type:
                    logger.debug(f"Content type {content_type} != expected {self.check_content_type}")
                    return False
            
            logger.info(f"S3 object condition satisfied: s3://{self.bucket_name}/{self.object_key}")
            
            # Store object info in XCom
            context['task_instance'].xcom_push(
                key='s3_object_info',
                value={
                    'bucket': self.bucket_name,
                    'key': self.object_key,
                    'size': object_size,
                    'last_modified': last_modified.isoformat() if last_modified else None,
                    'content_type': response.get('ContentType')
                }
            )
            
            return True
            
        except s3_client.exceptions.NoSuchKey:
            logger.debug(f"S3 object does not exist: s3://{self.bucket_name}/{self.object_key}")
            return False
        except Exception as e:
            logger.error(f"Error checking S3 object: {e}")
            return False
    
    def _check_objects_with_prefix(self, s3_client, context: Context) -> bool:
        """Check if S3 objects with prefix meet conditions"""
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)
            
            matching_objects = []
            
            for page in pages:
                for obj in page.get('Contents', []):
                    object_info = {
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    }
                    
                    # Check conditions
                    if obj['Size'] >= self.min_object_size:
                        if self.max_age_hours:
                            age = datetime.now(obj['LastModified'].tzinfo) - obj['LastModified']
                            max_age = timedelta(hours=self.max_age_hours)
                            if age <= max_age:
                                matching_objects.append(object_info)
                        else:
                            matching_objects.append(object_info)
            
            if matching_objects:
                logger.info(f"Found {len(matching_objects)} S3 objects matching conditions")
                
                # Store matching objects in XCom
                context['task_instance'].xcom_push(
                    key='matching_s3_objects',
                    value={
                        'bucket': self.bucket_name,
                        'prefix': self.prefix,
                        'objects': matching_objects
                    }
                )
                
                return True
            
            logger.debug(f"No S3 objects found matching conditions for prefix: {self.prefix}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking S3 objects with prefix: {e}")
            return False


class GCSSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors Google Cloud Storage objects
    """
    
    template_fields = ('bucket_name', 'object_name', 'prefix')
    
    def __init__(self,
                 bucket_name: str,
                 object_name: Optional[str] = None,
                 prefix: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 min_object_size: int = 0,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize GCSSensor
        
        Args:
            bucket_name: GCS bucket name
            object_name: Specific object name to monitor
            prefix: Prefix to match objects
            gcp_conn_id: Airflow GCP connection ID
            min_object_size: Minimum object size in bytes
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        self.min_object_size = min_object_size
        
        if not any([object_name, prefix]):
            raise ValueError("Must specify either object_name or prefix")
    
    def poke(self, context: Context) -> bool:
        """Check if GCS object condition is met"""
        
        try:
            from google.cloud import storage
            from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
            
            # Get GCP connection
            hook = GoogleBaseHook(gcp_conn_id=self.gcp_conn_id)
            credentials = hook._get_credentials()
            
            # Create GCS client
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(self.bucket_name)
            
            if self.object_name:
                return self._check_specific_gcs_object(bucket, context)
            else:
                return self._check_gcs_objects_with_prefix(bucket, context)
                
        except Exception as e:
            logger.error(f"GCSSensor poke failed: {str(e)}")
            return False
    
    def _check_specific_gcs_object(self, bucket, context: Context) -> bool:
        """Check if specific GCS object meets conditions"""
        
        try:
            blob = bucket.blob(self.object_name)
            
            if not blob.exists():
                logger.debug(f"GCS object does not exist: gs://{self.bucket_name}/{self.object_name}")
                return False
            
            # Reload to get metadata
            blob.reload()
            
            # Check object size
            if blob.size < self.min_object_size:
                logger.debug(f"Object size {blob.size} < minimum {self.min_object_size}")
                return False
            
            logger.info(f"GCS object condition satisfied: gs://{self.bucket_name}/{self.object_name}")
            
            # Store object info in XCom
            context['task_instance'].xcom_push(
                key='gcs_object_info',
                value={
                    'bucket': self.bucket_name,
                    'name': self.object_name,
                    'size': blob.size,
                    'updated': blob.updated.isoformat() if blob.updated else None,
                    'content_type': blob.content_type
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking GCS object: {e}")
            return False
    
    def _check_gcs_objects_with_prefix(self, bucket, context: Context) -> bool:
        """Check if GCS objects with prefix meet conditions"""
        
        try:
            blobs = bucket.list_blobs(prefix=self.prefix)
            matching_objects = []
            
            for blob in blobs:
                if blob.size >= self.min_object_size:
                    object_info = {
                        'name': blob.name,
                        'size': blob.size,
                        'updated': blob.updated.isoformat() if blob.updated else None
                    }
                    matching_objects.append(object_info)
            
            if matching_objects:
                logger.info(f"Found {len(matching_objects)} GCS objects matching conditions")
                
                # Store matching objects in XCom
                context['task_instance'].xcom_push(
                    key='matching_gcs_objects',
                    value={
                        'bucket': self.bucket_name,
                        'prefix': self.prefix,
                        'objects': matching_objects
                    }
                )
                
                return True
            
            logger.debug(f"No GCS objects found matching conditions for prefix: {self.prefix}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking GCS objects with prefix: {e}")
            return False


# Example configuration usage:
"""
tasks:
  - name: "wait_for_input_file"
    type: "FILE_SENSOR"
    description: "Wait for input CSV file to arrive"
    properties:
      file_path: "/data/input/daily_export.csv"
      min_file_size: 1024
      max_age_hours: 6
      poke_interval: 300
      timeout: 3600

  - name: "wait_for_s3_objects"
    type: "S3_SENSOR"
    description: "Wait for S3 objects with prefix"
    properties:
      bucket_name: "data-lake"
      prefix: "raw/daily/{{ ds }}/"
      min_object_size: 100
      aws_conn_id: "aws_default"
      poke_interval: 60
      timeout: 1800

  - name: "wait_for_multiple_files"
    type: "FILE_SENSOR" 
    description: "Wait for multiple CSV files"
    properties:
      directory_path: "/data/input"
      file_pattern: "*.csv"
      min_file_size: 1000
      recursive: false
      poke_interval: 120
"""