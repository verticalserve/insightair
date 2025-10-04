"""
Base Engine Operations Class
Provides core processing engine operations for distributed computing across different platforms
"""

import logging
import yaml
import json
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

try:
    from airflow.operators.python import PythonOperator
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    AIRFLOW_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Airflow libraries not available: {e}")
    AIRFLOW_AVAILABLE = False


class EngineConfigError(Exception):
    """Exception raised for engine configuration errors"""
    pass


class EngineOperationError(Exception):
    """Exception raised for general engine operation errors"""
    pass


class EngineJobError(Exception):
    """Exception raised for engine job execution errors"""
    pass


class BaseEngineOperations(ABC):
    """
    Base class for processing engine operations across different platforms
    Provides common functionality for job management, configuration, and monitoring
    """
    
    def __init__(self, context):
        """
        Initialize base engine operations
        
        Args:
            context: Workflow context containing configuration and environment settings
                Required context attributes:
                - config: Workflow configuration dictionary
                - env: Environment configuration
                - data_group: Data group configuration with region, role_id, role_name
                - cluster_profile: Cluster sizing profile (small, medium, large)
                - cluster_config: Cluster configuration profiles
                - cluster_name: Name of the cluster
                - gcp_conn_id: AWS connection ID (legacy naming)
                - region: AWS region
        """
        self.context = context
        self.initialized = False
        
        # Job management
        self.active_jobs = {}
        self.job_history = []
        
        # Configuration caching
        self._config_cache = {}
        
    def init_config(self, wf_params: Dict[str, Any] = None, 
                   run_dict: Dict[str, Any] = None,
                   dyn_params: Dict[str, Any] = None):
        """
        Initialize workflow configuration
        
        Args:
            wf_params: Workflow parameters
            run_dict: Runtime parameters
            dyn_params: Dynamic parameters
        """
        if not self.initialized:
            if hasattr(self.context, 'initialize'):
                self.context.initialize(wf_params or {}, run_dict or {}, dyn_params or {})
            self.initialized = True
            
    def get_config_path(self) -> str:
        """
        Get configuration path for the workflow
        
        Returns:
            Configuration path string
        """
        if hasattr(self.context, 'get_config_path'):
            return self.context.get_config_path()
        elif hasattr(self.context, 'config') and 'path' in self.context.config:
            config_path = self.context.config['path']
            return config_path.rstrip('/') if config_path.endswith('/') else config_path
        else:
            return f"workflows/{self.context.config.get('name', 'default')}"
            
    def get_workflow_name(self) -> str:
        """
        Get workflow name
        
        Returns:
            Workflow name string
        """
        return self.context.config.get('name', 'unnamed-workflow')
        
    def upload_task_config(self, task_name: str, task: Dict[str, Any]) -> str:
        """
        Upload task configuration to S3
        
        Args:
            task_name: Name of the task
            task: Task configuration dictionary
            
        Returns:
            S3 key where config was uploaded
            
        Raises:
            EngineOperationError: If upload fails
        """
        if not AIRFLOW_AVAILABLE:
            raise EngineOperationError("Airflow libraries not available for S3 operations")
            
        try:
            config_bucket = self.context.env['config_bucket'].replace("s3://", "")
            config_path = self.get_config_path()
            config_yaml = yaml.dump(task.get("properties", task))
            s3_key = f"dags/{config_path}/{task_name}.yaml"
            
            s3_hook = S3Hook(aws_conn_id=self.context.gcp_conn_id)
            s3_hook.load_string(
                string_data=config_yaml, 
                key=s3_key, 
                bucket_name=config_bucket, 
                replace=True
            )
            
            logger.info(f"Uploaded task config for {task_name} to s3://{config_bucket}/{s3_key}")
            return s3_key
            
        except Exception as e:
            error_msg = f"Failed to upload task config for {task_name}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_cluster_profile_config(self) -> Dict[str, Any]:
        """
        Get cluster configuration based on profile
        
        Returns:
            Cluster configuration dictionary
            
        Raises:
            EngineConfigError: If profile not found
        """
        try:
            cluster_profile = getattr(self.context, 'cluster_profile', 'medium')
            
            if hasattr(self.context, 'cluster_config') and 'profiles' in self.context.cluster_config:
                profiles = self.context.cluster_config['profiles']
                if cluster_profile in profiles:
                    return profiles[cluster_profile]
                    
            # Default configurations if not specified
            default_profiles = {
                'small': {
                    'master_machine': 'm5.large',
                    'worker_machine': 'm5.large',
                    'masters': 1,
                    'workers': 2,
                    'worker_type': 'G.1X',
                    'num_workers': 5
                },
                'medium': {
                    'master_machine': 'm5.xlarge',
                    'worker_machine': 'm5.xlarge',
                    'masters': 1,
                    'workers': 4,
                    'worker_type': 'G.1X',
                    'num_workers': 10
                },
                'large': {
                    'master_machine': 'm5.2xlarge',
                    'worker_machine': 'm5.2xlarge',
                    'masters': 1,
                    'workers': 8,
                    'worker_type': 'G.2X',
                    'num_workers': 20
                }
            }
            
            if cluster_profile in default_profiles:
                return default_profiles[cluster_profile]
            else:
                logger.warning(f"Unknown cluster profile '{cluster_profile}', using 'medium'")
                return default_profiles['medium']
                
        except Exception as e:
            error_msg = f"Failed to get cluster profile config: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def get_aws_config(self) -> Tuple[str, str, str]:
        """
        Get AWS configuration (region, role_id, role_name)
        
        Returns:
            Tuple of (region, role_id, role_name)
            
        Raises:
            EngineConfigError: If AWS config not available
        """
        try:
            if hasattr(self.context, 'data_group') and self.context.data_group:
                region = self.context.data_group.get('region', 'us-east-1')
                role_id = self.context.data_group.get('role_id')
                role_name = self.context.data_group.get('role_name')
                
                if not role_id or not role_name:
                    raise EngineConfigError("AWS role_id and role_name must be specified in data_group")
                    
                return region, role_id, role_name
            else:
                # Fallback to context attributes
                region = getattr(self.context, 'region', 'us-east-1')
                role_id = getattr(self.context, 'role_id', None)
                role_name = getattr(self.context, 'role_name', None)
                
                if not role_id or not role_name:
                    raise EngineConfigError("AWS role configuration not found in context")
                    
                return region, role_id, role_name
                
        except Exception as e:
            error_msg = f"Failed to get AWS configuration: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def get_s3_config(self) -> Tuple[str, str]:
        """
        Get S3 configuration (bucket, base_path)
        
        Returns:
            Tuple of (config_bucket, config_path)
        """
        config_bucket = self.context.env['config_bucket'].replace("s3://", "")
        config_path = self.get_config_path()
        return config_bucket, config_path
        
    def format_job_name(self, task_name: str, prefix: str = None) -> str:
        """
        Format standardized job name
        
        Args:
            task_name: Name of the task
            prefix: Optional prefix for the job name
            
        Returns:
            Formatted job name
        """
        workflow_name = self.get_workflow_name()
        base_name = f"{workflow_name}-{task_name}"
        
        if prefix:
            base_name = f"{prefix}-{base_name}"
            
        # Ensure job name meets platform requirements (alphanumeric, hyphens, underscores)
        job_name = "".join(c if c.isalnum() or c in '-_' else '-' for c in base_name)
        
        # Limit length to reasonable size
        if len(job_name) > 64:
            job_name = job_name[:61] + "..."
            
        return job_name
        
    def build_job_arguments(self, task_name: str, task: Dict[str, Any], 
                          job_type: str, extra_args: Dict[str, str] = None) -> Dict[str, str]:
        """
        Build common job arguments
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job being executed
            extra_args: Additional arguments to include
            
        Returns:
            Dictionary of job arguments
        """
        config_bucket, config_path = self.get_s3_config()
        region, _, _ = self.get_aws_config()
        
        arguments = {
            'config_path': f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
            'job_type': job_type,
            'task_name': task_name,
            'workflow_name': self.get_workflow_name(),
            'AWS_REGION': region,
        }
        
        if extra_args:
            arguments.update(extra_args)
            
        return arguments
        
    def monitor_job_status(self, job_id: str, timeout_minutes: int = 60) -> str:
        """
        Monitor job status until completion
        
        Args:
            job_id: Job identifier
            timeout_minutes: Maximum time to wait for completion
            
        Returns:
            Final job status
            
        Raises:
            EngineJobError: If job fails or times out
        """
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while True:
            try:
                status = self.get_job_status(job_id)
                
                if self.is_job_terminal_state(status):
                    if self.is_job_successful(status):
                        logger.info(f"Job {job_id} completed successfully with status: {status}")
                        return status
                    else:
                        error_msg = f"Job {job_id} failed with status: {status}"
                        logger.error(error_msg)
                        raise EngineJobError(error_msg)
                        
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > timeout_seconds:
                    error_msg = f"Job {job_id} timed out after {timeout_minutes} minutes"
                    logger.error(error_msg)
                    raise EngineJobError(error_msg)
                    
                # Wait before next check
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                if isinstance(e, EngineJobError):
                    raise
                error_msg = f"Error monitoring job {job_id}: {e}"
                logger.error(error_msg)
                raise EngineJobError(error_msg)
                
    def pre_process(self, task: Dict[str, Any]):
        """
        Execute pre-processing steps for a task
        
        Args:
            task: Task configuration
        """
        if 'pre_script' in task:
            logger.info(f"Executing pre-processing script for task")
            # Implementation would be platform-specific
            
    def post_process(self, task: Dict[str, Any]):
        """
        Execute post-processing steps for a task
        
        Args:
            task: Task configuration
        """
        if 'post_script' in task:
            logger.info(f"Executing post-processing script for task")
            # Implementation would be platform-specific
            
    def create_python_operator(self, task_name: str, python_callable, **kwargs) -> 'PythonOperator':
        """
        Create a Python operator for Airflow
        
        Args:
            task_name: Name of the task
            python_callable: Python function to call
            **kwargs: Additional keyword arguments
            
        Returns:
            PythonOperator instance
            
        Raises:
            EngineOperationError: If Airflow not available
        """
        if not AIRFLOW_AVAILABLE:
            raise EngineOperationError("Airflow libraries not available")
            
        return PythonOperator(
            task_id=task_name,
            python_callable=python_callable,
            op_kwargs=kwargs
        )
        
    def get_engine_info(self) -> Dict[str, Any]:
        """
        Get engine configuration information
        
        Returns:
            Dictionary with engine info (without sensitive data)
        """
        try:
            region, _, _ = self.get_aws_config()
            config_bucket, config_path = self.get_s3_config()
            cluster_config = self.get_cluster_profile_config()
            
            return {
                'engine_type': self.__class__.__name__,
                'workflow_name': self.get_workflow_name(),
                'cluster_profile': getattr(self.context, 'cluster_profile', 'medium'),
                'region': region,
                'config_bucket': config_bucket,
                'config_path': config_path,
                'cluster_config': cluster_config,
                'initialized': self.initialized,
                'active_jobs': len(self.active_jobs),
                'total_jobs_run': len(self.job_history)
            }
        except Exception as e:
            logger.warning(f"Error getting engine info: {e}")
            return {
                'engine_type': self.__class__.__name__,
                'error': str(e)
            }
            
    # Abstract methods that must be implemented by subclasses
    
    @abstractmethod
    def submit_job(self, task_name: str, task: Dict[str, Any], job_type: str) -> str:
        """
        Submit a job to the processing engine
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to submit
            
        Returns:
            Job identifier
        """
        pass
        
    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """
        Get current status of a job
        
        Args:
            job_id: Job identifier
            
        Returns:
            Current job status
        """
        pass
        
    @abstractmethod
    def is_job_terminal_state(self, status: str) -> bool:
        """
        Check if job status indicates terminal state
        
        Args:
            status: Job status string
            
        Returns:
            True if job is in terminal state
        """
        pass
        
    @abstractmethod
    def is_job_successful(self, status: str) -> bool:
        """
        Check if job status indicates success
        
        Args:
            status: Job status string
            
        Returns:
            True if job completed successfully
        """
        pass
        
    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancellation was successful
        """
        pass
        
    @abstractmethod
    def get_job_logs(self, job_id: str) -> str:
        """
        Get logs for a job
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job logs as string
        """
        pass