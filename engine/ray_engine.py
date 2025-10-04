"""
Ray Engine Operations
Provides Ray cluster job management and execution capabilities for distributed ML and data processing
"""

import logging
import json
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_engine import BaseEngineOperations, EngineOperationError, EngineJobError, EngineConfigError

logger = logging.getLogger(__name__)

try:
    from ray.job_submission import JobSubmissionClient, JobStatus
    RAY_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Ray libraries not available: {e}")
    RAY_AVAILABLE = False


class RayEngineOperations(BaseEngineOperations):
    """
    Ray engine operations for distributed ML and data processing
    Provides job submission and monitoring for Ray clusters
    """
    
    def __init__(self, context):
        """
        Initialize Ray engine operations
        
        Args:
            context: Workflow context containing Ray cluster configuration
        """
        super().__init__(context)
        
        # Ray-specific configuration
        self.ray_cluster_url = getattr(context, 'ray_cluster_url', 'http://localhost:8265/')
        self.working_dir_archive = getattr(context, 'working_dir_archive', 'ray/insightray_python15.zip')
        self.job_script_location = getattr(context, 'job_script_location', 'insightray/templates/ray_job.py')
        self.default_runtime_env = getattr(context, 'default_runtime_env', {})
        
        # Ray job status mappings
        self.terminal_states = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}
        self.success_states = {JobStatus.SUCCEEDED}
        
        # Job tracking
        self.ray_client = None
        self.active_ray_jobs = {}
        
    def _get_ray_client(self) -> 'JobSubmissionClient':
        """
        Get or create Ray job submission client
        
        Returns:
            JobSubmissionClient instance
            
        Raises:
            EngineOperationError: If Ray client cannot be created
        """
        if not RAY_AVAILABLE:
            raise EngineOperationError("Ray libraries not available")
            
        try:
            if not self.ray_client:
                self.ray_client = JobSubmissionClient(self.ray_cluster_url)
                
            return self.ray_client
            
        except Exception as e:
            error_msg = f"Failed to create Ray client for {self.ray_cluster_url}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def build_runtime_environment(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build Ray runtime environment configuration
        
        Args:
            task: Task configuration
            
        Returns:
            Runtime environment dictionary
        """
        try:
            config_bucket, _ = self.get_s3_config()
            
            # Base runtime environment
            runtime_env = {
                "working_dir": f's3://{config_bucket}/{self.working_dir_archive}',
                "pip": [
                    # Core dependencies for ML and data processing
                    "pdf2image",
                    "paddlepaddle",
                    "paddleocr",
                    "pillow",
                    "raydp",
                    "pymupdf",
                    "langchain_community",
                    "langchain_openai",
                    "requests_aws4auth",
                    "boto3",
                    "openai",
                    "pyyaml",
                    "opensearch-py",
                    "markitdown",
                    "markdown",
                    "langchain-text-splitters",
                    "msal",
                    "requests",
                    "mysql-connector-python",
                    "requests-aws4auth",
                    "langchain-core",
                    "langchain-community",
                    "langchain_aws",
                    "python-pptx",
                    # Additional ML libraries
                    "scikit-learn",
                    "pandas",
                    "numpy",
                    "pyarrow",
                    "torch",
                    "transformers",
                    "datasets"
                ],
                "env_vars": {
                    "HF_HUB_OFFLINE": "0",
                    "RAY_memory_monitor_refresh_ms": "0",
                    "PYTHONPATH": "/tmp/ray/session_latest/runtime_resources/_ray_pkg_working_dir/",
                    "RAY_DISABLE_IMPORT_WARNING": "1"
                }
            }
            
            # Override with default runtime environment
            if self.default_runtime_env:
                runtime_env.update(self.default_runtime_env)
                
            # Override with task-specific runtime environment
            task_runtime_env = task.get('runtime_env', {})
            if task_runtime_env:
                # Merge pip dependencies
                if 'pip' in task_runtime_env:
                    runtime_env['pip'].extend(task_runtime_env['pip'])
                    
                # Merge environment variables
                if 'env_vars' in task_runtime_env:
                    runtime_env['env_vars'].update(task_runtime_env['env_vars'])
                    
                # Update other runtime environment settings
                for key, value in task_runtime_env.items():
                    if key not in ['pip', 'env_vars']:
                        runtime_env[key] = value
                        
            return runtime_env
            
        except Exception as e:
            error_msg = f"Failed to build Ray runtime environment: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def build_job_config(self, task_name: str, task: Dict[str, Any], job_type: str) -> Dict[str, Any]:
        """
        Build Ray job configuration
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job being executed
            
        Returns:
            Job configuration dictionary
        """
        try:
            config_bucket, config_path = self.get_s3_config()
            region, _, _ = self.get_aws_config()
            workflow_name = f'{self.get_workflow_name()}-{task_name}'
            
            job_config = {
                "config_path": f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                "AWS_REGION": region,
                "job_type": job_type,
                "workflow": workflow_name,
                "task_name": task_name
            }
            
            # Add custom job configuration from task
            task_job_config = task.get('job_config', {})
            job_config.update(task_job_config)
            
            return job_config
            
        except Exception as e:
            error_msg = f"Failed to build Ray job config: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def submit_job(self, task_name: str, task: Dict[str, Any], job_type: str) -> str:
        """
        Submit a job to Ray cluster
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to submit
            
        Returns:
            Ray job ID
            
        Raises:
            EngineOperationError: If job submission fails
        """
        if not RAY_AVAILABLE:
            raise EngineOperationError("Ray libraries not available")
            
        try:
            # Initialize configuration
            self.init_config()
            
            # Upload task configuration
            self.upload_task_config(task_name, task)
            
            # Execute pre-processing
            self.pre_process(task)
            
            # Get Ray client
            client = self._get_ray_client()
            
            # Build job configuration
            job_config = self.build_job_config(task_name, task, job_type)
            runtime_env = self.build_runtime_environment(task)
            
            # Add job config to environment variables
            runtime_env['env_vars']['JOB_CONFIG'] = json.dumps(job_config)
            
            # Build entrypoint command
            entrypoint = f'python {self.job_script_location}'
            
            # Add custom entrypoint arguments from task
            entrypoint_args = task.get('entrypoint_args', [])
            if entrypoint_args:
                entrypoint = f'{entrypoint} {" ".join(entrypoint_args)}'
                
            # Submit job to Ray
            job_id = client.submit_job(
                entrypoint=entrypoint,
                runtime_env=runtime_env,
                job_id=task.get('custom_job_id'),  # Allow custom job IDs
                metadata=task.get('metadata', {}),
                submission_id=task.get('submission_id')
            )
            
            # Track the job
            self.active_ray_jobs[job_id] = {
                'task_name': task_name,
                'job_type': job_type,
                'start_time': datetime.now(),
                'client': client,
                'job_config': job_config,
                'runtime_env': runtime_env
            }
            
            logger.info(f"Submitted Ray job '{task_name}' with ID: {job_id}")
            return job_id
            
        except Exception as e:
            error_msg = f"Failed to submit Ray job for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_status(self, job_id: str) -> str:
        """
        Get current status of a Ray job
        
        Args:
            job_id: Ray job ID
            
        Returns:
            Current job status as string
            
        Raises:
            EngineOperationError: If status check fails
        """
        try:
            if job_id not in self.active_ray_jobs:
                # Try to get status from Ray cluster anyway
                client = self._get_ray_client()
                status = client.get_job_status(job_id)
                return status.name
                
            job_info = self.active_ray_jobs[job_id]
            client = job_info['client']
            
            status = client.get_job_status(job_id)
            
            # Update job info
            job_info['last_status'] = status
            job_info['last_check'] = datetime.now()
            
            return status.name
            
        except Exception as e:
            error_msg = f"Failed to get status for Ray job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def is_job_terminal_state(self, status: str) -> bool:
        """
        Check if job status indicates terminal state
        
        Args:
            status: Job status string
            
        Returns:
            True if job is in terminal state
        """
        try:
            if isinstance(status, str):
                status_enum = JobStatus[status]
            else:
                status_enum = status
                
            return status_enum in self.terminal_states
            
        except (KeyError, AttributeError):
            # If we can't parse the status, assume it's terminal to avoid infinite loops
            logger.warning(f"Unknown Ray job status: {status}")
            return True
            
    def is_job_successful(self, status: str) -> bool:
        """
        Check if job status indicates success
        
        Args:
            status: Job status string
            
        Returns:
            True if job completed successfully
        """
        try:
            if isinstance(status, str):
                status_enum = JobStatus[status]
            else:
                status_enum = status
                
            return status_enum in self.success_states
            
        except (KeyError, AttributeError):
            logger.warning(f"Unknown Ray job status: {status}")
            return False
            
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Ray job
        
        Args:
            job_id: Ray job ID
            
        Returns:
            True if cancellation was successful
            
        Raises:
            EngineOperationError: If cancellation fails
        """
        try:
            client = self._get_ray_client()
            
            # Stop the job
            client.stop_job(job_id)
            
            logger.info(f"Cancelled Ray job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to cancel Ray job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_logs(self, job_id: str) -> str:
        """
        Get logs for a Ray job
        
        Args:
            job_id: Ray job ID
            
        Returns:
            Job logs as string
            
        Raises:
            EngineOperationError: If log retrieval fails
        """
        try:
            client = self._get_ray_client()
            
            # Get job logs
            logs = client.get_job_logs(job_id)
            
            return logs if logs else f"No logs available for Ray job {job_id}"
            
        except Exception as e:
            error_msg = f"Failed to get logs for Ray job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_info(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a Ray job
        
        Args:
            job_id: Ray job ID
            
        Returns:
            Dictionary with job information
        """
        try:
            client = self._get_ray_client()
            
            # Get job info from Ray
            job_info = client.get_job_info(job_id)
            
            # Add local tracking information if available
            if job_id in self.active_ray_jobs:
                local_info = self.active_ray_jobs[job_id]
                job_info.update({
                    'task_name': local_info['task_name'],
                    'job_type': local_info['job_type'],
                    'start_time': local_info['start_time'].isoformat(),
                    'last_check': local_info.get('last_check', datetime.now()).isoformat()
                })
                
            return job_info
            
        except Exception as e:
            error_msg = f"Failed to get info for Ray job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def run_ray_job_sync(self, task_name: str, task: Dict[str, Any], 
                        job_type: str, timeout_minutes: int = 60,
                        poll_interval_seconds: int = 10) -> Dict[str, Any]:
        """
        Run a Ray job synchronously and wait for completion
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to run
            timeout_minutes: Maximum time to wait for completion
            poll_interval_seconds: How often to check job status
            
        Returns:
            Job execution results
            
        Raises:
            EngineJobError: If job execution fails
        """
        job_id = None
        
        try:
            # Submit the job
            job_id = self.submit_job(task_name, task, job_type)
            
            # Monitor job with custom polling
            start_time = time.time()
            timeout_seconds = timeout_minutes * 60
            
            while True:
                status = self.get_job_status(job_id)
                
                if self.is_job_terminal_state(status):
                    if self.is_job_successful(status):
                        logger.info(f"Ray job {job_id} completed successfully")
                        break
                    else:
                        error_msg = f"Ray job {job_id} failed with status: {status}"
                        logger.error(error_msg)
                        raise EngineJobError(error_msg)
                        
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > timeout_seconds:
                    error_msg = f"Ray job {job_id} timed out after {timeout_minutes} minutes"
                    logger.error(error_msg)
                    raise EngineJobError(error_msg)
                    
                logger.debug(f"Ray job {job_id} status: {status}")
                time.sleep(poll_interval_seconds)
                
            # Execute post-processing
            self.post_process(task)
            
            # Get final job information
            job_info = self.active_ray_jobs.get(job_id, {})
            
            # Move to history
            job_info['end_time'] = datetime.now()
            job_info['final_status'] = status
            job_info['duration'] = (job_info['end_time'] - job_info['start_time']).total_seconds()
            
            self.job_history.append(job_info)
            if job_id in self.active_ray_jobs:
                del self.active_ray_jobs[job_id]
                
            result = {
                'success': True,
                'job_id': job_id,
                'final_status': status,
                'task_name': task_name,
                'job_type': job_type,
                'duration_seconds': job_info.get('duration', 0),
                'ray_cluster_url': self.ray_cluster_url
            }
            
            logger.info(f"Ray job completed successfully: {result}")
            return result
            
        except Exception as e:
            # Clean up failed job from active jobs
            if job_id and job_id in self.active_ray_jobs:
                job_info = self.active_ray_jobs[job_id]
                job_info['end_time'] = datetime.now()
                job_info['final_status'] = 'FAILED'
                job_info['error'] = str(e)
                self.job_history.append(job_info)
                del self.active_ray_jobs[job_id]
                
            error_msg = f"Ray job execution failed for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineJobError(error_msg)
            
    def list_cluster_jobs(self) -> List[Dict[str, Any]]:
        """
        List all jobs on the Ray cluster
        
        Returns:
            List of job information dictionaries
        """
        try:
            client = self._get_ray_client()
            
            # Get all jobs from Ray cluster
            jobs = client.list_jobs()
            
            job_list = []
            for job in jobs:
                job_info = {
                    'job_id': job.job_id,
                    'status': job.status.name,
                    'start_time': job.start_time,
                    'end_time': job.end_time,
                    'submission_id': job.submission_id,
                    'metadata': job.metadata
                }
                
                # Add local tracking info if available
                if job.job_id in self.active_ray_jobs:
                    local_info = self.active_ray_jobs[job.job_id]
                    job_info.update({
                        'task_name': local_info['task_name'],
                        'job_type': local_info['job_type']
                    })
                    
                job_list.append(job_info)
                
            return job_list
            
        except Exception as e:
            logger.error(f"Failed to list Ray cluster jobs: {e}")
            return []
            
    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get Ray cluster status information
        
        Returns:
            Cluster status dictionary
        """
        try:
            client = self._get_ray_client()
            
            # Try to get cluster info by listing jobs (indicates cluster is reachable)
            jobs = client.list_jobs()
            
            return {
                'cluster_url': self.ray_cluster_url,
                'accessible': True,
                'total_jobs': len(jobs),
                'active_jobs_tracked': len(self.active_ray_jobs)
            }
            
        except Exception as e:
            return {
                'cluster_url': self.ray_cluster_url,
                'accessible': False,
                'error': str(e),
                'active_jobs_tracked': len(self.active_ray_jobs)
            }
            
    def get_ray_info(self) -> Dict[str, Any]:
        """
        Get Ray engine configuration information
        
        Returns:
            Dictionary with Ray info
        """
        base_info = self.get_engine_info()
        
        try:
            ray_info = {
                'ray_cluster_url': self.ray_cluster_url,
                'working_dir_archive': self.working_dir_archive,
                'job_script_location': self.job_script_location,
                'cluster_status': self.get_cluster_status(),
                'active_ray_jobs': len(self.active_ray_jobs)
            }
            
            base_info.update(ray_info)
            
        except Exception as e:
            base_info['ray_error'] = str(e)
            
        return base_info