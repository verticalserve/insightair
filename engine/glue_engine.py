"""
AWS Glue Engine Operations
Provides AWS Glue job management and execution capabilities for serverless data processing
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from .base_engine import BaseEngineOperations, EngineOperationError, EngineJobError, EngineConfigError

logger = logging.getLogger(__name__)

try:
    from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
    GLUE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS Glue libraries not available: {e}")
    GLUE_AVAILABLE = False


class GlueEngineOperations(BaseEngineOperations):
    """
    AWS Glue engine operations for serverless data processing
    Provides job submission, monitoring, and management for Glue ETL jobs
    """
    
    def __init__(self, context):
        """
        Initialize AWS Glue engine operations
        
        Args:
            context: Workflow context containing Glue and AWS configuration
        """
        super().__init__(context)
        
        # Glue-specific configuration
        self.glue_version = getattr(context, 'glue_version', '5.0')
        self.job_script_location = getattr(context, 'job_script_location', 'scripts/glue_job.py')
        self.requirements_file = getattr(context, 'requirements_file', 'install/glue_db_requirements.txt')
        self.spark_zip_location = getattr(context, 'spark_zip_location', 'spark/insightspark.zip')
        
        # Job status mappings
        self.terminal_states = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'}
        self.success_states = {'SUCCEEDED'}
        
        # Active Glue jobs tracking
        self.glue_jobs = {}
        
    def get_glue_cluster_config(self) -> Tuple[str, int]:
        """
        Get Glue cluster configuration based on profile
        
        Returns:
            Tuple of (worker_type, num_workers)
        """
        try:
            cluster_config = self.get_cluster_profile_config()
            
            worker_type = cluster_config.get('worker_type', 'G.1X')
            num_workers = cluster_config.get('num_workers', 10)
            
            # Validate Glue worker types
            valid_worker_types = ['Standard', 'G.1X', 'G.2X', 'G.025X', 'G.4X', 'G.8X', 'Z.2X']
            if worker_type not in valid_worker_types:
                logger.warning(f"Invalid Glue worker type '{worker_type}', using 'G.1X'")
                worker_type = 'G.1X'
                
            # Validate worker count
            if num_workers < 2:
                logger.warning(f"Glue requires minimum 2 workers, adjusting from {num_workers}")
                num_workers = 2
            elif num_workers > 100:
                logger.warning(f"Glue worker count {num_workers} is high, consider cost implications")
                
            return worker_type, num_workers
            
        except Exception as e:
            logger.warning(f"Error getting Glue cluster config: {e}, using defaults")
            return 'G.1X', 10
            
    def build_glue_job_arguments(self, task_name: str, task: Dict[str, Any], 
                                job_type: str) -> Dict[str, str]:
        """
        Build Glue job arguments including bookmarking
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job being executed
            
        Returns:
            Dictionary of Glue job arguments
        """
        config_bucket, config_path = self.get_s3_config()
        region, _, _ = self.get_aws_config()
        
        # Base arguments
        arguments = self.build_job_arguments(task_name, task, job_type, {
            'additional-python-modules': f's3://{config_bucket}/{self.requirements_file}',
            'python-modules-installer-option': '-r',
            'extra-py-files': f's3://{config_bucket}/{self.spark_zip_location}',
        })
        
        # Handle job bookmarking
        task_properties = task.get('properties', {})
        if 'bookmark' in task_properties and task_properties['bookmark']:
            arguments['job-bookmark-option'] = 'job-bookmark-enable'
            logger.info(f"Job bookmark enabled for task {task_name}")
        else:
            arguments['job-bookmark-option'] = 'job-bookmark-disable'
            
        # Add Glue-specific prefixes to arguments
        glue_arguments = {}
        for key, value in arguments.items():
            glue_key = f'--{key}' if not key.startswith('--') else key
            glue_arguments[glue_key] = str(value)
            
        return glue_arguments
        
    def create_glue_job_config(self, task_name: str, task: Dict[str, Any], 
                             job_type: str) -> Dict[str, Any]:
        """
        Create complete Glue job configuration
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job being executed
            
        Returns:
            Glue job configuration dictionary
        """
        try:
            config_bucket, _ = self.get_s3_config()
            region, role_id, _ = self.get_aws_config()
            worker_type, num_workers = self.get_glue_cluster_config()
            
            # Build job arguments
            default_arguments = self.build_glue_job_arguments(task_name, task, job_type)
            
            # Job configuration
            job_config = {
                'Role': role_id,
                'GlueVersion': self.glue_version,
                'WorkerType': worker_type,
                'NumberOfWorkers': num_workers,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{config_bucket}/{self.job_script_location}',
                },
                'DefaultArguments': default_arguments,
                'Timeout': task.get('timeout_minutes', 2880),  # 48 hours default
                'MaxRetries': task.get('max_retries', 1),
                'SecurityConfiguration': task.get('security_configuration'),
                'Tags': self._build_job_tags(task_name, task)
            }
            
            # Remove None values
            job_config = {k: v for k, v in job_config.items() if v is not None}
            
            # Add connections if specified
            if 'connections' in task:
                job_config['Connections'] = {'Connections': task['connections']}
                
            return job_config
            
        except Exception as e:
            error_msg = f"Failed to create Glue job config: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def _build_job_tags(self, task_name: str, task: Dict[str, Any]) -> Dict[str, str]:
        """
        Build tags for Glue job
        
        Args:
            task_name: Name of the task
            task: Task configuration
            
        Returns:
            Dictionary of tags
        """
        tags = {
            'Workflow': self.get_workflow_name(),
            'Task': task_name,
            'Engine': 'Glue',
            'CreatedBy': 'InsightAir',
            'Environment': getattr(self.context, 'environment', 'production')
        }
        
        # Add custom tags from task
        if 'tags' in task:
            tags.update(task['tags'])
            
        return tags
        
    def submit_job(self, task_name: str, task: Dict[str, Any], job_type: str) -> str:
        """
        Submit a job to AWS Glue
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to submit
            
        Returns:
            Job run ID
            
        Raises:
            EngineOperationError: If job submission fails
        """
        if not GLUE_AVAILABLE:
            raise EngineOperationError("AWS Glue libraries not available")
            
        try:
            # Initialize configuration
            self.init_config()
            
            # Upload task configuration
            self.upload_task_config(task_name, task)
            
            # Execute pre-processing
            self.pre_process(task)
            
            # Create job configuration
            glue_job_name = self.format_job_name(task_name, 'glue')
            job_config = self.create_glue_job_config(task_name, task, job_type)
            
            config_bucket, _ = self.get_s3_config()
            region, _, role_name = self.get_aws_config()
            
            # Create Glue hook
            glue_hook = GlueJobHook(
                aws_conn_id=self.context.gcp_conn_id,
                job_name=glue_job_name,
                region_name=region,
                s3_bucket=config_bucket,
                script_location=f'{config_bucket}/{self.job_script_location}',
                iam_role_name=role_name,
                desc=task.get('description', f'Glue job for {task_name}'),
                update_config=True,
                create_job_kwargs=job_config
            )
            
            # Initialize and start the job
            response = glue_hook.initialize_job()
            job_run_id = response['JobRunId']
            
            # Track the job
            self.active_jobs[job_run_id] = {
                'task_name': task_name,
                'job_type': job_type,
                'glue_job_name': glue_job_name,
                'start_time': datetime.now(),
                'hook': glue_hook
            }
            
            logger.info(f"Submitted Glue job '{glue_job_name}' with run ID: {job_run_id}")
            return job_run_id
            
        except Exception as e:
            error_msg = f"Failed to submit Glue job for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_status(self, job_id: str) -> str:
        """
        Get current status of a Glue job
        
        Args:
            job_id: Job run ID
            
        Returns:
            Current job status
            
        Raises:
            EngineOperationError: If status check fails
        """
        try:
            if job_id not in self.active_jobs:
                raise EngineOperationError(f"Job {job_id} not found in active jobs")
                
            job_info = self.active_jobs[job_id]
            glue_hook = job_info['hook']
            glue_job_name = job_info['glue_job_name']
            
            # Get job run details
            job_run = glue_hook.get_job_run(
                job_name=glue_job_name,
                run_id=job_id
            )
            
            status = job_run['JobRun']['JobRunState']
            
            # Update job info
            job_info['last_status'] = status
            job_info['last_check'] = datetime.now()
            
            return status
            
        except Exception as e:
            error_msg = f"Failed to get status for Glue job {job_id}: {e}"
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
        return status in self.terminal_states
        
    def is_job_successful(self, status: str) -> bool:
        """
        Check if job status indicates success
        
        Args:
            status: Job status string
            
        Returns:
            True if job completed successfully
        """
        return status in self.success_states
        
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Glue job
        
        Args:
            job_id: Job run ID
            
        Returns:
            True if cancellation was successful
            
        Raises:
            EngineOperationError: If cancellation fails
        """
        try:
            if job_id not in self.active_jobs:
                logger.warning(f"Job {job_id} not found in active jobs")
                return False
                
            job_info = self.active_jobs[job_id]
            glue_hook = job_info['hook']
            glue_job_name = job_info['glue_job_name']
            
            # Stop the job run
            glue_hook.stop_job_run(
                job_name=glue_job_name,
                run_id=job_id
            )
            
            logger.info(f"Cancelled Glue job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to cancel Glue job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_logs(self, job_id: str) -> str:
        """
        Get logs for a Glue job
        
        Args:
            job_id: Job run ID
            
        Returns:
            Job logs as string
            
        Raises:
            EngineOperationError: If log retrieval fails
        """
        try:
            if job_id not in self.active_jobs:
                raise EngineOperationError(f"Job {job_id} not found in active jobs")
                
            job_info = self.active_jobs[job_id]
            glue_hook = job_info['hook']
            glue_job_name = job_info['glue_job_name']
            
            # Get job run details which includes log information
            job_run = glue_hook.get_job_run(
                job_name=glue_job_name,
                run_id=job_id
            )
            
            # Extract log information
            logs = []
            if 'ErrorDetails' in job_run['JobRun']:
                logs.append(f"Error: {job_run['JobRun']['ErrorDetails']['ErrorMessage']}")
                
            # Note: For complete log access, you would typically need to:
            # 1. Get the CloudWatch log group/stream from job run details
            # 2. Use CloudWatch Logs API to retrieve full logs
            # This is a simplified implementation
            
            return '\n'.join(logs) if logs else f"No error logs found for job {job_id}"
            
        except Exception as e:
            error_msg = f"Failed to get logs for Glue job {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def run_glue_job_sync(self, task_name: str, task: Dict[str, Any], 
                         job_type: str, timeout_minutes: int = 60) -> Dict[str, Any]:
        """
        Run a Glue job synchronously and wait for completion
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to run
            timeout_minutes: Maximum time to wait for completion
            
        Returns:
            Job execution results
            
        Raises:
            EngineJobError: If job execution fails
        """
        try:
            # Submit the job
            job_run_id = self.submit_job(task_name, task, job_type)
            
            # Monitor until completion
            final_status = self.monitor_job_status(job_run_id, timeout_minutes)
            
            # Execute post-processing
            self.post_process(task)
            
            # Get job information
            job_info = self.active_jobs.get(job_run_id, {})
            
            # Move to history
            job_info['end_time'] = datetime.now()
            job_info['final_status'] = final_status
            job_info['duration'] = (job_info['end_time'] - job_info['start_time']).total_seconds()
            
            self.job_history.append(job_info)
            if job_run_id in self.active_jobs:
                del self.active_jobs[job_run_id]
                
            result = {
                'success': True,
                'job_run_id': job_run_id,
                'final_status': final_status,
                'task_name': task_name,
                'job_type': job_type,
                'duration_seconds': job_info.get('duration', 0),
                'glue_job_name': job_info.get('glue_job_name')
            }
            
            logger.info(f"Glue job completed successfully: {result}")
            return result
            
        except Exception as e:
            # Clean up failed job from active jobs
            if job_run_id and job_run_id in self.active_jobs:
                job_info = self.active_jobs[job_run_id]
                job_info['end_time'] = datetime.now()
                job_info['final_status'] = 'FAILED'
                job_info['error'] = str(e)
                self.job_history.append(job_info)
                del self.active_jobs[job_run_id]
                
            error_msg = f"Glue job execution failed for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineJobError(error_msg)
            
    def list_active_jobs(self) -> List[Dict[str, Any]]:
        """
        List all active Glue jobs
        
        Returns:
            List of active job information
        """
        active_list = []
        for job_id, job_info in self.active_jobs.items():
            active_list.append({
                'job_run_id': job_id,
                'task_name': job_info['task_name'],
                'job_type': job_info['job_type'],
                'glue_job_name': job_info['glue_job_name'],
                'start_time': job_info['start_time'].isoformat(),
                'last_status': job_info.get('last_status', 'UNKNOWN'),
                'last_check': job_info.get('last_check', datetime.now()).isoformat()
            })
        return active_list
        
    def get_glue_info(self) -> Dict[str, Any]:
        """
        Get Glue engine configuration information
        
        Returns:
            Dictionary with Glue info
        """
        base_info = self.get_engine_info()
        
        try:
            worker_type, num_workers = self.get_glue_cluster_config()
            
            glue_info = {
                'glue_version': self.glue_version,
                'worker_type': worker_type,
                'num_workers': num_workers,
                'job_script_location': self.job_script_location,
                'requirements_file': self.requirements_file,
                'spark_zip_location': self.spark_zip_location,
                'active_jobs': self.list_active_jobs()
            }
            
            base_info.update(glue_info)
            
        except Exception as e:
            base_info['glue_error'] = str(e)
            
        return base_info