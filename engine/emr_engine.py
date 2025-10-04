"""
AWS EMR Engine Operations
Provides AWS EMR cluster management and job execution capabilities for large-scale data processing
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

from .base_engine import BaseEngineOperations, EngineOperationError, EngineJobError, EngineConfigError

logger = logging.getLogger(__name__)

try:
    from airflow.providers.amazon.aws.hooks.emr import EmrHook
    from airflow.operators.python import PythonOperator
    EMR_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AWS EMR libraries not available: {e}")
    EMR_AVAILABLE = False


class EMREngineOperations(BaseEngineOperations):
    """
    AWS EMR engine operations for large-scale distributed data processing
    Provides cluster management and Spark job execution on EMR
    """
    
    def __init__(self, context):
        """
        Initialize AWS EMR engine operations
        
        Args:
            context: Workflow context containing EMR and AWS configuration
        """
        super().__init__(context)
        
        # EMR-specific configuration
        self.emr_release_label = getattr(context, 'emr_release_label', 'emr-7.1.0')
        self.custom_ami_id = getattr(context, 'custom_ami_id', 'ami-01252790382ba5844')
        self.job_script_location = getattr(context, 'job_script_location', 'spark_job.py')
        self.environment_archive = getattr(context, 'environment_archive', 'phi3env.zip')
        self.spark_archive = getattr(context, 'spark_archive', 'insightspark.zip')
        self.log_uri = getattr(context, 'log_uri', None)
        
        # EMR job step status mappings
        self.terminal_states = {'COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'}
        self.success_states = {'COMPLETED'}
        
        # Cluster and step tracking
        self.cluster_id = None
        self.active_steps = {}
        
    def get_emr_cluster_config(self) -> Dict[str, Any]:
        """
        Get EMR cluster configuration based on profile
        
        Returns:
            EMR cluster configuration dictionary
        """
        try:
            cluster_config = self.get_cluster_profile_config()
            
            master_machine = cluster_config.get('master_machine', 'm5.xlarge')
            worker_machine = cluster_config.get('worker_machine', 'm5.xlarge')
            masters = cluster_config.get('masters', 1)
            workers = cluster_config.get('workers', 4)
            
            # Build instance groups
            instance_groups = [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": master_machine,
                    "InstanceCount": masters
                },
                {
                    "Name": "Core nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": worker_machine,
                    "InstanceCount": workers
                }
            ]
            
            # Add task nodes if specified
            task_workers = cluster_config.get('task_workers', 0)
            if task_workers > 0:
                instance_groups.append({
                    "Name": "Task nodes",
                    "Market": "SPOT",
                    "InstanceRole": "TASK",
                    "InstanceType": worker_machine,
                    "InstanceCount": task_workers,
                    "BidPrice": cluster_config.get('spot_bid_price', 'OnDemandPrice')
                })
                
            # Determine log URI
            region, _, _ = self.get_aws_config()
            account_id = getattr(self.context, 'account_id', '867344468918')  # Default from original
            log_uri = self.log_uri or f"s3://aws-logs-{account_id}-{region}/elasticmapreduce/"
            
            emr_config = {
                "Name": getattr(self.context, 'cluster_name', f"{self.get_workflow_name()}-emr-cluster"),
                "ReleaseLabel": self.emr_release_label,
                "Instances": {
                    "InstanceGroups": instance_groups,
                    "KeepJobFlowAliveWhenNoSteps": True,
                    "TerminationProtected": False,
                    "Ec2KeyName": cluster_config.get('ec2_key_name'),
                    "Ec2SubnetId": cluster_config.get('subnet_id'),
                    "EmrManagedMasterSecurityGroup": cluster_config.get('master_security_group'),
                    "EmrManagedSlaveSecurityGroup": cluster_config.get('slave_security_group'),
                    "ServiceAccessSecurityGroup": cluster_config.get('service_access_security_group')
                },
                "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
                "JobFlowRole": cluster_config.get('instance_profile', 'EMR_EC2_DefaultRole'),
                "ServiceRole": cluster_config.get('service_role', 'EMR_DefaultRole'),
                "LogUri": log_uri,
                "VisibleToAllUsers": True,
                "Tags": self._build_cluster_tags()
            }
            
            # Add custom AMI if specified
            if self.custom_ami_id:
                emr_config["CustomAmiId"] = self.custom_ami_id
                
            # Add bootstrap actions if specified
            bootstrap_actions = cluster_config.get('bootstrap_actions', [])
            if bootstrap_actions:
                emr_config["BootstrapActions"] = bootstrap_actions
                
            # Remove None values
            emr_config = self._remove_none_values(emr_config)
            
            return emr_config
            
        except Exception as e:
            error_msg = f"Failed to create EMR cluster config: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def _build_cluster_tags(self) -> List[Dict[str, str]]:
        """
        Build tags for EMR cluster
        
        Returns:
            List of tag dictionaries
        """
        tags = [
            {"Key": "Workflow", "Value": self.get_workflow_name()},
            {"Key": "Engine", "Value": "EMR"},
            {"Key": "CreatedBy", "Value": "InsightAir"},
            {"Key": "Environment", "Value": getattr(self.context, 'environment', 'production')},
            {"Key": "AutoTerminate", "Value": "true"}
        ]
        
        # Add custom tags from context
        custom_tags = getattr(self.context, 'cluster_tags', {})
        for key, value in custom_tags.items():
            tags.append({"Key": key, "Value": str(value)})
            
        return tags
        
    def _remove_none_values(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively remove None values from configuration
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configuration with None values removed
        """
        if isinstance(config, dict):
            return {k: self._remove_none_values(v) for k, v in config.items() if v is not None}
        elif isinstance(config, list):
            return [self._remove_none_values(item) for item in config if item is not None]
        else:
            return config
            
    def check_and_launch_cluster(self) -> str:
        """
        Check for existing cluster or launch new one
        
        Returns:
            Cluster ID
            
        Raises:
            EngineOperationError: If cluster operations fail
        """
        if not EMR_AVAILABLE:
            raise EngineOperationError("AWS EMR libraries not available")
            
        try:
            region, _, _ = self.get_aws_config()
            
            # Create EMR hook
            hook = EmrHook(
                aws_conn_id=self.context.gcp_conn_id,
                region_name=region
            )
            
            cluster_name = getattr(self.context, 'cluster_name', f"{self.get_workflow_name()}-emr-cluster")
            
            # Try to find existing cluster
            try:
                cluster_id = hook.get_cluster_id_by_name(
                    emr_cluster_name=cluster_name,
                    cluster_states=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
                )
                
                if cluster_id:
                    logger.info(f"Found existing EMR cluster: {cluster_id}")
                    self.cluster_id = cluster_id
                    return cluster_id
                    
            except Exception as e:
                logger.debug(f"No existing cluster found: {e}")
                
            # Create new cluster
            cluster_spec = self.get_emr_cluster_config()
            logger.info(f"Creating new EMR cluster with config: {cluster_name}")
            
            cluster_info = hook.create_job_flow(job_flow_overrides=cluster_spec)
            cluster_id = cluster_info["JobFlowId"]
            
            logger.info(f"Created EMR cluster: {cluster_id}")
            self.cluster_id = cluster_id
            return cluster_id
            
        except Exception as e:
            error_msg = f"Failed to check and launch EMR cluster: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def create_cluster_task(self, task_name: str) -> 'PythonOperator':
        """
        Create Airflow task for cluster creation
        
        Args:
            task_name: Name of the task
            
        Returns:
            PythonOperator for cluster creation
        """
        return self.create_python_operator(
            task_name=task_name,
            python_callable=self.create_cluster_sync,
            task_name=task_name
        )
        
    def create_cluster_sync(self, task_name: str = None) -> str:
        """
        Synchronous cluster creation with initialization
        
        Args:
            task_name: Name of the task (optional)
            
        Returns:
            Cluster ID
        """
        self.init_config()
        return self.check_and_launch_cluster()
        
    def build_spark_step_config(self, task_name: str, task: Dict[str, Any], 
                               job_type: str, cluster_id: str) -> Dict[str, Any]:
        """
        Build Spark step configuration for EMR
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job being executed
            cluster_id: EMR cluster ID
            
        Returns:
            EMR step configuration dictionary
        """
        try:
            config_bucket, config_path = self.get_s3_config()
            region, _, _ = self.get_aws_config()
            
            job_name = self.format_job_name(task_name, 'emr')
            
            # Build Spark submit arguments
            spark_args = [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", f"spark.archives=s3://{config_bucket}/{self.environment_archive}#environment,s3://{config_bucket}/{self.spark_archive}#insightspark",
                "--conf", f"spark.submit.pyFiles=s3://{config_bucket}/{self.spark_archive}",
                "--conf", "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                "--conf", "spark.executorEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                "--conf", "spark.executorEnv.LD_LIBRARY_PATH=./environment/phi3env/lib/python3.12/site-packages/nvidia/cudnn/lib",
                "--conf", "spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./environment/phi3env/lib/python3.12/site-packages/nvidia/cudnn/lib",
                "--conf", f"spark.sql.parquet.compression.codec=snappy",
                "--conf", f"spark.yarn.appMasterEnv.PYTHONPATH=./environment:./insightspark:./environment/phi3env/lib/python3.12/site-packages:./environment/phi3env/lib64/python3.12/site-packages",
                "--conf", f"spark.executorEnv.PYTHONPATH=./environment:./insightspark:./environment/phi3env/lib/python3.12/site-packages:./environment/phi3env/lib64/python3.12/site-packages",
                "--conf", f"spark.executor.extraJavaOptions=-Dorg.apache.commons.compress.archivers.zip.allowStoredEntriesWithDataDescriptor=true",
                "--conf", f"spark.driver.extraJavaOptions=-Dorg.apache.commons.compress.archivers.zip.allowStoredEntriesWithDataDescriptor=true"
            ]
            
            # Add JAR dependencies
            jar_dependencies = task.get('jar_dependencies', [])
            if jar_dependencies:
                jars_str = ",".join([f"s3://{config_bucket}/jars/{jar}" for jar in jar_dependencies])
                spark_args.extend(["--jars", jars_str])
                
            # Add packages
            packages = task.get('packages', ['com.mysql:mysql-connector-j:8.0.33'])
            if packages:
                spark_args.extend(["--conf", f"spark.jars.packages={','.join(packages)}"])
                
            # Add the main script and arguments
            spark_args.extend([
                f"s3://{config_bucket}/{self.job_script_location}",
                "--job_type", job_type,
                "--config_path", f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                "--AWS_REGION", region
            ])
            
            # Add custom arguments from task
            custom_args = task.get('spark_args', {})
            for key, value in custom_args.items():
                spark_args.extend([f"--{key}", str(value)])
                
            step_config = {
                'Name': job_name,
                'ActionOnFailure': task.get('action_on_failure', 'CONTINUE'),
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': spark_args
                }
            }
            
            return step_config
            
        except Exception as e:
            error_msg = f"Failed to build Spark step config: {e}"
            logger.error(error_msg)
            raise EngineConfigError(error_msg)
            
    def submit_job(self, task_name: str, task: Dict[str, Any], job_type: str) -> str:
        """
        Submit a job to EMR cluster
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to submit
            
        Returns:
            Step ID
            
        Raises:
            EngineOperationError: If job submission fails
        """
        if not EMR_AVAILABLE:
            raise EngineOperationError("AWS EMR libraries not available")
            
        try:
            # Initialize configuration
            self.init_config()
            
            # Upload task configuration
            self.upload_task_config(task_name, task)
            
            # Execute pre-processing
            self.pre_process(task)
            
            # Ensure cluster is available
            if not self.cluster_id:
                self.cluster_id = self.check_and_launch_cluster()
                
            # Build step configuration
            step_config = self.build_spark_step_config(task_name, task, job_type, self.cluster_id)
            
            # Submit step to EMR
            region, _, _ = self.get_aws_config()
            
            hook = EmrHook(
                aws_conn_id=self.context.gcp_conn_id,
                region_name=region
            )
            
            step_ids = hook.add_job_flow_steps(
                job_flow_id=self.cluster_id,
                steps=[step_config],
                wait_for_completion=False
            )
            
            step_id = step_ids[0]
            
            # Track the step
            self.active_steps[step_id] = {
                'task_name': task_name,
                'job_type': job_type,
                'cluster_id': self.cluster_id,
                'start_time': datetime.now(),
                'hook': hook,
                'step_config': step_config
            }
            
            logger.info(f"Submitted EMR step '{step_config['Name']}' with ID: {step_id}")
            return step_id
            
        except Exception as e:
            error_msg = f"Failed to submit EMR job for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_status(self, job_id: str) -> str:
        """
        Get current status of an EMR step
        
        Args:
            job_id: Step ID
            
        Returns:
            Current step status
            
        Raises:
            EngineOperationError: If status check fails
        """
        try:
            if job_id not in self.active_steps:
                raise EngineOperationError(f"Step {job_id} not found in active steps")
                
            step_info = self.active_steps[job_id]
            hook = step_info['hook']
            cluster_id = step_info['cluster_id']
            
            # Get step details
            step = hook.get_step(cluster_id=cluster_id, step_id=job_id)
            status = step['Step']['Status']['State']
            
            # Update step info
            step_info['last_status'] = status
            step_info['last_check'] = datetime.now()
            
            return status
            
        except Exception as e:
            error_msg = f"Failed to get status for EMR step {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def is_job_terminal_state(self, status: str) -> bool:
        """
        Check if step status indicates terminal state
        
        Args:
            status: Step status string
            
        Returns:
            True if step is in terminal state
        """
        return status in self.terminal_states
        
    def is_job_successful(self, status: str) -> bool:
        """
        Check if step status indicates success
        
        Args:
            status: Step status string
            
        Returns:
            True if step completed successfully
        """
        return status in self.success_states
        
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running EMR step
        
        Args:
            job_id: Step ID
            
        Returns:
            True if cancellation was successful
            
        Raises:
            EngineOperationError: If cancellation fails
        """
        try:
            if job_id not in self.active_steps:
                logger.warning(f"Step {job_id} not found in active steps")
                return False
                
            step_info = self.active_steps[job_id]
            hook = step_info['hook']
            cluster_id = step_info['cluster_id']
            
            # Cancel the step
            hook.cancel_steps(cluster_id=cluster_id, step_ids=[job_id])
            
            logger.info(f"Cancelled EMR step {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to cancel EMR step {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def get_job_logs(self, job_id: str) -> str:
        """
        Get logs for an EMR step
        
        Args:
            job_id: Step ID
            
        Returns:
            Job logs as string
            
        Raises:
            EngineOperationError: If log retrieval fails
        """
        try:
            if job_id not in self.active_steps:
                raise EngineOperationError(f"Step {job_id} not found in active steps")
                
            step_info = self.active_steps[job_id]
            hook = step_info['hook']
            cluster_id = step_info['cluster_id']
            
            # Get step details which includes failure information
            step = hook.get_step(cluster_id=cluster_id, step_id=job_id)
            
            logs = []
            
            # Add step status information
            status = step['Step']['Status']
            logs.append(f"Step Status: {status['State']}")
            
            if 'FailureDetails' in status:
                logs.append(f"Failure Reason: {status['FailureDetails']['Reason']}")
                logs.append(f"Failure Message: {status['FailureDetails']['Message']}")
                
            if 'StateChangeReason' in status:
                logs.append(f"State Change Reason: {status['StateChangeReason']['Message']}")
                
            # Note: For complete log access, you would typically need to:
            # 1. Access the S3 log location specified in cluster configuration
            # 2. Download and parse the step logs from S3
            # This is a simplified implementation
            
            return '\n'.join(logs) if logs else f"No logs available for step {job_id}"
            
        except Exception as e:
            error_msg = f"Failed to get logs for EMR step {job_id}: {e}"
            logger.error(error_msg)
            raise EngineOperationError(error_msg)
            
    def run_emr_job_sync(self, task_name: str, task: Dict[str, Any], 
                        job_type: str, cluster_id: str = None, 
                        timeout_minutes: int = 120) -> Dict[str, Any]:
        """
        Run an EMR job synchronously and wait for completion
        
        Args:
            task_name: Name of the task
            task: Task configuration
            job_type: Type of job to run
            cluster_id: EMR cluster ID (optional)
            timeout_minutes: Maximum time to wait for completion
            
        Returns:
            Job execution results
            
        Raises:
            EngineJobError: If job execution fails
        """
        try:
            # Use provided cluster or ensure one exists
            if cluster_id:
                self.cluster_id = cluster_id
            elif not self.cluster_id:
                self.cluster_id = self.check_and_launch_cluster()
                
            # Submit the step
            step_id = self.submit_job(task_name, task, job_type)
            
            # Monitor until completion
            final_status = self.monitor_job_status(step_id, timeout_minutes)
            
            # Execute post-processing
            self.post_process(task)
            
            # Get step information
            step_info = self.active_steps.get(step_id, {})
            
            # Move to history
            step_info['end_time'] = datetime.now()
            step_info['final_status'] = final_status
            step_info['duration'] = (step_info['end_time'] - step_info['start_time']).total_seconds()
            
            self.job_history.append(step_info)
            if step_id in self.active_steps:
                del self.active_steps[step_id]
                
            result = {
                'success': True,
                'step_id': step_id,
                'cluster_id': self.cluster_id,
                'final_status': final_status,
                'task_name': task_name,
                'job_type': job_type,
                'duration_seconds': step_info.get('duration', 0)
            }
            
            logger.info(f"EMR job completed successfully: {result}")
            return result
            
        except Exception as e:
            # Clean up failed step from active steps
            if step_id and step_id in self.active_steps:
                step_info = self.active_steps[step_id]
                step_info['end_time'] = datetime.now()
                step_info['final_status'] = 'FAILED'
                step_info['error'] = str(e)
                self.job_history.append(step_info)
                del self.active_steps[step_id]
                
            error_msg = f"EMR job execution failed for task {task_name}: {e}"
            logger.error(error_msg)
            raise EngineJobError(error_msg)
            
    def terminate_cluster(self, cluster_id: str = None) -> bool:
        """
        Terminate EMR cluster
        
        Args:
            cluster_id: Cluster ID to terminate (uses current if not specified)
            
        Returns:
            True if termination was successful
        """
        try:
            target_cluster_id = cluster_id or self.cluster_id
            if not target_cluster_id:
                logger.warning("No cluster ID specified for termination")
                return False
                
            region, _, _ = self.get_aws_config()
            
            hook = EmrHook(
                aws_conn_id=self.context.gcp_conn_id,
                region_name=region
            )
            
            hook.terminate_job_flow(job_flow_id=target_cluster_id)
            
            if target_cluster_id == self.cluster_id:
                self.cluster_id = None
                
            logger.info(f"Terminated EMR cluster: {target_cluster_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to terminate EMR cluster: {e}"
            logger.error(error_msg)
            return False
            
    def get_emr_info(self) -> Dict[str, Any]:
        """
        Get EMR engine configuration information
        
        Returns:
            Dictionary with EMR info
        """
        base_info = self.get_engine_info()
        
        try:
            emr_info = {
                'emr_release_label': self.emr_release_label,
                'custom_ami_id': self.custom_ami_id,
                'job_script_location': self.job_script_location,
                'environment_archive': self.environment_archive,
                'spark_archive': self.spark_archive,
                'current_cluster_id': self.cluster_id,
                'active_steps': len(self.active_steps),
                'log_uri': self.log_uri
            }
            
            base_info.update(emr_info)
            
        except Exception as e:
            base_info['emr_error'] = str(e)
            
        return base_info