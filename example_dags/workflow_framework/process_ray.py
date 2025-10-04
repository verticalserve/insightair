from ray.job_submission import JobSubmissionClient, JobStatus
import json
import time
class ProcessRay:
    
    def __init__(self, context):
        self.context = context
        
    def run_ray_job(self, task_name, task, job_type):
        workflow_name = f'{self.context.config['name']}-{task_name}'
        config_bucket = self.context.env['config_bucket'].replace("s3://", "")
        working_dir = f's3://{config_bucket}/ray/insightray_python15.zip'
        job_script = "insightray/templates/ray_job.py"
        config_path = self.context.config['path']
        
        if config_path.endswith('/'):
            config_path = config_path.rstrip('/')
        region = self.context.data_group['region']
        # Define the job_config here (this is the config you provided)
        job_config = {
            "config_path": f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
            "AWS_REGION": region,
            "job_type": job_type, 
            "workflow": workflow_name
        }

        client = JobSubmissionClient("http://10.0.2.141:30826/")
        job_id = client.submit_job(
            entrypoint=f'python {job_script}',
            runtime_env={
                "working_dir": working_dir,
                "pip": [
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
                    "mysql.connector",
                    "opensearch-py",
                    "requests-aws4auth",
                    "langchain-core", 
                    "langchain-community",
                    "langchain_aws",
                    "python-pptx"
                ],
                "env_vars": {
                    "HF_HUB_OFFLINE": "0",
                    "RAY_memory_monitor_refresh_ms": "0",
                    "JOB_CONFIG": json.dumps(job_config)  # Requires `import json`
                }
            }
        )
        print(f"Submitted Ray job: {job_id}")

        # Wait for the job to complete or fail
        while True:
            status = client.get_job_status(job_id)
            print(f"Job {job_id} status: {status}")
            
            # Check if the job has reached a terminal state
            if status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}:
                break
            
            # Sleep for a short period to avoid overwhelming the server
            time.sleep(10)  # Adjust the polling interval as needed (e.g., 5, 10, 30 seconds)

        # Optionally, raise an exception if the job failed
        if status == JobStatus.FAILED:
            raise Exception(f"Ray job {job_id} failed")
        elif status == JobStatus.STOPPED:
            print(f"Ray job {job_id} was stopped")

        print(f"Ray job {job_id} completed with status: {status}")
        return job_id
    
    
