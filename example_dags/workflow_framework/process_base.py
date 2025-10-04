# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GCSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from workflow_framework.config import *
from datetime import datetime
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook

#from workflow_framework.process_script import *

# Class: ProcessBase
# Description: Base processing class, which handles common functions
class ProcessBase:
    def __init__(self, context):
        self.context = context

    def init_config(self, wf_params={},run_dict={},dyn_params={}):
        if self.context.initialized == False:
            self.context.initialize(wf_params,run_dict,dyn_params)

    def pre_process(self, task):
        if PRE_SCRIPT in task:
            self.process_script.run_remote_pre_script_job(task)

    def post_process(self, task):
        if POST_SCRIPT in task:
            self.process_script.run_remote_post_script_job(task)
    
    def get_glue_cluster(self):
        worker_type='G.1X'
        num_workers=10
        if self.context.cluster_profile == 'medium':
            worker_type='G.1X'
            num_workers=10
        elif self.context.cluster_profile == 'large':
            worker_type='G.1X'
            num_workers=10
        return worker_type,num_workers

    def get_glue_roles(self):
        region = self.context.data_group['region']
        role_id = self.context.data_group['role_id']
        role_name = self.context.data_group['role_name']
        return region,role_id,role_name

    def upload_task_config(self, task_name, task):
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        config_path = self.context.get_config_path()
        config_yaml = yaml.dump(task["properties"])
        s3_key = f"dags/{config_path}/{task_name}.yaml"
        s3_hook = S3Hook(aws_conn_id=self.context.gcp_conn_id)
        s3_hook.load_string(string_data=config_yaml, key=s3_key, bucket_name=config_bucket, replace=True)
        return

    def update_glue_bookmark(self, task, default_arguments):
        print(f"task - {task}")
        if "bookmark" in task["properties"]:
            print(task["properties"]["bookmark"])
            default_arguments['--job-bookmark-option'] = 'job-bookmark-enable'
        else:
            print("Bookmark not enabled")
    
        return
    
    def get_glue_job_params(self, job_type, task_name, task, config_path, config_bucket, job_script, requirement_file, role_id, region):
        worker_type,num_workers = self.get_glue_cluster()
        
        default_arguments = {
                '--additional-python-modules': requirement_file,
                '--python-modules-installer-option': '-r',
                '--AWS_REGION': region,
                '--extra-py-files': f's3://{config_bucket}/spark/insightspark.zip',
                '--config_path': f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                '--job_type': job_type,
                '--task_name': task_name,
                '--workflow_name': self.context.config['name'],
            }
        self.update_glue_bookmark(task, default_arguments)
        return {
                'Role': role_id,  # Required ARN
                'GlueVersion': '5.0',
                'WorkerType': worker_type,
                'NumberOfWorkers': num_workers,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{config_bucket}/{job_script}',
                },
                'DefaultArguments': default_arguments
            }
    
    def run_glue_job(self, task_name, task, job_type):
        glue_job_name = f'{self.context.config['name']}-{task_name}'
        job_script = "scripts/glue_job.py"
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        config_path = self.context.get_config_path()
        requirement_file = 's3://'+config_bucket+"/install/glue_db_requirements.txt"

        region,role_id,role_name = self.get_glue_roles()

        job_params = self.get_glue_job_params(job_type, task_name, task, config_path, config_bucket, job_script, requirement_file, role_id, region)

        glue_hook = GlueJobHook(
            aws_conn_id=self.context.gcp_conn_id,  # Changed from gcp_conn_id to AWS connection
            job_name=glue_job_name,
            region_name=region,
            s3_bucket=config_bucket,
            script_location=f'{config_bucket}/{job_script}',
            iam_role_name=role_name,
            desc=task['description'],
            update_config=True,
            create_job_kwargs=job_params
        )
        # Start the job run with proper arguments
        response = glue_hook.initialize_job()

        # Get Job Run ID for monitoring
        job_run_id = response['JobRunId']

        status = glue_hook.job_completion(job_name=glue_job_name, run_id=job_run_id)
        print(status)


