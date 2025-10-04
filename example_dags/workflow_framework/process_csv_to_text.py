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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from dbutils.pooled_db import PooledDB
import pymysql
import json
from typing import List
import base64

from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from workflow_framework.process_emr import ProcessEmr
from workflow_framework.process_ray import ProcessRay
import time


# Class: ProcessDB
# Description: DB processing functions
class ProcessCSVToText(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        
    def build_csv_to_text_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.csv_to_text_op,
            op_kwargs=args
         )
         return op
    
    def csv_to_text_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)
        task['kwargs']=kwargs
        stage_path = task[PROPERTIES][STAGE_PATH]
        run_dt=init_xcom(kwargs)
        stage_path = stage_path.replace("`date +%Y%m%d-%H%M`",run_dt)
        task[PROPERTIES][STAGE_PATH] = stage_path.lower()
        self.upload_task_config(task_name, task)

        engine = get_engine(self.context.config[PROPERTIES],task[PROPERTIES])
        print(stage_path)

        if engine == 'glue':
            self.run_glue_job(task_name, task, 'CSV_TO_TEXT')
        elif engine == 'emr':
            process_emr = ProcessEmr(self.context)
            emr_cluster_id=process_emr.create_cluster()
            process_emr.run_emr_job(task_name, task, 'CSV_TO_TEXT', emr_cluster_id)
        elif engine == 'ray':
            process_ray = ProcessRay(self.context)
            process_ray.run_ray_job(task_name, task, 'CSV_TO_TEXT')
        else:
            raise Exception(f"Unsupported engine: {engine}")
        return True


    
    
    def run_csv_to_text_emr_job(self, task_name, task):
        glue_job_name = task_name
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        job_script = "scripts/glue_job.py"
        requirement_file = 's3://'+config_bucket+"/install/glue_db_requirements.txt"
        config_path = self.context.config['path']
        if config_path.endswith('/'):
            config_path = config_path.rstrip('/')
        worker_type='G.1X'
        num_workers=10
        if self.context.cluster_profile == 'medium':
            worker_type='G.1X'
            num_workers=10
        elif self.context.cluster_profile == 'large':
            worker_type='G.1X'
            num_workers=10
        region = self.context.data_group['region']
        role_id = self.context.data_group['role_id']
        role_name = self.context.data_group['role_name']
        # Corrected GlueJobHook initialization
        glue_hook = GlueJobHook(
            aws_conn_id=self.context.gcp_conn_id,  # Changed from gcp_conn_id to AWS connection
            job_name=glue_job_name,
            region_name=region,
            s3_bucket=config_bucket,
            script_location=f'{config_bucket}/{job_script}',
            iam_role_name=role_name,
            desc=task['description'],
            update_config=True,
            create_job_kwargs={
                'Role': role_id,  # Required ARN
                'GlueVersion': '5.0',
                'WorkerType': worker_type,
                'NumberOfWorkers': num_workers,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{config_bucket}/{job_script}',
                },
                'DefaultArguments': {
                    '--additional-python-modules': requirement_file,
                    '--python-modules-installer-option': '-r',
                    '--AWS_REGION': region,
                    '--extra-py-files': f's3://{config_bucket}/dependencies.zip,s3://insightrag-job-config/glue_framework.zip',
                    '--config_path': f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                    '--job_type': 'DB'
                }
            }
        )
        # Start the job run with proper arguments
        response = glue_hook.initialize_job()

        # Get Job Run ID for monitoring
        job_run_id = response['JobRunId']

        status = glue_hook.job_completion(job_name=glue_job_name, run_id=job_run_id)
        print(status)
        return True
    
    
    
    
    
    
        
        

    

    
