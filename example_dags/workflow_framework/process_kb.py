# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

DB_HOST = 'edpnew.c7uqx6xqd0nl.us-east-1.rds.amazonaws.com'
DB_USER = 'edpadmin'
DB_PASSWORD = 'edpadmin'
DB_NAME = 'insurance'
DB_PORT = 4897
min_cached=5
max_cached=10
max_connections=20

# Class: ProcessDB
# Description: DB processing functions
class ProcessKB(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        
    
    def kb_op(self,task_name,dyn_params={}, **kwargs):
        print("inside kb_op")
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)
        task['kwargs']=kwargs
        stage_path = task[PROPERTIES][STAGE_PATH]
        run_dt=init_xcom(kwargs)
        stage_path = stage_path.replace("`date +%Y%m%d-%H%M`",run_dt)
        task[PROPERTIES][STAGE_PATH] = stage_path.lower()
        print(f"stage-path-- {stage_path}")
        self.upload_task_config(task_name, task)

        engine = get_engine(self.context.config[PROPERTIES],task[PROPERTIES])
        print(stage_path)

        kb_type = task[PROPERTIES]['kb_type']
        if kb_type == 'db':
            #requirement_file = config_bucket+"/install/glue_kb_db_requirements.txt"
            job_type = 'KB_DB'
        elif kb_type == 'sharepoint':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_SHAREPOINT'
        elif kb_type == 'outlook':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_OUTLOOK'
        elif kb_type == 'onedrive':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_ONEDRIVE'
        elif kb_type == 'confluence':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_CONFLUENCE'
        elif kb_type == 'jira':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_JIRA'
        elif kb_type == 'teams':
            #requirement_file = config_bucket+"/install/glue_kb_sharepoint_requirements.txt"
            job_type = 'KB_TEAMS'
        elif kb_type == 'csv_to_text':
            #requirement_file = config_bucket+"/install/glue_kb_csv2text_requirements.txt"
            job_type = 'KB_CSV_TO_TEXT'
        elif kb_type == 'db_to_text':
            #requirement_file = config_bucket+"/install/glue_kb_csv2text_requirements.txt"
            job_type = 'KB_DB_TO_TEXT'
        elif kb_type == 's3':
            #requirement_file = config_bucket+"/install/glue_kb_s3_requirements.txt"
            job_type = 'KB_S3'

        if engine == 'glue':
            self.run_glue_job(task_name, task, job_type)
        elif engine == 'emr':
            process_emr = ProcessEmr(self.context)
            emr_cluster_id=process_emr.create_cluster()
            process_emr.run_emr_job(task_name, task, job_type, emr_cluster_id)
        elif engine == 'ray':
            process_ray = ProcessRay(self.context)
            process_ray.run_ray_job(task_name, task, job_type)
        else:
            raise Exception(f"Unsupported engine: {engine}")
        return True
    

    def build_kb_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.kb_op,
            op_kwargs=args
         )
         return op
