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
import yaml


# Class: ProcessDB
# Description: DB processing functions
class ProcessDOC(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        
    def doc_op(self,task_name,dyn_params={}, **kwargs):
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
            self.run_glue_job(task_name, task, 'DOC')
        elif engine == 'emr':
            process_emr = ProcessEmr(self.context)
            emr_cluster_id=process_emr.create_cluster()
            process_emr.run_emr_job(task_name, task, 'DOC', emr_cluster_id)
        elif engine == 'ray':
            process_ray = ProcessRay(self.context)
            process_ray.run_ray_job(task_name, task, 'DOC')
        else:
            raise Exception(f"Unsupported engine: {engine}")
        return True
        

    

    def build_doc_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.doc_op,
            op_kwargs=args
         )
         return op
