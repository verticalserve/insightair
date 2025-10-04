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
from airflow.providers.amazon.aws.hooks.emr import EmrHook

import pymysql
import json
from typing import List
import base64

from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *
from workflow_framework.process_emr import ProcessEmr
from workflow_framework.process_ray import ProcessRay

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
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
class ProcessDB(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        self.pool = PooledDB(
            creator=pymysql,
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,  # Add the port parameter here
            mincached=min_cached,
            maxcached=max_cached,
            maxconnections=max_connections,
            blocking=True,
        )

    def build_db_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.db_op,
            op_kwargs=args
         )
         return op
    
    def db_op(self,task_name,dyn_params={}, **kwargs):
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
            self.run_glue_job(task_name, task, 'DB')
        elif engine == 'emr':
            process_emr = ProcessEmr(self.context)
            emr_cluster_id=process_emr.create_cluster()
            process_emr.run_emr_job(task_name, task, 'DB', emr_cluster_id)
        elif engine == 'ray':
            process_ray = ProcessRay(self.context)
            process_ray.run_ray_job(task_name, task, 'DB')
        else:
            raise Exception(f"Unsupported engine: {engine}")
        return True

    
    def get_connection(self):
        return self.pool.connection()

    

    def close(self):
        self.pool.close()

    def fetch_docs(self, query, target_folder, raw_bucket, blob_file_name):
        # Initialize the connection pool
        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        # Fetch all rows
        rows = cursor.fetchall()

        metadata={}
        
        glue_job_name = "my_glue_job"

        # Corrected GlueJobHook initialization
        glue_hook = GlueJobHook(
            aws_conn_id=self.context.gcp_conn_id,  # Changed from gcp_conn_id to AWS connection
            job_name=glue_job_name,
            region_name='us-east-1',
            s3_bucket='raw-policy-data',
            script_location='s3://raw-policy-data/policy/raw/policy.py',
            iam_role_name='AWSGlueServiceRole',
            desc='My Glue Job',
            update_config=True,
            create_job_kwargs={
                'Role': 'arn:aws:iam::388815509612:role/service-role/AWSGlueServiceRole',  # Required ARN
                'GlueVersion': '5.0',
                'WorkerType': 'G.1X',
                'NumberOfWorkers': 10,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': 's3://raw-policy-data/policy/raw/policy.py'
                },
                'DefaultArguments': {
                    '--additional-python-modules': 's3://raw-knowledge-base/requirements.txt',
                    '--python-modules-installer-option': '-r',
                    '--INPUT_S3_PATH': 's3://raw-policy-data/output',
                    '--OUTPUT_S3_PATH': 'raw-policy-data/output2',
                    '--AWS_REGION': 'us-east-1'
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
        print(f"Started Glue job '{glue_job_name}' with run ID: {response['JobRunId']}")
        if(True):
            return
        # Process the rows
        for row in rows:
            #print(row)
            for i in range(len(cursor.description)):
                column_name = cursor.description[i][0]
                column_type = cursor.description[i][1]
                print(i)
                value = row[i]
                print(column_name)
                print(column_type)
                print(pymysql.constants.FIELD_TYPE.BLOB)
                if column_type == pymysql.constants.FIELD_TYPE.BLOB or column_type == pymysql.constants.FIELD_TYPE.LONG_BLOB or str(value).startswith("data:application/pdf;base64,"):
                    # Create a file with blob content
                    blob_value = str(value, 'latin1')
                    #blob_value = str(value)
                    #print(blob_value)
                    extension = "blob"
                    if blob_value.startswith("data:application/pdf;base64,"):
                        blob_value = blob_value.replace("data:application/pdf;base64,", "")
                        extension = "pdf"
                    with open(f"{column_name}.{extension}", "wb") as file:
                        print(blob_value.encode('utf-8'))
                        file.write(base64.b64decode(blob_value.encode('utf-8')))
                    
                else:
                    metadata[column_name] = value
                    print(metadata)
            #data.append([str(value) for value in row])
            with open(f"metadata.json", "wb") as file:
                print(json.dumps(metadata).encode('utf-8'))
                file.write(json.dumps(metadata).encode('utf-8'))

            blob_file_name = blob_file_name.replace('${POLICYID}',metadata['POLICY_ID'])
            metadata_file_name = blob_file_name.replace('.pdf', '-metadata.json')
            hook = S3Hook(aws_conn_id=self.context.gcp_conn_id, transfer_config_args=None, extra_args=None)
            hook.load_file(filename='doc.pdf', key=target_folder+'/'+blob_file_name, replace=True, bucket_name=raw_bucket)    
            hook.load_file(filename='metadata.json', key=target_folder+'/'+metadata_file_name, replace=True, bucket_name=raw_bucket)   

        print(metadata)
        # Release the connection
        connection.close()
        return metadata
    
    
    
    
        
        

    

    
