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
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook

from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *


# Class: ProcessS3
# Description: S3 processing functions
class ProcessS3(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def s3_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)
        task['kwargs']=kwargs
        raw_bucket_uri = self.context.data_group[RAW_BUCKET]
        archive_bucket_uri = self.context.data_group[ARCHIVE_BUCKET]
        target_path = task[PROPERTIES][TARGET_PATH]
        source_path = task[PROPERTIES][SOURCE_PATH]
        source_bucket_uri = task[PROPERTIES][SOURCE_BUCKET]
        file_pattern = task[PROPERTIES][FILE_PATTERN]
        raw_bucket = raw_bucket_uri.replace("s3://","")
        archive_bucket = archive_bucket_uri.replace("s3://","")
        source_bucket = source_bucket_uri.replace("s3://","")

        is_zero_byte_chk = task[PROPERTIES].get(ZERO_BYTE_CHK, True)
        is_zero_byte_chk = str(is_zero_byte_chk).lower() in ['false', '0']
        
        hook = S3Hook(aws_conn_id=self.context.gcp_conn_id, transfer_config_args=None, extra_args=None)
        files = hook.list_keys(source_bucket, prefix=source_path)
        for file in files:
            print(file)
        run_dt=datetime.today().strftime('%Y%m%d-%H%M')
        target_path = target_path.replace("`date +%Y%m%d-%H%M`",run_dt)
        kwargs=task['kwargs']
        print(kwargs)
        ti = kwargs['ti']
        wf_params=ti.xcom_pull(key="wf_params")
        if wf_params is None:
            wf_params = {}
        wf_params['run_dt']=run_dt
        ti.xcom_push(key="wf_params", value=wf_params)
        print(ti.xcom_pull(key="wf_params"))
        target_path = target_path.replace("$raw_bucket",raw_bucket)
        task[PROPERTIES][TARGET_PATH]=target_path
        target_path = get_target_folder(target_path)
        print(target_path)

        for file in files:
            if file.endswith('/'):
                # Your code here
                pass
            filename = file.split('/')[-1]
            print(filename)
            hook.copy_object(source_bucket_name=source_bucket.replace("s3://",""), source_bucket_key=file,
                      dest_bucket_name=raw_bucket.replace("s3://",""), dest_bucket_key=target_path+'/'+filename)
        

    def get_oldest_file(self, bucket, mnf_files, hook):
        oldest_ts=0
        oldest_file=''
        for file in mnf_files:
            ts = hook.get_blob_update_time(bucket_name=bucket, object_name=file)
            print(ts)
            if oldest_ts==0 or ts < oldest_ts:
                oldest_ts=ts
                oldest_file=file
        return oldest_file

    def build_s3_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.s3_op,
            op_kwargs=args
         )
         return op

    def run_s3_glue_job(self, task_name, task):
        glue_job_name = f'{self.context.config['name']}-{task_name}'
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        job_script = "scripts/glue_job.py"
        requirement_file = 's3://'+config_bucket+"/install/glue_s3_requirements.txt"
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
                    '--job_type': 'S3'
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