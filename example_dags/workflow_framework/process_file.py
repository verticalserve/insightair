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
from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from airflow.exceptions import AirflowSkipException
from workflow_framework.process_base import *

# Class: ProcessFile
# Description: File processing functions
class ProcessFile(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def build_archive_task(self, task_name):

         args = {'task_name': task_name
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.archive,
            op_kwargs=args
         )
         return op

    def archive(self, task_name):
        super().init_config()
        task = self.context.config_obj.get_task(task_name)

        raw_bucket = self.context.data_group[RAW_BUCKET]
        archive_bucket = self.context.data_group[ARCHIVE_BUCKET]

        src_path = task[PROPERTIES]['src_path']
        target_path = task[PROPERTIES]['target_path']

        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        target_folder = get_target_folder(src_path)
        files = hook.list(raw_bucket.replace("gs://",""), prefix=target_folder)
        for file in files:
            hook.copy(source_bucket=raw_bucket.replace("gs://",""), source_object=file,
                      destination_bucket=archive_bucket.replace("gs://",""), destination_object= file.replace('landing','landing/archive'))
            hook.delete(bucket_name=raw_bucket.replace("gs://",""), object_name=file)

    def mainframe_task(self,task_name, **kwargs):
        super().init_config()
        task = self.context.config_obj.get_task(task_name)

        raw_bucket = self.context.data_group[RAW_BUCKET].replace("gs://","")
        archive_bucket = self.context.data_group[ARCHIVE_BUCKET]
        archive_path = task[PROPERTIES]['archive_path']
        source_path = task[PROPERTIES][SOURCE_PATH]
        mainframe_bucket = self.context.data_group[MAINFRAME_BUCKET].replace("gs://","")
        file_pattern = task[PROPERTIES][FILE_PATTERN]

        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        archive_folder = get_target_folder(archive_path)

        #2. Check the mainframe bucket
        file_prefix=file_pattern[:file_pattern.find('*')]
        print(file_prefix)
        print(mainframe_bucket)
        mnf_files = hook.list(mainframe_bucket, prefix=file_prefix)
        print(mnf_files)

        if len(mnf_files)==0:
            print('No files present in the mainframe bucket matching the pattern='+file_pattern+', failing the task')
            raise ValueError('No files present in the mainframe bucket matching the pattern='+file_pattern+', failing the task')

        oldest_file=self.get_oldest_file(mainframe_bucket, mnf_files, hook)
        print('oldest file='+oldest_file)
        timestamp=(oldest_file.split("#")[-1]).split("_")[0]
        dir='op_date_ts='+timestamp
        oldest_file_size = hook.get_size(bucket_name=mainframe_bucket, object_name=oldest_file)
        if oldest_file != '' and oldest_file_size==0:
            print(oldest_file)
            archive_path = archive_path.replace(raw_bucket+'/','')

            print(archive_path+dir+'/'+oldest_file)
            dest_path=archive_path.replace("gs://","")+dir+'/'+oldest_file
            hook.copy(source_bucket=mainframe_bucket, source_object=oldest_file,
                      destination_bucket=raw_bucket, destination_object=dest_path)
            hook.delete(bucket_name=mainframe_bucket, object_name=oldest_file)
            raise AirflowSkipException


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


    def build_delete_file_task(self, task_name):

         args = {'task_name': task_name
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.delete_file,
            op_kwargs=args
         )
         return op

    def delete_file(self, task_name):
        super().init_config()
        task = self.context.config_obj.get_task(task_name)

        src_path = task[PROPERTIES]['source_path']
        file_pattern = task[PROPERTIES][FILE_PATTERN]
        file_prefix = file_pattern[:file_pattern.find('*')] if file_pattern.find('*') > -1 else file_pattern
        print(file_prefix)

        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        files = hook.list(src_path.replace("gs://","").replace("/",""), prefix=file_prefix)
        for file in files:
            hook.delete(bucket_name=src_path.replace("gs://","").replace("/",""), object_name=file)

    




