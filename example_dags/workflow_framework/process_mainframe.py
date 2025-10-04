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
from workflow_framework.process_base import *


# Class: ProcessMainframe
# Description: Mainframe processing functions
class ProcessMainframe(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def mnf_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        raw_bucket_uri = self.context.data_group[RAW_BUCKET]
        archive_bucket_uri = self.context.data_group[ARCHIVE_BUCKET]
        target_path = task[PROPERTIES][TARGET_PATH]
        source_path = task[PROPERTIES][SOURCE_PATH]
        mainframe_bucket_uri = self.context.data_group[MAINFRAME_BUCKET]
        file_pattern = task[PROPERTIES][FILE_PATTERN]
        raw_bucket = raw_bucket_uri.replace("gs://","")
        archive_bucket = archive_bucket_uri.replace("gs://","")
        mainframe_bucket = mainframe_bucket_uri.replace("gs://","")

        is_zero_byte_chk = task[PROPERTIES].get(ZERO_BYTE_CHK, True)
        is_zero_byte_chk = str(is_zero_byte_chk).lower() in ['false', '0']

        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        target_folder = get_target_folder(target_path)

        #1. Check landing folder and if files are present then continue
        print(target_folder)
        landing_files = hook.list(raw_bucket, prefix=target_folder+'op_date_ts=')

        print(landing_files)

        # Only keep the file which matches prefix
        file_prefix = file_pattern[:file_pattern.find('*')] if file_pattern.find('*') > -1 else file_pattern
        print(file_prefix)

        for file in landing_files:
            if file_prefix in file:
                print(file)
            else:
                landing_files.remove(file)

        if len(landing_files)>0:
            print('Files exist in the landing folder, skipping the file move')
            return

        #2. Check the mainframe bucket
        print(mainframe_bucket_uri)
        mnf_files = hook.list(mainframe_bucket, prefix=file_prefix)
        print(mnf_files)

        if len(mnf_files)==0:
            print('No files present in the mainframe bucket, failing the task')
            raise AirflowFailException('No files present in the mainframe bucket, failing the task')

        oldest_file=self.get_oldest_file(mainframe_bucket, mnf_files, hook)
        print('oldest file='+oldest_file)
        timestamp=(oldest_file.split("#")[-1]).split("_")[0]
        dir='op_date_ts='+timestamp
        print(target_path.replace("gs://","").replace('landing','landing/archive')+dir)
        archive_files = hook.list(raw_bucket,
                                  prefix=target_path.replace("gs://","").replace('landing','landing/archive')+dir)

        print(archive_files)

        for file in archive_files:
            print(file)

        if len(archive_files)>0:
            print('Files exist in the archive folder, skipping the file move')
            return

        if oldest_file != '':
            oldest_file_size = hook.get_size(bucket_name=mainframe_bucket, object_name=oldest_file)
            if oldest_file_size==0:
                print(oldest_file)
                archive_path = target_path.replace('landing','landing/archive').replace(raw_bucket_uri+'/','')

                print(archive_path+dir+'/'+oldest_file)
                dest_path=archive_path.replace("gs://","")+dir+'/'+oldest_file
                hook.copy(source_bucket=mainframe_bucket.replace("gs://",""), source_object=oldest_file,
                        destination_bucket=raw_bucket, destination_object=dest_path)
                hook.delete(bucket_name=mainframe_bucket.replace("gs://",""), object_name=oldest_file)

                if not is_zero_byte_chk:
                    print("File {} is zero byte and it is mandatory file, so stopping workflow. If the file is optional set the 'zero_byte_chk' flag in config.yaml".format(oldest_file))
                    raise AirflowSkipException
                else:
                    print("File {} is zero byte and it is optional file, so continue the process".format(oldest_file))
                    return

            print(oldest_file)
            target_path = target_path.replace(raw_bucket_uri+'/','')

            print(target_path+'op_date_ts='+timestamp+'/'+oldest_file)
            dest_path=target_path.replace("gs://","")+'op_date_ts='+timestamp+'/'+oldest_file
            hook.copy(source_bucket=mainframe_bucket, source_object=oldest_file,
                      destination_bucket=raw_bucket, destination_object=dest_path)
            hook.delete(bucket_name=mainframe_bucket, object_name=oldest_file)


        print(target_path)

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

    def build_mnf_gcs_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.mnf_op,
            op_kwargs=args
         )
         return op
