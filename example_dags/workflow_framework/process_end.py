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
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *
from tempfile import NamedTemporaryFile

# Class: ProcessEnd
# Description: End class, cleans up resources and creates a done file
class ProcessEnd(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def run_end_step(self,task_name, **kwargs):
        super().init_config()
        if True:
            return True
        task = self.context.config_obj.get_task(task_name)

        raw_bucket = self.context.data_group[RAW_BUCKET]
        config_bucket = self.context.env[CONFIG_BUCKET]
        done_bucket = self.context.data_group[DONE_BUCKET]
        done_file_path = task[PROPERTIES]['done_file_path'].lower().replace("gs://","")
        print(done_file_path)
        done_folder = get_target_folder(done_file_path).replace("-","_").lower()
        date_value=datetime.today().strftime('%Y%m%d%H%M%S')
        done_file_name = task[PROPERTIES]['done_file_name'].replace("YYYYMMddHHmmss",date_value).replace("-","_").lower()

        print(done_folder+'/'+done_file_name)
        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id)
        #info = hook.copy(source_bucket=config_bucket.replace("gs://",""), source_object='sample.done',
         #             destination_bucket=done_bucket.replace("gs://",""), destination_object=done_folder+"/"+done_file_name)

        task_instance = kwargs['task_instance']

        backfeed_dag = task_instance.dag_id + "-backfeed"
        done_file_data = { 'backfill_dag_name': backfeed_dag}
        temp_file = NamedTemporaryFile(mode='w+')
        json.dump(done_file_data, temp_file)
        temp_file.flush()

        hook.upload(bucket_name=done_bucket.replace("gs://",""),
                        object_name=done_folder+"/"+done_file_name, filename=temp_file.name)

        try:
            dproc_hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
            if 'profiler' in self.context.config[PROPERTIES]:
                if self.context.config[PROPERTIES]['profiler']=='true':
                    print("Submitting profiler job...")
                    queries = "sh /opt/dreutil/dreutil/dreutil-run.sh"
                    profiler_job = {
                        "reference": {"project_id": self.context.project_id },
                        "placement": {"cluster_name": self.context.cluster_name},
                        "pig_job": {"query_list": {"queries": [queries]}}
                    }
                    dproc_hook.submit(job = profiler_job, region = self.context.region, project_id = self.context.project_id)
                    print("Profiler job submitted...")
            
            dproc_hook.delete_cluster(
                cluster_name = self.context.cluster_name,
                project_id = self.context.project_id,
                region = self.context.region)
        except:
            pass
        
    def build_end_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_end_step,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op




