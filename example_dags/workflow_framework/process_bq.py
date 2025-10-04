# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from workflow_framework.config import *
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GCSHook
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *


# Class: ProcessBQ
# Description: BQ Processing
class ProcessBQ(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def get_target_folder(self,target_dir):
        folder = target_dir.replace("run_dt=`date +%Y%m%d-%H%M`", "")
        folder = folder.replace("gs://", "")
        index = folder.index('/')
        folder = folder[index+1:-1]
        return folder

    def load_bq_data_hook(self, task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        query = task[PROPERTIES][QUERY]
        target_path = task[PROPERTIES][TARGET_PATH]
        delimiter = task[PROPERTIES][DELIMITER].replace('\\u0001','\u0001')
        dataset = task[PROPERTIES][DATASET]
        table = task[PROPERTIES][TABLE]
        project = task[PROPERTIES]['project']
        location = self.context.env[REGION]
        project_id = self.context.data_group[PROJECT_ID]
        data_format = task[PROPERTIES][DATA_FORMAT]

        raw_bucket = self.context.data_group[RAW_BUCKET]
        archive_bucket = self.context.data_group[ARCHIVE_BUCKET]
        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        target_folder = self.get_target_folder(target_path)

        print(target_folder)
        files = hook.list(raw_bucket.replace("gs://",""), prefix=target_folder)

        archive=True
        if 'archive' in task[PROPERTIES]:
            if task[PROPERTIES]['archive']=='false':
                archive=False

        if archive == True:
            for file in files:
                hook.copy(source_bucket=raw_bucket.replace("gs://",""), source_object=file,
                          destination_bucket=archive_bucket.replace("gs://",""), destination_object=file.replace('landing','landing/archive'))
                hook.delete(bucket_name=raw_bucket.replace("gs://",""), object_name=file)

        target_path = target_path.replace("`date +%Y%m%d-%H%M`",
                                          datetime.today().strftime('%Y%m%d-%H%M'))
        target_path = target_path.replace("$raw_bucket",raw_bucket).lower()+'/*'

        hook = BigQueryHook(
            gcp_conn_id=self.context.gcp_conn_id,
            location='US',
            use_legacy_sql=False
        )

        target_table = project_id+"."+dataset+"."+table

        hook.run_query(sql=query,
                       destination_dataset_table=target_table,
                       write_disposition='WRITE_TRUNCATE',
                       allow_large_results=True,
                       use_legacy_sql=False)

        result = hook.run_extract(
            source_project_dataset_table=target_table,
            destination_cloud_storage_uris=[target_path],
            export_format=data_format,
            field_delimiter=delimiter.replace('\\u0001','\u0001'),
            print_header=False
        )
        print(result)

    def build_bq_task(self, task_name, dyn_params={}):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.load_bq_data_hook,
            op_kwargs={
                'task_name': task_name,
                'dyn_params': dyn_params
            }
        )
        return op
