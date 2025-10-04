# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from workflow_framework.config import *
from workflow_framework.process_dataproc import *
from workflow_framework.process_base import *
from workflow_framework.utils import *

# Class: ProcessSpark
# Description: Spark processing functions
class ProcessSpark(ProcessBase):
    
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def run_spark_sql_job(self,task_name, dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        process_dataproc = ProcessDataproc(self.context)
        process_dataproc.check_and_launch()


        job={
                "reference": {"project_id": self.context.project_id},
                "placement": {"cluster_name": self.context.cluster_name},
                "spark_sql_job": {"query_list": {"queries": [task[PROPERTIES][QUERIES]]}},
            }

        dproc_hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        info = dproc_hook.submit_job(job=job, project_id=self.context.project_id, region=self.context.region)
        print(info)
        print(info.reference.job_id)
        dproc_hook.wait_for_job(job_id=info.reference.job_id, project_id=self.context.project_id, region=self.context.region, wait_time=15)

    def build_spark_sql_task(self, task_name, dyn_params={}):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_spark_sql_job,
            op_kwargs={
                'task_name': task_name,
                'dyn_params': dyn_params
            }
        )
        return op

    def build_pyspark_task(self, task):
        op = DataprocSubmitJobOperator(
            task_id=task[NAME],
            job={
                "reference": {"project_id": self.context.project_id},
                "placement": {"cluster_name": self.context.cluster_name},
                "pyspark_job": {"main_python_file_uri": task[PROPERTIES][CODE_LOCATION]},
            },
            region=self.context.region,
            project_id=self.context.project_id,
            gcp_conn_id=self.context.gcp_conn_id
        )
        return op

    def build_spark_task(self, task):
        op = DataprocSubmitJobOperator(
            task_id=task[NAME],
            job={
                "reference": {"project_id": self.context.project_id},
                "placement": {"cluster_name": self.context.cluster_name},
                "spark_job": {"main_python_file_uri": task[PROPERTIES][CODE_LOCATION]},
            },
            region=self.context.region,
            project_id=self.context.project_id,
            gcp_conn_id=self.context.gcp_conn_id
        )
        return op


