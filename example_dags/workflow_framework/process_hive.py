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

# Class: ProcessHive
# Description: Hive processing functions
class ProcessHive(ProcessBase):
    
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def run_hive_job(self,task_name,dyn_params={},**kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        process_dataproc = ProcessDataproc(self.context)
        process_dataproc.check_and_launch()

        if PRE_SCRIPT in task:
            self.process_script_remote(task)

        print(dyn_params)

        if dyn_params:
            task[PROPERTIES][QUERIES]=replace_vars(dyn_params, task[PROPERTIES][QUERIES])

        job={
                "reference": {"project_id": self.context.project_id},
                "placement": {"cluster_name": self.context.cluster_name},
                "hive_job": {"query_list": {"queries": [task[PROPERTIES][QUERIES]]}},
            }
        dproc_hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        info = dproc_hook.submit_job(job=job, project_id=self.context.project_id, region=self.context.region)
        print(info)
        print(info.reference.job_id)
        dproc_hook.wait_for_job(job_id=info.reference.job_id, project_id=self.context.project_id, region=self.context.region, wait_time=15)

        if POST_SCRIPT in task:
            self.process_script_remote(task)

    def build_hive_task(self, task_name, dyn_params={}):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_hive_job,
            op_kwargs={
                'task_name': task_name,
                'dyn_params': dyn_params
            }
        )
        return op

