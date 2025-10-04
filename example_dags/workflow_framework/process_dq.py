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


# Class: ProcessDQ
# Description: DQ class, performs data quality checks
class ProcessDQ(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def run_dq_job(self,task_name):
        super().init_config()
        task = self.context.config_obj.get_task(task_name)

        process_dataproc = ProcessDataproc(self.context)
        process_dataproc.check_and_launch()

        dq_params = self.context.config[PROPERTIES]["dq_params"]
        config_bucket = self.context.env['config_bucket']
        dq_framework_path = self.context.env['dq_framework_path']
        dq_file =task[PROPERTIES]["dq_file"]
        dq_file_path = config_bucket+"/dags/"+self.context.config['path']+dq_file

        print(dq_file_path)
        queries = "fs -cp -f "+config_bucket+"/"+dq_framework_path+" file:///tmp/; "+ "fs -cp -f "+dq_file_path+" file:///tmp/dataquality_framework/sample_yamls/; "+"fs -chmod -R 755 file:///tmp/dataquality_framework; "+"sh /tmp/dataquality_framework/bin/run-dq.sh -env dev -cluster "+self.context.config[CLUSTER_NAME]+" -region "+self.context.env[REGION]+" -project "+self.context.env[PROJECT_ID]+" -inputPath /tmp/dataquality_framework/sample_yamls/"+dq_file+" " +dq_params

        job={
                "reference": {"project_id": self.context.project_id},
                "placement": {"cluster_name": self.context.cluster_name},
                "pig_job": {"query_list": {"queries": [queries]}},
            }

        dproc_hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        info = dproc_hook.submit_job(job=job, project_id=self.context.project_id, region=self.context.region)
        print(info)
        print(info.reference.job_id)
        dproc_hook.wait_for_job(job_id=info.reference.job_id, project_id=self.context.project_id, region=self.context.region, wait_time=15)


    def build_dq_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_dq_job,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op