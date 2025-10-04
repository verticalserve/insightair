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

# Class: ProcessKafka
# Description: Kafka processing functions
class ProcessKafka(ProcessBase):
    
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def build_kafka_task(self, task):
        op = DataprocSubmitJobOperator(
            task_id=task[NAME],
            job={
                "reference": {"project_id": self.project_id},
                "placement": {"cluster_name": self.cluster_name},
                "pyspark_job": {"main_python_file_uri": task[PROPERTIES][CODE_LOCATION]},
            },
            region=self.region,
            project_id=self.project_id,
            gcp_conn_id=self.gcp_conn_id
        )
        return op

