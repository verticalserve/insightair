# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *


# Class: ProcessRagEval
# Description: RAG evaluation processing functions
class ProcessRagEval(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def rag_eval_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

    def build_rag_eval_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.rag_eval_op,
            op_kwargs=args
         )
         return op
