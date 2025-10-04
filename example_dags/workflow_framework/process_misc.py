# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from workflow_framework.config import *
from workflow_framework.process_base import *

# Class: ProcessMisc
# Description: Miscellaneous processing functions
class ProcessMisc(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def build_foreach(self, task):
        """This function is used to implement cross-DAG dependencies
        
        trigger_dag_id: The dag_id to trigger
        """

        op = TriggerDagRunOperator(
            task_id=task[NAME],
            trigger_dag_id=task[WORKFLOW],
            conf={},
        )
        return op

    def build_email(self, task):
        """
        Sends an email.

        to: list of emails to send the email to. (templated)
        subject: subject line for the email. (templated)
        html_content: str to represent in email
        """
        op = EmailOperator(
            task_id=task[NAME],
            to='test@mail.com',
            subject='Alert Mail',
            html_content=""" Mail Test """,
            dag=task[WORKFLOW]
        )
        return op

    def build_trigger_task(self, task_name):
        """This function is used to implement cross-DAG dependencies
        
        trigger_dag_id: The dag_id to trigger
        """
        op = TriggerDagRunOperator(
            task_id=task[NAME],
            trigger_dag_id=task[PROPERTIES][WORKFLOW],
            conf={},
        )
        return op


