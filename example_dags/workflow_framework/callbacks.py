# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
import json
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.models import Variable
from airflow.utils.email import send_email
from workflow_framework.config import *

# Function: delete_resources
# Description: Deletes the workflow resources like Dataproc cluster
# Input: Config object
# Output: None
def delete_resources(wf_name, config_path):
    path = Path('/usr/local/airflow/config_files/dags/'+config_path+'config.yaml')
    conf = Config(wf_name, path)
    conf.load_configs()
    config = conf.get_config()

    data_group = conf.get_data_group()
    gcp_conn_id = data_group[GCP_CONN_ID]
    project_id = data_group[PROJECT_ID]
    region = data_group[REGION]
    cluster_name = config[CLUSTER_NAME]

    hook = DataProcHook(gcp_conn_id=gcp_conn_id,
                              delegate_to=None)
    hook.delete_cluster(
        cluster_name = cluster_name,
        project_id = project_id,
        region = region
    )

# Function: failure_callback
# Description: On the DAG failure creates a Service Now ticket, sends an email and deletes the cluster.
# Input: Context - Workflow context, Config, Workflow Name, Priority
# Output: None
def failure_callback(context, path, wf_name, priority='P3'):

    # Spot Light info is stored in Airflow variables
    spotlight_url  = Variable.get('spotlight_url')
    spotlight_event_type = Variable.get('spotlight_event_type')
    email = Variable.get('spotlight_email')

    # Fetch the data classification from the workflow name
    data_class = 'NS'
    if 'secure' in wf_name:
        data_class = 'S'
    elif 'highsecure' in wf_name:
        data_class = 'HS'

    # Send a message to Spot Light, which will create a Service Now ticket
    submit_spotlight_ticket("Airflow DAG Failure Notification",context, data_class, spotlight_url, spotlight_event_type)

    #Send the DAG failure email
    send_priority_email('DAG',context, data_class, priority, email)

    # Finally delete the cluster
    delete_resources(wf_name, path)

# Function: sla_miss_callback
# Description: On the SLA failure creates a Service Now ticket and sends an email.
# Input: Context - Workflow context, Workflow Name, Priority
# Output: None
def sla_miss_callback(context, wf_name, priority='P3'):
    # Spot Light info is stored in Airflow variables
    spotlight_url  = Variable.get('spotlight_url')
    spotlight_event_type = Variable.get('spotlight_event_type')
    email = Variable.get('spotlight_email')

    # Fetch the data classification from the workflow name
    data_class = 'NS'
    if 'secure' in wf_name:
        data_class = 'S'
    elif 'highsecure' in wf_name:
        data_class = 'HS'

    # Send a message to Spot Light, which will create a Service Now ticket
    submit_spotlight_ticket("SLA Failure Notification",context, data_class, spotlight_url, spotlight_event_type)

    # Send the SLA failure email
    send_priority_email('SLA',context, data_class, priority, email)

# Function: submit_spotlight_ticket
# Description: On the SLA failure creates a Service Now ticket and sends an email.
# Input: Description, Workflow context, Data classification, Spot Light URL, Spot Light Event Queue Type
# Output: None
def submit_spotlight_ticket(description, context, data_class, spotlight_url, event_type):
    print("submit_spotlight_ticket: Submitting spotlight ticket")

    # Get the project details
    project_key = "PROJECT_" + data_class
    project = "UNKNOWN"
    region = "UNKNOWN"
    try:
        project = Variable.get(project_key)
        region = Variable.get("REGION")
    except:
        pass

    spotlight_host_ip = "127.0.0.1"
    system_id = {"ip": spotlight_host_ip}

    task_instance = context['task_instance']
    properties = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": str(context['run_id']),
        "log_url": str(task_instance.log_url),
        "run_date": str(context['ts']),
        "error_message": str(context['exception']),
        "gcp_project": project,
        "region": region,
        "status": task_instance.state
    }  # Properties are key value pairs defined in spotlight event.

    req_json = {
        "eventType": event_type,
        "shortDesc": description,
        "occurrenceTime": str(int(time.time() * 1000)),
        "systemIdentity": system_id,
        "user": "user",
        "properties": properties
    }
    result = requests.post(spotlight_url, json=req_json)
    print("submit_spotlight_ticket: response: {}", result.content)

# Function: send_priority_email
# Description: Sends P1 and other priority emails
# Input: Type (DAG Failure or SLA Failure, Workflow context, Data classification, Priority
# Output: None
def send_priority_email(type, context, data_class, priority, email):
    print("Sending email")

    # For now, only send P1 priority emails
    if priority != 'P1':
        return

    project_key = "PROJECT_" + data_class
    project = "UNKNOWN"
    try:
        project = Variable.get(project_key)
    except:
        pass

    task_instance = context['task_instance']
    email_subject = "ALARM: P1 Object: {} in {} is Failed ".format(task_instance.task_id, task_instance.dag_id)
    reason = str(context['exception'])
    if type == 'SLA':
        email_subject = "ALARM: P1 Object: {} in {} SLA Failure ".format(task_instance.task_id, task_instance.dag_id)
        reason = 'SLA Failure'
    email_message = """<table style="border: 1px solid black; border-collapse: collapse">
      <tr>
        <th>{}</th>
        <th>:</th>
        <th>{} is failed and it is <font color='red'>critical!!!<font></th>
      </tr>
      <tr>
        <td>TaskId</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>RunId</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>LogURL</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>RunDate</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>ErrorMessage</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>GCPProject</td>
        <td>:</td>
        <td>{}</td>
      </tr>
      <tr>
        <td>Status</td>
        <td>:</td>
        <td>{}</td>
      </tr>
    </table>""".format(task_instance.dag_id, task_instance.task_id, task_instance.task_id, str(context['run_id']), str(task_instance.log_url), str(context['ts']), reason, project, task_instance.state)

    send_email(to=email, subject=email_subject, html_content=email_message)