# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.dummy_operator import DummyOperator
from workflow_framework.config import *
from workflow_framework.process_dataproc import *
from workflow_framework.process_base import *
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from select import select
from airflow import AirflowException

# Class: ProcessSSH
# Description: Spark processing functions
class ProcessSSH(ProcessBase):
    
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        
    
    def get_ssh_command(self):
        
        command="""
        teamspace=""{0}""
        dpaas_env={1}
        cluster_name={2}
        whoami
        echo 'Doc Process Command'
        ls -ltr
        echo 'Command Started'
        if [ $? -eq 0 ]; then
            echo "Command is Successful"
        else
            echo "Command Failed"
            return 1
        fi
        """
        return command.format('a','b','c')
        
    
    

    def run_ssh_command(self, ssh, ssh_client, command):
        print(command)
        stdin, stdout, stderr = ssh_client.exec_command(command=command)
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b''
        agg_stderr = b''
        timeout=None
        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        # read from both stdout and stderr
        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], timeout)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    print(line.decode('utf-8').strip('\n'))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    print(line.decode('utf-8').strip('\n'))
            if stdout.channel.exit_status_ready()\
                    and not stderr.channel.recv_stderr_ready()\
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()
        
        exit_status = stdout.channel.recv_exit_status()
        if exit_status is 0:
            print(agg_stdout)
            print(agg_stdout.decode('utf-8'))
        else:
            print(agg_stdout)
            print(agg_stderr)
            print(agg_stdout.decode('utf-8'))
            print(agg_stderr.decode('utf-8'))
            #error_msg = stderr.read()
            #print(error_msg.decode('utf-8'))
            #error_msg_new = "\n".join(stderr.readlines())
            #print(error_msg_new)
            raise AirflowException("error running cmd: {0}, error: {1}"
                                        .format('ssh command', stderr))
    def ssh_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        ssh_conn_id='ssh-conn-id'

        ssh = SSHHook(ssh_conn_id=ssh_conn_id)
        
        ssh_client = None
        try:
            ssh_client = ssh.get_conn()
            ssh_client.load_system_host_keys()
            
            self.run_ssh_command(ssh, ssh_client, self.get_ssh_command())
        finally:
            if ssh_client:
                ssh_client.close()

    def build_ssh_task(self, task_name, dyn_params):
        args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }
        op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.ssh_op,
            op_kwargs=args
         )
        return op

    