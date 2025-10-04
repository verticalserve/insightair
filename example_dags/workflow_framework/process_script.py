# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid
import os
import subprocess
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from workflow_framework.config import *
from airflow.contrib.hooks.gcs_hook import GCSHook
from workflow_framework.process_dataproc import *
from workflow_framework.process_base import *
from workflow_framework.utils import *

# Class: ProcessScript
# Description: Script processing functions
class ProcessScript:
    def __init__(self, context):
        self.context = context

    def run_remote_script(self,task, params, script_file, script_file_path, wrapper, delim=","):
        raw_bucket = self.context.env['raw_bucket']
        config_bucket = self.context.env['config_bucket']
        script_processor = config_bucket+"/scripts/script_processor.py"
        result_file="r"+uuid.uuid4().hex

        if wrapper==True:
            queries = "fs -cp -f "+script_file_path+" file:///tmp/; "+ \
                      "fs -cp -f "+script_processor+" file:///tmp/; "+ \
                      "fs -chmod -R 755 file:///tmp/"+script_file+"; "+\
                      "fs -mkdir -p "+raw_bucket+"/results"+";"+ \
                      "sh python /tmp/script_processor.py -s /tmp/"+script_file+" -o "+result_file+" -p "+params+"; "+\
                      "fs -copyFromLocal /tmp/"+result_file+" "+raw_bucket+"/results/"+result_file+";"
        else:
            param_list=params.split(delim)
            param_str=""
            for param in param_list:
                kv=param.split('=')
                print(param)
                if len(kv)==2:
                    param_str=param_str+" "+kv[1].replace("'","\"")
                else:
                    param_str=param_str+" "+param.replace(kv[0]+'=','')
                print(param_str)

            queries = "fs -cp -f "+script_file_path+" file:///tmp/; "+ \
                      "fs -chmod -R 755 file:///tmp/"+script_file+"; "+\
                      "sh /tmp/"+script_file+" "+result_file+" "+param_str+"; "+\
                      "fs -mkdir -p "+raw_bucket+"/results"+";"+ \
                      "fs -copyFromLocal /tmp/"+result_file+" "+raw_bucket+"/results/"+result_file+";"
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
        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)


        output = hook.download(object_name="results/"+result_file, bucket_name=raw_bucket.replace("gs://",""))
        print(output)
        lines=output.decode("utf-8").split('\n')
        kwargs=task['kwargs']
        print(kwargs)
        ti = kwargs['ti']
        wf_params={}
        for line in lines:
            if len(line.strip())>0:
                vars=line.split("=")
                if len(vars)==2:
                    print(vars[0])
                    print(vars[1])
                    wf_params[vars[0]]=vars[1].replace('\n','').replace('\r','').replace('\'','')
        ti.xcom_push(key="wf_params", value=wf_params)
        print(ti.xcom_pull(key="wf_params"))


    def run_remote_pre_script_job(self,task):

        params = task[PRE_SCRIPT]["params"]
        config_bucket = self.context.env['config_bucket']
        raw_bucket = self.context.env['raw_bucket']
        script_file =task[PRE_SCRIPT]["script_file"]
        script_file_path = config_bucket+"/dags/"+self.config['path']+script_file

        print(script_file_path)
        self.run_remote_script(task,params,script_file, script_file_path, True)

    def run_remote_post_script_job(self,task):

        params = task[POST_SCRIPT]["params"]
        config_bucket = self.context.env['config_bucket']
        script_file =task[POST_SCRIPT]["script_file"]
        script_file_path = config_bucket+"/dags/"+self.config['path']+script_file

        print(script_file_path)
        self.run_remote_script(task,params,script_file, script_file_path, True)

    def run_remote_script_job(self,task_name,dyn_params={}, **kwargs):
        self.context.initialize(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        process_dataproc = ProcessDataproc(self.context)
        process_dataproc.check_and_launch()

        task['kwargs']=kwargs
        delim=","
        params = task[PROPERTIES]["params"]
        if 'params_delim' in task[PROPERTIES]:
            delim = task[PROPERTIES]["params_delim"]
        wrapper = True
        if 'wrapper' in task[PROPERTIES]:
            wrapper = False

        ti = kwargs['ti']
        print(ti)
        wf_params=ti.xcom_pull(key='wf_params')
        print(wf_params)

        if wf_params is not None:
            params=replace_vars(wf_params,params)

        config_bucket = self.context.env['config_bucket']
        script_file =task[PROPERTIES]["script_file"]
        script_file_path = config_bucket+"/dags/"+self.context.config['path']+script_file

        if 'script_location' in task[PROPERTIES]:
            script_location = task[PROPERTIES]["script_location"] 
            script_file_path = script_location+script_file
        print(script_file_path)
        self.run_remote_script(task,params,script_file, script_file_path, wrapper, delim)


    def build_remote_script_task(self, task_name, dyn_params={}):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_remote_script_job,
            op_kwargs={
                'task_name': task_name,
                'dyn_params': dyn_params
            }
        )
        return op

    def run_local_script_job(self,task_name, **kwargs):
        task = self.context.conf.get_task(task_name)

        task['kwargs']=kwargs
        params = task[PROPERTIES]["params"]
        config_bucket = self.context.env['config_bucket']
        script_file =task[PROPERTIES]["script_file"]
        script_file_path = config_bucket+"/"+self.context.config['path']+script_file

        print(script_file_path)
        print(os.getcwd())
        result_file="r"+uuid.uuid4().hex
        path="dags/"+self.context.config[PATH]

        subprocess.run(["chmod","u+x", path+script_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = subprocess.run(["sh", path+script_file,result_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.returncode, result.stdout, result.stderr)
        if result.returncode==0:
            result = subprocess.run(["cat", "/tmp/"+result_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(result.returncode, result.stdout, result.stderr)
            if result.returncode==0:
                vars=result.stdout.decode("utf-8").split("=")
                print(vars[0])
                print(vars[1])
                kwargs=task['kwargs']
                print(kwargs)
                ti = kwargs['ti']
                wf_params={}
                wf_params[vars[0]]=vars[1].replace('\n','').replace('\r','').replace('\'','')
                ti.xcom_push(key="wf_params", value=wf_params)
                print(ti.xcom_pull(key="wf_params"))
            else:
                raise ValueError(result.stderr)
        else:
            raise ValueError(result.stderr)


    def build_local_script_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_local_script_job,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op

    def run_remote_script_rest_job(self,task):
        print('Enable REST API')

    def build_remote_script_rest_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.run_remote_script_rest_job,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op
