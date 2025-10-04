# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.operators.dummy_operator import DummyOperator
from workflow_framework.config import *
from workflow_framework.process_sqoop import *
from workflow_framework.process_dataproc import *
from workflow_framework.process_dq import *
from workflow_framework.process_hive import *
from workflow_framework.process_spark import *
from workflow_framework.process_kafka import *
from workflow_framework.process_file import *
from workflow_framework.process_bq import *
from workflow_framework.process_misc import *
from workflow_framework.process_end import *
from workflow_framework.process_mainframe import *
from workflow_framework.process_file_gcs import *
from workflow_framework.process_script import *
from workflow_framework.process_s3 import *
from workflow_framework.process_sharepoint import *
from workflow_framework.process_doc_summary import *
from workflow_framework.process_vector_load import *
from workflow_framework.process_rag_eval import *
from workflow_framework.process_read_email import *
from workflow_framework.process_ssh import *
from workflow_framework.process_db import *
from workflow_framework.process_kb import *
from workflow_framework.process_doc import *
from workflow_framework.process_csv_to_text import *
from workflow_framework.process_db_to_text import *

#from workflow_framework.process_backfeed import *
from workflow_framework.workflow import *

gcs_client = None


# Class: Framework
# Description: Core Framework class, generates Operator chain out of config file
class Framework:

    def __init__(self, wf_name, config_path):
        self.config={}
        self.wf_name = wf_name
        print(config_path)
        #self.path = Path('/usr/local/airflow/config_files/dags/'+config_path+'config.yaml')
        self.path = Path('/home/ec2-user/airflow/dags/'+config_path+'config.yaml')
        print(self.path)
        self.context = WorkflowContext(wf_name, self.config, self.path)

        # Init processors
        self.process_sqoop = ProcessSqoop(self.context)
        self.process_dataproc = ProcessDataproc(self.context)
        self.process_dq = ProcessDQ(self.context)
        self.process_mainframe = ProcessMainframe(self.context)
        self.process_file_gcs = ProcessFileGcs(self.context)
        self.process_hive = ProcessHive(self.context)
        self.process_spark = ProcessSpark(self.context)
        self.process_kafka = ProcessKafka(self.context)
        self.process_bq = ProcessBQ(self.context)
        self.process_file = ProcessFile(self.context)
        self.process_misc = ProcessMisc(self.context)
        self.process_script = ProcessScript(self.context)
        self.process_end = ProcessEnd(self.context)
        self.process_s3 = ProcessS3(self.context)
        self.process_ssh = ProcessSSH(self.context)
        self.process_doc_summary = ProcessDocSummary(self.context)
        self.process_vector_load = ProcessVectorLoad(self.context)
        self.process_rag_eval = ProcessRagEval(self.context)
        self.process_sharepoint = ProcessSharePoint(self.context)
        self.process_read_email = ProcessReadEmail(self.context)
        self.process_db = ProcessDB(self.context)
        self.process_kb = ProcessKB(self.context)
        self.process_doc = ProcessDOC(self.context)
        self.process_csv_to_text = ProcessCSVToText(self.context)
        self.process_db_to_text = ProcessDBToText(self.context)
        
        #self.process_backfeed = ProcessBackfeed(self.context)

    def get_config(self):
        return self.config

    def get_tags(self):
        tags_str = self.config[PROPERTIES][TAGS]
        tags = []
        tag_parts=tags_str.split(",")
        for part in tag_parts:
            tags.append(part.strip())
        return tags

    def build_dag_from_config(self):
        """ Call the processes

         This function load the Tasks from congig files and call 
         its type specific process
        """
        conf = Config(self.wf_name, self.path)
        config = conf.load_config_light()
        taskdict = {}
        # Extend the graph with a task for each new name
        for task in config[TASKS]:
            task_name = task[NAME]
            task_type = task[TYPE]

            op = self.build_task(task_type,task_name)
            try:
                op.doc_md = task[DESCRIPTION]
                taskdict[task_name] = op
            except:
                pass

            if 'parents' in task and len(task[PARENTS]) > 0:
                for parent in task[PARENTS]:
                    try:
                        taskdict[parent] >> taskdict[task_name]
                    except:
                        pass

    def build_backfeed_dag(self,cluster_name, region, conn_id, backfeed_configs, dag):
        create_cluster = self.process_backfeed.create_cluster(cluster_name, region, conn_id, dag)
        copy = self.process_backfeed.build_backfeed_task('gcp-to-prod-copy', backfeed_configs)
        delete_cluster = self.process_backfeed.delete_cluster(cluster_name, region, conn_id, dag)
        return create_cluster >> copy >> delete_cluster

    def build_task(self,task_type,task_name, dyn_params={}):

        if task_type == END:
            op = self.process_end.build_end_task(task_name)
        elif task_type == DPROC_CREATE:
            op = self.process_dataproc.build_create_cluster_task(task_name)
        elif task_type == DPROC_DELETE:
            op = self.process_dataproc.build_delete_cluster_task(task_name)
        elif task_type == SQOOP:
            op = self.process_sqoop.build_sqoop_task(task_name, dyn_params)
        elif task_type == MAINFRAME_GCS:
            op = self.process_mainframe.build_mnf_gcs_task(task_name)
        elif task_type == FILE_GCS:
            op = self.process_file_gcs.build_file_gcs_task(task_name)
        elif task_type == S3:
            op = self.process_s3.build_s3_task(task_name)
        elif task_type == SSH:
            op = self.process_ssh.build_ssh_task(task_name, dyn_params)
        
        elif task_type == HIVE:
            op = self.process_hive.build_hive_task(task_name, dyn_params)
        #elif task_type == KAFKA:
            #op = self.process_kafka.build_kafka_task(task_name)
        elif task_type == DQ:
            op = self.process_dq.build_dq_task(task_name)
        elif task_type == SPARK_SQL:
            op = self.process_spark.build_spark_sql_task(task_name, dyn_params)
        elif task_type == ARCHIVE:
            op = self.process_file.build_archive_task(task_name)

        #elif task_type == TRIGGER:
            #op = self.process_misc.build_trigger_task(task_name)
        #elif task_type == EMAIL:
            #op = self.process_misc.build_email_task(task_name)
        elif task_type == BQ:
            op = self.process_bq.build_bq_task(task_name, dyn_params)
        elif task_type == DELETE_FILE:
            op = self.process_file.build_delete_file_task(task_name)
        #elif task_type == CREATE_FILE:
            #op = self.process_file.build_create_file_task(task_name)
        #elif task_type == DELETE_FILE:
            #op = self.process_file.build_delete_file_task(task_name)
        elif task_type == SCRIPT:
            op = self.process_script.build_remote_script_task(task_name)
        elif task_type == DOC_SUMMARY:
            op = self.process_doc_summary.build_doc_summary_task(task_name, dyn_params)
        elif task_type == VECTOR_LOAD:
            op = self.process_vector_load.build_vector_load_task(task_name, dyn_params)
        elif task_type == RAG_EVAL:
            op = self.process_rag_eval.build_rag_eval_task(task_name, dyn_params)
        elif task_type == SHAREPOINT:
            op = self.process_sharepoint.build_sharepoint_task(task_name, dyn_params)
        elif task_type == READ_EMAIL:
            op = self.process_read_email.build_read_email_task(task_name, dyn_params)
        elif task_type == DB:
            op = self.process_db.build_db_task(task_name, dyn_params)
        elif task_type == KB:
            op = self.process_kb.build_kb_task(task_name, dyn_params)
        elif task_type == DOC:
            op = self.process_doc.build_doc_task(task_name, dyn_params)
        elif task_type == CSV_TO_TEXT:
            op = self.process_csv_to_text.build_csv_to_text_task(task_name, dyn_params)
        elif task_type == DB_TO_TEXT:
            op = self.process_db_to_text.build_db_to_text_task(task_name, dyn_params)
        #elif task_type == BACKFEED:
            #op = self.process_backfeed.build_backfeed_task(task_name, dyn_params)
        #elif task_type == HTTP_REQUEST:
            #op = self.process_file.build_http_request_task(task_name)
        else:
            op = DummyOperator(
                task_id=task_name
            )
        return op

    def build_dag_emails(self):
        """return the email
        
        This function is used to return the email from the config files
        """ 
        emails = []
        wf_emails = self.config[EMAILS]
        emails.append(wf_emails)
        return emails

    def build_dag_tags(self):
        """return the tags

        This function is used to return the tags from the config files
        """
        tags = []
        wf_tags = self.config[TAGS]
        tags.append(wf_tags)
        return tags

    def build_trigger_task(self, task_name, wf, wait=True, params={}):
        """This function is used to implement cross-DAG dependencies

        trigger_dag_id: The dag_id to trigger
        """
        op = TriggerDagRunOperator(
            task_id=task_name,
            trigger_dag_id=wf,
            conf=params,
            wait_for_completion=wait
        )
        return op

    def build_event_task(self, task_name, conn, bucket, file_prefix, mode='poke', interval=300, timeout=1800, skip=True):
        op = GCSObjectsWithPrefixExistenceSensor(
            bucket=bucket,
            prefix=file_prefix,
            google_cloud_conn_id=conn,
            mode=mode,
            poke_interval = interval,
            timeout = timeout,
            task_id=task_name,
            soft_fail=skip
        )
        return op
    
    def build_event_dag_task(self, dag, task_name, conn, external_dag,external_task='end', mode='poke', interval=300, timeout=1800, time_delta = timedelta(minutes=24*60), skip=True):
        op = ExternalTaskSensor(
            task_id=task_name,
            poke_interval=interval,
            timeout=timeout,
            soft_fail=skip,
            retries=2,
            external_dag_id=external_dag,
            external_task_id=external_task,
            execution_delta=time_delta,
            mode=mode,
            dag=dag)
        return op

    def check_file(self, conn_id, path, run_date, timedelta_mins, soft=False):
        done_bucket=get_bucket(path)
        print(done_bucket)
        file_prefix = get_target_folder(path)
        print(file_prefix)
        print(run_date)
        run_date=str(run_date).split(".")[0]
        newdate = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=timedelta_mins)
        newdate = datetime.strftime(newdate,'%Y%m%d%H%M%S')
        file_prefix=file_prefix+'_'+newdate
        print(file_prefix)
        
        hook = GCSHook(gcp_conn_id=conn_id, delegate_to=None)
        files = hook.list(done_bucket, prefix=file_prefix)
        if len(files)>0:
            return True
        elif soft == False:
            return False
        else:
            return True

    def check_done_task(self,task_name, run_date):
        self.context.initialize()
        task = self.context.config_obj.get_task(task_name)
        print(task)
        dags = task[PROPERTIES]["dags"]
        dag_list=dags.split(",")
        
        for dag in dag_list:
            params = task[PROPERTIES][dag]
            param_list=params.split(",")

            conn_id=""
            path=""
            timedelta_mins=0
            soft=False

            for param in param_list:
                kv=param.split('=')
                print(param)
                if len(kv)==2:
                   if kv[0]=='gcp_conn':
                       conn_id=kv[1]
                   if kv[0]=='done_path':
                       path=kv[1] 
                   if kv[0]=='timedelta_minutes':
                       timedelta_mins=int(kv[1])
                   if kv[0]=='dep_type' and kv[1]=='soft':
                       soft=True
            if self.check_file(conn_id,path,run_date,timedelta_mins,soft) == False:
                return False
        return True
     
    def build_event_done_task(self, dag, task_name, mode='poke', interval=300, timeout=1800, skip=True):
        op = PythonSensor(
            task_id=task_name,
            python_callable=self.check_done_task,
            poke_interval=interval,
            timeout=timeout,
            soft_fail=skip,
            mode=mode,
            op_kwargs={"task_name": task_name, "run_date": "{{ dag_run.start_date }}"},
            dag=dag,
            )
        return op
