# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.gcs_hook import GCSHook
from workflow_framework.config import *
from datetime import datetime
from airflow.models import Variable
from workflow_framework.utils import *
from workflow_framework.process_base import *
from workflow_framework.process_dataproc import *

# Class: ProcessSqoop
# Description: SQOOP processing functions
class ProcessSqoop(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def build_sqoop_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.sqoop_op,
            op_kwargs=args
         )
         return op

    def get_db(self,connection):
        """Return the database type"""

        if connection.startswith("DB2"):
            return "DB2"
        elif connection.startswith("ORA"):
            return "ORACLE"
        elif connection.startswith("MSSQL"):
            return "MSSQL"
        elif connection.startswith("INFMX"):
            return "INFORMIX"
        elif connection.startswith("MYSQL"):
            return "MYSQL"
        else:
            return "MYSQL"


    def sqoop_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)

        config_bucket = self.context.env[CONFIG_BUCKET]
        sqoop_jars_folder = self.context.env[SQOOP_JARS_PATH]
        certs_folder = self.context.env[CERTS_PATH]

        raw_bucket = self.context.data_group[RAW_BUCKET]
        archive_bucket = self.context.data_group[ARCHIVE_BUCKET]
        target_path = task[PROPERTIES][TARGET_PATH]
        connection = task[PROPERTIES][SRC_CONNECTION]

        process_dataproc = ProcessDataproc(self.context)
        process_dataproc.check_and_launch()

        task['kwargs']=kwargs
        self.pre_process(task)

        hook = GCSHook(gcp_conn_id=self.context.gcp_conn_id, delegate_to=None)
        sqoop_jars = hook.list(config_bucket.replace("gs://",""), prefix=sqoop_jars_folder)  ##
        sqoop_jars = list(map(lambda x: config_bucket + '/' + x, sqoop_jars))
        sqoop_jars = list(filter(None, list(map(lambda x: None if x.split('/')[-1] == '' else x, sqoop_jars))))

        certs = hook.list(config_bucket.replace("gs://",""), prefix=certs_folder)
        certs = list(map(lambda x: config_bucket + '/' + x, certs))
        certs = list(filter(None, list(map(lambda x: None if x.split('/')[-1] == '' else x, certs))))

        sqoop_jars.extend(certs)
        print(sqoop_jars)
        #connection_info = json.loads(Variable.get(connection))
        connection_info = 'sm://astro-variables-'+connection
        print(connection_info)

        target_folder = get_target_folder(target_path)

        archive=True
        if 'archive' in task[PROPERTIES]:
            if task[PROPERTIES]['archive']=='false':
                archive=False

        if archive == True:
            files = hook.list(raw_bucket.replace("gs://",""), prefix=target_folder)
            for file in files:
                hook.copy(source_bucket=raw_bucket.replace("gs://",""), source_object=file,
                          destination_bucket=archive_bucket.replace("gs://",""), destination_object= file.replace('landing','landing/archive'))
                hook.delete(bucket_name=raw_bucket.replace("gs://",""), object_name=file)


        target_path = target_path.replace("`date +%Y%m%d-%H%M`",
                                          datetime.today().strftime('%Y%m%d-%H%M'))
        target_path = target_path.replace("$raw_bucket",raw_bucket)
        task[PROPERTIES][TARGET_PATH]=target_path
        print(target_path)
        db = self.get_db(connection)

        sqoop_args=self.get_args(db, task, connection_info)
        if 'options' in task[PROPERTIES]:
            opt_list=task[PROPERTIES]['options'].split(" ")
            for opt in opt_list:
                opt=opt.strip()
                if opt != "":
                    sqoop_args.append(opt)

        job = {
            "reference": {"project_id": self.context.project_id},
            "placement": {"cluster_name": self.context.cluster_name},
            "hadoop_job": {
                #"main_class": "org.apache.sqoop.Sqoop",
                "main_class": "com.wmt.SqoopSecretResolver",
                "jar_file_uris": sqoop_jars,
                "args": sqoop_args
            },
        }
        dproc_hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        info = dproc_hook.submit_job(job=job, project_id=self.context.project_id, region=self.context.region)
        print(job)
        print(info.reference.job_id)
        dproc_hook.wait_for_job(job_id=info.reference.job_id, project_id=self.context.project_id, region=self.context.region, wait_time=15)

        self.post_process(task)

    def get_args(self,db, task, connection_info):
        if db == 'DB2':
            return self.build_db2(task, connection_info)
        elif db == 'ORACLE':
            return self.build_oracle(task, connection_info)
        elif db == 'MSSQL':
            return self.build_mssql_jdbc(task, connection_info)
        elif db == 'MYSQL':
            return self.build_mysql(task, connection_info)
        elif db == 'INFORMIX':
            return self.build_informix(task, connection_info)
        else:
            return []


    def build_oracle(self,task, connection_info):
        return [
            "import",
            "-Dmapreduce.job.user.classpath.first=true",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "-Doracle.jdbc.timezoneAsRegion=false",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR].replace('\\u0001','\u0001')),
            "--as-textfile"
        ]


    def build_mssql_jdbc(self,task, connection_info):
        return [
            "import",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--connection-manager={0}".format("org.apache.sqoop.manager.SQLServerManager"),
            "--driver={0}".format("net.sourceforge.jtds.jdbc.Driver"),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
        ]


    def build_mssql_windows(self,task, connection_info):
        return [
            "import",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--connection-manager={0}".format("org.apache.sqoop.manager.SQLServerManager"),
            "--driver={0}".format("net.sourceforge.jtds.jdbc.Driver"),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
        ]


    def build_mssql_azure(self,task, connection_info):
        return [
            "import",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--connection-manager={0}".format("org.apache.sqoop.manager.SQLServerManager"),
            "--driver={0}".format("net.sourceforge.jtds.jdbc.Driver"),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
        ]


    def build_informix(self,task, connection_info):
        return [
            "import",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--driver={0}".format("com.informix.jdbc.IfxDriver"),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
        ]


    def build_mysql(self,task, connection_info):
        return [
            "import",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "--username={0}".format(connection_info+'/username'),
            "--password={0}".format(connection_info+'/password'),
            "--connect={0}".format(connection_info+'/connection_url'),
            "--driver={0}".format("com.mysql.jdbc.Driver"),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
        ]


    def build_db2(self,task, connection_info):

        if SPLIT_QUERY in task[PROPERTIES]:
            return [
            "import",
            "-Ddb2.jcc.alternateUTF8Encoding=1",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "-Ddb2.jcc.useCcsid420ShapedConverter=false",
            "-Ddb2.jcc.charsetDecoderEncoder=3",
            "-Dsun.jnu.encoding=UTF-8",
            "-Dfile.encoding=UTF8",
            "-Dclient.encoding.override=UTF-8",
            "-driver=com.ibm.db2.jcc.DB2Driver",
            "--connect={0}".format(connection_info+'/connection_url'),
            "--username={0}".format(connection_info+'/username'),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--boundary-query={0}".format(task[PROPERTIES][SPLIT_QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
            ]
        else:
            return [
            "import",
            "-Ddb2.jcc.alternateUTF8Encoding=1",
            "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
            "-Ddb2.jcc.useCcsid420ShapedConverter=false",
            "-Ddb2.jcc.charsetDecoderEncoder=3",
            "-Dsun.jnu.encoding=UTF-8",
            "-Dfile.encoding=UTF8",
            "-Dclient.encoding.override=UTF-8",
            "-driver=com.ibm.db2.jcc.DB2Driver",
            "--connect={0}".format(connection_info+'/connection_url'),
            "--username={0}".format(connection_info+'/username'),
            "--target-dir={0}".format(task[PROPERTIES][TARGET_PATH]),
            "--split-by={0}".format(task[PROPERTIES][SPLIT_BY]),
            "--query={0}".format(task[PROPERTIES][QUERY]),
            "--num-mappers={0}".format(task[PROPERTIES][NUM_MAPPERS]),
            "--fields-terminated-by={0}".format(task[PROPERTIES][SEPERATOR]),
            "--as-textfile"
            ]

