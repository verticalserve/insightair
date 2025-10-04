# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import nullcontext
import json
import os
from pathlib import Path
import yaml

from workflow_framework.utils import *

## Config Files
ENV_YAML = 'environment.yaml'
CLUSTER_CONFIG_YAML = 'cluster_config.yaml'
RAW_BUCKET = 'raw_bucket'
ARCHIVE_BUCKET = 'archive_bucket'
STAGE_BUCKET = 'stage_bucket'
MAINFRAME_BUCKET = 'mainframe_bucket'
SOURCE_BUCKET = 'source_bucket'

### Environment
GCS_TOKEN = 'gcs_token'
GCP_SE_CONN_ID = 'gcp_se_conn_id'
GCP_NS_CONN_ID = 'gcp_ns_conn_id'
GCP_HS_CONN_ID = 'gcp_hs_conn_id'

### Properties
NAME = 'name'
CATEGORY = 'category'
QUERIES = 'queries'
PROJECT_ID = 'project_id'
REGION = 'region'
QUERIES = 'queries'
PROPERTY_FILE = 'properties_file'
TASKS = 'tasks'
TASK_CONFIG_FILE = 'task_config_file'
PROPERTIES = 'properties'
WORKFLOW = 'workflow'
CLUSTER_PROFILE = 'cluster_profile'
CLUSTER_NAME = 'cluster_name'
GCP_CONN_ID = 'gcp_conn_id'
TABLE = 'table'
DEST_URI = 'dest_uri'
DATASET = 'dataset'
QUERY = 'query'
NUM_MAPPERS = 'num_mappers'
SEPERATOR = 'seperator'
SPLIT_BY = 'split_by'
OPS_EMAILS = 'ops_emails'
TARGET_PATH = 'target_path'
STAGE_PATH = 'stage_path'
SOURCE_PATH = 'source_path'
FILE_PATTERN = 'file_pattern'
SRC_CONNECTION = 'src_connection'
SPLIT_QUERY = 'split_query'
TYPE = 'type'
PARENTS = 'parents'
DESCRIPTION = 'description'
EMAILS = 'emails'
TAGS = 'tags'
BUCKET = 'bucket'
DEST_BUCKET = 'dest_bucket'
FILE_NAME = 'file_name'
OBJECT = 'object'
PATH = 'path'
RECEIVER = 'receiver'
SENDER = 'sender'
PASSWORD = 'password'
SUBJECT = 'subject'
DEST_DAG_ID = 'dest_dag_id'
CODE_LOCATION = 'code_location'
DEST_DATASET = 'dest_dataset'
DATA_FORMAT = 'data_format'
DELIMITER = 'delimiter'
DATA_GROUP = 'data_group'
DATA_GROUPS = 'data_groups'
CONFIG_BUCKET = 'config_bucket'
DONE_BUCKET = 'done_bucket'
SQOOP_JARS_PATH = 'sqoop_jars_path'
CERTS_PATH = 'certs_path'
MOVE = 'move'
SERVICE_ACCOUNT = 'service_account'
CLUSTER_PROFILE = 'cluster_profile'
SLA_HOURS = 'sla_hours'
PRE_SCRIPT = 'pre_script'
POST_SCRIPT = 'post_script'
ZERO_BYTE_CHK = 'zero_byte_chk'
PROFILER_SQL_INS = 'profiler_sql_ins'
NS_SA_SECRET_KEY = 'ns_sa_secret_key'
PHS_CLUSTER = 'phs_cluster'
PROFILER_BUCKET = 'profiler_bucket'
ZONE_URI = 'zone_uri'
PHS_BUCKET = 'phs_bucket'


#### Task Types
START = 'START'
END = 'END'
DPROC_CREATE = 'DPROC_CREATE'
DPROC_DELETE = 'DPROC_DELETE'
SQOOP = 'SQOOP'
MAINFRAME_GCS = 'MAINFRAME_GCS'
FILE_GCS = 'FILE_GCS'
S3 = 'S3'
SSH = 'SSH'
HIVE = 'HIVE'
KAFKA = 'KAFKA'
DQ = 'DQ'
SPARK_SQL = 'SPARK_SQL'
ARCHIVE = 'ARCHIVE'
TRIGGER = 'TRIGGER'
EMAIL = 'EMAIL'
BQ = 'BQ'
BQ_LOAD = 'BQ_LOAD'
SCRIPT = 'SCRIPT'
CHECK_FILE = 'CHECK_FILE'
FILE_CHECK = 'FILE_CHECK'
CREATE_FILE = 'CREATE_FILE'
DELETE_FILE = 'DELETE_FILE'
SEND_EMAIL = 'SEND_EMAIL'
TRIGGER_DAG = 'TRIGGER_DAG'
HTTP_REQUEST = 'HTTP_REQUEST'
SECRET = 'SECRET'
DOC_SUMMARY = 'DOC_SUMMARY'
VECTOR_LOAD = 'VECTOR_LOAD'
RAG_EVAL = 'RAG_EVAL'
SHAREPOINT = 'SHAREPOINT'
READ_EMAIL = 'READ_EMAIL'
DB = 'DB'
KB = 'KB'
DOC = 'DOC'
CSV_TO_TEXT = 'CSV_TO_TEXT'
DB_TO_TEXT = 'DB_TO_TEXT'

# cluster config
PROFILES = 'profiles'
CLUSTER_TYPE = 'micro'
MASTERS = 'masters'
WORKERS = 'workers'
MASTER_MACHINE = 'master_machine'
WORKER_MACHINE = 'worker_machine'
MASTER_DISK_CONFIG = 'master_disk_config'
WORKER_DISK_CONFIG = 'worker_disk_config'
DISK_TYPE = 'disk_type'
DISK_SIZE = 'disk_size'

# Class: Config
# Description: Core config class
class Config:
    def __init__(self, wf_name, path):
        self.wf_name = wf_name
        self.path = path
        self.tasks={}
        self.config={}
        self.cluster_config={}
        self.data_group=''
        self.env={}

    def load_config_light(self):
        try:
            with self.path.open() as f:
                try:
                    self.config = yaml.safe_load(f)
                except:
                    raise Exception("Config parsing failure")
            self.config = json.loads(json.dumps(self.config, default=str))
        except:
            raise Exception("Config.yaml not found")

        return self.config

    def load_configs(self, wf_params={},run_dict={}, dyn_params={}):
        self.env = self.load_env()
        self.cluster_config = self.load_cluster_config()
        print(f"WF Name: {self.wf_name}, Path: {self.path}")
        self.config = self.load_config(self.wf_name, self.path)
        self.data_group = self.env[DATA_GROUPS][self.config[DATA_GROUP]]
        self.update_env()
        self.update_properties(wf_params, run_dict, dyn_params)
        self.update_params(wf_params, run_dict, dyn_params)
        print(self.config)

    def update_env(self):
        for key in self.data_group:
            if key in self.config[PROPERTIES]:
                self.data_group[key]=self.config[PROPERTIES][key]
            self.env[key] = self.data_group[key]

    def get_config(self):
        return self.config

    def get_env(self):
        return self.env

    def get_data_group(self):
        return self.data_group

    def get_cluster_config(self):
        return self.cluster_config

    def load_env(self):
        try:
            env_content=open('/usr/local/airflow/config_files/environment.yaml','r').read()
            env=yaml.safe_load(env_content)
        except:
            path = self.get_path(ENV_YAML)
            env = self.load_yaml_file(path)
        return env

    def load_cluster_config(self):
        """Function to create cluster config by parsing yaml file
        """
        try:
            config_content=open('/usr/local/airflow/config_files/cluster_config.yaml','r').read()
            cluster_config=yaml.safe_load(config_content)
        except:
            path = self.get_path(CLUSTER_CONFIG_YAML)
            cluster_config = self.load_yaml_file(path)

        return cluster_config

    def update_props(self,dict):
        for key in dict:
            self.config[PROPERTIES][key] = dict[key]
            

    def update_properties(self,wf_params,run_dict,dyn_params):
        
        if wf_params is not None:
            self.update_props(wf_params)
        if run_dict is not None:
            self.update_props(run_dict)
            if 'params' in run_dict:
                kvs = json.loads(run_dict['params'].replace("'","\""))
                self.update_props(kvs)
        if dyn_params is not None:
            self.update_props(dyn_params)

    def update_params(self,wf_params,run_dict,dyn_params):
        for task in self.config[TASKS]:
            task[DESCRIPTION] = replace_params(self.config[PROPERTIES], self.env, task[DESCRIPTION])
            if task[PROPERTIES] is not None:
                for key in task[PROPERTIES]:
                    task[PROPERTIES][key] = replace_params(self.config[PROPERTIES], self.env, task[PROPERTIES][key])
                    if wf_params is not None:
                        task[PROPERTIES][key] = replace_vars(wf_params, task[PROPERTIES][key])
                    if run_dict is not None:
                        task[PROPERTIES][key] = replace_vars(run_dict, task[PROPERTIES][key])
                        if 'params' in run_dict:
                            kvs = json.loads(run_dict['params'].replace("'","\""))
                            task[PROPERTIES][key] = replace_vars(kvs, task[PROPERTIES][key])
                    if dyn_params is not None:
                        task[PROPERTIES][key] = replace_vars(dyn_params, task[PROPERTIES][key])

    def load_config(self, wf_name, path):
        try:
            with path.open() as f:
                try:
                    self.config = yaml.safe_load(f)
                except:
                    raise Exception("Config parsing failure")
            self.config = json.loads(json.dumps(self.config, default=str))

        except Exception as e:
            print(e)
            raise Exception("Config.yaml not found")

        if PROPERTY_FILE in self.config:
            self.load_properties(path)

        for task in self.config[TASKS]:
            if PROPERTY_FILE in task:
                self.load_task_config_file(path, task)
            self.tasks[task[NAME]]=task
        return self.config

    def get_task(self,task_name):
        return self.tasks[task_name]

    def load_properties(self, path):
        dir_name = path.parents[0]
        path = Path(dir_name, self.config[PROPERTY_FILE])
        with path.open() as f:
            properties = yaml.safe_load(f)
            self.config[PROPERTIES] = properties

    def load_task_config_file(self, path, task):
        task[PROPERTIES] = {}
        dir_name = path.parents[0]
        path = Path(dir_name, task[PROPERTY_FILE])
        with path.open() as f:
            properties = yaml.safe_load(f)
            for key in properties:
                task[PROPERTIES][key] = properties[key]

    def get_path(self, path_name):
        """Function returns the path of the file"""

        path = Path(__file__).with_name(path_name)
        return path

    def load_yaml_file(self, path):
        """Function to load and parse the yaml file"""

        with path.open() as f:
            env = yaml.safe_load(f)
        env = json.loads(json.dumps(env, default=str))
        return env
