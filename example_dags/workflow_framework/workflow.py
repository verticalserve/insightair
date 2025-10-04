# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Description: This class manages the workflow context, which stores the configuration and run time info for a workflow

from workflow_framework.config import *

# Class: WorkflowContext
# Description: workflow context
class WorkflowContext:

    def __init__(self, name, config, path):
        self.name = name
        self.config = config
        self.path = path
        self.config_obj = None
        self.cluster_config = {}
        self.env = {}
        self.data_group = []
        self.gcs_client = ''
        self.cluster_name = ''
        self.cluster_profile = ''
        self.gcp_conn_id = ''
        self.project_id = ''
        self.region = ''
        self.initialized = False

    def initialize(self, wf_params={},run_dict={}, dyn_params={}):
        print('Parameters - XCOM, Runtime Conf, Dynamic')
        print(wf_params)
        print(run_dict)
        print(dyn_params)
        conf = Config(self.name, self.path)
        conf.load_configs(wf_params,run_dict,dyn_params)
        self.config = conf.get_config()
        self.cluster_config = conf.get_cluster_config()
        self.env = conf.get_env()
        self.data_group = conf.get_data_group()
        self.gcs_client = ''
        self.initialized = True
        self.config_obj = conf
        self.cluster_name = self.config[CLUSTER_NAME]
        self.cluster_profile = self.config[CLUSTER_PROFILE]
        self.gcp_conn_id = self.data_group[GCP_CONN_ID]
        self.project_id = self.data_group[PROJECT_ID]
        self.region = self.data_group[REGION]

    def get_name(self):
        return self.name

    def get_path(self):
        return self.path.rstrip('/')
    
    def get_config_path(self):
        return self.config['path'].rstrip('/')

    def get_config(self):
        return self.config


