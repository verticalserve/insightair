# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime

# Function: replace_params
# Description: Replace parameters (Config and Environment) in a given value
# Input: Config properties, Environment properties, Value
# Output: Replaced value
def replace_params(conf_properties, env_properties, value):
    """This function is used to replace the values"""
    value=replace_vars(conf_properties,value)
    value=replace_vars(env_properties,value)
    return value

# Function: replace_vars
# Description: Generic function to replace parameters in a given value
# Input: Properties map, Value
# Output: Replaced value
def replace_vars(properties, value):
    var_list = list(properties.items())
    var_list = list(map(lambda x: x[0], var_list))
    var_list.sort(key=len, reverse=True)
    for key in var_list:
        try:
            value = value.replace('$' + key, str(properties[str(key)]))
        except:
            pass
    return value

# Function: get_target_folder
# Description: Returns target folder from given target bucket path
# Input: Target Bucket Path
# Output: Target folder
def get_target_folder(target_dir):
    folder = target_dir.replace("`date +%Y%m%d-%H%M`", "")
    folder = folder.replace("s3://", "")
    index = folder.index('/')+1
    folder = folder[index:]
    return folder

def get_engine(config, task_properties):
    if "engine" in task_properties:
        return task_properties["engine"]
    elif "engine" in config:
        return config["engine"]
    else:
        return "ray"

def init_xcom(kwargs):
    print(kwargs)
    run_dt=datetime.today().strftime('%Y%m%d-%H%M')
    ti = kwargs['ti']
    wf_params=ti.xcom_pull(key="wf_params")
    if wf_params is None:
        wf_params = {}
        metadata = {}
        metadata['run_dt'] = run_dt
        wf_params['metadata']=metadata
        wf_params['run_dt']=run_dt

        ti.xcom_push(key="wf_params", value=wf_params)
    else:
        run_dt = wf_params['run_dt']
    return run_dt
        
    
