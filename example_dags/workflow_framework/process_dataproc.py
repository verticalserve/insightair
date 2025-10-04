# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator
from workflow_framework.config import *
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.operators.python import PythonOperator
from workflow_framework.process_base import *
from airflow.models import Variable

# Class: ProcessDataproc
# Description: Dataproc class, creates amd deletes a cluster
class ProcessDataproc(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)

    def get_cluster_config(data_group, cluster_config, cluster):
        return {
            "gce_cluster_config": {
                "internal_ip_only": True,
                "service_account": data_group[SERVICE_ACCOUNT],
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/compute",
                    "https://www.googleapis.com/auth/sqlservice.admin",
                    "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write"
                ],
                "subnetwork_uri": cluster_config['subnet'],
            },
            "master_config": {
                "num_instances": cluster[MASTERS],
                "machine_type_uri": cluster[MASTER_MACHINE],
                "image_uri": cluster_config['image_uri'],
                "disk_config": {"boot_disk_type": cluster[MASTER_DISK_CONFIG][DISK_TYPE],
                                "boot_disk_size_gb": cluster[MASTER_DISK_CONFIG][DISK_SIZE]},
                },
            "worker_config": {
                "num_instances": cluster[WORKERS],
                "machine_type_uri": cluster[WORKER_MACHINE],
                "image_uri": cluster_config['image_uri'],
                "disk_config": {"boot_disk_type": cluster[WORKER_DISK_CONFIG][DISK_TYPE],
                                "boot_disk_size_gb": cluster[MASTER_DISK_CONFIG][DISK_SIZE]},
                },
            "software_config": {
                "properties": {
                    "hive:hive.metastore.schema.verification": "false",
                    #"hive:javax.jdo.option.ConnectionURL": cluster_config['hive_metastore_url'],
                    #"hive:javax.jdo.option.ConnectionUserName": cluster_config['hive_metastore_username'],
                    #"hive:javax.jdo.option.ConnectionPassword": cluster_config['hive_metastore_password'],
                    "hive:hive.security.authorization.sqlstd.confwhitelist.append": ".*",
                    "hive:hive.users.in.admin.role": "dataproc",
                    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                    "dataproc:dataproc.allow.zero.workers": "true",
                    "yarn:yarn.log-aggregation.enabled": "true",
                    "mapred:mapreduce.jobhistory.always-scan-user-dir": "true",
                    "mapred:mapreduce.map.java.opts": "-Xmx1024m -Ddb2.jcc.charsetDecoderEncoder=3",
                    "spark:spark.eventLog.compress": "true",
                    "spark:spark.history.fs.numReplayThreads": "3",
                    "dataproc:job.history.to-gcs.enabled": "true",
                    "yarn:yarn.nodemanager.remote-app-log-dir": data_group['phs_bucket']+"/history-server/yarn/logs/",
                    "mapred:mapreduce.jobhistory.done-dir": data_group['phs_bucket']+"/history-server/done-dir/",
                    "mapred:mapreduce.jobhistory.intermediate-done-dir": data_group['phs_bucket']+"/history-server/intermediate-done-dir/",
                    "spark:spark.eventLog.dir": data_group['phs_bucket']+"/history-server/spark-events/",
                    "spark:spark.history.fs.logDirectory": data_group['phs_bucket']+"/history-server/spark-events/"
                }
            },
            "security_config": {
                "kerberos_config": {"enable_kerberos": True}
            },
            "lifecycle_config": {
                "idle_delete_ttl": {
                  "seconds": 3600
                }
            },
            "initialization_actions": [
                {"executable_file": cluster_config['init_action']}
            ]
        }

    def update_profiler_spec(self, cluster_spec):
        try:
            data_group = self.context.data_group
            ### profiler metadata properties
            if "metadata" not in cluster_spec["gce_cluster_config"]:
                cluster_spec["gce_cluster_config"]["metadata"] = {}
            cluster_spec["gce_cluster_config"]["zone_uri"] = data_group[ZONE_URI]
            cluster_spec["gce_cluster_config"]["metadata"]["ha_flag"] = "no"
            cluster_spec["gce_cluster_config"]["metadata"]["profiler-bucket"] = data_group[PROFILER_BUCKET]
            cluster_spec["gce_cluster_config"]["metadata"]["ns-sa-secret-key"] = self.context.env[NS_SA_SECRET_KEY]
            cluster_spec["gce_cluster_config"]["metadata"]["profiler-sql-instance"] = self.context.env[PROFILER_SQL_INS]
            cluster_spec["gce_cluster_config"]["metadata"]["dre-metastore-host"] = Variable.get("DRE_METASTORE_HOST")
            cluster_spec["gce_cluster_config"]["metadata"]["dre-metastore-user"] =  Variable.get("DRE_METASTORE_USER")
            cluster_spec["gce_cluster_config"]["metadata"]["dre-metastore-password"] =  Variable.get("DRE_METASTORE_PASW")
            cluster_spec["gce_cluster_config"]["metadata"]["dre-bits-path"] = os.path.join(data_group[PROFILER_BUCKET], "artifacts/dr-elephant")
            cluster_spec["gce_cluster_config"]["metadata"]["dre-version"] = "v2.1.7.2"
            cluster_spec["gce_cluster_config"]["metadata"]["dreutil-bits-path"] = os.path.join(data_group[PROFILER_BUCKET], "artifacts/dreutil-tool")
            cluster_spec["gce_cluster_config"]["metadata"]["dreutil-version"] = "v2.1"
            cluster_spec["gce_cluster_config"]["metadata"]["dreutil-recom-path"] = os.path.join(data_group[PHS_BUCKET], "dreutil-analysis","mydag", "test")
            cluster_spec["gce_cluster_config"]["metadata"]["sparklens-bits-path"] = os.path.join(self.context.env[CONFIG_BUCKET], "jars", "sparklens_2.12-0.3.2.jar")
            ### end ###
            ### profiler software_config's properties 
            cluster_spec["software_config"]["properties"]["yarn:yarn.log-aggregation.retain-seconds"] = "-1"
            cluster_spec["software_config"]["properties"]["yarn:yarn.nodemanager.remote-app-log-dir"] = data_group[PROFILER_BUCKET]+"/history-server/yarn/logs"
            cluster_spec["software_config"]["properties"]["mapred:mapreduce.job.emit-timeline-data"] = "true"
            cluster_spec["software_config"]["properties"]["spark:spark.eventLog.enabled"] = "true"
            cluster_spec["software_config"]["properties"]["spark:spark.ui.enabled"] = "true"
            cluster_spec["software_config"]["properties"]["spark:spark.ui.filters"] = "org.apache.spark.deploy.yarn.YarnProxyRedirectFilter"
            cluster_spec["software_config"]["properties"]["yarn:yarn.log.server.url"] = "http://" + data_group[PHS_CLUSTER] + "-m:19888/jobhistory/logs"
            cluster_spec["software_config"]["properties"]["yarn:yarn.timeline-service.hostname"] = data_group[PHS_CLUSTER] + "-m"
            cluster_spec["software_config"]["properties"]["spark:spark.yarn.historyServer.address"] = data_group[PHS_CLUSTER] + "-m:18080"
            cluster_spec["software_config"]["properties"]["mapred:mapreduce.jobhistory.address"] = data_group[PHS_CLUSTER] + "-m:10020"
            cluster_spec["software_config"]["properties"]["mapred:mapreduce.jobhistory.webapp.address"] = data_group[PHS_CLUSTER] + "-m:19888"
            cluster_spec["software_config"]["properties"]["spark:spark.serializer"] = 'org.apache.spark.serializer.KryoSerializer'
            cluster_spec["software_config"]["properties"]["spark:spark.dynamicAllocation.maxExecutors"] = '900'
            cluster_spec["software_config"]["properties"]["spark:spark.extraListeners"] = "com.qubole.sparklens.QuboleJobListener"
            cluster_spec["software_config"]["properties"]["spark:spark.sparklens.data.dir"] = "{}/sparklens".format(data_group[PHS_BUCKET])
            cluster_spec["software_config"]["properties"]["spark:spark.sparklens.reporting.disabled"] = "false"
            cluster_spec["software_config"]["properties"]["spark:spark.history.custom.executor.log.url"] = '/gateway/default/apphistory/applicationhistory/logs/{NM_HOST}:{NM_PORT}/{CONTAINER_ID}/{CONTAINER_ID}/root/{FILE_NAME}?start=-4096'
            ### end ### 
            ### Disable kerberos property
            cluster_spec["security_config"]["kerberos_config"]["enable_kerberos"] = False
            ### Add init script for profiler
            if "initialization_actions" not in cluster_spec:
                cluster_spec['initialization_actions'] = []
            cluster_spec['initialization_actions'].append({'executable_file' : os.path.join(data_group[PROFILER_BUCKET], "artifacts/init-actions", "dr-elephant.sh")})
        
            cluster_spec["software_config"]["properties"]["spark:spark.extraListeners"] = "com.qubole.sparklens.QuboleJobListener"
            cluster_spec["software_config"]["properties"]["spark:spark.sparklens.data.dir"] = "{}/sparklens".format(data_group[PHS_BUCKET])
            cluster_spec["software_config"]["properties"]["spark:spark.sparklens.reporting.disabled"] = "false"
            
            if "metadata" in cluster_spec["gce_cluster_config"]:
                cluster_spec["gce_cluster_config"]["metadata"]["sparklens-jar-path"] = os.path.join(self.context.env[CONFIG_BUCKET], "jars", "sparklens_2.12-0.3.2.jar")
            else:
                cluster_spec["gce_cluster_config"]["metadata"] = {}
                cluster_spec["gce_cluster_config"]["metadata"]["sparklens-jar-path"] = os.path.join(self.context.env[CONFIG_BUCKET], "jars", "sparklens_2.12-0.3.2.jar")
            if "initialization_actions" in cluster_spec:
                cluster_spec['initialization_actions'].append({'executable_file' : os.path.join(self.context.env[CONFIG_BUCKET], "scripts", "init_action_sparklens.sh")})
            else:
                cluster_spec['initialization_actions'] = []
                cluster_spec['initialization_actions'].append({'executable_file' : os.path.join(self.context.env[CONFIG_BUCKET], "scripts", "init_action_sparklens.sh")})

        except:
            pass

        return cluster_spec


    def check_and_launch(self):

        hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        try:
            cluster_info=hook.get_cluster(
                region = self.context.region,
                project_id = self.context.project_id,
                cluster_name = self.context.cluster_name
            )
            print(cluster_info)
            print(cluster_info.status.state)
            if State.RUNNING == cluster_info.status.state:
                return
        except:
            print('not found')
        cluster = self.context.cluster_config[PROFILES][self.context.cluster_profile]
        cluster_spec=ProcessDataproc.get_cluster_config(self.context.data_group,
                                                        self.context.cluster_config,cluster)

        if 'profiler' in self.context.config[PROPERTIES]:
            if self.context.config[PROPERTIES]['profiler']=='true':
                cluster_spec = self.update_profiler_spec(cluster_spec)
        try:
            cluster_info=hook.create_cluster(
                region = self.context.region,
                project_id = self.context.project_id,
                cluster_name = self.context.cluster_name,
                cluster_config=cluster_spec
                )
            print(cluster_info)
            hook.wait_for_operation(cluster_info)
        except Exception as e:
            print(e)
            print('Cluster exists')


    def create_cluster(self):
        super().init_config()
        check_and_launch()

    def delete_cluster(self):
        super().init_config()

        hook = DataProcHook(gcp_conn_id=self.context.gcp_conn_id,
                              delegate_to=None)
        try:
            if 'profiler' in self.context.config[PROPERTIES]:
                if self.context.config[PROPERTIES]['profiler']=='true':
                    print("Submitting profiler job...")
                    queries = "sh /opt/dreutil/dreutil/dreutil-run.sh"
                    profiler_job = {
                        "reference": {"project_id": self.context.project_id },
                        "placement": {"cluster_name": self.context.cluster_name},
                        "pig_job": {"query_list": {"queries": [queries]}}
                    }
                    hook.submit(job = profiler_job, region = self.context.region, project_id = self.context.project_id)
                    print("Profiler job submitted...")

            hook.delete_cluster(
                cluster_name = self.context.cluster_name,
                project_id = self.context.project_id,
                region = self.context.region)
        except:
            pass

    def build_create_cluster_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.create_cluster,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op

    def build_delete_cluster_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.delete_cluster,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op