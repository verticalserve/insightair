from workflow_framework.config import *
from airflow.operators.python import PythonOperator
from workflow_framework.process_base import *
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class ProcessEmr(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        
    def get_cluster_config(self, cluster):
        return {
            "Name": self.context.cluster_name,
            "ReleaseLabel": "emr-7.1.0",
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": cluster[MASTER_MACHINE],
                        "InstanceCount": cluster[MASTERS]
                    },
                    {
                        "Name": "Core nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": cluster[WORKER_MACHINE],
                        "InstanceCount": cluster[WORKERS]
                    },
                    #{
                     #   "Name": "Task nodes",
                      #  "Market": "SPOT",  # Cost-effective spot instances
                       # "InstanceRole": "TASK",
                        #"InstanceType": cluster[WORKER_MACHINE],
                        #"InstanceCount": 0, #max(0, cluster[WORKERS] - 2),  # Remaining as task nodes
                        #"BidPrice": "OnDemandPrice"
                    #}
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False
            },
            "CustomAmiId": "ami-01252790382ba5844",
            "Applications": [{"Name": "Spark"}],
            "JobFlowRole": "EMR_EC2_DefaultRole",  
            "ServiceRole": "EMR_DefaultRole",       
            "LogUri": "s3://aws-logs-867344468918-us-east-1/elasticmapreduce/"
        }
      
    def check_and_launch(self):

        hook = EmrHook(aws_conn_id=self.context.gcp_conn_id,
                              region_name=self.context.region)
        try:
            cluster_id=hook.get_cluster_id_by_name(
                emr_cluster_name = self.context.cluster_name,
                cluster_states=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
            )
            print(cluster_id)
            if cluster_id is not None:
                return cluster_id
        except Exception as e:
            print(e)
            print('not found')
        cluster = self.context.cluster_config[PROFILES][self.context.cluster_profile]
        cluster_spec=self.get_cluster_config(cluster)

        try:
            cluster_info=hook.create_job_flow(job_flow_overrides=cluster_spec)
            print("--------------------------------------------------")
            print(cluster_info["JobFlowId"])
            return cluster_info["JobFlowId"]
        except Exception as e:
            print(e)
            print('Cluster exists')
        
    def create_cluster(self):
        super().init_config()
        return self.check_and_launch()
            
    def build_create_cluster_task(self, task_name):
        op = PythonOperator(
            task_id=task_name,
            python_callable=self.create_cluster,
            op_kwargs={
                'task_name': task_name
            }
        )
        return op
    
    def run_emr_job(self, task_name, task, job_type, emr_cluster_id):
        job_name = f'{self.context.config['name']}-{task_name}'
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        config_path = self.context.config['path']
        if config_path.endswith('/'):
            config_path = config_path.rstrip('/')
        step_config={
            'Name': job_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--conf", f"spark.archives=s3://{config_bucket}/phi3env.zip#environment,s3://{config_bucket}/insightspark.zip#insightspark",
                    "--conf", f"spark.submit.pyFiles=s3://{config_bucket}/insightspark.zip",
                    "--conf", "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                    "--conf", "spark.executorEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                    "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/phi3env/bin/python3.12",
                    "--conf", "spark.executorEnv.LD_LIBRARY_PATH=./environment/phi3env/lib/python3.12/site-packages/nvidia/cudnn/lib",
                    "--conf", "spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./environment/phi3env/lib/python3.12/site-packages/nvidia/cudnn/lib",
                    "--jars", f"s3://{config_bucket}/jars/mysql-connector-j-8.0.33.jar",
                    "--conf", f"spark.jars.packages=com.mysql:mysql-connector-j:8.0.33",
                    "--conf", f"spark.sql.parquet.compression.codec=snappy",
                    "--conf", f"spark.yarn.appMasterEnv.PYTHONPATH=./environment:./insightspark:./environment/phi3env/lib/python3.12/site-packages:./environment/phi3env/lib64/python3.12/site-packages",
                    "--conf", f"spark.executorEnv.PYTHONPATH=./environment:./insightspark:./environment/phi3env/lib/python3.12/site-packages:./environment/phi3env/lib64/python3.12/site-packages",
                    "--conf", f"spark.executor.extraJavaOptions=-Dorg.apache.commons.compress.archivers.zip.allowStoredEntriesWithDataDescriptor=true",
                    "--conf", f"spark.driver.extraJavaOptions=-Dorg.apache.commons.compress.archivers.zip.allowStoredEntriesWithDataDescriptor=true",
                    f"s3://{config_bucket}/spark_job.py",
                    "--job_type", job_type, 
                    "--config_path", f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                    "--AWS_REGION", "us-east-1"
                ]
            },
        }
        
        hook = EmrHook(aws_conn_id=self.context.gcp_conn_id,
                              region_name=self.context.region)
        
        try:
            hook.add_job_flow_steps(
                job_flow_id = emr_cluster_id,
                steps=[step_config],
                wait_for_completion=True
            )
            hook.test_connection()
            
        except:
            print('Failed to submit the spark job')
        return True