# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from workflow_framework.config import *
from datetime import datetime
from workflow_framework.utils import *
from workflow_framework.process_base import *
import json
import os
import boto3
from PIL import Image
import requests

from typing import List, Dict
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from pinecone.grpc import PineconeGRPC as Pinecone
from langchain_openai import OpenAIEmbeddings
from langchain_core.prompts import PromptTemplate
from langchain_openai import OpenAI
from langchain_pinecone import PineconeVectorStore
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from uuid import uuid4
#from utils.helpers import get_content
import io
from langchain_core.documents import Document
from workflow_framework.process_emr import ProcessEmr
from workflow_framework.process_ray import ProcessRay
import base64

#import pinecone
import fitz

os.environ["OPENAI_API_KEY"] = ""
os.environ['PINECONE_API_KEY'] = ''
bearer_info = ""
#API_BASE="http://127.0.0.1:9494/insightlake"
API_BASE="http://copilot.us-east-1.elasticbeanstalk.com/insightlake"

collection_name="user-workspace"
# Class: ProcessVectorLoad
# Description: Vector processing functions
class ProcessVectorLoad(ProcessBase):
    def __init__(self, context):
        ProcessBase.__init__(self,context)
        """pinecone_api_key = os.environ.get("PINECONE_API_KEY")
        self.collection = collection_name
        pc = Pinecone(api_key=pinecone_api_key)        
        self.embeddings = OpenAIEmbeddings()
        self.index = pc.Index(collection_name)
        self.chatmodel = ChatOpenAI(model="gpt-4o")
        self.vector_store = PineconeVectorStore(self.index, self.embeddings)"""

    
    
    def pushS3FileToPinecone(self, index_name,source_bucket, source_key, path, user, category='general', subcategory='general'):
        # Get the source bucket and key from the S3 event
        #source_bucket = 'raw-knowledge-base' #event['Records'][0]['s3']['bucket']['name']
        #source_key = 'Aarav Gaur Vaccinations.pdf' #event['Records'][0]['s3']['object']['key']
        s3 = boto3.client('s3')
        pinecone_api_key = os.environ.get("PINECONE_API_KEY")

        pc = Pinecone(api_key=pinecone_api_key)        
        llm = OpenAI()
        embeddings = OpenAIEmbeddings()
        index = pc.Index(index_name)
        chatmodel = ChatOpenAI(model="gpt-4o")
        vector_store = PineconeVectorStore(index, embeddings)
            
        # Download the PDF file from S3
        # Open the PDF using PyMuPDF
        # Download the PDF file from S3
        response = s3.get_object(Bucket=source_bucket, Key=path)
        body = response['Body']
        # Write the body to a local file
        """with open('/Users/vijaygaur/software/email_poller/app/output.pdf', 'wb') as f:
            f.write(body.read())
        print(body)"""
        
        # Open the PDF using PyMuPDF
        doc = fitz.open(stream=body.read(), filetype="pdf")
        
        # Extract text from each page
        docs = []
        
        page_texts = []
        final_text=''
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            page_texts.append({
                "page_number": page_num + 1,
                "text": text
            })
            final_text+=text+'\n\n'
            metadata = {"source": source_key, "page": page_num + 1, "user": user}
            docs.append(Document(page_content=text, metadata=metadata))
        
        # Close the document
        doc.close()
        chunks = self.chunk_documents(docs)
        print(chunks)
        vectordb = PineconeVectorStore.from_documents(
                    chunks,
                    index_name=index_name,
                    embedding=embeddings
                )
    def get_content(self, type, content):
        marker_start = '```'+type
        marker_end = '```'
        if content.find(marker_start) == -1:
            return content
        start = content.find(marker_start)+len(marker_start)
        end = content.rfind(marker_end, start)
        content = content[start:end].strip()
        return content
    
    def convert_image_to_markdown_sections(self, image_bytes, category='general', subcategory='general'):
        image_data = base64.b64encode(image_bytes).decode("utf-8")
        prompt="Convert this given image of a document in markdown sections. create a json array of markdown sections of this document, keep the length of each section around 512 to 2000 characters, keep the tables or graphs in complete sections, keep the json in this format: content (markdown section), type (text, table, chart if the section contains these include comma separated), section (heading of the section), security(PII, PCI, PHI - if the content contains this include comma separated). Look for checkboxes correctly sometimes checkboxes are before and other times after the label, review correctly."
        if category == 'form' and subcategory == 'accord':
            prompt = "Convert this given image of an Accord form in markdown sections. create a json array of markdown sections of this document, keep the length of each section around 512 to 2000 characters, keep the tables or graphs in complete sections, keep the json in this format: content (markdown section), type (text, table, chart if the section contains these include comma separated), section (heading of the section), security(PII, PCI, PHI - if the content contains this include comma separated). Look for checkboxes correctly sometimes checkboxes are before and other times after the label, review correctly."
        message = HumanMessage(
            content=[
                {"type": "text", "text": prompt},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_data}"},
                },
            ],
        )
        response = self.chatmodel.invoke([message])
        print(response.content)
        markdown_array = self.get_content('json', response.content)
        markdown_array = json.loads(markdown_array)
        for markdown_section in markdown_array:
            print(markdown_section['content'])
        #markdown_text = response.content
        #self.markdown_content.append(markdown_text)
        return markdown_array
    
    def markdown_push(self, request):
        index_name = request.index
        source_bucket = request.bucket
        source_key = request.filename
        path = request.path+'/'+source_key
        guid = request.guid


        # Get the source bucket and key from the S3 event
        #source_bucket = 'raw-knowledge-base' #event['Records'][0]['s3']['bucket']['name']
        #source_key = 'Aarav Gaur Vaccinations.pdf' #event['Records'][0]['s3']['object']['key']
        s3 = boto3.client('s3')
        pinecone_api_key = os.environ.get("PINECONE_API_KEY")

        pc = Pinecone(api_key=pinecone_api_key)        
        llm = OpenAI()
        embeddings = OpenAIEmbeddings()
        index = pc.Index(index_name)
        chatmodel = ChatOpenAI(model="gpt-4o")
        vector_store = PineconeVectorStore(index, embeddings)
            
        # Download the PDF file from S3
        # Open the PDF using PyMuPDF
        # Download the PDF file from S3
        response = s3.get_object(Bucket=source_bucket, Key=path)
        body = response['Body']
        # Write the body to a local file
        """with open('/Users/vijaygaur/software/email_poller/app/output.pdf', 'wb') as f:
            f.write(body.read())
        print(body)"""
        
        # Open the PDF using PyMuPDF
        doc = fitz.open(stream=body.read(), filetype="pdf")
        docs = []
        page_num = 0
        num_chunks=0
        chunk=1
        for page in doc:
            page_num+=1
            pix = page.get_pixmap()
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='png')
            page_file = source_key.replace('.pdf', '')+'-page-'+str(page_num)+'.png'
            img.save(page_file, format='PNG')
            # Upload the image to S3
            

            img_byte_arr = img_byte_arr.getvalue()
            s3.put_object(
                Body=img_byte_arr,
                Bucket=source_bucket,
                Key=page_file
            )
            
            markdown_array = self.convert_image_to_markdown_sections(img_byte_arr)
            num_chunks+=len(markdown_array)
            
            for markdown_section in markdown_array:
                chunkid = str(uuid4())
                print(chunkid)
                print(markdown_section['content'])
                chunkMetadata = {}
                for key, value in request.metadata.items():
                    chunkMetadata[key] = value
                
                chunkMetadata["page"] = page_num
                chunkMetadata["chunk"] = chunk
                chunkMetadata["content"]=markdown_section['type']
                chunkMetadata["chunkid"] = chunkid
                chunkMetadata["security"] = markdown_section['security']
                chunkMetadata["section"] = markdown_section['section']
                print(chunkMetadata)
                docs.append(Document(page_content=markdown_section['content'], metadata=chunkMetadata))
                chunk+=1
        # Close the document
        doc.close()
        ids=[]
        for doc in docs:
            print(doc.metadata)
            ids.append(doc.metadata['chunkid'])
        for id in ids:
            print(id+", ")
            
        vector_store = PineconeVectorStore(index=self.index, embedding=embeddings)
        vector_store.add_documents(documents=docs, ids=ids)
        #vectordb = PineconeVectorStore.from_documents(
         #           docs,
          #          index_name=index_name,
           #         embedding=embeddings
            #    )
        datainfo={
            "chunks": num_chunks,
            "pages": page_num,
            "guid": guid
        }

    def markdown_push(self, index_name,source_bucket, source_key, path, metadata):
        s3 = boto3.client('s3')
        pinecone_api_key = os.environ.get("PINECONE_API_KEY")

        pc = Pinecone(api_key=pinecone_api_key)        
        llm = OpenAI()
        embeddings = OpenAIEmbeddings()
        index = pc.Index(index_name)
        chatmodel = ChatOpenAI(model="gpt-4o")
        vector_store = PineconeVectorStore(index, embeddings)
            
        # Download the PDF file from S3
        # Open the PDF using PyMuPDF
        # Download the PDF file from S3
        response = s3.get_object(Bucket=source_bucket, Key=path)
        body = response['Body']
        
        # Open the PDF using PyMuPDF
        doc = fitz.open(stream=body.read(), filetype="pdf")
        docs = []
        page_num = 0
        num_chunks=0
        chunk=1
        for page in doc:
            page_num+=1
            pix = page.get_pixmap()
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='png')
            page_file = source_key.replace('.pdf', '')+'-page-'+str(page_num)+'.png'
            img.save(page_file, format='PNG')
            # Upload the image to S3
            

            img_byte_arr = img_byte_arr.getvalue()
            s3.put_object(
                Body=img_byte_arr,
                Bucket=source_bucket,
                Key=page_file
            )
            
            markdown_array = self.convert_image_to_markdown_sections(img_byte_arr)
            num_chunks+=len(markdown_array)

            for markdown_section in markdown_array:
                chunkid = str(uuid4())
                print(chunkid)
                print(markdown_section['content'])
                chunkMetadata = {}
                for key, value in metadata.items():
                    chunkMetadata[key] = value
                chunkMetadata["index"] = index_name
                chunkMetadata["page"] = page_num
                chunkMetadata["chunk"] = chunk
                chunkMetadata["content"]=markdown_section['type']
                chunkMetadata["chunkid"] = chunkid
                chunkMetadata["security"] = markdown_section['security']
                chunkMetadata["section"] = markdown_section['section']
                print(chunkMetadata)
                docs.append(Document(page_content=markdown_section['content'], metadata=chunkMetadata))
                chunk+=1

        # Close the document
        doc.close()
        
        ids=[]
        for doc in docs:
            print(doc.metadata)
            ids.append(doc.metadata['chunkid'])
            doc.id = doc.metadata['chunkid']
            
        for id in ids:
            print(id+", ")
        
        print(docs)
        vector_store = PineconeVectorStore(index=self.index, embedding=embeddings)
        #vector_store.add_documents(documents=docs, ids=ids)
        vectordb = PineconeVectorStore.from_documents(
                    docs,
                    index_name=index_name,
                    embedding=embeddings
                )
        """datainfo={
            "chunks": num_chunks,
            "pages": page_num,
            "guid": metadata['fileid']
        }"""
    def create_api(self, endpoint, payload, bearer_info):
        print(payload)
        headers = {
            "Authorization": bearer_info
        }
        print(payload)
        response = requests.post(API_BASE+endpoint, json=payload, headers=headers)
        
        payload = response.json()
        print(payload)
        return payload['data']
    
    def run_vector_load_glue_job(self, task_name, task):
        glue_job_name = f'{self.context.config['name']}-{task_name}'
        config_bucket = self.context.env['config_bucket'].replace("s3://","")
        job_script = "scripts/glue_job.py"
        requirement_file = 's3://'+config_bucket+"/install/glue_db_requirements.txt"
        config_path = self.context.config['path']
        if config_path.endswith('/'):
            config_path = config_path.rstrip('/')
        worker_type='G.1X'
        num_workers=10
        if self.context.cluster_profile == 'medium':
            worker_type='G.1X'
            num_workers=10
        elif self.context.cluster_profile == 'large':
            worker_type='G.1X'
            num_workers=10
        region = self.context.data_group['region']
        role_id = self.context.data_group['role_id']
        role_name = self.context.data_group['role_name']
        config_path = self.context.config['path']
        if config_path.endswith('/'):
            config_path = config_path.rstrip('/')
        # Corrected GlueJobHook initialization
        config_yaml = yaml.dump(task["properties"])
        s3_key = f"dags/{config_path}/{task_name}.yaml"
        s3_hook = S3Hook(aws_conn_id=self.context.gcp_conn_id)
        s3_hook.load_string(string_data=config_yaml, key=s3_key, bucket_name=config_bucket, replace=True)

        glue_hook = GlueJobHook(
            aws_conn_id=self.context.gcp_conn_id,  # Changed from gcp_conn_id to AWS connection
            job_name=glue_job_name,
            region_name=region,
            s3_bucket=config_bucket,
            script_location=f'{config_bucket}/{job_script}',
            iam_role_name=role_name,
            desc=task['description'],
            update_config=True,
            create_job_kwargs={
                'Role': role_id,  # Required ARN
                'GlueVersion': '5.0',
                'WorkerType': worker_type,
                'NumberOfWorkers': num_workers,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{config_bucket}/{job_script}',
                },
                'DefaultArguments': {
                    '--additional-python-modules': requirement_file,
                    '--python-modules-installer-option': '-r',
                    '--AWS_REGION': region,
                    '--extra-py-files': f's3://{config_bucket}/insightspark.zip',
                    '--config_path': f's3://{config_bucket}/dags/{config_path}/{task_name}.yaml',
                    '--job_type': 'VECTOR_LOAD',
                    '--task_name': task_name,
                    '--workflow_name': self.context.config['name'],
                }
            }
        )
        # Start the job run with proper arguments
        response = glue_hook.initialize_job()

        # Get Job Run ID for monitoring
        job_run_id = response['JobRunId']

        status = glue_hook.job_completion(job_name=glue_job_name, run_id=job_run_id)
        print(status)
        return True
    
    def vector_load_op2(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)
        task['kwargs']=kwargs
        index = task[PROPERTIES]['index']
        source_bucket = task[PROPERTIES]['source_bucket']
        source_path = task[PROPERTIES]['source_path']
        user = 'vijay@verticalserve.com'
        hook = S3Hook(aws_conn_id=self.context.gcp_conn_id, transfer_config_args=None, extra_args=None)
        
        kwargs=task['kwargs']
        print(kwargs)
        ti = kwargs['ti']
        wf_params=ti.xcom_pull(key="wf_params")
        run_dt=wf_params['run_dt']
        metadata = wf_params['metadata']
        print(run_dt)

        category = self.context.config[PROPERTIES]['category']
        subcategory = self.context.config[PROPERTIES]['subcategory']
        catalog = self.context.config[PROPERTIES]['catalog']
        datagroup = self.context.config[PROPERTIES]['datagroup']
        lob = self.context.config[PROPERTIES]['lob']
        product = self.context.config[PROPERTIES]['product']
        redaction = self.context.config[PROPERTIES]['redaction']
        source = self.context.config[PROPERTIES]['source']
        subcategory = self.context.config[PROPERTIES]['subcategory']
        user = self.context.config[PROPERTIES]['user']
        workflow = self.context.config[PROPERTIES]['workflow']
        extractor = self.context.config[PROPERTIES]['extractor']

        metadata["catalog"] = catalog
        metadata["datagroup"] = datagroup
        metadata["lob"] = lob
        metadata["product"] = product
        metadata["redaction"] = redaction
        metadata["source"] = source
        metadata["category"] = category
        metadata["subcategory"] = subcategory
        metadata["createdby"] = user
        metadata["workflow"] = workflow

        source_path = source_path.replace("`date +%Y%m%d-%H%M`",run_dt)
        source_path = get_target_folder(source_path)
        print(source_path)
        s3 = boto3.client('s3')
        files = hook.list_keys(source_bucket, prefix=source_path)
        for file in files:
            print(file)
            print(file.endswith('.json'))
            if file.endswith('/') or file.endswith('.json'):
                # Your code here
                print("Skipping folder or json file")
                continue
            filename = file.split('/')[-1]
            print(filename)

            #open metadata file if there
            metadata_file = filename.replace('.pdf', '-metadata.json')
            metadata_path = source_path + '/' + metadata_file
            print(metadata_path)
            properties = {}
            print(files)
            if metadata_path in files:
                print("Found metadata file")
                
                metadata_response = s3.get_object(Bucket=source_bucket, Key=metadata_path)
                metadata_body = metadata_response['Body']
                properties = json.load(metadata_body)
                print(properties)
                metadata.update(properties)
                print(metadata)
            fileid=str(uuid4())
            metadata["filename"] = filename
            metadata["fileid"] = fileid
            datainfo={
                "guid": fileid,
                "filename": filename,
                "category": category,
                "subcategory": subcategory,
                "lob": lob,
                "product": product,
                "catalog": catalog,
                "datagroup": datagroup,
                "redaction": redaction,
                "source": source,
                "rawBucket": source_bucket,
                "createdBy": user,
                "workflow": workflow,
                "extractor": extractor,
                "rawPath": source_path,
                "properties": json.dumps(metadata)
            }
            print(datainfo)
            data = self.create_api("/api/insightcopilot/datainfo/pipeline", datainfo, bearer_info)

            response = self.markdown_push(index,source_bucket,filename,source_path+'/'+filename,metadata)

    def vector_load_op(self,task_name,dyn_params={}, **kwargs):
        super().init_config(kwargs['ti'].xcom_pull(key='wf_params'),kwargs['dag_run'].conf, dyn_params)
        task = self.context.config_obj.get_task(task_name)
        task['kwargs']=kwargs
        stage_path = task[PROPERTIES][STAGE_PATH]
        run_dt=init_xcom(kwargs)
        stage_path = stage_path.replace("`date +%Y%m%d-%H%M`",run_dt)
        task[PROPERTIES][STAGE_PATH] = stage_path.lower()
        self.upload_task_config(task_name, task)

        engine = get_engine(self.context.config[PROPERTIES],task[PROPERTIES])
        print(stage_path)

        if engine == 'glue':
            self.run_glue_job(task_name, task, 'VECTOR_LOAD')
        elif engine == 'emr':
            process_emr = ProcessEmr(self.context)
            emr_cluster_id=process_emr.create_cluster()
            process_emr.run_emr_job(task_name, task, 'VECTOR_LOAD', emr_cluster_id)
        elif engine == 'ray':
            process_ray = ProcessRay(self.context)
            process_ray.run_ray_job(task_name, task, 'VECTOR_LOAD')
        else:
            raise Exception(f"Unsupported engine: {engine}")
        return True

    def build_vector_load_task(self, task_name, dyn_params={}):

         args = {'task_name': task_name,
                 'dyn_params': dyn_params
                }

         op = PythonOperator(
            task_id=task_name,
            provide_context=True,
            python_callable=self.vector_load_op,
            op_kwargs=args
         )
         return op
