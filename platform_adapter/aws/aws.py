from platform_adapter.interface import PlatformAdapter
import os
import re
import json
import time
import boto3
import base64
import docker
import shutil
import botocore
import threading
import subprocess
import pandas as pd
from config import tmp_dir, bench_dir

aws_handler="""import time, importlib, os, sys

os.environ['OPENBLAS_NUM_THREADS'] = '2'

for req in %s:
    sys.path.insert(0, f"/packages/{req}")

start = time.time()
for mod in %s:
    try:
        importlib.import_module(mod)
    except Exception as e:
        pass
end = time.time()
        
def handler(event, context):
    return (end-start)*1000
"""

dockerfile = '''
FROM nogil/python

COPY . .
RUN mkdir -p /tmp/.cache
RUN python3 /install_all.py /pkg_list.txt
RUN pip install cmake 
RUN pip install awslambdaric

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
'''


def write_if_different(file_path, new_content):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            if file.read() == new_content:
                return
    with open(file_path, 'w') as file:
        file.write(new_content)

class AWS(PlatformAdapter):
    def __init__(self):
        self.load_config("platform_adapter/aws/config.json")
        
        self.home_dir = os.path.expanduser('~')
        
        self.iam_arn = self.config["iam_arn"]
        self.region_name = self.config["region_name"]
        self.lock = threading.Lock()
        
        if not os.path.exists(f"{self.home_dir}/.aws/credentials"):
            os.makedirs(f"{self.home_dir}/.aws", exist_ok=True)
            with open(f"{self.home_dir}/.aws/credentials", "w") as f:
                f.write(["[default]\n", 
                         f"aws_access_key_id = {self.config['aws_access_key_id']}\n",
                         f"aws_secret_access_key = {self.config['aws_secret_access_key']}\n"])
                
        self.metrics_lock = threading.Lock()
        self.metrics = pd.DataFrame(columns=["request_id", "function name", "duration (ms)", "billed duration (ms)", "memory size (mb)", "max memory used (mb)", "init duration (ms)", "import duration (ms)"])
               
        self.lambda_client = boto3.client('lambda', region_name=self.region_name)
        self.log_client = boto3.client('logs', region_name=self.region_name)
        
        function_names = []
        paginator = self.lambda_client.get_paginator('list_functions')
        for page in paginator.paginate():
            functions = page['Functions']
            names = [function['FunctionName'] for function in functions]
            function_names.extend(names)
            
        self.function_names = set(function_names)
    
    def start_worker(self, options={}):
        print("building shared ecr base image")
        #fetch workload and dependencies from workload.json
        with open(options["workload_path"], "r") as f:
            workload = json.load(f)
            
        os.makedirs(tmp_dir, exist_ok=True)
        with open(os.path.join(tmp_dir, ".dockerignore"), "w") as f:
            f.write("\n")
        
        #generate list of all packages used for the bench
        pkg_list = [f"{pkg}=={ver}" for pkg, vers in workload["pkg_with_version"].items() for ver in vers]
        write_if_different(os.path.join(tmp_dir, "pkg_list.txt"), "\n".join(pkg_list))
        
        #write test lambda handler function
        for func in workload["funcs"]:
            dependencies = re.findall(r'([a-zA-Z0-9_-]+\s*==\s*[0-9.]+)', func["meta"]["requirements_txt"])
            handler_code = aws_handler % (dependencies, func["meta"]["import_mods"])   
                     
            write_if_different(os.path.join(tmp_dir, f"{func['name']}.py"), handler_code)
        
        #install_all.py will install all packages in pkg_list.txt during docker build
        source_install_all = os.path.join(os.path.dirname(os.path.abspath(__file__)), "install_all.py")
        destination_install_all = os.path.join(tmp_dir, "install_all.py")
        if not os.path.exists(destination_install_all) or open(source_install_all).read() != open(destination_install_all).read():
            shutil.copy(source_install_all, tmp_dir)
        
        #write dockerfile and build img
        #TODO: use buildx instead of build
        write_if_different(os.path.join(tmp_dir, "Dockerfile"), dockerfile)
        
        #authenticate docker to AWS ECR
        ecr_client = boto3.client('ecr', region_name=self.region_name)
        response = ecr_client.get_authorization_token()
        
        username,  password = base64.b64decode(response['authorizationData'][0]['authorizationToken']).decode('utf-8').split(":")
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        
        login_command = f'docker login --username {username} --password-stdin {account_id}.dkr.ecr.{self.region_name}.amazonaws.com'
        subprocess.run(login_command, input=password.encode('utf-8'), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        #create repository
        try:
            response = ecr_client.describe_repositories(repositoryNames=["req_bench"])
            repository_uri = response['repositories'][0]['repositoryUri']
        except ecr_client.exceptions.RepositoryNotFoundException:
            response = ecr_client.create_repository(
                repositoryName="req_bench",
                imageScanningConfiguration={
                    'scanOnPush': True
                    },
                imageTagMutability='MUTABLE'
                )
            repository_uri = response['repository']['repositoryUri']
        
        docker_client = docker.from_env()
        # build and push docker img
        print("building docker image... (this may take few minuites)")
        self.image_name = f'{repository_uri}:latest'
        docker_client.images.build(path=tmp_dir, platform="linux/amd64", tag=self.image_name)
        print("pushing docker image to ECR... (this may take few minuites)")
        out = docker_client.images.push(self.image_name)
        print("ECR ready")
        return

    def kill_worker(self, options={}):
        self.metrics.to_csv(f'{bench_dir}/aws_metrics.csv', index=True)

    def deploy_func(self, func_config):
        if func_config['name'] not in self.function_names:
            try:
                self.lambda_client.create_function(
                    FunctionName=func_config['name'],
                    Role=self.iam_arn,
                    PackageType='Image',
                    Code={'ImageUri': self.image_name},
                    ImageConfig={'Command': [f"{func_config['name']}.handler"]},
                    MemorySize=1024
                )
                return
            except self.lambda_client.exceptions.ResourceConflictException:
                pass        
            
        #TODO trigger cold start by setting new version number of the lambda
        
        return
        
    def invoke_func(self, func_name, options={}):
        try:
            lambda_response = self.lambda_client.invoke(
                FunctionName=func_name
            )
            
            import_duration = json.loads(lambda_response['Payload'].read())
            request_id = lambda_response['ResponseMetadata']['RequestId']
        except Exception as e:
            return "", Exception(f"Couldn't invoke function {func_name}: {e}")
        
        time.sleep(5)
        
        for _ in range(5):
            try:
                log_group_name=f'/aws/lambda/{func_name}'
                response = self.log_client.describe_log_streams(
                    logGroupName=log_group_name,
                    orderBy='LastEventTime',
                    limit=1,
                    descending=True
                )
                
                log_stream_name = response['logStreams'][0]['logStreamName']
                print(log_stream_name)
                filter_pattern = ' '.join([f'"{request_id}"'])
                response = self.log_client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=[log_stream_name],
                    filterPattern=filter_pattern
                )
                
                #print(response)
                pattern = r"RequestId: (.+?)\s*Duration: (\d+\.\d+?)\s*ms\s*Billed Duration: (\d+)\s*ms\s*Memory Size: (\d+)\s*MB\s*Max Memory Used: (\d+)\s*MB\s*Init Duration: (\d+\.\d+?)\s*ms"
                for event in response['events']:
                    if "Init Duration" in event['message']:
                        #print(event['message'])
                        result = re.findall(pattern, event['message'])[0]
                        metric = {"request_id":result[0], "function name":func_name, "duration (ms)":result[1], 
                                    "billed duration (ms)": result[2], "memory size (mb)": result[3], 
                                    "max memory used (mb)":result[4], "init duration (ms)": result[5],
                                    "import duration (ms)": import_duration}
                        with self.metrics_lock:
                            self.metrics = self.metrics._append(metric, ignore_index=True)
            
                return func_name, None
            except Exception as e:
                time.sleep(1)
                print("retry fetching log:", e)
        print("failed to fetch log")
        return "", False
    
    def delete_all_func(self):
        response = self.lambda_client.list_functions()
        functions = response['Functions']
        while(len(functions) > 0):
            for func in functions:
                function_name = func['FunctionName']
                print(f"Deleting Lambda function: {function_name}")
                self.lambda_client.delete_function(FunctionName=function_name)
                print(f"Deleted: {function_name}")
                
            response = self.lambda_client.list_functions()
            functions = response['Functions']

        print("All Lambda functions have been deleted.")   
