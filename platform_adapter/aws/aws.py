from platform_adapter.interface import PlatformAdapter
import os
import re
import json
import boto3
import base64
import docker
import shutil
import botocore
import threading
import subprocess
from config import tmp_dir

aws_handler="""import time, importlib, os, sys

os.environ['OPENBLAS_NUM_THREADS'] = '2'

for req in %s:
    sys.path.insert(0, f"/packages/{req}")

for mod in %s:
    try:
        importlib.import_module(mod)
    except Exception as e:
        pass
        
def handler(event, context):
    return '%s'
"""

dockerfile = '''
FROM python:3.10 as build-image

COPY . .
RUN mkdir -p /tmp/.cache
RUN python3 /install_all.py /pkg_list.txt
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
        self.client = boto3.client('lambda', region_name=self.region_name)
    
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
            handler_code = aws_handler % (dependencies, func["meta"]["import_mods"], func["name"])   
                     
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
        pass

    def kill_worker(self, options={}):
        pass

    def deploy_func(self, func_config):
        try:
            self.client.create_function(
                FunctionName=func_config['name'],
                Role=self.iam_arn,
                PackageType='Image',
                Code={'ImageUri': self.image_name},
                ImageConfig={'Command': [f"{func_config['name']}.handler"]}
            )
        except self.client.exceptions.ResourceConflictException:
             return
        except Exception as e:
            return Exception(f"Couldn't create function {func_config['name']}: {e}")
    
        
    def invoke_func(self, func_name, options={}):
        try:
            response = self.client.invoke(
                FunctionName=func_name
            )
        except Exception as e:
            return "", Exception(f"Couldn't invoke function {func_name}.")
        return json.loads(response['Payload'].read()), None
