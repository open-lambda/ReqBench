from platform_adapter.interface import PlatformAdapter
import os
import json
import boto3
import shutil
import subprocess

class AWS(PlatformAdapter):
    def __init__(self):
        self.load_config("platform_adapter/aws/config.json")
        
        self.home_dir = os.path.expanduser('~')
        
        self.iam_arn = self.config["iam_arn"]
        self.region_name = self.config["region_name"]
        
        if not os.path.exists(f"{self.home_dir}/.aws/credentials"):
            os.makedirs(f"{self.home_dir}/.aws", exist_ok=True)
            with open(f"{self.home_dir}/.aws/credentials", "w") as f:
                f.write(["[default]\n", 
                         f"aws_access_key_id = {self.config['aws_access_key_id']}\n",
                         f"aws_secret_access_key = {self.config['aws_secret_access_key']}\n"])        
        self.client = boto3.client('lambda', region_name=self.region_name)
    
    def start_worker(self, options={}):
        pass

    def kill_worker(self, options={}):
        pass

    def deploy_func(self, func_config):
        #configure zip file for lambda
        #https://docs.aws.amazon.com/lambda/latest/dg/python-package.html
        temp_dir = f"{self.home_dir}/.tmp-lambda"
        
        try:
            os.makedirs(f"{temp_dir}")
        except OSError:
            shutil.rmtree(temp_dir)
        with open(f"{temp_dir}/requirements.txt", "w") as f:
            f.write(func_config["requirements_txt"])
        
        subprocess.check_output(["pip", "install", "-r", f"{temp_dir}/requirements.txt", 
                                "--target", f"{temp_dir}"]) #install packages at temp_dir
        
        os.remove(f"{temp_dir}/requirements.txt")
        with open(f"{temp_dir}/lambda_function.py", "w") as f:
            #replace f(event) to f(event, context)
            f.write(func_config["code"].replace("f(event):", "f(event, context):"))
        
        wd = os.getcwd()
        os.chdir(temp_dir)
        subprocess.check_output(["zip", "-r", f"{wd}/fn.zip", "."])
        os.chdir(wd)
        shutil.rmtree(temp_dir)
        
        #create function in aws
        try:
            self.client.create_function(
                FunctionName=func_config['name'],
                Handler='lambda_function.f',
                Runtime='python3.10',
                Role=self.iam_arn,
                Code={'ZipFile': open('./fn.zip', 'rb').read()}
            )
        except boto3.ClientError:
            return Exception(f"Couldn't create function {func_config['name']}")
        
        os.remove("./fn.zip")
        
    def invoke_func(self, func_name, options={}):
        try:
            response = self.client.invoke(
                FunctionName=func_name
            )
        except boto3.ClientError:
            return "", Exception(f"Couldn't invoke function {func_name}.")
        return json.loads(response['Payload'].read()), None
