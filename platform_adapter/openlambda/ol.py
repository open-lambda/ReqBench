from platform_adapter.interface import PlatformAdapter
import os
import time
import subprocess
import requests
import re
import shutil

class OL(PlatformAdapter):
    def __init__(self):
        self.load_config("platform_adapter/openlambda/config.json")
        self.ol_dir = self.config["ol_dir"]
    
    def start_worker(self, options={}):
        optstr = ",".join(["%s=%s" % (k, v) for k, v in options.items()])
        os.chdir(self.ol_dir)
        cmd = ['./ol', 'worker', 'up', '-d']
        if optstr:
            cmd.extend(['-o', optstr])
        print(cmd)
        out = subprocess.check_output(cmd)
        print(str(out, 'utf-8'))

        match = re.search(r"PID: (\d+)", str(out, 'utf-8'))
        if match:
            pid = match.group(1)
            self.pid = pid
            print(f"The PID is {pid}")
            if "features.warmup" in options and options['features.warmup'] == "true":
                time.sleep(10)  # wait for worker to warm up
            return 0
        else:
            print("No PID found in the text.")
            return -1

    def kill_worker(self, options={}):
        if not self.pid:
            print("PID has not been set")
            return -1
        os.chdir(self.ol_dir)
        try:
            cmd = ['./ol', 'worker', 'down']
            out = subprocess.check_output(cmd)
            print(str(out, 'utf-8'))
        except Exception as e:
            print(e)
            print("force kill")

            print(f"Killing process {self.pid} on port 5000")
            subprocess.run(['kill', '-9', self.pid])

            cmd = ['./ol', 'worker', 'force-cleanup']
            subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            process = subprocess.Popen(['./ol', 'worker', 'up'])
            os.kill(process.pid, signal.SIGINT)

            cmd = ['./ol', 'worker', 'force-cleanup']
            subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        return 0

    def deploy_func(self, func_config):
        func_path = f"{self.ol_dir}default-ol/registry/{func_config['name']}"

        if os.path.exists(func_path):
            shutil.rmtree(func_path)
        os.makedirs(func_path, exist_ok=True)

        code_lines = func_config["code"]
        code = "\n".join(code_lines)

        with open(os.path.join(func_path, "f.py"), 'w') as f:
            f.write(code)
        with open(os.path.join(func_path, "requirements.in"), 'w') as f:
            f.write(func_config["requirements_in"])
        with open(os.path.join(func_path, "requirements.txt"), 'w') as f:
            f.write(func_config["requirements_txt"])
        
    def invoke_func(self, func_name, options={}):
        if not options:
            url = f"http://localhost:5000/run/{func_name}"
            return requests.post(url)
        else:
            url = options["url"] if options["url"] != "" else f"http://localhost:5000/run/{func_name}"
            return request.post(url, json=options["req_body"])
            
        