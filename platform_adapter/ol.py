from platform_adapter.interface import PlatformAdapter
import os
import time
import subprocess
import requests
import re

class OL(PlatformAdapter):
    def __init__(self, ol_dir):
        self.ol_dir = ol_dir
    
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

    def deploy_func():
        pass
        
    def invoke_func(self, url, req_body=None):
        return requests.post(url, json=req_body)
        