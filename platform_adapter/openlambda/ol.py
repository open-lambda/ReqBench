from platform_adapter.interface import PlatformAdapter
import os
import time
import subprocess
import requests
import re
import shutil
import pandas as pd
import signal
import glob
import psutil
import threading

# PSS is the most accurate metric for memory usage,
# I didn't use the RSS, as it doesn't exclude shared memory.
# I also didn't use memory.usage_in_bytes in cgroup, as it include kernel memory allocation.
def get_pss(pid):
    process = psutil.Process(int(pid))
    return process.memory_full_info().pss / 1024


def get_rss(pid):
    process = psutil.Process(int(pid))
    return process.memory_info().rss / 1024


def get_total_mem(base, MODE="PSS"):
    total_pss = 0
    if MODE == "CG":
        mem_in_bytes = int(open(os.path.join(base, "memory.current"), "r").readline())
        return mem_in_bytes / 1024

    for cg_folder in glob.glob(os.path.join(base, "cg-*")):
        procs_file = os.path.join(cg_folder, "cgroup.procs")

        if os.path.exists(procs_file):
            with open(procs_file, "r") as f:
                pids = f.readlines()
            for pid in pids:
                pid = pid.strip()
                if MODE == "PSS":
                    total_pss += get_pss(pid)
                elif MODE == "RSS":
                    total_pss += get_rss(pid)
    return total_pss


metrics_lock = threading.Lock()
class OL(PlatformAdapter):
    def __init__(self):
        self.load_config("platform_adapter/openlambda/config.json")
        self.ol_dir = self.config["ol_dir"]
        self.metrics = pd.DataFrame(
            columns=["invoke_id", "req", "received", "start_pullHandler", "end_pullHandler",
                    "start_create", "end_create", "start_import", "end_import", "end_execute",
                     "failed", "split_generation"]
        )

    def start_worker(self, options={}):
        optstr = ",".join(["%s=%s" % (k, v) for k, v in options.items()])
        cmd = ['./ol', 'worker', 'up', '-d']
        if optstr:
            cmd.extend(['-o', optstr])
        print(cmd)
        out = subprocess.check_output(cmd, cwd=self.ol_dir)
        print(str(out, 'utf-8'))

        if options.get("features.warmup", False):
            self.warmup_mem = get_total_mem(self.config["cg_dir"], MODE="PSS")

        match = re.search(r"PID: (\d+)", str(out, 'utf-8'))
        if match:
            pid = match.group(1)
            self.pid = pid
            print(f"The PID is {pid}")
            return 0
        else:
            print("No PID found in the text.")
            return -1

    def kill_worker(self, options={}):
        if not self.pid:
            print("PID has not been set")
            return -1
        try:
            cmd = ['./ol', 'worker', 'down']
            out = subprocess.check_output(cmd, cwd=self.ol_dir)
            print(str(out, 'utf-8'))
        except Exception as e:
            print(e)
            print("force kill")

            print(f"Killing process {self.pid} on port 5000")
            subprocess.run(['kill', '-9', self.pid], cwd=self.ol_dir)

            cmd = ['./ol', 'worker', 'force-cleanup']
            subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=self.ol_dir)

            process = subprocess.Popen(['./ol', 'worker', 'up'], cwd=self.ol_dir)
            os.kill(process.pid, signal.SIGINT)

            cmd = ['./ol', 'worker', 'force-cleanup']
            subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=self.ol_dir)

        if options.get("save_metrics", False):
            csv_name = options.get("csv_name", "metrics.csv")
            self.metrics.to_csv(csv_name, index=False)
        # reset metrics
        self.metrics = pd.DataFrame(
            columns=["invoke_id", "req", "received", "start_pullHandler", "end_pullHandler",
                     "start_create", "end_create", "start_import", "end_import", "end_execute",
                     "failed", "split_generation"]
        )
        return 0

    def deploy_func(self, func_config):
        func_path = f"{self.ol_dir}default-ol/registry/{func_config['name']}"

        if os.path.exists(func_path):
            shutil.rmtree(func_path)
        os.makedirs(func_path, exist_ok=True)


        with open(os.path.join(func_path, "f.py"), 'w') as f:
            f.write(func_config["code"])
        with open(os.path.join(func_path, "requirements.txt"), 'w') as f:
            f.write(func_config["requirements_txt"])
        
    def invoke_func(self, func_name, options={}):
        if not options:
            url = f"http://localhost:5000/run/{func_name}"
            resp = requests.post(url)
        else:
            if "url" in options.keys() and options["url"] != "":
                url = options["url"]
            else:
                url = f"http://localhost:5000/run/{func_name}"
            resp = requests.post(url, json=options["req_body"])

        if resp.status_code != 200:
            raise Exception(f"Request to {url} failed with status {resp.status_code}")

        resp_body = resp.json()
        if isinstance(resp_body, dict):
            with metrics_lock:
                self.metrics = self.metrics._append({
                    "invoke_id": resp_body["name"],
                    "req": resp_body["req"],
                    "received": time.time() * 1000,
                    "start_pullHandler": resp_body["start_pullHandler"],
                    "end_pullHandler": resp_body["end_pullHandler"],
                    "start_create": resp_body["start_create"] if "start_create" in resp_body else 0,
                    "end_create": resp_body["end_create"] if "end_create" in resp_body else 0,
                    "start_import": resp_body["start_import"],
                    "end_import": resp_body["end_import"],
                    "end_execute": resp_body["end_execute"],
                    "failed": resp_body["failed"],
                    "split_generation": resp_body["split_gen"],
                }, ignore_index=True)
                if len(resp_body["failed"])>0:
                    print("failed to import:",resp_body["failed"])
        return resp_body, None
