import gc

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
import csv
import logging
from flask import Flask, request, jsonify

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
        memory_current_path = os.path.join(base, "memory.current")
        with open(memory_current_path, "r") as file:
            mem_in_bytes = int(file.readline())
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


# clear cache is important, it affects the performance
def clear_cache():
    try:
        with open('/proc/sys/vm/drop_caches', 'w') as f:
            f.write('1')
            f.write('2')
            f.write('3')
        print("Cache cleared successfully.")
    except Exception as e:
        print(f"Error clearing cache: {e}")


def read_pid_cgroup_memory(pid):
    def read_file(file_path):
        try:
            with open(file_path, 'r') as file:
                return file.read().strip()
        except IOError as e:
            return f"Error reading file {file_path}: {e}"

    cgroup_path = f"/proc/{pid}/cgroup"
    cgroup_info = read_file(cgroup_path)
    cgroup_path_components = cgroup_info.split(':')[-1].strip()
    cgroup_memory_path = f"/sys/fs/cgroup{cgroup_path_components}/memory.current"
    memory_usage = int(read_file(cgroup_memory_path)) / 1024
    return memory_usage


def read_cgroup_memory():
    # read memory usage from default-ol-sandboxes sandbox
    cgroup_path = "/sys/fs/cgroup/default-ol-sandboxes"
    mem_in_bytes = 0
    for cg_folder in glob.glob(os.path.join(cgroup_path, "cg-*")):
        mem = os.path.join(cg_folder, "memory.current")
        if os.path.exists(mem):
            with open(mem, "r") as f:
                mem_in_bytes += int(f.readline())
    return mem_in_bytes / 1024


metrics_lock = threading.Lock()


class OL(PlatformAdapter):
    def __init__(self):
        self.load_config("platform_adapter/openlambda/config.json")
        self.ol_dir = self.config["ol_dir"]

        self.metrics = []
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        self.app = Flask(__name__)
        self.app.add_url_rule('/fork', view_func=self.recv_fork, methods=['POST'])
        self.app.add_url_rule('/start', view_func=self.recv_start, methods=['POST'])
        self.collectT = None
        self.fork_list = []
        self.start_list = []
    def recv_fork(self):
        json_data = request.get_json()
        self.fork_list.append(json_data)
        if json_data:
            return jsonify({}), 200
        else:
            return jsonify({}), 400
    def recv_start(self):
        json_data = request.get_json()
        self.start_list.append(json_data)
        if json_data:
            return jsonify({}), 200
        else:
            return jsonify({}), 400


    def start_worker(self, options={}):
        # start restAPI
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
        self.collectT = threading.Thread(
            target=lambda: self.app.run(host='localhost', port=4998, debug=False, use_reloader=False)
        )
        self.collectT.setDaemon(True)
        self.collectT.start()

        if os.path.exists(os.path.join(self.current_dir, "fork.csv")):
            os.remove(os.path.join(self.current_dir, "fork.csv"))
        if os.path.exists(os.path.join(self.current_dir, "start.csv")):
            os.remove(os.path.join(self.current_dir, "start.csv"))
        # remove tmp.csv (if any)
        if os.path.exists(os.path.join(self.current_dir, "tmp.csv")):
            os.remove(os.path.join(self.current_dir, "tmp.csv"))

        optstr = ",".join(["%s=%s" % (k, v) for k, v in options.items()])
        # restrict OL in a cgroup
        cg_name = "ol"
        cgroup_path = f"/sys/fs/cgroup/{cg_name}"
        if os.path.exists(cgroup_path):
            os.rmdir(cgroup_path)
        os.makedirs(cgroup_path, exist_ok=True)
        cmd = ["sudo", "cgexec", "-g", f"memory,cpu:{cg_name}", './ol', 'worker', 'up', '-d']
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
            print(f"The OL PID is {pid}")
            return 0
        else:
            print("No PID found in the text.")
            return -1

    def kill_worker(self, options={}):
        if self.collectT:
            write_list_to_file(os.path.join(self.current_dir, "fork.csv"), self.fork_list)
            write_list_to_file(os.path.join(self.current_dir, "start.csv"), self.start_list)
            self.fork_list = []
            self.start_list = []

        if options.get("save_metrics", False):
            write_list_to_file(os.path.join(self.current_dir, "tmp.csv"), self.metrics)
            csv_name = options.get("csv_name", "metrics.csv")
            if os.path.exists(csv_name):
                os.remove(csv_name)
            os.rename(os.path.join(self.current_dir, "tmp.csv"),  csv_name)
        self.metrics = []

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

    # use go collector
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
            raise Exception(f"Request to {url} failed, resp: {resp.text}, status code: {resp.status_code}")

        try:
            resp_body = resp.json()
        except Exception as e:
            raise Exception(f"Error parsing response: {e}, the response is {resp.text}")
        if isinstance(resp_body, dict):
            resp_body["received"] = time.time() * 1000
            with metrics_lock:
               record = {
                    "invoke_id": resp_body["invoke_id"],
                    "req": resp_body["req"],
                    "received": time.time() * 1000,
                    "start_pullHandler": resp_body["start_pullHandler"],
                    "end_pullHandler": resp_body["end_pullHandler"],
                    "start_create": resp_body["start_create"] if "start_create" in resp_body else 0,
                    "end_create": resp_body["end_create"] if "end_create" in resp_body else 0,
                    "start_import": resp_body["start_import"],
                    "end_import": resp_body["end_import"],
                    "start_execute": resp_body["start_execute"],
                    "end_execute": resp_body["end_execute"],
                    "failed": resp_body["failed"],
                    "split_generation": resp_body["split_gen"],
                    "zygote_miss": resp_body["zygote_miss"] if "zygote_miss" in resp_body else 0,
                }
            self.metrics.append(record)
        resp.close()
        return resp_body, None

def write_list_to_file(file_path, list_data):
    mode = 'a' if os.path.exists(file_path) else 'w'

    s = time.time()
    with open(file_path, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if mode == 'w':
            writer.writerow(list_data[0].keys())
        for item in list_data:
            writer.writerow(item.values())
    e = time.time()
    print(f"write to file {file_path} takes {e-s} seconds")