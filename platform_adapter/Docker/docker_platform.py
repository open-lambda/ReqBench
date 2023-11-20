import json
import threading

from platform_adapter.interface import PlatformAdapter
import os
import docker
import time
import subprocess
import requests
import re
import shutil
from util import cache_pkgs_dir, tmp_dir
from collections import OrderedDict
from io import BytesIO

run_handler_code = """
import sys
import f
import json

if __name__ == "__main__":
    req = json.loads(sys.argv[1])
    res = f.f(req)
    print(json.dumps(res))
"""

package_base_dockerfile = '''
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y python3 python3-pip
COPY .cache/ /tmp/.cache/
CMD ["tail", "-f", "/dev/null"]
'''

class Dockerplatform(PlatformAdapter):
    def __init__(self, config_path):
        if os.path.exists(config_path):
            self.load_config(path=config_path)
        self.client = docker.DockerClient(base_url=self.config["base_url"])
        self.handlers_dir = self.config["handlers_dir"]
        self.images = {}
        self.containers = {}  # {func_name: containerObj}
        self.cache = OrderedDict()  # lru cache {func_name: timestamp}
        self.docker_thread_lock = threading.Lock()

    def start_worker(self, options={}):
        # build a shared package base image (to save some time downloading packages)
        with open(os.path.join(tmp_dir, "Dockerfile"), "w") as f:
            f.write(package_base_dockerfile)
        with open(os.path.join(tmp_dir, ".dockerignore"), "w") as f:
            f.write("\n")
        self.client.images.build(path=tmp_dir, tag="package-base")
        os.remove(os.path.join(tmp_dir, ".dockerignore"))

    def kill_worker(self, options={}):
        # kill Docker containers takes minutes, defaultly not kill
        t1 = time.time()
        if options.get("kill_containers", False):
            for container in self.client.containers.list():
                container.remove(force=True)
        self.client.containers.prune()
        self.client.images.prune()
        t2 = time.time()
        print(f"kill docker worker time: {t2 - t1}")

    def deploy_func(self, func_config):
        func_path = os.path.join(self.handlers_dir, func_config["name"])
        if os.path.exists(func_path):
            shutil.rmtree(func_path)
        os.makedirs(func_path, exist_ok=True)
        os.makedirs(os.path.join(func_path, "tmp"), exist_ok=True)

        with open(os.path.join(func_path, "f.py"), "w") as f:
            f.write(func_config["code"])
        with open(os.path.join(func_path, "run_handler.py"), "w") as f:
            f.write(run_handler_code)
        with open(os.path.join(func_path, "requirements.txt"), "w") as f:
            f.write(func_config["requirements_txt"])

        # write to Dockerfile
        with open(os.path.join(func_path, "Dockerfile"), "w") as f:
            f.write("FROM package-base\n")
            f.write("COPY ./ /app/\n")
            f.write("RUN pip install -r /app/requirements.txt --cache-dir /tmp/.cache\n")

        self.images[func_config["name"]] = func_path
        # build Docker images for each function
        self.client.images.build(path=func_path, tag=func_config["name"])

    def invoke_func(self, func_name, options={}):
        # before invoking a function, check if memory usage is too high
        # if so, evict the least recently used container
        # with self.docker_thread_lock:
        #     self.evict_containers()
        # start container if current func does not have a container
        if func_name not in self.containers:
            self.containers[func_name] = self.client.containers.run(
                    func_name,
                    detach=True,
            )
        self.cache[func_name] = time.time()
        container = self.containers[func_name]
        t1 = time.time()
        try:
            if container.status != 'running':
                container.start()
            req_str = json.dumps(options["req_body"]) if "req_body" in options else "{}"
            res = container.exec_run(f"python3 /app/run_handler.py '{req_str}'")
            #print("res: ",res.output)
        except Exception as e:  # container has been force killed
            print(e)
            res = "{}"
        t2 = time.time()
        # print(f"container start time: {t1-t0}, container exec time: {t2-t1}")
        return json.loads(res.output), res.exit_code

""" #get_memory_usage is too slow to run, sometimes took seconds
    def evict_containers(self):
        t0 = time.time()
        _, mem_sum = self.get_memory_usage()
        t1 = time.time()

        mem_sum_in_mb = mem_sum / 1024 / 1024
        while mem_sum_in_mb > self.config["memory_limit"]:
            least_used_func = self.cache.popitem(last=False)[0]
            if container:
                container.stop()
        t2= time.time()
        print(f"get memory usage time: {t1 - t0}, evict time: {t2 - t1}")

    def get_memory_usage(self):
        memory_usage_info = {}
        sum = 0
        for func_name, container in self.containers.items():
            stats = container.stats(stream=False)
            memory_usage = stats['memory_stats']['usage']
            memory_usage_info[func_name] = memory_usage
            sum += memory_usage

        return memory_usage_info, sum
"""