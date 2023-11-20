import json
import math
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

run_handler_code = """
import sys
import f
import json

if __name__ == "__main__":
    # change sys.path to import packages
    with open("/app/requirements.txt", "r") as file:
        reqs = file.read().splitlines()
        for req in reqs:
            req = req.strip() 
            if req != "" and not req.strip().startswith("#"):
                sys.path.insert(0, f"/packages/{req}")
    req = json.loads(sys.argv[1])
    res = f.f(req)
    print(json.dumps(res))
"""

package_base_dockerfile = '''
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y python3 python3-pip
COPY .cache/ /tmp/.cache/

COPY pkg_list.txt /pkg_list.txt
COPY run_handler.py /app/run_handler.py

COPY install_all.py /install_all.py
RUN python3 /install_all.py /pkg_list.txt
CMD ["tail", "-f", "/dev/null"]
'''

def write_if_different(file_path, new_content):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            if file.read() == new_content:
                return
    with open(file_path, 'w') as file:
        file.write(new_content)

class Dockerplatform(PlatformAdapter):
    def __init__(self, config_path):
        if os.path.exists(config_path):
            self.load_config(path=config_path)
        self.client = docker.DockerClient(base_url=self.config["base_url"])
        self.handlers_dir = self.config["handlers_dir"]
        self.containers = {}  # {func_name: containerObj}
        self.cache = OrderedDict()  # lru cache {func_name: timestamp}
        self.docker_thread_lock = threading.Lock()

        # due to the limitation of network bridge docker0, we cannot run more than 1024 containers
        # in the same network, and this number is hardcore in the kernel
        # thus create multiple networks
        self.network = {"docker0": 0} # {network_name: #containers}
        self.evict_thread = threading.Thread(target=self.evict_containers)
        self.quit_evict_thread = False

    def start_worker(self, options={}):
        # build a shared package base image (to save some time downloading packages)
        write_if_different(os.path.join(tmp_dir, "pkg_list.txt"), "\n".join(options["packages"]))
        write_if_different(os.path.join(tmp_dir, "run_handler.py"), run_handler_code)
        write_if_different(os.path.join(tmp_dir, "pkg_base.Dockerfile"), package_base_dockerfile)
        source_install_all = os.path.join(os.path.dirname(os.path.abspath(__file__)), "install_all.py")
        destination_install_all = os.path.join(tmp_dir, "install_all.py")
        if not os.path.exists(destination_install_all) or open(source_install_all).read() != open(
                destination_install_all).read():
            shutil.copy(source_install_all, tmp_dir)

        self.client.images.build(path=tmp_dir, tag="package-base", dockerfile="pkg_base.Dockerfile")
        self.evict_thread.start()
        # network_bridges = math.ceil(options['unique_containers']/1000)
        # for i in range(network_bridges):
        #     self.network[f"reqbench_docker_network{i}"] = 0
        #     self.client.networks.create(f"reqbench_docker_network{i}", driver="bridge")
        # os.remove(os.path.join(tmp_dir, ".dockerignore"))
        # os.remove(os.path.join(tmp_dir, "Dockerfile"))
        # os.remove(os.path.join(tmp_dir, "pkg_list.txt"))
        # os.remove(os.path.join(tmp_dir, "run_handler.py"))

    def kill_worker(self, options={}):
        # kill Docker containers takes minutes, defaultly not kill
        t1 = time.time()
        self.quit_evict_thread = True
        self.evict_thread.join()
        if options.get("kill_containers", True):
            for container in self.client.containers.list():
                container.remove(force=True)
        self.client.containers.prune()
        self.client.images.prune()
        self.client.networks.prune()
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
        # run_handler.py will change the sys.path based on the requirements.txt
        with open(os.path.join(func_path, "run_handler.py"), "w") as f:
            f.write(run_handler_code)
        with open(os.path.join(func_path, "requirements.txt"), "w") as f:
            f.write(func_config["requirements_txt"])


    def invoke_func(self, func_name, options={}):
        # evict containers if the number of containers exceeds 1024
        # with self.docker_thread_lock:
        #     self.evict_containers()

        with self.docker_thread_lock:
            cnt = self.network["docker0"]
        while cnt > 1024 * 0.9:
            with self.docker_thread_lock:
                cnt = self.network["docker0"]
        with self.docker_thread_lock:
            self.network["docker0"] += 1

        # start container if current func does not have a container
        if func_name not in self.containers:
            # mount requirements.txt and f.py to the container
            self.containers[func_name] = self.client.containers.run(
                    "package-base",
                    mounts=[docker.types.Mount(
                        type="bind",
                        source=os.path.join(self.handlers_dir, func_name),
                        target="/app/",
                        read_only=True,
                        )
                    ],
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
            print("container forcibly killed:", e)
            res = "{}"
        t2 = time.time()
        # print(f"container start time: {t1-t0}, container exec time: {t2-t1}")
        return json.loads(res.output), res.exit_code


    # due to the limitation of network bridge docker0, we cannot run more than 1024 containers
    # in the same network, and this number is hardcore in the kernel
    # I also tried to create multiple networks, but the network IO was dragging down the performance
    def evict_containers(self):
        while self.quit_evict_thread is False:
            with self.docker_thread_lock:
                cnt = self.network["docker0"]
            if cnt < 1024*0.7:
                continue
            while cnt > 1024*0.4:
                try:
                    with self.docker_thread_lock:
                        container_name = self.cache.popitem(last=False)[0]
                        container = self.containers[container_name]
                        del self.containers[container_name]

                    container.remove(force=True)
                    with self.docker_thread_lock:
                        self.network["docker0"] -= 1
                        cnt = self.network["docker0"]
                except Exception as e:
                    print("err: ", e)

""" 
# get_memory_usage is too slow to run, sometimes took seconds, which dramatically enlarge latency
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