import os
import shutil

import requests
import json
import threading
import time
import sys
from concurrent.futures import ThreadPoolExecutor

seen = {}
seen_lock = threading.Lock()


def get_curr_time():
    return time.time() * 1000


def get_id(name):
    with seen_lock:
        if name not in seen:
            seen[name] = 1
        id = str(seen[name])
        seen[name] += 1
        return name + "_" + id


def task(call, latency, platform):
    url = "http://localhost:5000/run/" + call['name']
    req_body = {}

    if latency:
        req_body["name"] = get_id(call['name'])
        req_body["req"] = get_curr_time()
        response = platform.invoke_func(url, req_body=req_body)
    else:
        response = platform.invoke_func(url, req_body=None)

    if response.status_code != 200:
        raise Exception(f"Request to {url} failed with status {response.status_code}")

    body = response.json()
    if latency:
        body["received"] = get_curr_time()
        platform.invoke_func("http://localhost:4998/latency", req_body=body)

    if not latency and body != call['name']:
        raise Exception(f"Response body does not match: {body} != {call['name']}")


def deploy_funcs(workload, platform):
    funcs = workload["funcs"]

    for fn in funcs:
        meta = fn["meta"]
        func_config = {
            "name" : fn["name"],
            "code"  : fn["code"],
            "requirements_in" : meta["requirements_in"],
            "requirements_txt" : meta["requirements_txt"]
        }

        platform.deploy_func(func_config)
        
    return workload


def run(workload, num_tasks, latency, platform):
    deploy_funcs(workload, platform)
    calls = workload['calls']

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = [executor.submit(task, call, latency, platform) for call in calls]
        for future in futures:
            future.result()

    end_time = time.time()
    seconds = end_time - start_time
    return seconds, len(calls) / seconds

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python bench.py <workload-path.json> <tasks> <latency bool>")
        sys.exit()

    path = sys.argv[1]
    tasks = int(sys.argv[2])
    measure_latency = sys.argv[3].lower() == 'true'
    workload = json.load(open(path, 'r'))

    sec, ops = run(workload, tasks, measure_latency)
    print(json.dumps({"seconds": sec, "ops/s": ops}))
