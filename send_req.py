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


def task(call, latency):
    url = "http://localhost:5000/run/" + call['name']
    req_body = {}

    if latency:
        req_body["name"] = get_id(call['name'])
        req_body["req"] = get_curr_time()
        response = requests.post(url, json=req_body)
    else:
        response = requests.post(url, json=None)

    if response.status_code != 200:
        raise Exception(f"Request to {url} failed with status {response.status_code}")

    body = response.json()
    if latency:
        body["received"] = get_curr_time()
        requests.post("http://localhost:4998/latency", json=body)

    if not latency and body != call['name']:
        raise Exception(f"Response body does not match: {body} != {call['name']}")


def deploy_funcs(workload):
    funcs = workload["funcs"]

    for fn in funcs:
        meta = fn["meta"]
        func_name = fn["name"]
        func_path = f"/root/open-lambda/default-ol/registry/{func_name}"

        if os.path.exists(func_path):
            shutil.rmtree(func_path)
        os.makedirs(func_path, exist_ok=True)

        code_lines = fn["code"]
        code = "\n".join(code_lines)

        with open(os.path.join(func_path, "f.py"), 'w') as f:
            f.write(code)
        with open(os.path.join(func_path, "requirements.in"), 'w') as f:
            f.write(meta["requirements_in"])
        with open(os.path.join(func_path, "requirements.txt"), 'w') as f:
            f.write(meta["requirements_txt"])

    return workload


def run(workload, num_tasks, latency):
    deploy_funcs(workload)
    calls = workload['calls']

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = [executor.submit(task, call, latency) for call in calls]
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
