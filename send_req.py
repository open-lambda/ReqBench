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
    if latency:
        req_body = {
            "name": get_id(call['name']),
            "req": get_curr_time()
        }

        options = {
            "url": "",
            "req_body": req_body
        }
        response = platform.invoke_func(call['name'], options=options)
    else:
        response = platform.invoke_func(call['name'])

    if response.status_code != 200:
        raise Exception(f"Request to {url} failed with status {response.status_code}")

    body = response.json()
    if latency:
        body["received"] = get_curr_time()
        options = {
            "url": "http://localhost:4998/latency",
            "req_body": body
        }
        requests.post(options["url"], json=options["req_body"])

    if not latency and body != call['name']:
        raise Exception(f"Response body does not match: {body} != {call['name']}")


# deploy concurrently
def deploy_funcs(workload, platform):
    funcs = workload["funcs"]

    with ThreadPoolExecutor() as executor:
        futures = []
        for fn in funcs:
            meta = fn["meta"]
            code = "\n".join(fn["code"])
            func_config = {
                "name": fn["name"],
                "code": code,
                "requirements_txt": meta["requirements_txt"]
            }
            future = executor.submit(deploy_func, platform, func_config)
            futures.append(future)
        for future in futures:
            future.result()
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
