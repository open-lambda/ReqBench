import os
import shutil

import requests
import json
import threading
import time
import sys
import concurrent.futures
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


def task(call, platform):
    req_body = {
        "name": get_id(call['name']),
        "req": get_curr_time()
    }
    options = {
        "req_body": req_body
    }
    resp_body,err = platform.invoke_func(call['name'], options=options)

    if resp_body is None or resp_body == "":
        raise Exception(f"Error: {err}")


# deploy concurrently
def deploy_funcs(workload, platform):
    funcs = workload["funcs"]
    call_set = {call["name"] for call in workload["calls"]}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for fn in funcs:
            # only deploy functions that are called to save time
            if fn["name"] not in call_set:
                continue
            meta = fn["meta"]
            code = "\n".join(fn["code"])
            func_config = {
                "name": fn["name"],
                "code": code,
                "requirements_txt": meta["requirements_txt"]
            }
            future = executor.submit(platform.deploy_func, func_config)
            futures.append(future)
        for future in futures:
            future.result()
    return workload


def run(workload, num_tasks, platform, timeout=None):
    deploy_funcs(workload, platform)
    calls = workload['calls']
    finished = 0
    total = len(calls)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = [executor.submit(task, call, platform) for call in calls]
        for future in futures:
            try:
                if timeout is None:
                    future.result()
                else:
                    future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                print("Task timed out")
            except Exception as e:
                print(f"Task resulted in an exception: {e}")
            finished += 1
            if finished % 100 == 0:
                print(f"Finished {finished}/{total} tasks")

    # clean up
    with seen_lock:
        seen={}
    end_time = time.time()
    seconds = end_time - start_time
    return seconds, len(calls) / seconds
