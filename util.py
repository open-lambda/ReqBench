import glob
import json
import os
import pkgutil
import re
import shutil
import signal
import subprocess
import time
from collections import deque

import pandas as pd
import requests

from config import *


pattern = re.compile(r'([^<>=!]+)([<>!=]=?)([^<>=!]+)?')
dep_pattern = re.compile(r'#\s+via\s+(.+)')

pattern = re.compile(r'([^<>=!;]+)([<>!=]=?=?)([^<>=!;]+)?(?:\s*;\s*([^<>=!]+)([<>!=]=?)([^<>=!]+))?')
end_string = "The following packages are considered to be unsafe in a requirements file"

# todo: add direct option
def parse_requirements(line_str, direct=False):
    """
    Parse the given pip-compile generated requirements string.

    Args:
        line_str (str): The string content of the requirements file.

    Returns:
        requirements: {'cython': ['==', '3.0.2'], 'numpy': ['==', '1.25.2'], 'packaging': ['==', '23.1'], 'scipy': ['==', '1.11.2']}
        versioned_dependencies: {'cython==3.0.2': ['cython==3.0.2', 'direct_req'], 'numpy==1.25.2': ['numpy==1.25.2', 'direct_req', 'scipy==1.11.2'], ...}
        (dependencies = {'cython': ['direct_req'], 'numpy': ['direct_req', 'scipy'], ...} )
    Raises:
        Exception: If the input string is None.

    Notes:
        dependencies(A)=[B, C] means B,C depends on A
    """

    if line_str is None:
        raise Exception("requirement.in or txt is None")
    lines = line_str.splitlines()

    requirements = {}
    dependencies = {}

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if end_string in line:
            break
        match = pattern.match(line)
        if match and not lines[i].startswith('#'):
            # todo: handle condition operator and value
            package, operator, version, _, condition_operator, condition_value = match.groups()
            current_package = package.strip().split("[")[0]
            requirements[current_package] = [operator.strip(), version.strip()] if version else None

            i += 1
            isFirstComment = True
            # there could be a few comments followed <pkg>==<ver>, specifying who requires this pkg
            while i < len(lines) and lines[i].strip().startswith("#"):
                line = lines[i].strip()
                if isFirstComment:
                    isFirstComment = False
                    dependency = line.removeprefix("# via").strip()  # get rid of the '# via', the rest is the package name
                else:
                    dependency = line.removeprefix("#").strip()

                if '-r' in dependency:  # Replace with shorthand
                    dependency = "direct_req"

                if dependency is None or dependency == "":
                    pass
                elif current_package in dependencies:
                    dependencies[current_package].append(dependency)
                else:
                    dependencies[current_package] = [dependency]
                i += 1
        else:
            i += 1

    # add the keys and values of the dependencies dict with version
    versioned_dependencies = {}
    for pkg, deps in dependencies.items():
        pkg_key = f"{pkg}=={requirements[pkg][1]}"
        versioned_dependencies[pkg_key] = [pkg_key]
        for dep in deps:
            if dep in requirements:
                if dep is None or requirements.get(dep) is None:
                    print(dep)
                dep_key = f"{dep}=={requirements[dep][1]}"
                versioned_dependencies[pkg_key].append(dep_key)
            else:
                versioned_dependencies[pkg_key].append(dep)
    if direct:
        requirements = {}
        for pkg in versioned_dependencies.keys():
            if "direct_req" in versioned_dependencies[pkg]:
                requirements[pkg.split("==")[0]] = ['==', pkg.split("==")[1]]
    return requirements, versioned_dependencies


def normalize_pkg(pkg: str) -> str:
    return pkg.lower().replace("_", "-")


def handle_sets(obj):
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: handle_sets(v) for k, v in obj.items()}
    return obj


def start_worker(options={}):
    optstr = ",".join(["%s=%s" % (k, v) for k, v in options.items()])
    os.chdir(ol_dir)
    cmd = ['./ol', 'worker', 'up', '-d']
    if optstr:
        cmd.extend(['-o', optstr])
    print(cmd)
    out = subprocess.check_output(cmd)
    print(str(out, 'utf-8'))

    match = re.search(r"PID: (\d+)", str(out, 'utf-8'))
    if match:
        pid = match.group(1)
        print(f"The PID is {pid}")
        if "features.warmup" in options and options['features.warmup'] == "true":
            time.sleep(10)  # wait for worker to warm up
        return pid
    else:
        print("No PID found in the text.")
        return -1


def kill_worker(pid, options={}):
    os.chdir(ol_dir)
    try:
        cmd = ['./ol', 'worker', 'down']
        out = subprocess.check_output(cmd)
        print(str(out, 'utf-8'))
    except Exception as e:
        print(e)
        print("force kill")

        print(f"Killing process {pid} on port 5000")
        subprocess.run(['kill', '-9', pid])

        cmd = ['./ol', 'worker', 'force-cleanup']
        subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        process = subprocess.Popen(['./ol', 'worker', 'up'])
        os.kill(process.pid, signal.SIGINT)

        cmd = ['./ol', 'worker', 'force-cleanup']
        subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# it is an implementation of interface on OL platform
def get_memory_usage():
    get_total_pss("/sys/fs/cgroup/default-ol-sandboxes")

def get_pss(pid):
    pss = 0
    smaps_file = f"/proc/{pid}/smaps_rollup"
    if os.path.exists(smaps_file):
        with open(smaps_file, "r") as f:
            for line in f:
                if "Pss:" in line:
                    pss_value = int(line.split()[1])
                    pss += pss_value
    return pss

def get_total_pss(base):
    total_pss = 0
    for cg_folder in glob.glob(os.path.join(base, "cg-*")):
        procs_file = os.path.join(cg_folder, "cgroup.procs")

        if os.path.exists(procs_file):
            with open(procs_file, "r") as f:
                pids = f.readlines()
            for pid in pids:
                pid = pid.strip()
                total_pss += get_pss(pid)
    return total_pss


def remove_dirs_with_pattern(path, pattern):
    for dir_name in os.listdir(path):
        if re.match(pattern, dir_name):
            dir_path = os.path.join(path, dir_name)
            if os.path.isdir(dir_path):
                shutil.rmtree(dir_path)


def clean_registry():
    registry_dir = os.path.join(ol_dir, "default-ol", "registry")

    for item in os.listdir(registry_dir):
        item_path = os.path.join(registry_dir, item)

        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)


def construct_dependency_matrix(dependencies):
    all_packages = list(set(dependencies.keys()).union(*dependencies.values()))

    matrix = pd.DataFrame(0, index=all_packages, columns=all_packages)
    for pkg, deps in dependencies.items():
        for dep in deps:
            matrix.loc[pkg, dep] = 1

    return matrix


def get_package_dependencies(matrix):
    """Get dependencies for each package based on the matrix."""
    package_deps = {}
    for pkg in matrix.columns:
        package_deps[pkg] = get_recursive_dependencies(pkg, matrix)
    return package_deps

def get_recursive_dependencies(pkg, matrix):
    visited = set()
    all_deps = []
    queue = deque([pkg])

    while queue:
        current_pkg = queue.popleft()

        if current_pkg in visited:
            continue

        visited.add(current_pkg)

        immediate_deps = matrix.index[matrix[current_pkg] == 1].tolist()
        all_deps.extend(immediate_deps)

        for dep in immediate_deps:
            if dep not in visited:
                queue.append(dep)

    return list(set(all_deps))  # Removing duplicates



def compressed_size(pkg_name, version):
    url = f"https://pypi.org/pypi/{pkg_name}/{version}/json"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Invalid JSON received.")
        print(url)
        return None

    whl_size = None
    linux_whl_size = None
    tar_gz_size = None
    for release in data["urls"]:
        if release["packagetype"] == "bdist_wheel":
            if "linux" in release["filename"]:
                linux_whl_size = release["size"]
            else:
                whl_size = release["size"]
        elif release["packagetype"] == "sdist":
            tar_gz_size = release["size"]

    if linux_whl_size is not None:
        # the extreme case is about 10 times larger than the compressed size, 7 is a reasonable estimate
        return linux_whl_size, "whl"
    elif whl_size is not None:
        return whl_size, "whl"
    elif tar_gz_size is not None:
        # the max compressed size is about 1/2 of the uncompressed size
        return tar_gz_size, "tar.gz"


def get_top_modules(path):
    return [name for _, name, _ in pkgutil.iter_modules([path])]
