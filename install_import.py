import json
import pkgutil
import subprocess
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import requests
import glob

from util import compressed_size

measure_ms = """
import time, sys, importlib, os
import json

dep_pkg = {dep_pkgs}
for pkg in dep_pkg:
    sys.path.insert(0, os.path.join("/tmp/packages", pkg))

os.environ['OPENBLAS_NUM_THREADS'] = '2'

def is_module_imported(module_name):
    return module_name in sys.modules

try:
    dep_mods = {dep_mods}
    for mod in dep_mods:
        importlib.import_module(mod) 
    t0 = time.time()
    mods = {mods}
    for mod in mods:
        importlib.import_module(mod) 
    t1 = time.time()
    print(json.dumps({{'ms': (t1 - t0) * 1000}}))
except Exception as e:
    print(json.dumps({{'error': str(e)}}))
"""

measure_mb = """
import tracemalloc, gc, sys, importlib, os
import json

dep_pkg = {dep_pkgs}
for pkg in dep_pkg:
    sys.path.insert(0, os.path.join("/tmp/packages", pkg))

os.environ['OPENBLAS_NUM_THREADS'] = '2'

try:
    dep_mods = {dep_mods}
    for mod in dep_mods:
        importlib.import_module(mod) 
    gc.collect()
    tracemalloc.start()
    mods = {mods}
    for mod in mods:
        importlib.import_module(mod)  
    mb = (tracemalloc.get_traced_memory()[0] -  tracemalloc.get_tracemalloc_memory()) / 1024 / 1024
    print(json.dumps({{'mb': mb}}))
except Exception as e:
    print(json.dumps({{'error': str(e)}}))
"""

installed_packages = []
installed_packages_lock = threading.Lock()
MAX_DISK_SPACE = 5120 * 1024 * 1024  # 5GB

packages_size = {}
packages_lock = threading.Lock()

top_mods = {}
top_mods_lock = threading.Lock()


def fetch_package_size(pkg_name, version):
    if "github.com" in pkg_name:
        return 0  # cannot estimate self-defined package's size from pypi
    with packages_lock:
        if packages_size.get(pkg_name) is not None and packages_size.get(pkg_name).get(version) is not None:
            return pkg_name, version, packages_size[pkg_name][version]

    size, type = compressed_size(pkg_name, version)
    if size is None:
        print(f"cannot find size for {pkg_name} {version} in pypi")
        return None
    if type == "whl":
        uncomp_size = size * 7
    elif type == "tar.gz":
        uncomp_size = size * 3

    with packages_lock:
        if pkg_name not in packages_size:
            packages_size[pkg_name] = {}
        packages_size[pkg_name][version] = uncomp_size
    return uncomp_size, size


def get_top_modules(path):
    return [name for _, name, _ in pkgutil.iter_modules([path])]


def install_package(pkg, install_dir):
    with installed_packages_lock:
        if pkg in installed_packages:
            return
    name = pkg.split("==")[0]
    version = pkg.split("==")[1]
    uncomp_size, comp_size = fetch_package_size(name, version)

    try:
        install_dir = os.path.join(install_dir, pkg)

        # download, then install. by doing this, we could eliminate the time of downloading affected by network
        t0 = time.time()
        subprocess.check_output(
            ['pip3', 'download', '--no-deps', pkg, '--dest', '/tmp/.cache'],
            stderr=subprocess.STDOUT
        )
        t1 = time.time()
        subprocess.check_output(
            ['pip3', 'install', '--no-deps', pkg, '--cache-dir', '/tmp/.cache', '-t', install_dir],
            stderr=subprocess.STDOUT
        )
        t2 = time.time()

        pkg_disk_size = get_folder_size(install_dir)
        with top_mods_lock:
            if name not in top_mods:
                top_mods[name] = {}
            if version not in top_mods[name]:
                top_mods[name][version] = {}

            top_mods[name][version]["install_time"] = t2 - t1
            top_mods[name][version]["compressed_size"] = comp_size
            top_mods[name][version]["disk_size"] = pkg_disk_size
            top_mods[name][version]["top"] = get_top_modules(install_dir)

        with installed_packages_lock:
            installed_packages.append(pkg)
    except subprocess.CalledProcessError as e:
        print(f"Error installing {pkg}: {e.output.decode()}")


def get_folder_size(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size
def parse_output(metric, stdout, stderr):
    if stderr:
        print("Error:", stderr.decode())
    else:
        result = json.loads(stdout.decode())
        if 'error' in result:
            print("Script error:", result['error'])
            return None
        else:
            return result[metric]
def get_most_freq_deps(deps):
    return max(deps, key=deps.get)

def measure_import(pkg, pkgs_and_deps):
    pkg_name, version = pkg.split("==")[0], pkg.split("==")[1]
    mods = top_mods[pkg_name][version]["top"]
    dep_mods = []

    most_freq_deps = get_most_freq_deps(pkgs_and_deps[pkg])

    measure_mb_script = measure_mb.format(dep_pkgs=json.dumps(most_freq_deps.split(",")),
                                            dep_mods="[]",
                                            pkg=json.dumps(pkg_name+"=="+version),
                                            mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_mb_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    mem = parse_output('mb', stdout, stderr)
    if mem is None:
        print("Error measuring memory for", pkg)

    measure_ms_script = measure_ms.format(dep_pkgs=json.dumps(most_freq_deps.split(",")),
                                          dep_mods="[]",
                                          pkg=json.dumps(pkg_name+"=="+version),
                                          mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_ms_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    t = parse_output('ms', stdout, stderr)
    if t is None:
        print("Error measuring time for", pkg)

    return t, mem

# install first, then measure the import top-level modules time/memory
def main(pkgs_and_deps):
    global stop_remove

    install_dir = '/tmp/packages'
    futures = []

    # install packages one by one
    # although concurrently install could be faster, the installing time won't be accurate
    with ThreadPoolExecutor(max_workers=1) as executor:
        for pkg in pkgs_and_deps:
            future = executor.submit(install_package, pkg, install_dir)
            futures.append(future)

            # also install the most frequent dependency
            deps = get_most_freq_deps(pkgs_and_deps[pkg])
            for dep in deps.split(","):
                future = executor.submit(install_package, dep, install_dir)
                futures.append(future)

    for future in futures:
        future.result()

    # import top-level modules one by one
    for pkg in pkgs_and_deps:
        name, version = pkg.split("==")[0], pkg.split("==")[1]
        t, mem = measure_import(pkg, pkgs_and_deps)
        with top_mods_lock:
            top_mods[name][version]["time_ms"] = t
            top_mods[name][version]["mem_mb"] = mem

    with open("/files/install_import.json", "w") as f:
        json.dump(top_mods, f, indent=2)

    shutil.rmtree(install_dir, ignore_errors=True)


if __name__ == "__main__":
    if not os.path.exists("/tmp/packages"):
        os.mkdir("/tmp/packages")

    pattern = "/files/top_[0-9]*_pkgs.json"
    files = glob.glob(pattern)
    for file_name in files:
        with open(file_name, "r") as f:
            pkgs_and_deps = json.load(f)
            main(pkgs_and_deps)