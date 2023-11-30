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

from util import compressed_size, normalize_pkg, find_compressed_files

install_dir = "/packages"

measure_ms = """
import time, sys, importlib, os
import json

to_remove = []
for path in sys.path:
    if "packages" in path:
        to_remove.append(path)
sys.path = [path for path in sys.path if path not in to_remove]

dep_pkg = {dep_pkgs}
for pkg in dep_pkg:
    sys.path.insert(0, os.path.join("/packages", pkg))

os.environ['OPENBLAS_NUM_THREADS'] = '2'

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

to_remove = []
for path in sys.path:
    if "packages" in path:
        to_remove.append(path)
sys.path = [path for path in sys.path if path not in to_remove]

dep_pkg = {dep_pkgs}
for pkg in dep_pkg:
    sys.path.insert(0, os.path.join("/packages", pkg))

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

install_failed = {}
import_failed = {}

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


def get_suffix(name, version):
    files = find_compressed_files("/tmp/.cache/", f"{normalize_pkg(name)}-{version}*")
    if files:
        return os.path.splitext(files[0])[1]
    else:
        return None  # should not happen

def get_top_modules(path):
    return [name for _, name, _ in pkgutil.iter_modules([path])]

# it can match most of the compressed file, but not all.
# e.g. zope-event==5.0's compressed file is zope.event-5.0...('-' is replaced by '.')
# simply ignore this case, as this func is used to save time, not for accuracy
def downloaded_packages(name, version):
    files = find_compressed_files("/tmp/.cache/", f"{normalize_pkg(name)}-{version}*")
    return len(files) > 0

def install_package(pkg, install_dir):
    with installed_packages_lock:
        if pkg in installed_packages:
            return
    name = pkg.split("==")[0]
    version = pkg.split("==")[1]
    try:
        install_dir = os.path.join(install_dir, pkg)
        # download, then install. by doing this, we could eliminate the time of downloading affected by network
        t0 = time.time()
        if not downloaded_packages(name, version):
            print(f"downloading {pkg}")
            subprocess.check_output(
                ['pip3', 'download', '--no-deps', pkg, '--dest', '/tmp/.cache'],
                stderr=subprocess.STDOUT
            )
        comp_file = find_compressed_files("/tmp/.cache/", f"{normalize_pkg(name)}-{version}*")[0]
        comp_size = os.path.getsize(comp_file)
        comp_size = 0
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

            top_mods[name][version]["install_time"] = (t2 - t1)*1000
            top_mods[name][version]["compressed_size"] = comp_size
            top_mods[name][version]["disk_size"] = pkg_disk_size
            top_mods[name][version]["top"] = get_top_modules(install_dir)
            top_mods[name][version]["suffix"] = ".tar"#get_suffix(name, version)
        with installed_packages_lock:
            installed_packages.append(pkg)
            if len(installed_packages) % 10 == 0:
                print(f"installed {len(installed_packages)} packages")
    except Exception as e:
        install_failed[pkg] = str(e)
        print(f"Error installing {pkg}: {e}")


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
        return None, stderr.decode()
    else:
        result = json.loads(stdout.decode())
        if 'error' in result:
            return None, result['error']
        else:
            return result[metric], None


def get_most_freq_deps(deps):
    return max(deps, key=deps.get)


def measure_import(pkg, pkgs_and_deps):
    pkg_name, version = pkg.split("==")[0], pkg.split("==")[1]
    mods = top_mods[pkg_name][version]["top"]

    most_freq_deps = get_most_freq_deps(pkgs_and_deps[pkg])
    most_freq_deps = most_freq_deps.split(",")
    deps_mods = []
    for dep in most_freq_deps:
        if dep == pkg:
            continue
        deps_mods += top_mods[dep.split("==")[0]][dep.split("==")[1]]["top"]
    # i-mb
    measure_imb_script = measure_mb.format(dep_pkgs=json.dumps(most_freq_deps),
                                          dep_mods="[]",
                                          pkg=json.dumps(pkg_name + "==" + version),
                                          mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_imb_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    i_mem, err = parse_output('mb', stdout, stderr)
    if i_mem is None:
        i_mem = 0
    # i-ms
    measure_ms_script = measure_ms.format(dep_pkgs=json.dumps(most_freq_deps),
                                          dep_mods="[]",
                                          pkg=json.dumps(pkg_name + "==" + version),
                                          mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_ms_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    i_t, err = parse_output('ms', stdout, stderr)
    if i_t is None:
        i_t = 0

    # mb
    measure_mb_script = measure_mb.format(dep_pkgs=json.dumps(most_freq_deps),
                                          dep_mods=json.dumps(deps_mods),
                                          pkg=json.dumps(pkg_name + "==" + version),
                                          mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_mb_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    mem, err = parse_output('mb', stdout, stderr)
    if mem is None:
        mem = 0
    # ms
    measure_ms_script = measure_ms.format(dep_pkgs=json.dumps(most_freq_deps),
                                          dep_mods=json.dumps(deps_mods),
                                          pkg=json.dumps(pkg_name + "==" + version),
                                          mods=mods)
    process = subprocess.Popen(['python3', '-c', measure_ms_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    t, err = parse_output('ms', stdout, stderr)
    if t is None:
        print("Error measuring time and memory for", pkg)
        import_failed[pkg] = err
        t = 0

    return [i_t, t], [i_mem, mem]


# install first, then measure the import top-level modules time/memory
def main(pkgs_and_deps):
    install_dir = '/packages'

    # install packages one by one
    # although concurrently install could be faster, the installing time won't be accurate
    for pkg in pkgs_and_deps:
        install_package(pkg, install_dir)
        # also install the most frequent dependency
        deps = get_most_freq_deps(pkgs_and_deps[pkg])
        # for their deps, only install, don't measure
        for dep in deps.split(","):
            install_package(dep, install_dir)

    # import top-level modules one by one
    cnt = 0
    for pkg in pkgs_and_deps:
        name, version = pkg.split("==")[0], pkg.split("==")[1]
        ts, mems = measure_import(pkg, pkgs_and_deps)
        cnt += 1
        if cnt % 10 == 0:
            print(f"imported {cnt} packages")
        with top_mods_lock:
            top_mods[name][version]["i-ms"] = ts[0]
            top_mods[name][version]["i-mb"] = max(mems[0], 0)
            top_mods[name][version]["ms"] = max(ts[1], 0)
            top_mods[name][version]["mb"] = max(mems[1], 0)

    # if a pkg==ver is not measured, delete them as they only serve as dependencies
    keys_to_delete = set()
    for name in top_mods:
        versions_to_delete = set()
        for version in top_mods[name]:
            if "ms" not in top_mods[name][version]:
                versions_to_delete.add(version)
        for version in versions_to_delete:
            del top_mods[name][version]
        if len(top_mods[name]) == 0:
            keys_to_delete.add(name)
    for name in keys_to_delete:
        del top_mods[name]

    print(f"the number of install failed: {len(install_failed)}, names: {install_failed.keys()}")
    print(f"the number of import failed: {len(import_failed)}, names: {import_failed.keys()}")
    with open("/files/install_import.json", "w") as f:
        json.dump(top_mods, f, indent=2)
    if len(install_failed) > 0:
        with open("/files/install_failed.json", "w") as f:
            json.dump(install_failed, f, indent=2)
    if len(import_failed) > 0:
        with open("/files/import_failed.json", "w") as f:
            json.dump(import_failed, f, indent=2)
    shutil.rmtree(install_dir, ignore_errors=True)


if __name__ == "__main__":
    if not os.path.exists("/packages"):
        os.mkdir("/packages")

    pattern = "/files/top_[0-9]*_pkgs.json"
    files = glob.glob(pattern)
    for file_name in files:
        with open(file_name, "r") as f:
            pkgs_and_deps = json.load(f)
            main(pkgs_and_deps)
