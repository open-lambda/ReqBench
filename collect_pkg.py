import glob
import json
import subprocess
import sys

import pandas as pd

from config import *
from util import parse_requirements
from workload import generate_workloads_from_txts

# 1. install netifaces could cause error in openlambda
# 2. self-defined packages are not considered
blacklist = ["https://", "http://"]


def get_top_n_packages(filtered_df, n=500):
    packages_appear_times = {}

    for col in filtered_df["compiled"]:
        requirements, _ = parse_requirements(col)
        if any([x in requirements.keys() for x in blacklist]):
            continue
        for pkg_name, op_version in requirements.items():
            # ignore the extra options, it does nothing except installing some extra packages
            # those extra packages will be eventually specified in the requirements.txt
            pkg_name = pkg_name.split("[")[0]
            version = op_version[1]
            key = f"{pkg_name}=={version}"
            packages_appear_times[key] = packages_appear_times.get(key, 0) + 1

    print(f"there are {len(packages_appear_times)} unique packages in total")
    if n == -1:
        return _, packages_appear_times

    sorted_packages = sorted(packages_appear_times.items(), key=lambda x: x[1], reverse=True)
    top_n_packages = sorted_packages[:n]
    return dict(top_n_packages), packages_appear_times


# Usage: python3 collect_pkg.py <requirements.csv> -l <#packages>
#
# Step 1. pick the packages that are not in the top 500 (except those in the blacklist)
# Step 2. install and import them in a docker container, measure on-disk size, import time, memory
if __name__ == '__main__':
    if len(sys.argv) != 4 or sys.argv[2] != "-l":
        print("Usage: python3 collect_pkg.py <requirements.csv> -l <packages>")
        sys.exit()
    requirements_csv = sys.argv[1]
    pkg_num = int(sys.argv[3])
    try:
        requirements_df = pd.read_csv(requirements_csv)
    except:
        print("Error: requirements.csv not found")
        sys.exit()
    filtered_df = requirements_df[(requirements_df['compiled'] != "") &
                                  (requirements_df['compiled'].notnull())]

    wl = generate_workloads_from_txts(filtered_df["compiled"].tolist())
    with open(os.path.join(bench_file_dir, "deps.json"), 'w') as file:
        deps_dict, _, _ = wl.parse_deps()
        json.dump(deps_dict, file, indent=2)

    pkgs, _ = get_top_n_packages(filtered_df, pkg_num)

    for pkg in pkgs:
        name = pkg.split("==")[0]
        version = pkg.split("==")[1]
        deps = deps_dict[name][version]
        pkgs[pkg] = deps

    pattern = "top_[0-9]*_pkgs.json"
    files = glob.glob(os.path.join(bench_file_dir, pattern))
    for f in files:
        os.remove(f)
    with open(os.path.join(bench_file_dir, f"top_{pkg_num}_pkgs.json"), 'w') as file:
        json.dump(pkgs, file, indent=2)
    print(f"collected top {min(pkg_num, len(pkgs))} packages")

    subprocess.run("docker build -t install_import .", shell=True, cwd=bench_dir)
    subprocess.run(f"docker run "
                   f"-v {bench_dir}/tmp/.cache:/tmp/.cache "
                   f"-v {bench_file_dir}:/files install_import", shell=True)
