import random
import threading
from subprocess import check_output
from typing import List

import requests
import numpy as np
import concurrent.futures
import json
import send_req
from util import *
from version import Package, versionMeta

packages_size = {}
packages_lock = threading.Lock()

def generate_non_measure_code_lines(modules, return_val):
    return [
        "import time, importlib, os\n",
        "os.environ['OPENBLAS_NUM_THREADS'] = '2'\n",
        f"for mod in {modules}:\n",
        "    try:\n",
        "        importlib.import_module(mod)\n",
        "    except Exception as e:\n",
        "        pass\n",
        f"def f(event):\n",
        f"    return \"{return_val}\"\n"
    ]

def generate_measure_code_lines(modules):
    return [
        "import time, importlib, os\n",
        "os.environ['OPENBLAS_NUM_THREADS'] = '2'\n",
        "t_StartImport = time.time()*1000\n",
        f"for mod in {modules}:\n",
        "    try:\n",
        "        importlib.import_module(mod)\n",
        "    except Exception as e:\n",
        "        pass\n",
        "t_EndImport = time.time()*1000\n",
        "def f(event):\n",
        "    t_EndExecute = time.time()*1000\n",
        "    event['start_import'] = t_StartImport\n",
        "    event['end_import'] = t_EndImport\n",
        "    event['end_execute'] = t_EndExecute\n",
        "    return event\n"
    ]


def get_top_n_packages(csv_path, n=500):
    df = pd.read_csv(csv_path)
    filtered_df = df[(df['compiled'] != "") & (df['compiled'].notnull())]
    packages_appear_times = {}

    valid_txt = 0

    for col in filtered_df["compiled"]:
        valid = 1
        requirements, _ = parse_requirements(col)
        for pkg_name, op_version in requirements.items():
            pkg_name = pkg_name.split("[")[0]
            version = op_version[1]
            if any([x in pkg_name for x in blacklist]):
                valid = 0
                continue
            key = f"{pkg_name}=={version}"
            packages_appear_times[key] = packages_appear_times.get(key, 0) + 1
        valid_txt += valid

    print(f"there are {len(packages_appear_times)} unique packages")
    if n == -1:
        return packages_appear_times

    sorted_packages = sorted(packages_appear_times.items(), key=lambda x: x[1], reverse=True)
    # if n=-1, return all pkgs
    top_n_packages = sorted_packages[:n]
    return dict(top_n_packages)


def generate_workloads_from_txts(txts):
    if isinstance(txts, str):
        txts = json.load(open(os.path.join(bench_file_dir, txts)))
    elif isinstance(txts, list):
        txts = txts
    wl = Workload()
    for txt in txts:
        meta_dict = {"requirements_in": txt, "requirements_txt": txt,
                     "direct_import_mods": [], "import_mods": []}
        meta = Meta.from_dict(meta_dict)
        name = wl.addFunc(meta=meta)
        wl.addCall(name)
    return wl


# we dump requirements.txt and requirements.in to json file, and reparse them to get the versioned packages
# direct_pkg_with_version: {pkg_name: (operator, version)}
# first step is to generate requirements.in and txt. unless these 2 args are provided
# then parse direct_pkg_with_version, package_with_version.
class Meta:
    # direct_pkg_with_version is a dict of {pkg_name: (operator, version)}
    def __init__(self, direct_pkg_with_version=None, pkg_with_version=None,
                 requirements_in=None, requirements_txt=None,
                 direct_import_mods=None, import_mods=None):
        self.direct_pkg_with_version = {} if direct_pkg_with_version is None else direct_pkg_with_version
        self.direct_import_mods = set() if direct_import_mods is None else set(direct_import_mods)

        if requirements_in is None:
            self.requirements_in = self.gen_requirements_in()
        else:
            self.requirements_in = requirements_in

        # when call try_gen_requirements_txt(), make sure requirements_in is not None first
        if requirements_txt is None:
            self.try_gen_requirements_txt()
        else:
            self.requirements_txt = requirements_txt
        assert self.requirements_txt is not None

        # always re-parse requirements.in because it might be changed during "try_gen_requirements_txt"
        if direct_pkg_with_version is not None:
            self.direct_pkg_with_version = direct_pkg_with_version
        else:
            self.direct_pkg_with_version, _ = parse_requirements(self.requirements_in) # todo: mark for change

        if pkg_with_version is not None:
            self.pkg_with_version = pkg_with_version
        else:
            self.pkg_with_version, _ = parse_requirements(self.requirements_txt)

        self.import_mods = set() if import_mods is None else set(import_mods)

    # return true means we can generate requirements.txt from current requirements.in
    # return false means we generate requirements.txt after throw away some versions,
    # the worst case is all versions are ignored
    def try_gen_requirements_txt(self):
        self.requirements_txt = self.gen_requirements_txt()
        if self.requirements_txt is None:
            for pkg_name in self.direct_pkg_with_version:
                self.direct_pkg_with_version[pkg_name][0] = ""
                self.direct_pkg_with_version[pkg_name][1] = ""
                self.requirements_in = self.gen_requirements_in()
                self.requirements_txt = self.gen_requirements_txt()
                if self.requirements_txt is not None:
                    break
            return False
        return True

    def gen_requirements_in(self):
        requirements_in_str = ""
        for pkg in self.direct_pkg_with_version:
            op, version = self.direct_pkg_with_version[pkg][0], self.direct_pkg_with_version[pkg][1]
            if op is None:
                requirements_in_str += f"{pkg}\n"
            else:
                requirements_in_str += f"{pkg}{op}{version}\n"
        return requirements_in_str

    # return None means we cannot generate requirements.txt from current requirements.in
    def gen_requirements_txt(self, print_err=False):
        process = subprocess.Popen(
            ["pip-compile", "--output-file=-", "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(input=self.requirements_in)
        # todo: you should probably delete this line, as it print a lot of annoyed message to console
        if process.returncode != 0:
            if print_err:
                print("err requirements in: \n", self.requirements_in)
                print("err in pip compile: \n", stderr)
            return None
            # raise Exception(f"pip-compile failed with error: {stderr}")
        return stdout

    def to_dict(self):
        return {
            "requirements_in": self.requirements_in,
            "requirements_txt": self.requirements_txt,
            "direct_import_mods": list(self.direct_import_mods),
            "import_mods": list(self.import_mods)
        }

    def __str__(self):
        return "install_pkgs: %s, import_mods: %s" % (
            self.pkg_with_version, self.import_mods)

    # todo: verify this
    @staticmethod
    def from_dict(meta_dict):
        requirement_txt = meta_dict['requirements_txt']
        requirement_in = meta_dict['requirements_in']

        direct_pkg_with_version, _ = parse_requirements(requirement_txt, direct=True)
        pkg_with_version, _ = parse_requirements(requirement_txt)
        return Meta(direct_pkg_with_version, pkg_with_version,
                    requirements_in=requirement_in, requirements_txt=requirement_txt,
                    direct_import_mods=meta_dict["direct_import_mods"],
                    import_mods=meta_dict["import_mods"])


class Func:
    # direct_pkg_with_version is a dict of {pkg_name: (operator, version)}
    # if meta_dict is not None, then other args are ignored
    def __init__(self, name, code, direct_pkg_with_version=None, direct_import_mods=None, meta=None):
        self.name = name

        if meta is not None:
            self.meta = meta
        else:
            self.meta = Meta(direct_pkg_with_version=direct_pkg_with_version,
                             direct_import_mods=direct_import_mods)
        self.code = code

    def to_dict(self):
        return {"name": self.name, "meta": self.meta.to_dict(), "code": self.code}

    @staticmethod
    def from_dict(d):
        meta = Meta.from_dict(d['meta'])
        f = Func(name=d['name'], meta=meta, code=d['code'])
        return f

    def __str__(self):
        return "name: %s, meta: %s, code: %s" % (self.name, self.meta.to_dict(), self.code)

    def lambda_to_measure(self):
        idx = 0
        while idx < len(self.code):
            line = self.code[idx]
            if line.startswith("# ol-install:"):
                self.code.insert(idx + 1, "import sys, time\n")
                idx += 1
                for mod in self.meta.import_mods:
                    self.code.insert(idx + 1, f"print('{mod} imported or not?', '{mod}' in sys.modules)\n")
                    idx += 1
                self.code.insert(idx + 1, "t1=time.time()\n")
                idx += 1
            if line.startswith("def f(event):"):
                self.code.insert(idx, "t2=time.time()\n")
                self.code.insert(idx + 1, "print('duration: %.3f' % (t2-t1))\n")
                idx += 2
            idx += 1


class Workload:
    def __init__(self, path=None):
        self.funcs = []
        self.calls = []
        self.pkg_with_version = {}  # {pkg_name: (v1, v2, ...), ...}
        self.name = 1
        if path:
            with open(path) as f:
                j = json.load(f)
                self.funcs = [Func.from_dict(d) for d in j['funcs']]
                self.calls = j['calls']
                self.name = max([int(f.name[2:]) for f in self.funcs]) + 1
                self.pkg_with_version = j['pkg_with_version']
                for pkg, versions in self.pkg_with_version.items():
                    self.pkg_with_version[pkg] = set(versions)

    # if deps' name exist in one txt, then they can serve a compatible deps
    # deps= {pkg_name: {v1: {dep:ver, dep:ver, ...}, v2: {dep:ver, dep:ver, ...}}, ...}
    def parse_deps(self):
        # deps_dict = {name: {v1:{deps_str: #used, deps_str: #used, ...}, v2: ...}
        # deps_set = {name: {v1: [dep_set, dep_set, ...], v2: ...}
        # '#used' is the number of times this deps_set is used
        deps_dict = {} # deps_dict shows frequency
        deps_set = {} # deps_set shows the deps as a set
        dep_matrix_dict = {}

        # learn from workload
        for func in self.funcs:
            _, deps_from_func = parse_requirements(func.meta.requirements_txt)
            matrix = construct_dependency_matrix(deps_from_func)
            full_deps = get_package_dependencies(matrix)

            for pkg_name, dependencies in full_deps.items():
                if pkg_name == 'direct_req':
                    continue
                if '==' in pkg_name:
                    name, version = pkg_name.split('==')
                    dependencies_str = ",".join(sorted(dependencies))
                    deps_key = dependencies_str

                    # update deps_dict
                    if name not in deps_dict:
                        deps_dict[name] = {}
                    if version not in deps_dict[name]:
                        deps_dict[name][version] = {}
                    if name in deps_dict and version in deps_dict[name]:
                        if deps_key in deps_dict[name][version]:
                            deps_dict[name][version][deps_key] += 1  # Increment the number of uses
                        else:
                            deps_dict[name][version][deps_key] = 1

                    if name not in deps_set:
                        deps_set[name] = {}
                    if version not in deps_set[name]:
                        deps_set[name][version] = []
                    if set(dependencies) not in deps_set[name][version]:
                        deps_set[name][version].append(set(dependencies))

                    for dep in dependencies:
                        if dep not in dep_matrix_dict:
                            dep_matrix_dict[dep] = {}
                        if pkg_name not in dep_matrix_dict[dep]:
                            dep_matrix_dict[dep][pkg_name] = 0
                        dep_matrix_dict[dep][pkg_name] += 1

        dep_matrix = pd.DataFrame.from_dict(dep_matrix_dict, orient='index')
        dep_matrix = dep_matrix.sort_index(axis=0).sort_index(axis=1).fillna(0).astype(int)
        return deps_dict, deps_set, dep_matrix

    # packages_with_version is {pkg1: (v1, v2, ...), ...}
    # import should be a set of strings, also accept list, but will be convert to set
    def addFunc(self, packages_with_version=None, imports=None, meta=None):
        if packages_with_version is None:
            packages_with_version = {}

        if imports is None:
            imports = set()
        if type(imports) == list:
            imports = set(imports)

        name = 'fn%d' % self.name
        self.name += 1

        code = []
        import_arr_str = json.dumps(list(imports))
        if imports:
            code = generate_non_measure_code_lines(import_arr_str, name)
        else:
            code = generate_non_measure_code_lines("[]", name)

        f = Func(name=name, code=code,
                 direct_pkg_with_version=packages_with_version,
                 direct_import_mods=imports, meta=meta)

        # add all deps versioned pkgs' to the workload's pkgs dict
        for pkg, op_version in f.meta.pkg_with_version.items():
            if pkg not in self.pkg_with_version:
                self.pkg_with_version[pkg] = set()
            self.pkg_with_version[pkg].add(op_version[1])  # only add version instead of operator

        self.funcs.append(f)
        return name

    def addCall(self, name):
        self.calls.append({"name": name})

    # actually return a df
    # todo: test if the matrix is generated correctly
    def call_matrix(self):
        df_rows = []
        for call in self.calls:
            func = self.find_func(call['name'])
            assert func is not None
            df_row = {}
            for pkg, op_version in func.meta.pkg_with_version.items():
                df_row[pkg + op_version[0] + op_version[1]] = 1
            df_rows.append(df_row)
        df = pd.DataFrame(df_rows).fillna(0).astype(int)
        return df[sorted(df.columns)]

    # get all versioned packages used in workload, return a df
    # in previous experiments, we use "deps" in trace, jus name but no version is provided
    # however, since now we came up with pip-compile, such info can be easily obtained
    """
    you will get a matrix like this:
      A B C D
    A 1 0 0 0
    B 1 1 0 0
    C 1 1 1 0
    D 1 1 1 1
    [B,A] = 1 means A requires B
    """

    def dep_matrix(self, pkg_factory: List[Package]):
        pnames = []
        for pkg in self.pkg_with_version:
            for v in self.pkg_with_version[pkg]:
                pnames.append(pkg + "==" + v)
        pnames = sorted(pnames)
        df = pd.DataFrame(index=pnames, columns=pnames).fillna(0)

        # get some deps info from our pkg_factory
        for name_op_version in df.columns:
            name = name_op_version.split("==")[0]
            version = name_op_version.split("==")[1]
            df.loc[name_op_version, name + "==" + version] = 1
            pkg = pkg_factory[name]
            # todo: this should not happen, and if it happens, we should pip-compile it
            if pkg is None:
                continue
            if version not in pkg.available_versions:
                print("Warning: %s not in %s's version" % (version, pkg))
            version_meta = pkg.available_versions[version]
            deps = version_meta.requirements_dict
            for dep_name, op_version in deps.items():
                df.loc[dep_name + "==" + op_version[1], name_op_version] = 1

        return df

    def add_metrics(self, metrics):
        if "latency" in metrics:
            for func in self.funcs:
                mods_arr = json.dumps(list(func.meta.import_mods))
                new_code = generate_measure_code_lines(mods_arr)
                func.code = new_code

    def shuffleCalls(self):
        random.shuffle(self.calls)

    # return 2 workloads, one for training, one for testing
    def random_split(self, ratio):
        wl_train = Workload()
        wl_test = Workload()
        for func in self.funcs:
            if random.random() < ratio:
                name = wl_train.addFunc(None, func.meta.import_mods, func.meta)
                wl_train.addCall(name)
            else:
                name = wl_test.addFunc(None, func.meta.import_mods, func.meta)
                wl_test.addCall(name)
        return wl_train, wl_test

    def to_dict(self):
        funcs_dict = [f.to_dict() for f in self.funcs]
        return {'funcs': funcs_dict, 'calls': self.calls, 'pkg_with_version': handle_sets(self.pkg_with_version)}

    def save(self, path, workload_dict=None):
        with open(path, 'w') as f:
            if workload_dict is not None:
                json.dump(workload_dict, f, indent=2)
            else:
                json.dump(self.to_dict(), f, indent=2)
        return

    def play(self, options={}, tasks=TASKS, collect=False):
        if collect:
            cmd = ["go", "build", "-o", "collector", "collector.go", "info.go"]
            subprocess.run(cmd, cwd=os.path.join(bench_file_dir, "collector"))
            # ugly implementation, have to build collector1 first or signals cannot be caught
            subprocess.cmd, cwd=os.path.join(bench_file_dir, "collector")
            restAPI = subprocess.Popen(
                ["./collector1", "."],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=os.path.join(bench_file_dir, "collector"))
            time.sleep(2)  # wait for collector to start

        pid = start_worker(options)
        wl_path = os.path.join(bench_file_dir, "tmp.json")
        wl_dict = self.to_dict()
        self.save(wl_path, wl_dict)

        # although bench.go is in current directory, it show be run at ol_dir
        sec, ops = send_req.run(wl_dict, tasks, collect)
        stat_dict = {"seconds": sec, "ops/s": ops}
        print(stat_dict)
        kill_worker(pid)

        if collect:
            restAPI.terminate()
            for l in restAPI.stdout:
                print(f"{str(l.strip())}")
                if b'exit' in l:
                    break

        # os.remove(wl_path)
        return stat_dict

    def find_func(self, name):
        for f in self.funcs:  # todo: use a dict could be faster
            if f.name == name:
                return f
        return None

    # traverse all the meta.direct_pkg_with_version to find the matching func
    # pkg should be {pkg_name: versioned_package}
    def find_funcs_by_pkg(self, pkg):
        pkg = {key.lower(): value for key, value in pkg.items()}
        funcs = []
        for f in self.funcs:
            meta = f.meta
            # PEP 426: All comparisons of distribution names MUST be case insensitive
            f_dir_pkgs = {key.lower(): value for key, value in meta.direct_pkg_with_version.items()}
            if meta and pkg == f_dir_pkgs:
                funcs.append(f)
        return funcs

    # assume the name is like fn1, fn2, fn3 ...
    # and the call is in the same order (no repeated call)
    def add(self, workload):
        func_name_map = {}  # map from old name to new name
        for f in workload.funcs:
            old_name = f.name
            f.name = 'fn%d' % self.name
            f.code[-1] = "    return '%s'\n" % f.name
            func_name_map[old_name] = f.name
            self.funcs.append(f)
            self.name += 1

        for c in workload.calls:
            # rename calls
            c['name'] = func_name_map[c['name']]
            self.calls.append(c)

        # add pkg_with_version
        for pkg, versions_set in workload.pkg_with_version.items():
            if pkg not in self.pkg_with_version:
                self.pkg_with_version[pkg] = set()
            self.pkg_with_version[pkg] = self.pkg_with_version[pkg].union(versions_set)

    # randomly select some functions, add them to the workload with new name
    def expand(self, target):
        if target < len(self.calls):
            return
        for i in range(target - len(self.calls)):
            func = random.choice(self.funcs)
            name = self.addFunc(None, func.meta.import_mods, func.meta)
            self.addCall(name)


    # repeat the calls in the workload
    def repeat(self, target, weights=None):
        if target < len(self.calls):
            return
        function_names = [f.name for f in self.funcs]
        if weights is None:
            random_weights = np.random.random(len(function_names))
            weights = random_weights / sum(random_weights)

        additional_calls = random.choices(function_names, weights, k=target)
        for call_name in additional_calls:
            self.addCall(call_name)

# load the deps from deps.json, parse the deps_str to a frozenset of deps
# now it looks like: {name: {version: {deps_set: count}}}, count is the number of times this deps_set appears
def load_all_deps(path):
    with open(path, 'r') as f:
        deps = json.load(f)
    new_deps = {}
    for name in deps:
        for version in deps[name]:
            for deps_str in deps[name][version]:
                if name not in new_deps:
                    new_deps[name] = {}
                if version not in new_deps[name]:
                    new_deps[name][version] = {}

                new_deps[name][version][frozenset(deps_str.split(","))] = deps[name][version][deps_str]
    return new_deps


def main():
    pkgs = json.load(open("files/install_import.json", 'r'))
    requirements_csv = os.path.join(bench_file_dir, "requirements.csv")

    # filter the valid requirements
    df = pd.read_csv(requirements_csv)
    valid_cols = []
    filtered_df = df[(df['compiled'] != "") & (df['compiled'].notnull())]
    for col in filtered_df["compiled"]:
        valid = 1
        requirements, _ = parse_requirements(col)
        for pkg_name, op_version in requirements.items():
            pkg_name = pkg_name.split("[")[0]
            version = op_version[1]
            if pkg_name not in pkgs or version not in pkgs[pkg_name]:
                valid = 0
        if valid:
            valid_cols.append(col)
    with open(os.path.join(bench_file_dir, "valid_txt.json"), 'w') as f:
        json.dump(valid_cols, f, indent=2)

    wl = generate_workloads_from_txts(os.path.join(bench_file_dir, "valid_txt.json"))
    with open(os.path.join(bench_file_dir, "deps.json"), 'w') as file:
        deps_dict, _, _ = wl.parse_deps()
        json.dump(deps_dict, file, indent=2)

    # get top mods from install_import.json
    for pkg, versions in wl.pkg_with_version.items():
        Package.add_version({pkg: versions})
        for version in versions:
            top_mods = pkgs[pkg][version]["top"]
            time_cost = pkgs[pkg][version]["time_ms"]
            mem_cost = pkgs[pkg][version]["mem_mb"]
            cost = {
                "i-ms": time_cost,
                "i-mb": mem_cost
            }

            if Package.packages_factory[pkg].available_versions[version] is None:
                Package.packages_factory[pkg].available_versions[version] = versionMeta(top_mods, None, cost)
            else:
                Package.packages_factory[pkg].available_versions[version].top_level = top_mods
    Package.save(os.path.join(bench_file_dir, "packages.json"))

    wl_with_top_mods = Workload()

    # generate functions with top mods
    for f in wl.funcs:
        for pkg in f.meta.direct_pkg_with_version:
            version = f.meta.direct_pkg_with_version[pkg][1]
            if Package.packages_factory[pkg].available_versions[version] is not None:
                f.meta.import_mods.update(Package.packages_factory[pkg].available_versions[version].top_level)
        name = wl_with_top_mods.addFunc(None, f.meta.import_mods, f.meta)
        wl_with_top_mods.addCall(name)
    wl_with_top_mods.repeat(3000)
    wl_with_top_mods.save(os.path.join(bench_file_dir, "workloads.json"))


if __name__ == '__main__':
    main()

# after workload generated, run the empty ones to install them(pkgs are about 3 GB in total),
#   then search through packages dir to find top_mods.
# after top_mods are found, measure the top pkgs importing cost, generate the tree
# then test the dataset

# deps like parcon @ git+https://github.com/javawizard/parcon not supported
