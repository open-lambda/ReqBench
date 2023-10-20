import copy
import json
import re

import pandas as pd

from config import *
from version import Package
from workload import Workload

costs = None

# it is a simpler Meta, only used to keep consistent with ol code
class Meta:
    def __init__(self, mods=None, pkgs=None):
        self.mods = mods
        self.pkgs = pkgs


class Node:
    # packages:
    def __init__(self, parent, packages, split_generation, indirect_packages):
        self.parent = parent
        # we only import one package in each node, thus packages should be a set containing only one element
        self.packages = packages
        # 'indirect_packages' means all the ancestors' pkgs
        self.indirect_packages = indirect_packages

        self.tops = set()
        for pkg in self.packages:
            self.tops |= Package.get_top_mods(pkg.split("==")[0], pkg.split("==")[1])

        self.meta = Meta(self.tops, self.packages)

        self.split_generation = split_generation
        self.children = []
        self.zygoteCreated = False
        self.count = 0  # record how many times the zygote is used

    def __str__(self):
        return f"Node: {self.split_generation}"

    @staticmethod
    def from_json(path):
        with open(path) as f:
            data = json.load(f)
        root = Node.construct_node(data, None)
        return root

    @staticmethod
    def construct_node(child_dict, parent):
        packages = set(child_dict["packages"])
        split_generation = child_dict["split_generation"]
        children = child_dict["children"]

        indirect_packages = set()
        if parent is not None:
            indirect_packages.update(parent.packages)
            indirect_packages |= parent.indirect_packages
        node = Node(parent, packages, split_generation, indirect_packages)
        for child in children:
            node.children.append(Node.construct_node(child, node))
        return node

    # req is a set of pkgs
    def import_cost(self, reqs):
        for pkg in self.indirect_packages | self.packages:
            if pkg in reqs:
                reqs.remove(pkg)

        import_cost = 0
        for pkg in reqs:
            import_cost += costs[pkg]["ms"]
        return import_cost

    def serve_requests_cost(self, reqs):
        return self.import_cost(reqs)

    def bootstrap_cost(self, meta):
        reqs = copy.deepcopy(meta.pkgs)
        return self.import_cost(reqs)

    def fork_cost(self):
        return 0.7

    def to_dict(self):
        return {"packages": [p for p in self.packages],
                "self.count": self.count,
                "split_generation": self.split_generation,
                "children": [node.to_dict() for node in self.children]
                }


class Tree:
    def __init__(self, root):
        self.root = root

    def lookup(self, reqs, node):
        for pkg in node.packages:
            if pkg in reqs:
                continue
            else:
                return 100000, None  # return a large cost, so that this node will not be selected

        for child in node.children:
            min_cost, best_node = self.lookup(reqs, child)
            if best_node is not None:
                return min_cost, best_node

        min_cost = node.import_cost(reqs)
        return min_cost, node

    # lookup2 does a traversal of the tree, and find the best node to serve the request
    # req is a set of packages, we calculate the cost of importing all the top modules
    # def:
    # input: required packages reqs, a given zygote tree
    # output: the best node n, and its cost to import all the top modules of package p, where p is in reqs.
    def lookup2(self, reqs, node):
        # extend the reqs to its dependencies
        req_with_deps = copy.deepcopy(reqs)
        # if the node contains one of the package p not in reqs, return a large cost
        for pkg in node.deps:
            if pkg in reqs:
                continue
            else:
                return 100000, None

        min_cost = node.import_cost(reqs)
        best_node = node

        for child in node.children:
            cost, candidate_node = self.lookup2(req_with_deps, child)
            if cost < min_cost:
                min_cost = cost
                best_node = candidate_node
        return min_cost, best_node

    # this function corresponds to the cost of (linst *LambdaInstance) Task()
    # LambdaInstance-ServeRequests
    def get_task_cost(self, meta=Meta(), lookup_mode=0):
        # the following part is corresponding to `(cache *ImportCache) Create`
        best_node = self.root
        reqs = meta.pkgs
        if lookup_mode == 0:
            cost, best_node = self.lookup(reqs, self.root)
        elif lookup_mode == 1:
            cost, best_node = self.lookup2(reqs, self.root)
        create_leaf_cost = self.create_child_cost(best_node, meta)

        # the following part is corresponding to `LambdaInstance-ServeRequests`
        serve_requests_cost = best_node.serve_requests_cost(reqs)
        if cost != serve_requests_cost:
            print("cost not equal")
            print(best_node)
        # create_leaf_cost = 0
        return create_leaf_cost + serve_requests_cost

    # todo: this function corresponds to the cost of importCache.Create
    # this function corresponds to the cost of importCache.createChildSandboxFromNode,
    # createChildSandboxFromNode can do following things:
    # gets zygote(because the zygote might not be created) and creates leaf sandbox(do a fork)
    # or gets zygote(because the zygote might not be created) and creates another zygote based on parent
    # new zygote = get zygote + fork + import meta
    def create_child_cost(self, node, meta):
        # get_zygote_cost + fork_cost = createChildSandboxFromNode = Create
        get_zygote_cost = self.get_zygote_cost(node)
        create_cost = get_create_cost(node, meta)
        return get_zygote_cost + create_cost

    # this function corresponds to cost of importCache.getSandboxInNode
    def get_zygote_cost(self, node):
        if node.zygoteCreated:
            return 0
        else:
            return self.create_zygote_cost(node)

    # this function corresponds to cost of importCache.createSandboxInNode
    # importCache.createSandboxInNode = pip-install + createChildSandboxFromNode or SOCKPool.Create
    # pip-install is equal in each workload, so we only measure the cost of createChildSandboxFromNode/SOCKPool.Create
    def create_zygote_cost(self, node):
        # ignore the pip-install cost
        # node.meta is created in createSandboxInNode in ol, but I create that when initializing the node
        if node.parent is not None:
            # todo: probably need to change meta
            meta = node.meta
            create_zygote_cost = self.create_child_cost(node.parent, meta)
            node.zygoteCreated = True
            return create_zygote_cost
        else:
            # todo: measure the cost of create root (now assume negligible)
            create_root_cost = get_create_cost(None, node.meta)
            node.zygoteCreated = True
            return create_root_cost

    def json(self, path):
        with open(path, 'w') as f:
            json.dump(self.root.to_dict(), f, indent=2)


# this function corresponds to cost of SOCKPool.Create function
# SOCKPool.Create does 3 things: populate rootfs, generate bootstrap.py(both negligible),
# and told its parent to fork the new process, new process will run bootstrap.py (do import)
# it's like creating a house(rootfs), then invite people(process) to live in
# the bootstrap.py is the most heavy part
def get_create_cost(parent_node, meta):
    if parent_node is not None:
        # the following 2 lines are corresponding to the cost of sock.fork
        fork_cost = parent_node.fork_cost()
        bootstrap_cost = parent_node.bootstrap_cost(meta)
        return fork_cost + bootstrap_cost
    else:
        freshProc_cost = 0  # todo: measure the cost of freshProc
        return freshProc_cost


def estimate_cost(workload, tree_path):
    tree = Tree(Node.from_json(tree_path))
    tree_name = os.path.splitext(os.path.basename(tree_path))[0]
    cost = 0
    for call in workload.calls:
        call_name = call['name']
        func_meta = workload.find_func(call_name).meta
        pkg_with_version = [pkg_name + "==" + func_meta.pkg_with_version[pkg_name][1]
                            for pkg_name in func_meta.pkg_with_version]
        pkg_with_version = set(pkg_with_version)
        v, best_node = tree.lookup(pkg_with_version, tree.root)
        cost += v
    print(f"Zygote tree:{tree_name}, LambdaInstance-ServeRequests estimate time: {cost}ms")
    return cost


# run all trees in the trial dir
def benchmark(workload):
    columns = ['nodes', 'trial']
    df = pd.DataFrame(columns=columns)
    os.chdir(experiment_dir)
    for subdir in os.listdir('trials'):
        subdir_path = os.path.join('trials', subdir)
        if os.path.isdir(subdir_path):
            for tree_file in os.listdir(subdir_path):
                if tree_file.endswith('.json'):
                    match = re.match(r"tree-v(\d+).node-(\d+).json", tree_file)
                    if match:
                        tree_path = os.path.join(subdir_path, tree_file)
                        try:
                            cost = estimate_cost(workload, tree_path)
                        except Exception as e:
                            print(f"error in {tree_path}, {e}")
                            continue

                        v_num = int(match.group(1))
                        nodes = int(match.group(2))
                        if f"v{v_num}" not in df.columns:
                            df[f"v{v_num}"] = None

                        row_index = df[(df['nodes'] == nodes) & (df['trial'] == subdir)].index
                        if row_index.empty:
                            new_row = {'nodes': nodes, 'trial': subdir, f"v{v_num}": cost}
                            df = df._append(new_row, ignore_index=True)
                        else:
                            df.at[row_index[0], f"v{v_num}"] = cost
    df = df.sort_values(by=['trial', 'nodes'])
    other_cols = sorted([col for col in df.columns if col not in ['nodes', 'trial']])
    cols = ['nodes'] + other_cols + ['trial']
    df = df[cols]
    return df


def get_to_hit_ratio(df):
    result_df = pd.DataFrame()

    for trial in df['trial'].unique():
        trial_df = df[df['trial'] == trial].copy()

        base_values = trial_df[trial_df['nodes'] == 1].drop(columns=['nodes', 'trial']).iloc[0]

        for col in base_values.index:
            trial_df[col] = (base_values[col] - trial_df[col]) / base_values[col]

        result_df = pd.concat([result_df, trial_df])
    return result_df


if __name__ == "__main__":
    # todo: for now, we choose to import all the top-level modules
    # however, for some packages, e.g. pyqt5, import top-level modules does nothing,
    # we need to import sub-modules to use it
    # we simply ignore this problem for now and set meta to be all indirect and direct required packages
    Package.from_json(path=os.path.join(experiment_dir, "packages_tops_costs.json"))
    costs_dict = Package.cost_dict()
    costs = costs_dict

    w1 = Workload(os.path.join(experiment_dir, "workload_with_top_mods.json"))
    w1, w2 = w1.random_split(0.5)

    res = benchmark(w1)
    hit_ratio = get_to_hit_ratio(res)

    res.to_csv(os.path.join(experiment_dir, "estimate_perf.csv"), index=False)
    hit_ratio.to_csv(os.path.join(experiment_dir, "estimate_hit_ratio.csv"), index=False)
