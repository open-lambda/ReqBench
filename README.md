## General instruction 
how to use ReqBench:
1. pull repo and generate requirements.txt from google (a placeholder for KJ)
2. generate the trees 
3. generate the to-be-tested workload
4. run it

## Generate Workload
`requirements.csv` is a file that contains a bunch of `requirements.txt`.

Make sure `requirements.csv` existed in current folder. Then run
```sh
python3 workload.py
```
It will pick out the most commonly used 500 hundred packages in the `requirements.csv`,
and filter out the requirements.csv which only use those packages.
In the end, it will generate a file called `workload_with_top_mods.json`, which can be used as the workload for `find_cost.py`.

Also, it will generate a file called `deps.json`, which contains the dependency info of packages parsed from `requirements.csv`.
generate a file called `package.json` is a file that contains the package name, version, top-level modules.

## Fetch Package weight
After generating the workload, we need to fetch more package info, i.e. each packages' top-level modules import cost.
To do so, run
```sh
python3 deps_and_costs.py <deps_json> <package_json> 
```
`<deps_json>` is the dependency info of packages parsed from `requirements.csv`.
`<package_json>` is a json file that contains the package name, version, top-level modules, and costs.

Of course, if you use files generated by `workload.py`, just remove the arguments.

It will generate a file called `packages_tops_costs.json` is a file that contains the package name, version, top-level modules, and costs.

## Run ReqBench
**dry-run**:

put the trees in the `trials/<num>` folder. `<num>` is the number of iterations.
run the `find_cost.py` script to estimate the costs. `find_cost.py` will go through all the trees in the trials folder,
parse the name of tree in the format of `tree-v<#ver>-node-<#nodes>.json`, and generate a file called `results.csv` indicating the throughput of each tree.

`<#ver>` is the version of the tree, different version corresponds to different tree generating rules, 
and `<#nodes>` is the number of nodes in the tree.

```sh
python3 ReqBench.py <package_top_costs_json> <workload.csv>
```

`<package_top_costs_json>` is a json file that contains the package name, version, top-level modules, and costs.
`<workload.csv>` is the requirements file that contains a bunch of python functions, each import a bunch of modules.

Of course, if you use `<workload.csv>` generated by `workload.py`, and use `<package_top_costs_json>` generated by `deps_and_costs.py`, just remove the arguments.

**ol-run**:

In a python file, run 
```python
from workload import Workload
tree_path = ""
mem = 600
TASKS = 5
wl = Workload("workload_with_top_mods.json")
wl.play({"import_cache_tree": tree_path, "limits.mem_mb": mem}, 
        tasks=TASKS)
```
to have a real run on OL. `tree_path` is the path to the tree, mem is the memory limit for each container,
`TASKS` is the number of maximum parallel requests. 


### TODO: 1. explain collector.go and bench.go; 2. compare our zygote with other FaaS platforms
