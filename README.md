## General instruction 
how to use ReqBench:
1. pull the data from google pulbic dataset
2. pip-compile requirements.txt
3. generate the trees 
4. generate the to-be-tested workload
5. run it

## About Dataset
The `requirements.csv` file provided in this repository contains `requirements.txt` files retrieved from the BigQuery [GitHub Repository public dataset](https://console.cloud.google.com/marketplace/product/github/github-repos). The public dataset was last modified on Nov 26, 2022 and was retrieved on Sep 12, 2023. 

From the public dataset, we selected all repositories that were last updated after April 21, 2022 (Ubuntu 22.04 release date) and contained a requirements.txt file. This dataset comprises 9,678 unique requirements.txt files. The raw requirements have been pip-compiled using `compile.go` with Python 3.10 on Sep 21, 2023.

## Run pip-compile
To use `compile.go`, the input `requirements.csv` should have at least two columns: `id` and `raw` (representing the `requirements.in` file you want to compile). The script will sequentially run pip-compile using each row in the `raw` column. You can enable multi-threading by adjusting the `NUM_THREAD` constant. 

`compile.go` generates two output files: `output.csv` and `failed.csv`. If the pip-compile process succeeds, the result will be stored in the `compiled` column and written to the `output.csv` file. In the event of a pip-compile failure, the `compiled` column in `output.csv` will remain blank, and the corresponding row will be written to the `failed.csv` file.

## Generate Workload
`requirements.csv` is a file that contains a bunch of `requirements.txt`.

Make sure `requirements.csv` existed in `ReqBench/files` folder. Then run
```sh
python3 workload.py
```
It will pick out the most commonly used 500 hundred packages in the `requirements.csv`,
and filter out the requirements.csv which only use those packages.
In the end, it will generate a file called `workload_with_top_mods.json`, which can be used as the workload for `find_cost.py`.

There are other output files, `deps.json` contains the dependency info of packages parsed from `requirements.csv`.
`package.json` is a file that contains the package name, version, top-level modules.

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
