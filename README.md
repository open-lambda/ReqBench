## General instruction 
how to use ReqBench:
1. pull the data from google pulbic dataset
2. pip-compile requirements.txt
3. install and import packages in docker
4. generate the to-be-tested workload
5. run it

## About Dataset
The `requirements.csv` file provided in this repository contains `requirements.txt` files retrieved from the BigQuery [GitHub Repository public dataset](https://console.cloud.google.com/marketplace/product/github/github-repos). The public dataset was last modified on Nov 26, 2022 and was retrieved on Sep 12, 2023. 

More specifically, `requirements.csv` can be generated by running
```sql
SELECT Contents.id, Contents.content AS raw
FROM bigquery-public-data.github_repos.contents AS Contents
JOIN (
  SELECT id, repo_name
  FROM bigquery-public-data.github_repos.files
  WHERE path = 'requirements.txt' OR path LIKE '%/requirements.txt'
  ) AS Files ON Files.id = Contents.id
JOIN (
  SELECT repo_name[0] AS repo_name FROM bigquery-public-data.github_repos.commits WHERE author.date.seconds > 1650499200 GROUP BY repo_name[0]
  ) AS Repos ON Repos.repo_name = Files.repo_name
```
in google BigQuery.

From the public dataset, we selected all repositories that were last updated after April 21, 2022 (Ubuntu 22.04 release date) and contained a requirements.txt file. This dataset comprises 9,678 unique requirements.txt files. The raw requirements have been pip-compiled using `compile.go` with Python 3.10 on Sep 21, 2023.

## Run pip-compile
To use `compile.go`, the input `requirements.csv` should have at least two columns: `id` and `raw` (representing the `requirements.in` file you want to compile). The script will sequentially run pip-compile using each row in the `raw` column. You can enable multi-threading by adjusting the `NUM_THREAD` constant. 

`compile.go` generates two output files: `output.csv` and `failed.csv`. If the pip-compile process succeeds, the result will be stored in the `compiled` column and written to the `output.csv` file. In the event of a pip-compile failure, the `compiled` column in `output.csv` will remain blank, and the corresponding row will be written to the `failed.csv` file.

## Collect packages' info
In this step, we will collect more info about each package by installing them in docker.

run
```sh
python3 collect_pkg.py <requirements.csv> -l <packages>
```
`<requirements.csv>` is the requirements.csv you want to learn about, 
it should be the output of `compile.go`.
`#<packages>` of most commonly used packages will be installed in docker,
and then the info will be stored in `ReqBench/files/install_import.json`, including dependencies, install time, compressed size, on-disk size, 
top-level modules, and the time/memory cost of importing each top-level module.

## Generate Workload
Make sure `install_import.json` and `requirements.csv` existed in `ReqBench/files` folder. Then run
```sh
python3 workload.py
```
and filter out the requirements.csv which only use the packages in `install_import.json` and generate a series of handlers.

It will output a file called `workloads.json` which contains a series of functions and call trace. 
The frequency of each function is determined by a zipf distribution ($s=1.5$ by default).
`packages.json` is another output that contains the package name, version, top-level modules info (name, and the time/memory cost of importing it).

## Call handlers
Interfaces are defined in [api.go](https://github.com/open-lambda/ReqBench/blob/main/src/platform_adapter/api.go), we have provide 3 sample implementations:
[aws, Docker, OpenLambda](https://github.com/open-lambda/ReqBench/tree/main/src/platform_adapter).

We have also provided sample testers, they are [Platforms_test.go](https://github.com/open-lambda/ReqBench/blob/go-refactor/src/tests/Platforms_test.go), [lockStat_test.go](https://github.com/open-lambda/ReqBench/blob/go-refactor/src/tests/lockStat_test.go), [nsbpf_test.go](https://github.com/open-lambda/ReqBench/blob/go-refactor/src/tests/nsbpf_test.go)
