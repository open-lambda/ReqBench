import os

REQ_COUNT = 500
TRIALS = 5
TASKS = 5

# remove pip and setuptools from the list of packages, these 2 packages are not used in the serverless functions
# (no one will use serverless functions for packaging)
# but, the latest version of pip and setuptools will still be installed by default

ol_dir = "/root/open-lambda/"
worker_out = os.path.join(ol_dir, "default-ol", "worker.out")
bench_dir = "/root/ReqBench" # "/root/ReqBench"
tmp_dir = os.path.join(bench_dir, "tmp")
cache_pkgs_dir = os.path.join(bench_dir, "tmp", ".cache")
bench_file_dir = os.path.join(bench_dir, "files")
