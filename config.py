import os

REQ_COUNT = 500
TREE_SIZES = [1, 20, 40, 80, 200, 300]
# TREE_SIZES = [1,10,20,30,40,80,120]
TRIALS = 5
TASKS = 5

# remove pip and setuptools from the list of packages, these 2 packages are not used in the serverless functions
# (no one will use serverless functions for packaging)
# but, the latest version of pip and setuptools will still be installed by default

direct = [
    'pandas', 'scipy', 'matplotlib', 'sqlalchemy',
    'django', 'flask', 'numpy', 'simplejson', 'protobuf', 'jinja2',
    'requests', 'mock', 'werkzeug', 'dnspython', 'six', 'pyqt5'
    # "numpy"
]
ol_dir = "/root/open-lambda/"
worker_out = os.path.join(ol_dir, "default-ol", "worker.out")
experiment_dir = "/root/open-lambda/paper-tree-cache/analysis/16/"
WORKER_OUT_DIR = "/root/open-lambda/paper-tree-cache/analysis/16/worker_out/"
TRACE_PATH = experiment_dir + "dep-trace.json"
COSTS_PATH = experiment_dir + "costs.json"
