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
bench_dir = os.path.join("/root", "ReqBench")
bench_file_dir = os.path.join("/root", "ReqBench", "files")

