import os.path
from config import *
from workload import *
from platform_adapter.openlambda import ol

ol = ol.OL()
workload = Workload(platform=ol,
                    workload_path=os.path.join(bench_file_dir, 'workload.json'))
wl,_ = workload.random_split(0.5)
wl.add_metrics(["latency"])
wl.play(
    {
        "kill_options": {
            "save_metrics": True,
            "csv_name": "metrics.csv",
        },
    }
)

