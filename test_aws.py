from workload import Workload
from platform_adapter.aws.aws import AWS

TASKS = 5
workload_path = "workload_with_top_mods.json"
wl = Workload(AWS(), workload_path)

wl.play(
    tasks=TASKS,
    options={"workload_path": workload_path}
)
