from workload import Workload
from platform_adapter.Docker.docker_platform import Dockerplatform

TASKS = 5
docker = Dockerplatform("platform_adapter/Docker/config.json")
wl = Workload(docker, "files/workloads.json")
pkg_dict = wl.pkg_with_version
pkg_list = [f"{pkg}=={ver}" for pkg, vers in pkg_dict.items() for ver in vers]
sorted_pkg_list = sorted(pkg_list)
wl.gen_trace(2000, skew=False)
call_set = {call["name"] for call in wl.calls}

# add_metrics will modify the workload functions code to collect data
# also, you could add_metrics to workload, but not collect it in 'wl.play'
# because there might be multiple appraches, e.g. CloudWatch in AWS, to collect data

wl.add_metrics(["latency", "memory"])
wl.play(
    options={
        "start_options": {
            "kill_containers": True,
            "network": None,
            "packages": sorted_pkg_list,
            "unique_containers": len(call_set)
        },
    }
)
