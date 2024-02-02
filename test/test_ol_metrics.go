package main

import (
	"rb"
	"rb/platform_adapter"
	"rb/workload"
	"rb/platform_adapter/openlambda"
)

func main() {
	var ol_platform platform_adapter.PlatformAdapter = &openlambda.OpenLambdaMetrics{}

	w1, err := workload.ReadWorkloadFromJson("../files/workloads.json")
	if err != nil {
		panic(err)
	}
	w1.AddMetrics([]string{"latency"})

	rb, err := rb.NewReqBench(ol_platform, "")
	if err != nil {
		panic(err)
	}

	rb.SetWorkload(w1)

	err = rb.StartWorker(nil)
	if err != nil {
		panic(err)
	}

	defer rb.KillWorker(nil)

	err = rb.DeployFuncs()
	if err != nil {
		panic(err)
	}

	tasks, timeout, totalTime := 1, 2, 10
	_, err = rb.Play(tasks, timeout, totalTime, nil)
	if err != nil {
		panic(err)
	}
}
