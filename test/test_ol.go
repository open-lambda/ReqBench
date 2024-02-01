package main

import (
	"rb"
	"rb/platform_adapter"
	"rb/platform_adapter/openlambda"
)

func main() {
	var ol_platform platform_adapter.PlatformAdapter = &openlambda.OpenLambda{}
	
	rb, err := rb.NewReqBench(ol_platform, "../files/workloads.json")
	if err != nil {
		panic(err)
	}

	//tasks, timeout, totalTime := 2, 2, 10

	err = rb.StartWorker(nil)
	if err != nil {
		panic(err)
	}

	defer rb.KillWorker(nil)

	err = rb.DeployFuncs()
	if err != nil {
		panic(err)
	}

	tasks, timeout, totalTime := 2, 2, 10
	_, err = rb.Play(tasks, timeout, totalTime, nil)
	if err != nil {
		panic(err)
	}
}
