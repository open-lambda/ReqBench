package main

import (
	"rb"
	"rb/platform_adapter"
	"rb/platform_adapter/aws"
	"fmt"
)

func main() {
	var a = aws.AWSAdapter{
		Workload_path: "../files/workloads_5.json",
	}
	var m platform_adapter.PlatformAdapter = &a
	
	rb, err := rb.NewReqBench(m, "../files/workloads_5.json")
	if err != nil {
		panic(err)
	}

	err = rb.StartWorker(nil) //start 
	err = rb.DeployFuncs()
	tasks, timeout, totalTime := 1, 2, 1
	rb.Play(tasks, timeout, totalTime, nil)
	//rb.KillWorker(nil)
	//a.DeleteAll()
	
	if err != nil {
		fmt.Println(err)
	}
}
