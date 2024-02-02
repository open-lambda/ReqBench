package main

import (
	"rb" // TODO: Use GitHub URL when rb is public for direct import access.
	"rb/platform_adapter"
	"rb/platform_adapter/mock" // TODO: I think it would be better have all PlatformAdapter implementations in one package. 
							   // That way we do not need to import different package for each platform	
)

func main() {
	var m platform_adapter.PlatformAdapter = &mock.MockPlatform{}
	
	rb, err := rb.NewReqBench(m, "../files/workloads.json")
	if err != nil {
		panic(err)
	}

	tasks, timeout, totalTime := 5, 2, 10

	rb.StartWorker(nil) //start 
	rb.DeployFuncs()
	rb.Play(tasks, timeout, totalTime, nil)
	rb.KillWorker(nil)
}
