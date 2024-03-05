package main

import (
	"fmt"
	"sort"
	"testing"
)

func TestDockerPlatform(t *testing.T) {
	wl, err := readWorkload("/root/ReqBench/filtered_workloads.json")
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	pkgDict := wl.PkgWithVersion
	var pkgList []string
	for pkg, vers := range pkgDict {
		for _, ver := range vers {
			pkgList = append(pkgList, fmt.Sprintf("%s==%s", pkg, ver))
		}
	}
	sort.Strings(pkgList)

	config := "./platform_adapter/docker/config.json"
	tasks := 5
	timeout := 30
	totalTime := 10

	startOptions := map[string]interface{}{
		"packages": pkgList,
	}

	opts := RunOptions{
		PlatformType: "docker",
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: startOptions,
		KillOptions:  nil,
	}
	_, err = AutoRun(opts)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}
