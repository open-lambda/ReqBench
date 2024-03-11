package main

import (
	"fmt"
	"rb/util"
	"sort"
	"testing"
)

func TestDockerPlatform(t *testing.T) {
	wl, err := util.ReadWorkload("/root/ReqBench/filtered_workloads.json")
	wl.GenerateTrace(len(wl.Funcs), false, nil, 0)
	wl.AddMetrics([]string{"latency"})
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
	totalTime := 0
	// docker need a package list to build the image
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

func TestOlPlatform(t *testing.T) {
	wl, err := util.ReadWorkload("/root/ReqBench/filtered_workloads.json")
	wl.GenerateTrace(100, false, nil, 0)
	wl.AddMetrics([]string{"latency"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	config := "./platform_adapter/openlambda/config.json"
	tasks := 5
	timeout := 30
	totalTime := 0

	opts := RunOptions{
		PlatformType: "openlambda",
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: nil,
		KillOptions:  nil,
	}
	_, err = AutoRun(opts)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}
