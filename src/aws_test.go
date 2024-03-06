package main


import (
	"fmt"
	"sort"
	"testing"
)

func TestAWS(t *testing.T) {
	wl, err := readWorkload("/root/ReqBench/workloads_5.json")
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
	tasks := 1
	timeout := 2
	totalTime := 1

	startOptions := map[string]interface{}{
		"packages": pkgList,
	}

	opts := RunOptions{
		PlatformType: "aws",
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
