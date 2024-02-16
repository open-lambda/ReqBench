package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"rb/platform_adapter"
	"rb/platform_adapter/openlambda"
	"rb/util"
	"rb/workload"
	"strconv"
	"sync"
	"time"
)

type RunOptions struct {
	PlatformType string
	Workload     *workload.Workload
	StartOptions map[string]interface{}
	KillOptions  map[string]interface{}
	ConfigPath   string
	Tasks        int
	Timeout      int
	TotalTime    int
}

var seen = make(map[string]int)
var seenLock = &sync.Mutex{}

func getCurrTime() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func getId(name string) string {
	seenLock.Lock()
	defer seenLock.Unlock()
	if _, ok := seen[name]; !ok {
		seen["id"] = 0
	}
	seen[name] += 1
	return name + "_" + strconv.Itoa(seen[name])
}

func deployFuncs(funcs []workload.Function, platform platform_adapter.PlatformAdapter) error {
	err := platform.DeployFuncs(funcs)
	return err
}

func task(platform platform_adapter.PlatformAdapter, timeout int, reqQ chan workload.Call, errQ chan error) {
	for {
		select {
		case req, ok := <-reqQ:
			if !ok {
				return
			}

			done := make(chan error, 1)
			go func() {
				options := make(map[string]interface{})
				options["invokeId"] = getId(req.Name)
				options["req"] = getCurrTime()
				err := platform.InvokeFunc(req.Name, timeout, options)
				done <- err
			}()

			select {
			case err := <-done:
				if err != nil {
					errQ <- fmt.Errorf("failed to invoke function %s: %s", req.Name, err)
				} else {
					errQ <- nil
				}
			}
		}
	}
}

func run(calls []workload.Call, tasks int, platform platform_adapter.PlatformAdapter, timeout int, totalTime int) (map[string]interface{}, error) {
	/*	workload: the workload to run
		num_tasks: the number of tasks to run
		platform: the platform to run on
		timeout: the timeout for each task
		total_time: the total time to run the workload
		returns: the number of tasks that were run
	*/
	reqQ := make(chan workload.Call, 64)
	errQ := make(chan error)
	for i := 0; i < tasks; i++ {
		go task(platform, timeout, reqQ, errQ)
	}
	t0 := time.Now()

	callIdx := 0
	waiting := 0
	fails := 0
	successes := 0
	start := time.Now()
	for {
		elapsed := time.Since(start).Seconds()
		if elapsed > float64(totalTime) || ((successes+fails+waiting) == len(calls) && totalTime <= 0) {
			for waiting > 0 {
				err := <-errQ
				if err != nil {
					fails += 1
				} else {
					successes += 1
				}
				waiting -= 1
			}
			break
		}

		select {
		case reqQ <- workload.Call{Name: calls[callIdx].Name}:
			waiting += 1
			callIdx = (callIdx + 1) % len(calls)
		case err := <-errQ:
			if err != nil {
				fails += 1
			} else {
				successes += 1
			}
			waiting -= 1
		}
	}
	close(reqQ)

	t1 := time.Now()

	seconds := t1.Sub(t0).Seconds()
	throughput := float64(len(calls)) / seconds

	return map[string]interface{}{
		"throughput": throughput,
		"seconds":    seconds,
	}, nil
}

func readWorkload(path string) (wl workload.Workload, err error) {
	file, err := os.Open(path)
	if err != nil {
		return wl, err
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return wl, err
	}
	err = json.Unmarshal(bytes, &wl)
	if err != nil {
		return wl, err
	}
	return wl, nil
}

func newPlatformAdapter(platformType string) platform_adapter.PlatformAdapter {
	switch platformType {
	case "openlambda":
		return openlambda.NewOpenLambda()
	case "docker":
		return nil
	case "awslambda":
		return nil
	default:
		return nil
	}
}

// AutoRun start worker, deploy functions, run workload, kill worker
func AutoRun(opts RunOptions) (map[string]interface{}, error) {
	platform := newPlatformAdapter(opts.PlatformType)
	platform.LoadConfig(opts.ConfigPath)

	if err := deployFuncs(opts.Workload.Funcs, platform); err != nil {
		log.Fatalf("failed to deploy functions: %v", err)
	}

	err := platform.StartWorker(opts.StartOptions)
	if err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	runStats, err := run(opts.Workload.Calls, opts.Tasks, platform, opts.Timeout, opts.TotalTime)
	if err != nil {
		platform.KillWorker(nil)
		log.Fatalf("failed to run workload: %v", err)
	}

	err = platform.KillWorker(opts.KillOptions)
	if err != nil {
		log.Fatalf("failed to kill worker: %v", err)
	}
	runStats = util.Union(runStats, platform.GetStats())

	return runStats, nil
}
