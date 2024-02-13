package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"rb/platform_adapter"
	"rb/platform_adapter/openlambda"
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
				errQ <- nil
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
	for _, call := range calls {
		select {
		case reqQ <- workload.Call{Name: call.Name}:
		case err := <-errQ:
			panic(err)
		}
	}
	close(reqQ)

	t1 := time.Now()

	seconds := t1.Sub(t0).Seconds()
	througput := float64(len(calls)) / seconds

	return map[string]interface{}{
		"throughput": througput,
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

	err := platform.StartWorker(nil)
	if err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	resMap, err := run(opts.Workload.Calls, opts.Tasks, platform, opts.Timeout, opts.TotalTime)
	if err != nil {
		platform.KillWorker(nil)
		log.Fatalf("failed to run workload: %v", err)
	}

	err = platform.KillWorker(nil)
	if err != nil {
		log.Fatalf("failed to kill worker: %v", err)
	}

	return resMap, nil
}

func main() {
	if len(os.Args) < 7 {
		panic("Not enough arguments")
	}

	tasks, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}

	timeout, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	totalTime, err := strconv.Atoi(os.Args[6])
	if err != nil {
		panic(err)
	}

	wl, err := readWorkload(os.Args[2])
	if err != nil {
		panic(err)
	}

	opts := RunOptions{
		PlatformType: os.Args[1],
		Workload:     &wl,
		ConfigPath:   os.Args[3],
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
	}

	AutoRun(opts)
}
