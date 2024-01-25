package platform

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"platform_adapter_go/openlambda"
	"strconv"
	"sync"
	"time"
)

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

func deployFuncs(funcs []Function, platform PlatformAdapter) error {
	var wg sync.WaitGroup
	for _, fn := range funcs {
		wg.Add(1)
		go func(fn Function) {
			defer wg.Done()
			_ = platform.DeployFunc(fn)
		}(fn)
	}
	wg.Wait()

	return nil
}

func task(platform PlatformAdapter, timeout int, reqQ chan Call, errQ chan error) {
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

func run(calls []Call, tasks int, platform PlatformAdapter, timeout int, totalTime int) (map[string]interface{}, error) {
	/*	workload: the workload to run
		num_tasks: the number of tasks to run
		platform: the platform to run on
		timeout: the timeout for each task
		total_time: the total time to run the workload
		returns: the number of tasks that were run
	*/
	reqQ := make(chan Call, 64)
	errQ := make(chan error)
	for i := 0; i < tasks; i++ {
		go task(platform, timeout, reqQ, errQ)
	}
	t0 := time.Now()
	for _, call := range calls {
		select {
		case reqQ <- Call{Name: call.Name}:
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

func readWorkload(path string) (wl Workload, err error) {
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

func newPlatformAdapter(platformType string) PlatformAdapter {
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
func AutoRun(platformType string, wl *Workload,
	startOptions map[string]interface{}, killOptions map[string]interface{},
	configPath string, tasks int, timeout int, totalTime int) (int, error) {
	// platform
	platform := newPlatformAdapter(platformType)
	// config
	platform.LoadConfig(configPath)

	// deploy functions
	deployFuncs(wl.Funcs, platform)

	platform.StartWorker(startOptions)
	run(wl.Calls, tasks, platform, timeout, totalTime)
	platform.KillWorker(killOptions)
	return nil
}

func main(argc int, argv []string) {
	// platform
	platformType := argv[1]
	platform := newPlatformAdapter(platformType)
	// workload
	wl, err := readWorkload(argv[2])
	if err != nil {
		panic(err)
	}
	// config
	platform.LoadConfig(argv[3])
	// # tasks
	tasks, err := strconv.Atoi(argv[4])
	if err != nil {
		panic(err)
	}
	// timeout
	timeout, err := strconv.Atoi(argv[5])
	if err != nil {
		panic(err)
	}
	// total_time
	totalTime, err := strconv.Atoi(argv[6])
	if err != nil {
		panic(err)
	}

	// deploy functions
	deployFuncs(wl.Funcs, platform)

	platform.StartWorker(nil)
	run(wl.Calls, tasks, platform, timeout, totalTime)
	platform.KillWorker(nil)
}
