package rb

import (
	"fmt"
	"rb/platform_adapter"
	"rb/workload"
	"time"
)

type ReqBench struct {
	platform platform_adapter.PlatformAdapter
	wl 		 *workload.Workload
	
}

func NewReqBench(platform platform_adapter.PlatformAdapter, workload_path string) (*ReqBench, error) {
	wl, err := workload.ReadWorkloadFromJson(workload_path)
	if err != nil {
		return nil, err
	}

	return  &ReqBench{
		platform: platform,
		wl: wl,
	}, nil
}

func (rb *ReqBench) StartWorker(options map[string]interface{}) error {
	return rb.platform.StartWorker(options)
}

func (rb *ReqBench) KillWorker(options map[string]interface{}) error {
	return rb.platform.KillWorker(options)
}

func (rb *ReqBench) DeployFuncs() error {
	return rb.platform.DeployFuncs(rb.wl.Funcs)
}


func (rb *ReqBench) Play(tasks int, timeout int, totalTime int, options map[string]interface{}) (map[string]interface{}, error) {
	reqQ := make(chan workload.Call, tasks)
	errQ := make(chan error, tasks)

	for i := 0; i < tasks; i++ {
		go task(rb.platform, timeout, reqQ, errQ, options)
	}

	errors := 0
	successes := 0
	waiting := 0
	callIdx := 0
	
	progressSnapshot := 0.0
	progressSuccess := 0
	start := time.Now()

	for {
		elapsed := time.Since(start).Seconds()

		if elapsed >= float64(totalTime) {
			break
		}

		select {
		case reqQ <- rb.wl.Calls[callIdx]:
			waiting += 1
			callIdx = (callIdx + 1) % len(rb.wl.Calls)
		case err := <- errQ:
			if err != nil {
				errors += 1
				fmt.Printf("%s\n", err.Error())
			} else {
				successes += 1
				progressSuccess += 1
			}
			waiting -= 1
		}

		if elapsed > progressSnapshot + 1 {
			fmt.Printf("throughput: %.1f/second\n", float64(progressSuccess) / (elapsed-progressSnapshot))
			progressSnapshot = elapsed
			progressSuccess = 0
		}
	}
	seconds := time.Since(start).Seconds()

	fmt.Printf("cleanup\n")
	close(reqQ)
	waiting += tasks
	for waiting > 0 {
		if err := <-errQ; err != nil {
			errors += 1
			fmt.Printf("%s\n", err.Error())
		}
		waiting -= 1
	}

	throughput := float64(successes) / seconds

	result := map[string]interface{}{
		"seconds":				seconds,
		"successes":			successes,
		"errors":				errors,
		"throughput (ops/s)":	throughput,
	}

	fmt.Printf("{\"seconds\": %.3f, \"successes\": %d, \"errors\": %d, \"throughput(ops/s)\": %.3f}\n", 
	seconds, successes, errors, throughput)

	return result, nil
}

func (rb *ReqBench) LoadWorkload(path string) error {
	wl, err := workload.ReadWorkloadFromJson(path)
	if err != nil {
		return err
	}

	rb.wl = wl
	return nil
}


func task(platform platform_adapter.PlatformAdapter, timeout int, reqQ chan workload.Call, errQ chan error, options map[string]interface{}) {
	for {
		select {
		case req, ok := <-reqQ:
			if !ok {
				errQ <- nil
				return
			}

			err := platform.InvokeFunc(req.Name, timeout, options)
			if err != nil {
				errQ <- fmt.Errorf("failed to invoke function %s: %s", req.Name, err)
			} else {
				errQ <- nil
			}
		}
	}
}