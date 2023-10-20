package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var idMutex = sync.Mutex{}
var seen = map[string]int{}

type Call struct {
	name string
}

func getCurrTime() float64 {
	return float64(time.Now().UnixNano()) / float64(time.Millisecond)
}

func (c Call) getID() string {
	idMutex.Lock()
	defer idMutex.Unlock()
	if _, ok := seen[c.name]; !ok {
		seen[c.name] = 1
	}
	id := strconv.Itoa(seen[c.name])
	seen[c.name]++
	return c.name + "_" + id
}

func task(task int, reqQ chan Call, errQ chan error, latency bool) {
	for {
		call, ok := <-reqQ
		if !ok {
			errQ <- nil
			return
		}

		url := "http://localhost:5000/run/" + call.name
		var resp *http.Response
		var err error
		reqBody := map[string]interface{}{}
		if latency {
			reqBody["name"] = call.getID()
			reqBody["req"] = getCurrTime()
			jsonData, err := json.Marshal(reqBody)
			if err != nil {
				log.Fatalf("failed to marshal latency dict: %v", err)
				return
			}
			resp, err = http.Post(url, "text/json", bytes.NewBuffer(jsonData))
		} else {
			resp, err = http.Post(url, "text/json", bytes.NewBuffer([]byte("null")))
		}

		if err != nil {
			errQ <- fmt.Errorf("failed req to %s: %v", url, err)
			return
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		// if latency is on, then expect a dict like:
		// Name        string `json:"name"`
		// Req 	   	   int64  `json:"req"`
		//
		// StartCreate int64  `json:"start-create"`
		// EndCreate   int64  `json:"end-create"`
		//
		// StartImport int64  `json:"start-import"`
		// EndImport   int64  `json:"end-import"`
		// EndExecute  int64  `json:"end-execute"`

		if err != nil {
			errQ <- fmt.Errorf("failed to %s, could not read body: %v", url, err)
			return
		} else {
			if latency {
				latencyDict := map[string]interface{}{}
				//println(string(body))
				if err := json.Unmarshal(body, &latencyDict); err != nil {
					log.Printf("failed to unmarshal latency dict: %v", err)
					log.Fatalf("problematic body: %s", string(body))
					return
				}
				latencyDict["received"] = getCurrTime()
				latencyBytes, _ := json.Marshal(latencyDict)
				http.Post("http://localhost:4998/latency", "text/json", bytes.NewBuffer(latencyBytes))
			}

			bodyStr := string(body)
			if !latency && bodyStr[1:len(bodyStr)-1] != call.name { // remove the quotes
				errQ <- fmt.Errorf("resp body does not match: %s != %s", body, call.name)
			}
		}

		if resp.StatusCode != http.StatusOK {
			errQ <- fmt.Errorf("failed req to %s: status %d, text '%s'", url, resp.StatusCode, string(body))
			return
		}
	}
}

func deployFuncs(path string) (map[string]interface{}, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	var workload map[string]interface{}
	if err := json.Unmarshal(raw, &workload); err != nil {
		panic(err)
	}

	funcs := workload["funcs"].([]interface{})
	// write code to registry dir
	for _, _fn := range funcs {
		fn := _fn.(map[string]interface{})
		meta := fn["meta"].(map[string]interface{})
		path := fmt.Sprintf("/root/open-lambda/default-ol/registry/%s", fn["name"].(string))
		if os.IsExist(os.MkdirAll(path, 0777)) {
			err := os.RemoveAll(path)
			if err != nil {
				panic(err)
			}
			err = os.MkdirAll(path, 0777)
			if err != nil {
				panic(err)
			}
		}

		_lines := fn["code"].([]interface{})
		lines := []string{}
		for _, line := range _lines {
			lines = append(lines, line.(string))
		}
		code := strings.Join(lines, "\n")

		funcPath := filepath.Join(path, "f.py")
		requirementsInPath := filepath.Join(path, "requirements.in")
		requirementsTxtPath := filepath.Join(path, "requirements.txt")

		if err := ioutil.WriteFile(funcPath, []byte(code), 0777); err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile(requirementsInPath, []byte(meta["requirements_in"].(string)), 0777); err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile(requirementsTxtPath, []byte(meta["requirements_txt"].(string)), 0777); err != nil {
			panic(err)
		}
	}
	return workload, nil
}

func run(path string, tasks int, latency bool) error {
	workload, _ := deployFuncs(path)

	calls := workload["calls"].([]interface{})

	// todo: to measure a func being called multiple times, add name to each call, also a Req timestamp

	reqQ := make(chan Call, 8)
	errQ := make(chan error)
	for i := 0; i < tasks; i++ {
		go task(i, reqQ, errQ, latency)
	}

	t0 := time.Now()
	for _, ucall := range calls {
		call := ucall.(map[string]interface{})
		select {
		case reqQ <- Call{name: call["name"].(string)}:
		case err := <-errQ:
			panic(err)
		}
	}
	close(reqQ)

	for i := 0; i < tasks; i++ {
		if err := <-errQ; err != nil {
			panic(err)
		}
	}
	t1 := time.Now()

	seconds := t1.Sub(t0).Seconds()
	// fmt.Printf("call count: %d\n", len(calls))
	fmt.Printf("{\"seconds\": %.3f, \"ops/s\": %.3f}", seconds, float64(len(calls))/seconds)

	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: go run bench.go <workload-path.json> <tasks> <latency bool>")
		return
	}
	path := os.Args[1]
	tasks, err := strconv.Atoi(os.Args[2])
	measureLatency, err := strconv.ParseBool(os.Args[3])

	if err != nil {
		panic(err)
	}

	err = run(path, tasks, measureLatency)
	if err != nil {
		return
	}
}
