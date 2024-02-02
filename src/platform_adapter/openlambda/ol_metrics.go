package openlambda

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"rb/platform_adapter"
	"rb/workload"
	"rb/util"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"bytes"
	"reflect"
	"encoding/json"
	"log"
)

type LatencyRecord struct {
	Name             string   `csv:"name"`
	SplitGen         int      `csv:"split_gen"`
	Req              float64  `csv:"req"`
	Received         float64  `csv:"received"`
	StartCreate      float64  `csv:"start_create"`
	EndCreate        float64  `csv:"end_create"`
	StartPullHandler float64  `csv:"start_pullHandler"`
	EndPullHandler   float64  `csv:"end_pullHandler"`
	Unpause          float64  `csv:"unpause"`
	StartImport      float64  `csv:"start_import"`
	EndImport        float64  `csv:"end_import"`
	StartExecute     float64  `csv:"start_execute"`
	EndExecute       float64  `csv:"end_execute"`
	ZygoteMiss       int      `csv:"zygote_miss"`
	SbID             string   `csv:"sb_id"`
	Failed           []string `csv:"failed"`
}

func (record LatencyRecord) ToSlice() []string {
	failedStr := "[]"
	if len(record.Failed) > 0 {
		failedStr = fmt.Sprintf("[%s]", strings.Join(record.Failed, ","))
	}
	return []string{
		record.Name,
		strconv.Itoa(record.SplitGen),
		fmt.Sprintf("%.3f", record.Req),
		fmt.Sprintf("%.3f", record.Received),
		fmt.Sprintf("%.3f", record.StartCreate),
		fmt.Sprintf("%.3f", record.EndCreate),
		fmt.Sprintf("%.3f", record.StartPullHandler),
		fmt.Sprintf("%.3f", record.EndPullHandler),
		fmt.Sprintf("%.3f", record.Unpause),
		fmt.Sprintf("%.3f", record.StartImport),
		fmt.Sprintf("%.3f", record.EndImport),
		fmt.Sprintf("%.3f", record.StartExecute),
		fmt.Sprintf("%.3f", record.EndExecute),
		strconv.Itoa(record.ZygoteMiss),
		record.SbID,
		failedStr,
	}
}

func (record *LatencyRecord) parseJSON(jsonData []byte) error {
	println(string(jsonData))
	err := json.Unmarshal(jsonData, &record)
	if err != nil {
		return err
	}

	// Use reflection to ensure fields() and fieldByName() are available
	v := reflect.ValueOf(record).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		// Only check for nil values for pointers, slices, and maps
		if field.Kind() == reflect.Ptr || field.Kind() == reflect.Slice || field.Kind() == reflect.Map {
			if field.IsNil() {
				switch field.Kind() {
				case reflect.Int:
					field.SetInt(0)
				case reflect.Slice:
					field.Set(reflect.MakeSlice(field.Type(), 0, 0))
				}
			}
		}
	}
	return nil
}

type OpenLambdaMetrics struct {
	platform_adapter.BasePlatformAdapter
	PID            		int
	
	olDir          		string
	olUrl         		string
	csvPath				string
	latencyDf			*util.DataFrame
}

func (o *OpenLambdaMetrics) StartWorker(options map[string]interface{}) error {
	fmt.Println("Starting OL")
	
	//load config
	o.LoadConfig("/root/ReqBench/src/platform_adapter/openlambda/config.json") //TODO: other ways to avoid hard-coding?
	o.olDir = o.Config["ol_dir"].(string)
	o.olUrl = o.Config["ol_url"].(string)
	o.csvPath = o.Config["csv_path"].(string)

	// //create temp file
	// tmpFilePath := o.currentDir + "/tmp.csv"
	// if _, err := os.Stat(tmpFilePath); err == nil {
	// 	os.Remove(tmpFilePath)
	// }

	//create df
	var r LatencyRecord
	o.latencyDf = util.NewDataFrame(r)

	//combine start option in to a string
	var optParts []string
	for k, v := range options {
		optParts = append(optParts, k+"="+v.(string))
	}
	optstr := strings.Join(optParts, ",")
	

	//create cgroup
	cgName := "ol"
	cgroupPath := "/sys/fs/cgroup/" + cgName
	if _, err := os.Stat(cgroupPath); err == nil {
		os.Remove(cgroupPath)
	}
	os.MkdirAll(cgroupPath, 0755)

	//start ol
	cmdArgs := []string{"cgexec", "-g", "memory,cpu:" + cgName, "./ol", "worker", "up", "-d"}
	if optstr != "" {
		cmdArgs = append(cmdArgs, "-o", optstr)
	}
	cmd := exec.Command("sudo", cmdArgs...)
	cmd.Dir = o.olDir
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to start worker: %v", err)
	}

	output := string(out)

	// todo: warmup and profiling(maybe move the start to the config.json)
	// if warmup, ok := options["features.warmup"].(bool); ok && warmup {
	// 	o.warmupMemory = getTotalMem(o.Config["cg_dir"].(string), "CG")
	// 	o.warmupTime = extractWarmupTime(output)
	// }
	// if profileLock, ok := options["profile_lock"].(bool); ok && profileLock {

	// }

	re := regexp.MustCompile(`PID: (\d+)`)
	match := re.FindStringSubmatch(output)
	if len(match) > 1 {
		pid, err := strconv.Atoi(match[1])
		if err != nil {

		}
		o.PID = pid
		fmt.Println("The OL PID is", pid)
		return nil
	} else {
		return fmt.Errorf("failed to parse PID from output: %s", output)
	}
}

func (o *OpenLambdaMetrics) KillWorker(options map[string]interface{}) error {
	fmt.Println("Killing OL")
	// kill worker
	if o.PID == 0 {
		fmt.Println("PID has not been set")
		return fmt.Errorf("PID has not been set")
	}

	cmd := exec.Command("./ol", "worker", "down")
	cmd.Dir = o.olDir
	_, err := cmd.Output()
	if err != nil {
		fmt.Println(err)
		fmt.Println("force kill")

		fmt.Printf("Killing process %d on port 5000\n", o.PID)
		killCmd := exec.Command("kill", "-9", fmt.Sprint(o.PID))
		killCmd.Dir = o.olDir
		killCmd.Run()

		cleanupCmd := exec.Command("./ol", "worker", "force-cleanup")
		cleanupCmd.Dir = o.olDir
		cleanupCmd.Stdout = nil
		cleanupCmd.Stderr = nil
		cleanupCmd.Run()

		upCmd := exec.Command("./ol", "worker", "up")
		upCmd.Dir = o.olDir
		if err := upCmd.Start(); err != nil {
			fmt.Println("Failed to start the command:", err)
		} else {
			// Send SIGINT to the process
			if err := upCmd.Process.Signal(syscall.SIGINT); err != nil {
				fmt.Println("Failed to send SIGINT:", err)
			}
		}

		cleanupCmd2 := exec.Command("./ol", "worker", "force-cleanup")
		cleanupCmd2.Dir = o.olDir
		cleanupCmd2.Stdout = nil
		cleanupCmd2.Stderr = nil
		cleanupCmd2.Run()
		fmt.Printf("force kill done\n")

		//fmt.Printf("%v\n", o.latencyDf)
		return nil
	} else {
		return nil
	}
}

func (o *OpenLambdaMetrics) DeployFuncs(funcs []workload.Function) error {
	fmt.Println("Deploying functions")
	deployChan := make(chan workload.Function, 64)
	errChan := make(chan error)
	for i := 0; i < 8; i++ {
		go o.DeployFunction(deployChan, errChan)
	}
	for _, f := range funcs {
		select {
		case deployChan <- f:
		case err := <-errChan:
			return err
		}
	}
	close(deployChan)
	close(errChan)
	return nil
}

func (o *OpenLambdaMetrics) DeployFunction(deployTask chan workload.Function, errChan chan error) {
	for {
		f, ok := <-deployTask
		if !ok {
			return
		}
		// write code to registry dir
		meta := f.Meta
		path := fmt.Sprintf(o.olDir+"/default-ol/registry/%s", f.Name)
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

		_lines := f.Code
		var lines []string
		for _, line := range _lines {
			lines = append(lines, line)
		}
		code := strings.Join(lines, "\n")

		funcPath := filepath.Join(path, "f.py")
		requirementsInPath := filepath.Join(path, "requirements.in")
		requirementsTxtPath := filepath.Join(path, "requirements.txt")

		if err := ioutil.WriteFile(funcPath, []byte(code), 0777); err != nil {
			errChan <- err
		}
		if err := ioutil.WriteFile(requirementsInPath, []byte(meta.RequirementsIn), 0777); err != nil {
			errChan <- err
		}
		if err := ioutil.WriteFile(requirementsTxtPath, []byte(meta.RequirementsTxt), 0777); err != nil {
			errChan <- err
		}
	}
}

func (o *OpenLambdaMetrics) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	// invoke function
	url := "http://" + o.olUrl+ "/run/" + funcName
	var resp *http.Response
	var err error

	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	resp, err = client.Post(url, "text/json", bytes.NewBuffer([]byte("null")))
	if err != nil {
		return fmt.Errorf("failed to post to %s: %v", url, err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}
	resp.Body.Close()

	var record LatencyRecord
	err = record.parseJSON(body)
	if err != nil {
		log.Fatalf("failed to parse latency record: %v", err)
		return err
	}
	
	fmt.Println(record)
	
	return nil
}

func extractWarmupTime(out string) float64 {
	logFilePathRegex := regexp.MustCompile(`Log File: (.+\.out)`)
	matches := logFilePathRegex.FindStringSubmatch(out)
	var logFilePath string
	if len(matches) > 1 {
		logFilePath = matches[1]
	}

	if logFilePath != "" {
		fileContent, err := ioutil.ReadFile(logFilePath)
		if err != nil {
			fmt.Printf("Error reading file: %s\n", err)
			return 0
		}

		warmupTimeRegex := regexp.MustCompile(`warmup time is (\d+(\.\d+)?) ms`)
		warmupMatches := warmupTimeRegex.FindStringSubmatch(string(fileContent))
		if len(warmupMatches) > 1 {
			var warmupTime float64
			fmt.Sscanf(warmupMatches[1], "%f", &warmupTime)
			return warmupTime
		}
	}
	return 0
}

func getTotalMem(cg_dir string, memType string) int {
	return 0
}