package openlambda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	platform "platform_adapter_go"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type LatencyRecord struct {
	Name             string   `json:"name"`
	SplitGen         int      `json:"split_gen"`
	Req              float64  `json:"req"`
	Received         float64  `json:"received"`
	StartCreate      float64  `json:"start_create"`
	EndCreate        float64  `json:"end_create"`
	StartPullHandler float64  `json:"start_pullHandler"`
	EndPullHandler   float64  `json:"end_pullHandler"`
	Unpause          float64  `json:"unpause"`
	StartImport      float64  `json:"start_import"`
	EndImport        float64  `json:"end_import"`
	StartExecute     float64  `json:"start_execute"`
	EndExecute       float64  `json:"end_execute"`
	ZygoteMiss       int      `json:"zygote_miss"`
	SbID             string   `json:"sb_id"`
	Failed           []string `json:"failed"`
}

func (record *LatencyRecord) toSlice() []string {
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

func (record *LatencyRecord) getHeaders() []string {
	return []string{"name", "split_gen", "req", "received", "start_create", "end_create", "start_pullHandler", "end_pullHandler",
		"unpause", "start_import", "end_import", "start_execute", "end_execute",
		"zygote_miss", "sb_id", "failed"}
}

func (record *LatencyRecord) parseJSON(jsonData []byte) error {
	//println(string(jsonData))
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

type OpenLambda struct {
	platform.BasePlatformAdapter
	PID            int
	olDir          string
	runUrl         string
	collectLatency bool
	latencyRecords []LatencyRecord
	LatenciesMutex *sync.Mutex

	currentDir string
}

func (o *OpenLambda) StartWorker(options map[string]interface{}) error {
	tmpFilePath := o.currentDir + "/tmp.csv"
	if _, err := os.Stat(tmpFilePath); err == nil {
		os.Remove(tmpFilePath)
	}

	var optParts []string
	for k, v := range options {
		optParts = append(optParts, k+"="+v.(string))
	}
	optstr := strings.Join(optParts, ",")

	cgName := "ol"
	cgroupPath := "/sys/fs/cgroup/" + cgName
	if _, err := os.Stat(cgroupPath); err == nil {
		os.Remove(cgroupPath)
	}
	os.MkdirAll(cgroupPath, 0755)

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

	// todo: warmup and profiling

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

func (o *OpenLambda) KillWorker(options map[string]interface{}) error {
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
		return nil
	} else {
		return nil
	}
}

func (o *OpenLambda) DeployFunc(f platform.Function) error {
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
		panic(err)
	}
	if err := ioutil.WriteFile(requirementsInPath, []byte(meta.RequirementsIn), 0777); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(requirementsTxtPath, []byte(meta.RequirementsTxt), 0777); err != nil {
		panic(err)
	}

	return nil
}

func (o *OpenLambda) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	// invoke function
	url := o.runUrl + funcName
	var resp *http.Response
	var err error

	jsonData, err := json.Marshal(options)
	if err != nil {
		log.Fatalf("failed to marshal latency dict: %v", err)
		return err
	}
	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	resp, err = client.Post(url, "text/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("failed to post to %s: %v", url, err)
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response body: %v", err)
		return err
	}
	resp.Body.Close()

	if o.collectLatency {
		var record LatencyRecord
		err = record.parseJSON(body)
		if err != nil {
			log.Fatalf("failed to parse latency record: %v", err)
			return err
		}
		o.LatenciesMutex.Lock()
		o.latencyRecords = append(o.latencyRecords, record)
		o.LatenciesMutex.Unlock()
	}
	return nil
}

func NewOpenLambda() *OpenLambda {
	return &OpenLambda{}
}
