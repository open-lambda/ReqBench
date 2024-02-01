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
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"bytes"
)

type OpenLambda struct {
	platform_adapter.BasePlatformAdapter
	PID            		int
	olDir          		string
	olUrl         		string
}

func (o *OpenLambda) StartWorker(options map[string]interface{}) error {
	fmt.Println("Starting OL")
	//load config
	o.LoadConfig("/root/ReqBench/src/platform_adapter/openlambda/config.json") //TODO: other ways to avoid hard-coding?
	o.olDir = o.Config["ol_dir"].(string)
	o.olUrl = o.Config["ol_url"].(string)

	//start ol
	cmd := exec.Command("./ol", "worker", "up" ,"-d")
	cmd.Dir = o.olDir
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to start worker: %v", err)
	}

	output := string(out)

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
		return nil
	} else {
		return nil
	}
}

func (o *OpenLambda) DeployFuncs(funcs []workload.Function) error {
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

func (o *OpenLambda) DeployFunction(deployTask chan workload.Function, errChan chan error) {
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

func (o *OpenLambda) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
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

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}
	resp.Body.Close()
	
	return nil
}