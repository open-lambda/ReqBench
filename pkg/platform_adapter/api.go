package platform_adapter

import (
	"encoding/json"
	"os"
	"rb/workload"
)

type PlatformAdapter interface {
	StartWorker(options map[string]interface{}) error
	KillWorker(options map[string]interface{}) error
	DeployFuncs(funcs []workload.Function) error
	InvokeFunc(funcName string, timeout int, options map[string]interface{}) error
	LoadConfig(path string) error
}

type BasePlatformAdapter struct {
	Config map[string]interface{}
}

func (c *BasePlatformAdapter) LoadConfig(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&c.Config)
	return err
}
