package platform_adapter

import (
	"encoding/json"
	"os"
	"github.com/open-lambda/ReqBench/rb/workload"
)

type PlatformAdapter interface {
	StartWorker(options map[string]interface{}) error
	KillWorker(options map[string]interface{}) error
	DeployFunc(funcs []workload.Function) error
	InvokeFunc(funcName string, options map[string]interface{}) error
	LoadConfig(path string) error
}

type ConfigLoader struct {
	Config map[string]interface{}
}

func (c *ConfigLoader) LoadConfig(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&c.Config)
	return err
}
