package platform_adapter

import (
	"encoding/json"
	"errors"
	"os"
	"rb/workload"
)

type PlatformAdapter interface {
	StartWorker(options map[string]interface{}) error
	KillWorker(options map[string]interface{}) error
	DeployFuncs(funcs []workload.Function) error
	InvokeFunc(funcName string, timeout int, options map[string]interface{}) error
	LoadConfig(config interface{}) error
	GetStats() map[string]interface{}
}

type BasePlatformAdapter struct {
	Config map[string]interface{}

	// after each run, there might be some stats collected by the platform
	// this map help to store those stats and can be easily accessed by the caller
	Stats map[string]interface{}
}

// if config is a string, it is treated as a path to a json file; if it is a map, it is treated as a json object
func (c *BasePlatformAdapter) LoadConfig(config interface{}) error {
	switch v := config.(type) {
	case string:
		file, err := os.Open(v)
		if err != nil {
			return err
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		err = decoder.Decode(&c.Config)
		if err != nil {
			return err
		}
	case map[string]interface{}:
		c.Config = v
	default:
		return errors.New("unsupported config type")
	}
	return nil
}

func (c *BasePlatformAdapter) GetStats() map[string]interface{} {
	return c.Stats
}
