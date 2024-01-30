package mock

import (
	"rb/platform_adapter"
	"rb/workload"
	"fmt"
	"time"
)

type MockPlatform struct {
	platform_adapter.BasePlatformAdapter
	PID		int
}

func (m *MockPlatform) StartWorker(options map[string]interface{}) error {
	m.PID = 1234
	fmt.Printf("started mock worker with PID %d\n", m.PID)
	return nil
}

func (m *MockPlatform) KillWorker(options map[string]interface{}) error {
	fmt.Printf("killed mock worker with PID %d\n", m.PID)
	return nil
}

func (m *MockPlatform) DeployFuncs(funcs []workload.Function) error {
	fmt.Printf("deployed %d functions\n", len(funcs))
	return nil
}

func (m *MockPlatform) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	time.Sleep(100 * time.Millisecond)
	return nil
}