package platform_adapter

import (
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"io/ioutil"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type LockStatMonitor struct {
	index     int
	Intervals int
	FilePath  string
	cpuUsages []float64
	startTime time.Time
	endTime   time.Time
	lock      sync.Mutex
	stopChan  chan struct{}
}

func NewLockStatMonitor(intervals int, filePath string) *LockStatMonitor {
	return &LockStatMonitor{
		index:     0,
		Intervals: intervals,
		FilePath:  filePath,
		cpuUsages: make([]float64, 0),
		stopChan:  make(chan struct{}),
	}
}

func (ls *LockStatMonitor) clearLockStat() {
	cmd := "echo 0 > /proc/lock_stat"
	exec.Command("bash", "-c", cmd).Run()
}

func (ls *LockStatMonitor) StartMonitor() {
	ls.clearLockStat()
	ls.startTime = time.Now()

	cpu.Percent(0, false)

	ticker := time.NewTicker(time.Duration(ls.Intervals) * time.Second)
	defer ticker.Stop()

	// if ls.Intervals <= 0, then only collect lock stat once
	if ls.Intervals <= 0 {
		return
	}
	for {
		select {
		case <-ticker.C:
			cpuUsage, _ := cpu.Percent(0, false)
			ls.cpuUsages = append(ls.cpuUsages, cpuUsage[0])

			go func() {
				output := ls.readLockStat()
				ls.writeToFile(output)
			}()
		case <-ls.stopChan:
			return
		}
	}
}

func (ls *LockStatMonitor) StopMonitor() {
	ls.clearLockStat()
	ls.endTime = time.Now()
	ls.stopChan <- struct{}{}
	ls.index = 0
	ls.cpuUsages = make([]float64, 0)

	if ls.Intervals > 0 {
		return
	}
	// collect last cpu usage and write to file
	if cpuUsage, err := cpu.Percent(0, false); err == nil && len(cpuUsage) > 0 {
		ls.cpuUsages = append(ls.cpuUsages, cpuUsage[0])
	}
	cpuUsageFilePath := strings.Split(ls.FilePath, ".")[0] + "_cpu_usages.txt"
	cpuUsagesData := fmt.Sprintf("%v", ls.cpuUsages)
	if err := ioutil.WriteFile(cpuUsageFilePath, []byte(cpuUsagesData), 0644); err != nil {
		fmt.Printf("Error writing CPU usages to file: %s\n", err)
	}

	output := ls.readLockStat()
	ls.writeToFile(output)
}

func (ls *LockStatMonitor) readLockStat() string {
	data, err := ioutil.ReadFile("/proc/lock_stat")
	if err != nil {
		fmt.Printf("Error reading /proc/lock_stat: %s\n", err)
		return ""
	}
	return string(data)
}

func (ls *LockStatMonitor) writeToFile(output string) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	fileName := fmt.Sprintf("%s_%d", ls.FilePath, ls.index)
	ls.index++
	if err := ioutil.WriteFile(fileName, []byte(output), 0644); err != nil {
		fmt.Printf("Error writing to file: %s\n", err)
	}
}
