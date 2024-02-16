package main

import (
	"fmt"
	"os"
	"strconv"
)

//this will be the main scripts for ReqBench
//I'll move things from send_req.go here
func main() {
	if len(os.Args) < 7 {
		panic("Not enough arguments")
	}
	PlatformType := os.Args[1]

	wl, err := readWorkload(os.Args[2])
	if err != nil {
		panic(err)
	}

	configPath := os.Args[3]

	tasks, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}

	timeout, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	totalTime, err := strconv.Atoi(os.Args[6])
	if err != nil {
		panic(err)
	}

	opts := RunOptions{
		PlatformType: PlatformType,
		Workload:     &wl,
		ConfigPath:   configPath,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: nil,
		KillOptions:  nil,
	}

	stats, err := AutoRun(opts)
	fmt.Println(stats)
}
