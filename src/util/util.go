package util

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	ps "github.com/shirou/gopsutil/process"
)

func getProcessMemoryInfo(pid int32, mode string) (uint64, error) {
	process, err := ps.NewProcess(pid)
	if err != nil {
		return 0, err
	}

	switch mode {
	case "PSS":
		memMap, err := process.MemoryMaps(true)
		if err != nil {
			return 0, err
		}
		return memMap.Pss / 1024, nil
	case "RSS":
		memInfo, err := process.MemoryInfo()
		if err != nil {
			return 0, err
		}
		return memInfo.Rss / 1024, nil
	default:
		return 0, fmt.Errorf("unsupported mode: %s", mode)
	}
}

func GetTotalMem(base, mode string) (uint64, error) {
	var totalMem uint64

	if mode == "CG" {
		memoryCurrentPath := filepath.Join(base, "memory.current")
		content, err := ioutil.ReadFile(memoryCurrentPath)
		if err != nil {
			return 0, err
		}
		memInBytes, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
		if err != nil {
			return 0, err
		}
		return uint64(memInBytes) / 1024, nil
	}

	cgFolders, err := filepath.Glob(filepath.Join(base, "cg-*"))
	if err != nil {
		return 0, err
	}

	for _, cgFolder := range cgFolders {
		procsFilePath := filepath.Join(cgFolder, "cgroup.procs")
		if file, err := os.Open(procsFilePath); err == nil {
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				pid, err := strconv.ParseInt(scanner.Text(), 10, 32)
				if err != nil {
					continue
				}
				mem, err := getProcessMemoryInfo(int32(pid), mode)
				if err != nil {
					continue
				}
				totalMem += mem
			}
		}
	}

	return totalMem, nil
}

func Union(map1, map2 map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range map1 {
		result[key] = value
	}
	for key, value := range map2 {
		result[key] = value
	}

	return result
}
