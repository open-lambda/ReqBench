package main

import (
	"fmt"
	"path"
	"rb/workload"
)

type trial struct {
	treeName string
	treeSize int
	treePath string
}

func runOneTrial(dir string, treeSizes []int, tasks int, dryRun bool, ingoreIndices []int,
	warmup bool, reuseCg bool, downsizeOnPause bool, poolSize int,
	profileLock bool, monitorContainer bool, saveMetrics bool,
	skew bool, invokeLength int, totalTime int,
	useCacheWorkload bool, useCacheTree bool,
	sleepTime int) error {
	var wl, _ = workload.ReadWorkloadFromJson("workload.json")
	if ingoreIndices == nil {
		ingoreIndices = []int{0, 1, 2}
	}
	if treeSizes == nil {
		treeSizes = []int{1}
	}

	var w1, w2 *workload.Workload
	var err error
	if useCacheWorkload {
		w1, err = workload.ReadWorkloadFromJson(path.Join(dir, "w1.json"))
		if err != nil {
			return err
		}
		w2, err = workload.ReadWorkloadFromJson(path.Join(dir, "w2.json"))
		if err != nil {
			return err
		}
		if saveMetrics {
			w2.AddMetrics([]string{"latency"})
		}
	}

	// record how many calls are having empty importing modules, used to calculate hit rate
	//emptyPkgCallsW1 := w1.GetEmptyPkgCallsCnt()
	//emptyPkgCallsW2 := w2.GetEmptyPkgCallsCnt()

	trials := make([]trial, 0)
	// todo: trialsResults := make([]
	// train the tree with w1

	// run each tree
	for _, trial := range trials {
		treePath := trial.treePath
		treeSize := trial.treeSize
		treeName := trial.treeName
		csvPath := path.Join(dir, fmt.Sprintf("%s-%d.csv", treeName, treeSize))
		if !dryRun {
			startOptions := map[string]interface{}{
				"import_cache_tree":            treePath,
				"mem_pool_mb":                  poolSize,
				"limits.mem_mb":                600,
				"features.warmup":              warmup,
				"features.reuse_cgroups":       reuseCg,
				"features.downsize_paused_mem": downsizeOnPause,
			}
			killOptions := map[string]interface{}{
				"save_metrics": saveMetrics,
				"csv_name":     csvPath,
			}
			AutoRun("openlambda", w1, startOptions, killOptions,
				"config.json", tasks, invokeLength, totalTime)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
