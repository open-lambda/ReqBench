package main

import (
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"os"
	"path"
	. "rb/workload"
	"time"
)

type trial struct {
	treePath string
	treeSize int
	treeRule string
}

func runOneTrial(dir string, pkgWeightPath string, treeSizes []int, tasks int, dryRun bool, ignoreIndices []int,
	warmup bool, reuseCg bool, downsizeOnPause bool, poolSize int,
	profileLock bool, monitorContainer bool, saveMetrics bool,
	skew bool, invokeLength int, totalTime int,
	useCacheWorkload bool, useCacheTree bool,
	sleepTime int) (dataframe.DataFrame, error) {
	if ignoreIndices == nil {
		ignoreIndices = []int{0, 1, 2}
	}
	if treeSizes == nil {
		treeSizes = []int{1}
	}

	dfCombined := dataframe.New(
		series.New([]int{}, series.Int, "nodes"),
		series.New([]float64{}, series.Float, "throughput"),
		series.New([]float64{}, series.Float, "mem_usage"),
	)
	var w1, w2 *Workload
	var err error
	if useCacheWorkload {
		w1, err = ReadWorkloadFromJson(path.Join(dir, "w1.json"))
		if err != nil {
			return dfCombined, err
		}
		w2, err = ReadWorkloadFromJson(path.Join(dir, "w2.json"))
		if err != nil {
			return dfCombined, err
		}
		if saveMetrics {
			w2.AddMetrics([]string{"latency"})
		}
	} else {
		// todo: generate workload trace, and do a split, better do this in python

	}

	// todo: record how many calls are having empty importing modules, used to calculate hit rate
	//emptyPkgCallsW1 := w1.getEmptyPkgCallsCnt()
	//emptyPkgCallsW2 := w2.getEmptyPkgCallsCnt()

	trials := make([]trial, 0)
	// train the tree with w1

	// run each tree
	for i, trial := range trials {
		treePath := trial.treePath
		treeSize := trial.treeSize
		treeRule := trial.treeRule
		csvPath := path.Join(dir, fmt.Sprintf("%s-%d.csv", treeRule, treeSize))
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

			ro := RunOptions{
				PlatformType: "openlambda",
				Workload:     w1,
				StartOptions: startOptions,
				KillOptions:  killOptions,
				ConfigPath:   "config.json",
				Tasks:        tasks,
				TotalTime:    totalTime,
			}
			stats, err := AutoRun(ro)
			if err != nil {
				return dfCombined, err
			}

			// save metrics
			dfCombined = dfCombined.RBind(dataframe.New(
				series.New([]int{treeSize}, series.Int, "nodes"),
				series.New(treeRule, series.String, "rule"),
				series.New(stats["throughput"], series.Float, "throughput"),
				series.New(stats["seconds"], series.Float, "mem_usage"),
			))
			if i != len(trials)-1 {
				// sleep between trials
				time.Sleep(time.Duration(sleepTime) * time.Second)
			}
		}
	}

	return dfCombined, nil
}

func main() {
	var wl, _ = ReadWorkloadFromJson("workload.json")

	useCacheWorkload := true
	useCacheTree := true
	TRIALS := 10
	treeSizes := []int{1, 20, 40, 80, 160, 320, 640}
	dryRun := false
	invokeLength := len(wl.Funcs)
	experimentDir := "/root/open-lambda/paper-tree-cache/analysis/17/"
	pkgWeightPath := "/root/open-lambda/paper-tree-cache/analysis/17/pacakges.json"

	concatedDF := dataframe.New(
		series.New([]int{}, series.Int, "trial"),
		series.New([]int{}, series.Int, "nodes"),
		series.New([]string{}, series.String, "rule"),
		series.New([]float64{}, series.Float, "throughput"),
		series.New([]float64{}, series.Float, "mem_usage"),
	)
	for i := 0; i < TRIALS; i++ {
		dirName := path.Join(experimentDir, fmt.Sprintf("trials/tree_gen/%d", i))
		if !useCacheTree && !useCacheWorkload {
			os.RemoveAll(dirName)
		}
		if _, err := os.Stat(dirName); os.IsNotExist(err) {
			os.MkdirAll(dirName, os.ModePerm)
		}
		trialDF, err := runOneTrial(dirName, pkgWeightPath, treeSizes, 5, dryRun, []int{0, 1, 2},
			true, true, true, 600,
			false, false, true,
			false, invokeLength, 0,
			useCacheWorkload, useCacheTree,
			120)
		if err != nil {
			fmt.Println(err)
		}
		trialSlice := make([]int, trialDF.Nrow())
		for j := range trialSlice {
			trialSlice[j] = i
		}
		trialSeries := series.New(trialSlice, series.Int, "trial")
		trialDF = trialDF.Mutate(trialSeries)

		concatedDF = concatedDF.RBind(trialDF)
	}

}
