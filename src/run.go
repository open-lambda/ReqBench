package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"os"
	"os/exec"
	"path"
	. "rb/workload"
	"strings"
	"time"
)

type treeGenOpts struct {
	SinglePkg         bool
	entropyPenalty    int    // 0 for false, 1 for true
	Costs             string // a path to costs dict
	AvgDistWeights    bool
	BiasedDistWeights bool
}

func (t treeGenOpts) Marshal() string {
	b, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(b)
}

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
		series.New([]int{}, series.String, "rule"),
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
		wl.GenerateTrace(invokeLength, skew, nil, 0)
		w1, w2 = wl.RandomSplit(0.5)
		w1.SaveToJson(path.Join(dir, "w1.json"))
		w2.SaveToJson(path.Join(dir, "w2.json"))
		w2.AddMetrics([]string{"latency"})
	}

	// todo generate tree rules
	var opts []treeGenOpts
	strSizes := []string{}
	for _, size := range treeSizes {
		strSizes = append(strSizes, fmt.Sprintf("%d", size))
	}
	treeSizesStr := strings.Join(strSizes, ",")
	if useCacheTree {

	} else {
		for i, opt := range opts {
			opt := opt
			i := i
			go func() {
				cmd := exec.Command("python", "tree_gen.py",
					"--opts_json", opt.Marshal(),
					"--opts_name", fmt.Sprintf("v%d", i),
					"--workload_path", path.Join(dir, fmt.Sprintf("w%d.json", i)),
					"--dir_name", dir,
					"--tree_sizes", treeSizesStr,
				)
				if err := cmd.Start(); err != nil {
					panic(err)
				}

				if err := cmd.Wait(); err != nil {
					panic(err)
				}
			}()
		}
	}

	// todo: record how many calls are having empty importing modules, used to calculate hit rate
	//emptyPkgCallsW1 := w1.getEmptyPkgCallsCnt()
	//emptyPkgCallsW2 := w2.getEmptyPkgCallsCnt()

	trials := make([]trial, 0)
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
				Config:       "config.json",
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

var wl, _ = ReadWorkloadFromJson("workload.json")

func main() {
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
