package workload

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Workload struct {
	Funcs    []Function `json:"funcs"`
	Calls    []Call     `json:"calls"`
	fnIndex  int
	emptyPkg map[string]bool
}

type Call struct {
	Name string `json:"name"`
}

type Function struct {
	Name string   `json:"name"`
	Meta Meta     `json:"meta"`
	Code []string `json:"code"`
}

type Meta struct {
	RequirementsIn  string   `json:"requirements_in"`
	RequirementsTxt string   `json:"requirements.txt"`
	ImportMods      []string `json:"import_mods"`
}

func generateNonMeasureCodeLines(modules []string, returnVal string) []string {
	lines := []string{
		"import time, importlib, os",
		"os.environ['OPENBLAS_NUM_THREADS'] = '2'",
		fmt.Sprintf("for mod in %s:", formatPythonList(modules)),
		"    try:",
		"        importlib.import_module(mod)",
		"    except Exception as e:",
		"        pass",
		fmt.Sprintf("def f(event):"),
		fmt.Sprintf("    return \"%s\"", returnVal),
	}
	return lines
}

func genMeasureCode(modules []string, measureLatency bool, measureMem bool) []string {
	lines := []string{
		"import time, importlib, os",
		"os.environ['OPENBLAS_NUM_THREADS'] = '2'",
		"called = False",
		"split_gen = -1",
	}
	if measureMem {
		lines = append(lines,
			"import tracemalloc, gc, sys, json",
			"gc.collect()",
			"tracemalloc.start()",
		)
	}
	if measureLatency {
		lines = append(lines, "t_StartImport = time.time()*1000")
	}
	lines = append(lines,
		"failed = []",
		fmt.Sprintf("for mod in %s:", formatPythonList(modules)),
		"    try:",
		"        importlib.import_module(mod)",
		"    except Exception as e:",
		"        failed.append(mod)",
		"        pass",
	)
	if measureLatency {
		lines = append(lines, "t_EndImport = time.time()*1000")
	}
	lines = append(lines,
		"def f(event):",
		"    global t_StartImport, t_EndImport, t_EndExecute, failed",
		"    time_start = time.time()*1000",
	)
	if measureLatency {
		lines = append(lines,
			"    t_EndExecute = time.time()*1000",
			"    event['start_import'] = t_StartImport",
			"    event['end_import'] = t_EndImport",
			"    event['start_execute'] = time_start",
			"    event['end_execute'] = t_EndExecute",
			"    event['failed'] = failed",
		)
	}
	if measureMem {
		lines = append(lines,
			"    mb = (tracemalloc.get_traced_memory()[0] - tracemalloc.get_traced_memory()[1]) / 1024 / 1024",
			"    event['memory_usage_mb'] = mb",
		)
	}
	lines = append(lines, "    return event")
	return lines
}

// formatPythonList convert go slice to python list
func formatPythonList(list []string) string {
	var quoted []string
	for _, item := range list {
		quoted = append(quoted, fmt.Sprintf("'%s'", item))
	}
	return fmt.Sprintf("[%s]", strings.Join(quoted, ", "))
}

func (wl *Workload) AddMetrics(metrics []string) {
	generateLatency := contains(metrics, "latency")
	generateMem := contains(metrics, "memory")

	for _, f := range wl.Funcs {
		f.Code = genMeasureCode(f.Meta.ImportMods, generateLatency, generateMem)
	}
}

func (wl *Workload) ShuffleCalls() {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(wl.Calls), func(i, j int) { wl.Calls[i], wl.Calls[j] = wl.Calls[j], wl.Calls[i] })
}

// generateTrace is not necessary to be implemented in go
// func (wl *Workload) generateTrace(invokeLength int, skew bool, weight []float64, s float64)

func (wl *Workload) RandomSplit(ratio float64) (Workload, Workload) {
	wlTrain := Workload{}
	wlTest := Workload{}
	wlTrainAdded := make(map[string]bool)
	wlTestAdded := make(map[string]bool)

	wl.ShuffleCalls()

	trainSize := int(float64(len(wl.Calls)) * ratio)
	wlTrain.Calls = wl.Calls[:trainSize]
	wlTest.Calls = wl.Calls[trainSize:]
	for _, call := range wlTrain.Calls {
		f := wl.getFunction(call.Name)
		if _, exists := wlTrainAdded[call.Name]; !exists {
			wlTrain.addFunction(f.Meta, f.Code)
		}
		wlTrain.addCall(call.Name)
		wlTrainAdded[call.Name] = true
	}

	for _, call := range wlTest.Calls {
		f := wl.getFunction(call.Name)
		if _, exists := wlTestAdded[call.Name]; !exists {
			wlTest.addFunction(f.Meta, f.Code)
		}
		wlTest.addCall(call.Name)
		wlTestAdded[call.Name] = true
	}
	return wlTrain, wlTest
}

func (wl *Workload) GetEmptyPkgCallsCnt() int {
	cnt := 0
	for _, call := range wl.Calls {
		if wl.emptyPkg[call.Name] {
			cnt++
		}
	}
	return cnt
}

func (wl *Workload) getFunction(name string) Function {
	for _, f := range wl.Funcs {
		if f.Name == name {
			return f
		}
	}
	return Function{}
}

func (wl *Workload) addFunction(meta Meta, code []string) {
	name := fmt.Sprintf("fn%d", wl.fnIndex)
	wl.fnIndex++
	wl.Funcs = append(wl.Funcs, Function{Name: name, Meta: meta, Code: code})
}

func (wl *Workload) addCall(name string) {
	wl.Calls = append(wl.Calls, Call{Name: name})
}

func (wl *Workload) saveToJson(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(wl)
	if err != nil {
		return err
	}

	return nil
}

func ReadWorkloadFromJson(path string) (*Workload, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var workload Workload
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&workload)
	if err != nil {
		return nil, err
	}

	return &workload, nil
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
