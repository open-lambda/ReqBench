package workload

import (
	"encoding/json"
	"os"
)

type Workload struct {
	Funcs    []Function `json:"funcs"`
	Calls    []Call     `json:"calls"`
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

// todo
func (wl *Workload) addMetrics(metrics []string) {

}

// todo
func (wl *Workload) generateTrace() {

}

// todo
func (wl *Workload) randomSplit(ratio float64) (Workload, Workload) {
	return Workload{}, Workload{}
}

func (wl *Workload) getEmptyPkgCallsCnt() int {
	cnt := 0
	for _, call := range wl.Calls {
		if wl.emptyPkg[call.Name] {
			cnt++
		}
	}
	return cnt
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

func readWorkloadFromJson(path string) (*Workload, error) {
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
