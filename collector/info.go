package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
)

var forkFileName = "fork.csv"
var createFileName = "create.csv"
var lookupFileName = "lookup.csv"
var reqFileName = "req.csv"
var latencyFileName = "latency.csv"

type Info interface {
	getCsvHeaders() []string
	getWriter() *csv.Writer
	String() []string
	getName() string
	getFileName() string
}

// deprecated
type Fork struct {
	SplitGeneration int     `json:"splitGeneration"`
	ForkTime        float32 `json:"forkTime"` // in ms
	// ForkCount: number of forks executed directly in this zygote, probably need to count indirect forks
	ForkCount int `json:"forkCount"`
	Pages     int `json:"pages"`  // number of pages
	MaxRss    int `json:"maxRss"` // in KB
	Pss       int `json:"pss"`    // in KB
}

// deprecated
type Create struct {
	SandboxId      string           `json:"sandbox-id"`
	ParentId       string           `json:"parent-id"`
	ParentGen      int              `json:"parent-split-gen"`
	SandboxGen     int              `json:"sandbox-split-gen"`
	CreateDuration map[string]int64 `json:"create-duration"`
}

// deprecated
type Lookup struct {
	SplitGen       int   `json:"split_gen"`
	LookupDuration int64 `json:"lookup_duration"`
}

// deprecated
type Req struct {
	FuncName    string `json:"func-name"`
	ReqDuration int64  `json:"duration"`
}

// StartCreate, StartPullHandler, EndPullHandler, EndCreate are platform-dependent timestamps,
// which only used in opanlambda.
// For other platforms, they are all 0
type Latency struct {
	Name string `json:"name"`
	// SplitGen indicate hit which zygote
	SplitGen int `json:"split_gen"`
	// float64 is millisecond timestamp
	Req              float64 `json:"req"`
	StartCreate      float64 `json:"start_create"`
	StartPullHandler float64 `json:"start_pullHandler"`
	EndPullHandler   float64 `json:"end_pullHandler"`
	EndCreate        float64 `json:"end_create"`
	StartImport      float64 `json:"start_import"`
	EndImport        float64 `json:"end_import"`
	EndExecute       float64 `json:"end_execute"`
	Received         float64 `json:"received"`
}

func (req Req) getWriter() *csv.Writer {
	file, _ := os.OpenFile(reqFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := csv.NewWriter(file)
	return writer
}

func (f Fork) getWriter() *csv.Writer {
	file, _ := os.OpenFile(forkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := csv.NewWriter(file)
	return writer
}

func (c Create) getWriter() *csv.Writer {
	file, _ := os.OpenFile(createFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := csv.NewWriter(file)
	return writer
}

func (l Lookup) getWriter() *csv.Writer {
	file, _ := os.OpenFile(lookupFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := csv.NewWriter(file)
	return writer
}

func (Latency) getWriter() *csv.Writer {
	file, _ := os.OpenFile(latencyFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := csv.NewWriter(file)
	return writer
}

func (req Req) getName() string {
	return "Req"
}
func (req Req) getFileName() string {
	return reqFileName
}

func (f Fork) getName() string {
	return "Fork"
}
func (f Fork) getFileName() string {
	return forkFileName
}

func (c Create) getName() string {
	return "Create"
}
func (c Create) getFileName() string {
	return createFileName
}

func (l Lookup) getName() string {
	return "Lookup"
}
func (l Lookup) getFileName() string {
	return lookupFileName
}

func (Latency) getName() string {
	return "Latency"
}

func (Latency) getFileName() string {
	return latencyFileName
}

func (f Fork) getCsvHeaders() []string {
	var headers []string
	t := reflect.TypeOf(Fork{})
	for i := 0; i < t.NumField(); i++ {
		headers = append(headers, t.Field(i).Tag.Get("json"))
	}
	return headers
}

func (c Create) getCsvHeaders() []string {
	var headers []string
	t := reflect.TypeOf(Create{})
	for i := 0; i < t.NumField(); i++ {
		headers = append(headers, t.Field(i).Tag.Get("json"))
	}
	headers = headers[:len(headers)-1]
	durations := []string{"acquire-mem", "acquire-cgroup", "make-root-fs", "fork-proc", "fresh-proc", "total"}
	headers = append(headers, durations...)
	return headers
}

func (l Lookup) getCsvHeaders() []string {
	var headers []string
	t := reflect.TypeOf(Lookup{})
	for i := 0; i < t.NumField(); i++ {
		headers = append(headers, t.Field(i).Tag.Get("json"))
	}
	return headers
}

func (req Req) getCsvHeaders() []string {
	var headers []string
	t := reflect.TypeOf(Req{})
	for i := 0; i < t.NumField(); i++ {
		headers = append(headers, t.Field(i).Tag.Get("json"))
	}
	return headers
}

func (Latency) getCsvHeaders() []string {
	var headers []string
	t := reflect.TypeOf(Latency{})
	for i := 0; i < t.NumField(); i++ {
		headers = append(headers, t.Field(i).Tag.Get("json"))
	}
	return headers
}

func (f Fork) String() []string {
	return []string{
		fmt.Sprintf("%d", f.SplitGeneration),
		fmt.Sprintf("%f", f.ForkTime),
		fmt.Sprintf("%d", f.ForkCount),
		fmt.Sprintf("%d", f.Pages),
		fmt.Sprintf("%d", f.MaxRss),
		fmt.Sprintf("%d", f.Pss),
	}
}

func (c Create) String() []string {
	strings := []string{
		c.SandboxId,
		c.ParentId,
		fmt.Sprintf("%d", c.ParentGen),
		fmt.Sprintf("%d", c.SandboxGen),
		fmt.Sprintf("%d", c.CreateDuration["acquire-mem"]),
		fmt.Sprintf("%d", c.CreateDuration["acquire-cgroup"]),
		fmt.Sprintf("%d", c.CreateDuration["make-root-fs"]),
		// only one of fork-proc and fresh-proc will be non-zero
		fmt.Sprintf("%d", c.CreateDuration["fork-proc"]),
		fmt.Sprintf("%d", c.CreateDuration["fresh-proc"]),
		fmt.Sprintf("%d", c.CreateDuration["total"]),
	}
	return strings
}

func (l Lookup) String() []string {
	return []string{
		fmt.Sprintf("%d", l.SplitGen),
		fmt.Sprintf("%d", l.LookupDuration),
	}
}

func (req Req) String() []string {
	return []string{
		req.FuncName,
		fmt.Sprintf("%d", req.ReqDuration),
	}
}

func (l Latency) String() []string {
	return []string{
		l.Name,
		fmt.Sprintf("%d", l.SplitGen),
		fmt.Sprintf("%f", l.Req),
		fmt.Sprintf("%f", l.StartCreate),
		fmt.Sprintf("%f", l.StartPullHandler),
		fmt.Sprintf("%f", l.EndPullHandler),
		fmt.Sprintf("%f", l.EndCreate),
		fmt.Sprintf("%f", l.StartImport),
		fmt.Sprintf("%f", l.EndImport),
		fmt.Sprintf("%f", l.EndExecute),
		fmt.Sprintf("%f", l.Received),
	}
}

func forkHandler(w http.ResponseWriter, r *http.Request) {
	fork := Fork{}
	bytes, _ := io.ReadAll(r.Body)
	json.Unmarshal(bytes, &fork)
	batch <- fork
	w.Write([]byte("ok"))
}

func createHandler(w http.ResponseWriter, r *http.Request) {
	create := Create{}
	bytes, _ := io.ReadAll(r.Body)
	json.Unmarshal(bytes, &create)
	batch <- create
	w.Write([]byte("ok"))
}

func lookupHandler(w http.ResponseWriter, r *http.Request) {
	lookup := Lookup{}
	bytes, _ := io.ReadAll(r.Body)
	json.Unmarshal(bytes, &lookup)
	batch <- lookup
	w.Write([]byte("ok"))
}

func reqHandler(w http.ResponseWriter, r *http.Request) {
	req := Req{}
	bytes, _ := io.ReadAll(r.Body)
	json.Unmarshal(bytes, &req)
	batch <- req
	w.Write([]byte("ok"))
}

func latencyHandler(w http.ResponseWriter, r *http.Request) {
	latency := Latency{}
	bytes, _ := io.ReadAll(r.Body)
	json.Unmarshal(bytes, &latency)
	batch <- latency
	w.Write([]byte("ok"))
}

func writeLog(info *interface{}, writers map[string]*csv.Writer) {
	switch info := (*info).(type) {
	case Fork:
		writers["Fork"].Write(info.String())
	case Create:
		writers["Create"].Write(info.String())
	case Lookup:
		writers["Lookup"].Write(info.String())
	case Req:
		writers["Req"].Write(info.String())
	case Latency:
		writers["Latency"].Write(info.String())
	}
}
