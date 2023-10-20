package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var batchSize = 1000
var batch chan interface{}
var included []Info

// timely flush the batch to the file
func flush() {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			if len(batch) > 0 {
				writeAvailableLogs()
			}
		default:
			if len(batch) > batchSize {
				writeAvailableLogs()
			}
		}
	}
}

func writeAvailableLogs() {
	writers := make(map[string]*csv.Writer)
	for _, info := range included {
		writers[info.getName()] = info.getWriter()
	}

	for len(batch) > 0 {
		info := <-batch
		writeLog(&info, writers)
	}

	defer func() {
		for _, writer := range writers {
			writer.Flush()
		}
	}()
}

func createFiles() {
	// use reflection to get the headers(json), write them to the csv
	for _, info := range included {
		os.Truncate(info.getFileName(), 0)
		writer := info.getWriter()
		writer.Write(info.getCsvHeaders())
		defer writer.Flush()
	}
}

func main() {
	fmt.Println("Start collector")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)

	args := os.Args
	if len(args) != 2 {
		panic("Usage: go run collector.go <output-dir>")
	}
	os.Chdir(args[1])

	included = []Info{
		//Fork{},
		//Create{},
		//Lookup{},
		//Req{},
		Latency{},
	}

	batch = make(chan interface{}, batchSize*2)
	createFiles()
	http.HandleFunc("/fork", forkHandler)
	http.HandleFunc("/create", createHandler)
	http.HandleFunc("/lookup", lookupHandler)
	http.HandleFunc("/req", reqHandler)
	http.HandleFunc("/latency", latencyHandler)

	server := &http.Server{Addr: ":4998"}
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	go flush()

	<-signals
	fmt.Println("exit")
	// Close the server
	if err := server.Close(); err != nil {
		fmt.Printf("Error shutting down server: %s\n", err)
	}
	writeAvailableLogs()
	time.Sleep(10 * time.Second) // if quit too fast, the lastest output cannot be captured
}
