package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

func watchDir(path string, watcher *fsnotify.Watcher) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			fmt.Printf("Watching directory: %s\n", path)
			return watcher.Add(path)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}

func main() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				fmt.Printf("%s", event.Name)
				if event.Op&fsnotify.Write == fsnotify.Write && filepath.Base(event.Name) == "memory.current" {
					fmt.Printf("Modified file: %s\n", event.Name)
				}
				// Check for new directories and add them to the watcher
				if event.Op&fsnotify.Create == fsnotify.Create {
					fi, err := os.Stat(event.Name)
					if err == nil && fi.IsDir() {
						fmt.Printf("Added directory: %s\n", event.Name)
						watcher.Add(event.Name)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	dirToWatch := "/sys/fs/cgroup/default-ol-sandboxes"
	watchDir(dirToWatch, watcher)

	<-done
}
