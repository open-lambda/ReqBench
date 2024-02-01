//restructured base on this: https://github.com/golang-standards/project-layout
package main

import (
	"os"
	"os/exec"
	"log"
	"fmt"
	"strings"
	"github.com/urfave/cli/v2"
)

func nothing(ctx *cli.Context) error{
	fmt.Println("nothing")
	return nil
}

func getPlatforms(path string) []string {
	cmd := exec.Command("go", "list", path) // path = "../src/platform_adapter/..."
	output, err := cmd.Output()
    if err != nil {
        panic(err)
    }
    packages := strings.Split(string(output), "\n")

	platform_adapter := packages[0]
	var platforms []string
	for _, pkg := range packages[1:] {
		p := strings.Replace(pkg, platform_adapter+"/", "", -1)
		platforms = append(platforms, p)
	}
	
    return platforms
}


// main CLI
func main() {
	//TODO
}