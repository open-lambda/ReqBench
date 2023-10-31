package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
)

const (
	INPUTFILE      = "requirements.csv"
	OUTPUTFILE     = "output.csv"
	OUTPUTFILEFAIL = "failed.csv"
	NUM_THREAD     = 1
)

type Content struct {
	id      string
	row     []string
	content string
	err     bool
}

type IOChan struct {
	inputChan  chan *Content
	outputChan chan *Content
}

func main() {
	inputFile, err := os.Open(INPUTFILE)
	if err != nil {
		fmt.Println("Error opening input file:", err)
		return
	}
	defer inputFile.Close()

	outputFile, err := os.OpenFile(OUTPUTFILE, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error creating output file 0:", err)
		return
	}
	outputFileFail, err := os.OpenFile(OUTPUTFILEFAIL, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error creating output file 1:", err)
		return
	}
	defer outputFile.Close()
	defer outputFileFail.Close()

	reader := csv.NewReader(inputFile)
	writer := csv.NewWriter(outputFile)
	writerFail := csv.NewWriter(outputFileFail)

	compiler := &IOChan{
		inputChan:  make(chan *Content, NUM_THREAD),
		outputChan: make(chan *Content, NUM_THREAD),
	}

	//find "raw" and "compiled" column
	raw_col, compiled_col := -1, -1
	header, err := reader.Read()
	for i := 0; i < len(header); i++ {
		if header[i] == "raw" {
			raw_col = i
		}
		if header[i] == "compiled" {
			compiled_col = i
		}
	}

	if raw_col < 0 {
		fmt.Println("Error raw column not found")
		os.Exit(1)
	}

	if compiled_col < 0 {
		header = append(header, "compiled")
	}

	//write header to output files
	writer.Write(header)
	writerFail.Write(header)

	for i := 0; i < NUM_THREAD; i++ {
		go worker(i, compiler)
	}

	//reader
	go func() {
		for {
			record, err := reader.Read()
			if err == io.EOF {
				for i := 0; i < NUM_THREAD; i++ {
					compiler.inputChan <- nil
				}
				compiler.outputChan <- nil
				break
			} else if err != nil {
				fmt.Println("Error reading from input file:", err)
				os.Exit(1)
			}
			compiler.inputChan <- &Content{
				id:      record[0],
				row:     record,
				content: record[raw_col],
				err:     false,
			}
		}
	}()

	//writer
	for {
		output := <-compiler.outputChan
		if output == nil {
			break
		}

		row := []string{}
		if compiled_col < 0 {
			row = append(output.row, output.content)
		} else {
			row = output.row
			row[compiled_col] = output.content
		}

		if err := writer.Write(row); err != nil {
			fmt.Println("Error writing to output file:", err)
			os.Exit(1)
		}
		writer.Flush()

		if err := writer.Error(); err != nil {
			fmt.Println("Error flushing writer:", err)
			os.Exit(1)
		}

		if !output.err {
			if err := writerFail.Write(row); err != nil {
				fmt.Println("Error writing to output file:", err)
				os.Exit(1)
			}
			writerFail.Flush()

			if err := writerFail.Error(); err != nil {
				fmt.Println("Error flushing writer:", err)
				os.Exit(1)
			}
		}
	}
}

func worker(workerid int, compiler *IOChan) {
	count := 0
	for {
		input := <-compiler.inputChan
		if input == nil {
			break
		}

		tempInPath := fmt.Sprintf("temp_%d.in", workerid)
		tempTxtPath := fmt.Sprintf("temp_%d.txt", workerid)

		cmd := exec.Command("rm", tempInPath, tempTxtPath)
		_ = cmd.Run()

		cmd = exec.Command("/bin/bash", "-c", "echo \""+input.content+"\" > "+tempInPath)
		if err := cmd.Run(); err != nil {
			fmt.Printf("Error creating temporary requirements.in file for id %s: %v\n", input.id, err)
			compiler.outputChan <- input
			continue
		}

		cmd = exec.Command("pip-compile", "--rebuild", tempInPath)
		fmt.Printf("running pip-compile for id %s\n", input.id)
		output, err := cmd.CombinedOutput()
		requirements := string(output)
		if err != nil {
			fmt.Printf("Error running pip-compile for id %s\n", input.id)
			input.err = true
			compiler.outputChan <- input
		} else {
			compiler.outputChan <- &Content{
				id:      input.id,
				row:     input.row,
				content: requirements,
				err:     false,
			}

			cmd = exec.Command("rm", tempInPath, tempTxtPath)
			_ = cmd.Run()
		}
		count += 1
	}

}
