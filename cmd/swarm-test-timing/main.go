package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/division-sh/swarm/internal/testtiming"
)

func main() {
	var inputPath string
	var markdownPath string
	var topN int
	flag.StringVar(&inputPath, "input", "-", "path to go test -json output, or - for stdin")
	flag.StringVar(&markdownPath, "markdown", "-", "path to write Markdown report, or - for stdout")
	flag.IntVar(&topN, "top", 20, "number of slow packages and tests to include")
	flag.Parse()

	if err := run(inputPath, markdownPath, topN); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(inputPath string, markdownPath string, topN int) error {
	input, closeInput, err := openInput(inputPath)
	if err != nil {
		return err
	}
	defer closeInput()

	report, err := testtiming.ParseReport(input, testtiming.Options{TopN: topN})
	if err != nil {
		return err
	}

	output, closeOutput, err := openOutput(markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	return testtiming.WriteMarkdown(output, report)
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "" || path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open input %s: %w", path, err)
	}
	return file, func() { _ = file.Close() }, nil
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "" || path == "-" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("create markdown report %s: %w", path, err)
	}
	return file, func() { _ = file.Close() }, nil
}
