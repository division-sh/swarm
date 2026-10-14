package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

type diskRewriteInput struct {
	Path       string        `json:"path"`
	Before     string        `json:"before"`
	BeforeHash string        `json:"before_hash"`
	AfterHash  string        `json:"after_hash"`
	Counts     rewriteCounts `json:"counts"`
}

func prepareDiskInputs(paths []string) ([]diskRewriteInput, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("select node declaration files explicitly")
	}
	plan := []diskRewriteInput{}
	for _, path := range paths {
		if filepath.Base(path) != "nodes.yaml" {
			return nil, fmt.Errorf("%s is not nodes.yaml", path)
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		output, counts, err := rewriteNodeSource(source)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		plan = append(plan, diskRewriteInput{
			Path: filepath.ToSlash(path), Before: string(source),
			BeforeHash: fmt.Sprintf("%x", sha256.Sum256(source)),
			AfterHash:  fmt.Sprintf("%x", sha256.Sum256(output)), Counts: counts,
		})
	}
	return plan, nil
}
