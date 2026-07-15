package cliapp

import (
	"log"
	"os"
)

func RepoRoot() string {
	root := DiscoverRepoRoot()
	if root != "" {
		return root
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve cwd: %v", err)
	}
	log.Fatalf("locate repo root from %s", dir)
	return ""
}
