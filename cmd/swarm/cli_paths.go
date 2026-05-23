package main

import (
	"os"
	"path/filepath"
	"strings"
)

const cliContractsPathEnv = "SWARM_CONTRACTS_PATH"

type cliContractPlatformSpecPathOptions struct {
	ContractsPath    string
	PlatformSpecPath string
}

type cliContractPlatformSpecPaths struct {
	ContractsPath    string
	PlatformSpecPath string
}

func resolveCLIContractPlatformSpecPaths(repoRoot string, opts cliContractPlatformSpecPathOptions) (cliContractPlatformSpecPaths, error) {
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return cliContractPlatformSpecPaths{}, err
	}
	contractsPath := firstNonEmpty(
		opts.ContractsPath,
		os.Getenv(cliContractsPathEnv),
		cfg.ContractsPath,
		discoverRepoContractsPath(repoRoot),
	)
	platformSpecPath := firstNonEmpty(
		opts.PlatformSpecPath,
		cfg.PlatformSpecPath,
		defaultPlatformSpecPath,
	)
	return cliContractPlatformSpecPaths{
		ContractsPath:    resolvePath(repoRoot, contractsPath),
		PlatformSpecPath: resolvePath(repoRoot, platformSpecPath),
	}, nil
}

func discoverRepoContractsPath(repoRoot string) string {
	candidate := filepath.Join(repoRoot, "contracts")
	if regularFileExists(filepath.Join(candidate, "package.yaml")) {
		return candidate
	}
	return ""
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func resolveContractsPath(repoRoot, raw string) string {
	if resolved := resolvePath(repoRoot, raw); strings.TrimSpace(resolved) != "" {
		return resolved
	}
	return discoverRepoContractsPath(repoRoot)
}
