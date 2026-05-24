package main

import (
	"fmt"
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
	repoRoot = strings.TrimSpace(repoRoot)
	if repoRoot == "" {
		repoRoot = discoverRepoRoot()
	}
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
	configPlatformSpecPath := strings.TrimSpace(cfg.PlatformSpecPath)
	platformSpecPath := firstNonEmpty(
		opts.PlatformSpecPath,
		configPlatformSpecPath,
	)
	if platformSpecPath == "" {
		embedded, err := embeddedPlatformSpecPath()
		if err != nil {
			return cliContractPlatformSpecPaths{}, fmt.Errorf("resolve embedded platform spec: %w", err)
		}
		platformSpecPath = embedded
	}
	return cliContractPlatformSpecPaths{
		ContractsPath:    resolvePath(repoRoot, contractsPath),
		PlatformSpecPath: resolvePath(repoRoot, platformSpecPath),
	}, nil
}

func discoverRepoContractsPath(repoRoot string) string {
	repoRoot = strings.TrimSpace(repoRoot)
	if repoRoot == "" {
		return ""
	}
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
