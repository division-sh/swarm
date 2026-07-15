package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const cliContractsPathEnv = "SWARM_CONTRACTS_PATH"

type CLIContractPlatformSpecPathOptions struct {
	ContractsPath    string
	PlatformSpecPath string
	ConfigPath       string
}

type CLIContractPlatformSpecPaths struct {
	ContractsPath    string
	PlatformSpecPath string
}

func ResolveCLIContractPlatformSpecPaths(RepoRoot string, opts CLIContractPlatformSpecPathOptions) (CLIContractPlatformSpecPaths, error) {
	RepoRoot = strings.TrimSpace(RepoRoot)
	if RepoRoot == "" {
		RepoRoot = DiscoverRepoRoot()
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: RepoRoot, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLIContractPlatformSpecPaths{}, err
	}
	contractsPath := firstNonEmpty(
		opts.ContractsPath,
		os.Getenv(cliContractsPathEnv),
		cfg.Paths.ContractsPath,
		discoverRepoContractsPath(RepoRoot),
	)
	configPlatformSpecPath := strings.TrimSpace(cfg.Paths.PlatformSpecPath)
	platformSpecPath := firstNonEmpty(
		opts.PlatformSpecPath,
		configPlatformSpecPath,
	)
	if platformSpecPath == "" {
		embedded, err := EmbeddedPlatformSpecPath()
		if err != nil {
			return CLIContractPlatformSpecPaths{}, fmt.Errorf("resolve embedded platform spec: %w", err)
		}
		platformSpecPath = embedded
	}
	return CLIContractPlatformSpecPaths{
		ContractsPath:    ResolvePath(RepoRoot, contractsPath),
		PlatformSpecPath: ResolvePath(RepoRoot, platformSpecPath),
	}, nil
}

func discoverRepoContractsPath(RepoRoot string) string {
	RepoRoot = strings.TrimSpace(RepoRoot)
	if RepoRoot == "" {
		return ""
	}
	candidate := filepath.Join(RepoRoot, "contracts")
	if regularFileExists(filepath.Join(candidate, "package.yaml")) {
		return candidate
	}
	return ""
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func ResolveContractsPath(RepoRoot, raw string) string {
	if resolved := ResolvePath(RepoRoot, raw); strings.TrimSpace(resolved) != "" {
		return resolved
	}
	return discoverRepoContractsPath(RepoRoot)
}
