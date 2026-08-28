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

func ResolveCLIContractPlatformSpecPaths(invocationRootPath string, opts CLIContractPlatformSpecPathOptions) (CLIContractPlatformSpecPaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLIContractPlatformSpecPaths{}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: invocationRootPath, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLIContractPlatformSpecPaths{}, err
	}
	return resolveCLIContractPlatformSpecPathsFromConfig(invocationRootPath, opts, cfg)
}

func resolveCLIContractPlatformSpecPathsFromConfig(invocationRootPath string, opts CLIContractPlatformSpecPathOptions, cfg cliCommandConfig) (CLIContractPlatformSpecPaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLIContractPlatformSpecPaths{}, err
	}
	contractsPath := firstNonEmpty(
		opts.ContractsPath,
		os.Getenv(cliContractsPathEnv),
		cfg.Paths.ContractsPath,
		discoverInvocationRootContractsPath(invocationRootPath),
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
		ContractsPath:    ResolvePath(invocationRootPath, contractsPath),
		PlatformSpecPath: ResolvePath(invocationRootPath, platformSpecPath),
	}, nil
}

func discoverInvocationRootContractsPath(invocationRootPath string) string {
	invocationRootPath = strings.TrimSpace(invocationRootPath)
	if invocationRootPath == "" {
		return ""
	}
	candidate := filepath.Join(invocationRootPath, "contracts")
	if regularFileExists(filepath.Join(candidate, "package.yaml")) {
		return candidate
	}
	return ""
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
