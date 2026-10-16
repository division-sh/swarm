package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type CLISourcePathOptions struct {
	SourceRoot string
	ConfigPath string
}

type CLISourcePaths struct {
	SourceRoot string
}

func ResolveCLISourcePaths(invocationRootPath string, opts CLISourcePathOptions) (CLISourcePaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLISourcePaths{}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: invocationRootPath, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLISourcePaths{}, err
	}
	return resolveCLISourcePathsFromConfig(invocationRootPath, opts, cfg)
}

func resolveCLISourcePathsFromConfig(invocationRootPath string, opts CLISourcePathOptions, _ cliCommandConfig) (CLISourcePaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLISourcePaths{}, err
	}
	sourceRoot, err := ResolveSourceRoot(invocationRootPath, opts.SourceRoot)
	if err != nil {
		return CLISourcePaths{}, err
	}
	return CLISourcePaths{
		SourceRoot: sourceRoot,
	}, nil
}

func ResolveSourceRoot(invocationRootPath, raw string) (string, error) {
	invocationRootPath, err := requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return "", err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = invocationRootPath
	}
	if !filepath.IsAbs(raw) {
		// Resolve symlinks before collapsing parent traversals in the operand.
		raw = invocationRootPath + string(filepath.Separator) + raw
	}
	root, err := filepath.EvalSymlinks(raw)
	if err != nil {
		return "", &cliAPIValidationError{message: fmt.Sprintf("resolve source directory %q: %v", raw, err)}
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", &cliAPIValidationError{message: fmt.Sprintf("inspect source directory %q: %v", raw, err)}
	}
	if !info.IsDir() {
		return "", &cliAPIValidationError{message: fmt.Sprintf("source root must be a directory: %s", raw)}
	}
	return filepath.Clean(root), nil
}
