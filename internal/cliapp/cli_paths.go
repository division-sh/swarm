package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type CLISourcePlatformSpecPathOptions struct {
	SourceRoot       string
	PlatformSpecPath string
	ConfigPath       string
}

type CLISourcePlatformSpecPaths struct {
	SourceRoot       string
	PlatformSpecPath string
}

func ResolveCLISourcePlatformSpecPaths(invocationRootPath string, opts CLISourcePlatformSpecPathOptions) (CLISourcePlatformSpecPaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLISourcePlatformSpecPaths{}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: invocationRootPath, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLISourcePlatformSpecPaths{}, err
	}
	return resolveCLISourcePlatformSpecPathsFromConfig(invocationRootPath, opts, cfg)
}

func resolveCLISourcePlatformSpecPathsFromConfig(invocationRootPath string, opts CLISourcePlatformSpecPathOptions, cfg cliCommandConfig) (CLISourcePlatformSpecPaths, error) {
	var err error
	invocationRootPath, err = requireInvocationRootPath(invocationRootPath)
	if err != nil {
		return CLISourcePlatformSpecPaths{}, err
	}
	sourceRoot, err := ResolveSourceRoot(invocationRootPath, opts.SourceRoot)
	if err != nil {
		return CLISourcePlatformSpecPaths{}, err
	}
	configPlatformSpecPath := strings.TrimSpace(cfg.Paths.PlatformSpecPath)
	platformSpecPath := firstNonEmpty(
		opts.PlatformSpecPath,
		configPlatformSpecPath,
	)
	if platformSpecPath == "" {
		embedded, err := EmbeddedPlatformSpecPath()
		if err != nil {
			return CLISourcePlatformSpecPaths{}, fmt.Errorf("resolve embedded platform spec: %w", err)
		}
		platformSpecPath = embedded
	}
	return CLISourcePlatformSpecPaths{
		SourceRoot:       sourceRoot,
		PlatformSpecPath: ResolvePath(invocationRootPath, platformSpecPath),
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
