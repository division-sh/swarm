package cliapp

import (
	"fmt"
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
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = invocationRootPath
	}
	if !filepath.IsAbs(raw) {
		raw = filepath.Join(invocationRootPath, raw)
	}
	root, err := filepath.Abs(raw)
	if err != nil {
		return "", fmt.Errorf("resolve source directory %q: %w", raw, err)
	}
	return filepath.Clean(root), nil
}
