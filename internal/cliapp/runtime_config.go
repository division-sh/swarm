package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

var runtimeConfigExecutablePath = os.Executable

type RuntimeConfigLoadOptions struct {
	RepoRoot        string
	ExplicitPath    string
	BackendOverride string
}

type RuntimeConfigLoadResult struct {
	Config      *config.Config
	Source      string
	Path        string
	Layers      []unifiedConfigLayer
	KeyOrigins  map[string]unifiedConfigKeyOrigin
	Diagnostics []unifiedConfigDiagnostic
	cli         cliCommandConfig
}

// DeploymentConfigured uses admitted layer facts, not a default backend or an
// unrelated user's global connection settings, to select strict verification.
func (r RuntimeConfigLoadResult) DeploymentConfigured() bool {
	for _, layer := range r.Layers {
		switch layer.Name {
		case unifiedLayerExplicit, unifiedLayerLocalOperator, unifiedLayerProject:
			return true
		}
	}
	return false
}

func (r RuntimeConfigLoadResult) ResolveServeListeners(opts ServeOptions, apiFlagSet, mcpFlagSet bool) (string, string, error) {
	if r.Config == nil {
		return "", "", fmt.Errorf("admitted runtime configuration is required")
	}
	api, mcp, _, err := resolveCLIServeListenerAddressesFromConfig(cliServeListenerAddressOptions{
		APIListenAddr: opts.APIListenAddr, MCPListenAddr: opts.MCPListenAddr,
		APIListenAddrFlagSet: apiFlagSet, MCPListenAddrFlagSet: mcpFlagSet,
	}, r.cli)
	return api, mcp, err
}

func (r RuntimeConfigLoadResult) ResolveServeAPIAuth(root InvocationRoot, opts ServeOptions) (apiv1.AuthTokenResolution, error) {
	if r.Config == nil {
		return apiv1.AuthTokenResolution{}, fmt.Errorf("admitted runtime configuration is required")
	}
	return resolveServeAPIAuthFromConfig(root, opts, r.cli)
}

func (r RuntimeConfigLoadResult) Detail() string {
	source := strings.TrimSpace(r.Source)
	if source == "" {
		source = "unknown"
	}
	path := strings.TrimSpace(r.Path)
	if path == "" {
		return source
	}
	return fmt.Sprintf("%s:%s", source, filepath.Clean(path))
}

func LoadRuntimeConfigWithOptions(opts RuntimeConfigLoadOptions) (RuntimeConfigLoadResult, error) {
	loaded, err := loadUnifiedConfig(unifiedConfigLoadOptions{
		RepoRoot:        opts.RepoRoot,
		ExplicitPath:    opts.ExplicitPath,
		BackendOverride: opts.BackendOverride,
	})
	result := RuntimeConfigLoadResult{
		Config:      loaded.Config,
		Source:      loaded.Source,
		Path:        loaded.Path,
		Layers:      loaded.Layers,
		KeyOrigins:  loaded.KeyOrigins,
		Diagnostics: loaded.Diagnostics,
		cli:         loaded.CLI,
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func executableAdjacentRuntimeConfigPath() (string, bool, error) {
	executable, err := runtimeConfigExecutablePath()
	if err != nil {
		return "", false, fmt.Errorf("resolve executable config path: %w", err)
	}
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return "", false, nil
	}
	path := filepath.Join(filepath.Dir(executable), "config.yaml")
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return "", false, fmt.Errorf("executable-adjacent runtime config %s is a directory", path)
		}
		return path, true, nil
	}
	if os.IsNotExist(err) {
		return "", false, nil
	}
	return "", false, fmt.Errorf("inspect executable-adjacent runtime config %s: %w", path, err)
}

func defaultRuntimeConfig() (*config.Config, error) {
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: true,
		},
		Database: config.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     5432,
			Name:     "swarm",
			User:     "postgres",
			SSLMode:  "disable",
			PoolSize: 5,
		},
		LLM: config.LLMConfig{
			Backend: llmselection.DefaultBackendID(),
			Session: config.LLMSessionConfig{
				LockTTL:               10 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              "claude",
				Timeout:              time.Hour,
				OutputFormat:         "stream-json",
				Retries:              1,
				NoSessionPersistence: false,
				UseTMux:              false,
			},
			OpenAICompatible: config.OpenAICompatibleConfig{},
		},
	}
	return cfg, nil
}

func DefaultRuntimeConfig() (*config.Config, error) {
	if err := validateSwarmEnvSources(swarmEnvGuardContext{}); err != nil {
		return nil, err
	}
	return defaultRuntimeConfig()
}
