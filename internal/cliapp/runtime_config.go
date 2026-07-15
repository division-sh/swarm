package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

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

func loadRuntimeConfig(path string) (*config.Config, error) {
	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{ExplicitPath: path})
	if err != nil {
		return nil, err
	}
	return result.Config, nil
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
	if err := rejectUnsupportedRuntimeControlEnv(); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredEnvBackend(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredEnvRuntimeMode(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredOpenAICompatibleBaseURLEnv(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredModelEnv(os.LookupEnv); err != nil {
		return nil, err
	}
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: false,
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
			ClaudeAPI: config.ClaudeAPIConfig{},
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func DefaultRuntimeConfig() (*config.Config, error) {
	return defaultRuntimeConfig()
}

func rejectUnsupportedRuntimeControlEnv() error {
	unsupported := make([]string, 0, 2)
	if strings.TrimSpace(os.Getenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS")) != "" {
		unsupported = append(unsupported, "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS")
	}
	if strings.TrimSpace(os.Getenv("SWARM_RUNTIME_EVENT_POLL_INTERVAL")) != "" {
		unsupported = append(unsupported, "SWARM_RUNTIME_EVENT_POLL_INTERVAL")
	}
	if len(unsupported) == 0 {
		return nil
	}
	sort.Strings(unsupported)
	return fmt.Errorf("unsupported inert runtime controls configured: %s", strings.Join(unsupported, ", "))
}
