package main

import (
	"fmt"
	"os"
	"strings"

	"swarm/internal/config"
	workspace "swarm/internal/runtime/workspace"
)

const (
	envWorkspaceBackend     = "SWARM_WORKSPACE_BACKEND"
	defaultWorkspaceBackend = workspace.BackendDocker
)

type workspaceBackendSelection struct {
	Backend string
	Source  string
}

type workspaceBackendInput struct {
	FlagBackend string
	FlagSet     bool

	ConfigBackend string
	ConfigSet     bool

	EnvBackend string
	EnvSet     bool
}

func resolveWorkspaceBackend(flagBackend string, flagSet bool, cfg *config.Config) (workspaceBackendSelection, error) {
	envBackend, envSet := os.LookupEnv(envWorkspaceBackend)
	configBackend, configSet := runtimeConfigWorkspaceBackend(cfg)
	return resolveWorkspaceBackendFromInput(workspaceBackendInput{
		FlagBackend:   flagBackend,
		FlagSet:       flagSet,
		ConfigBackend: configBackend,
		ConfigSet:     configSet,
		EnvBackend:    envBackend,
		EnvSet:        envSet,
	})
}

func resolveWorkspaceBackendFromInput(in workspaceBackendInput) (workspaceBackendSelection, error) {
	switch {
	case in.FlagSet:
		backend, err := normalizeWorkspaceBackend(in.FlagBackend, "--workspace-backend")
		return workspaceBackendSelection{Backend: backend, Source: "--workspace-backend"}, err
	case in.ConfigSet:
		backend, err := normalizeWorkspaceBackend(in.ConfigBackend, "workspace.backend")
		return workspaceBackendSelection{Backend: backend, Source: "workspace.backend"}, err
	case in.EnvSet:
		backend, err := normalizeWorkspaceBackend(in.EnvBackend, envWorkspaceBackend)
		return workspaceBackendSelection{Backend: backend, Source: envWorkspaceBackend}, err
	default:
		return workspaceBackendSelection{Backend: defaultWorkspaceBackend, Source: "default"}, nil
	}
}

func runtimeConfigWorkspaceBackend(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	return cfg.Workspace.Backend, cfg.Workspace.BackendConfigured()
}

func normalizeWorkspaceBackend(raw string, source string) (string, error) {
	backend := strings.ToLower(strings.TrimSpace(raw))
	if backend == "" {
		return "", fmt.Errorf("workspace backend from %s must be non-empty", source)
	}
	switch backend {
	case workspace.BackendDocker, workspace.BackendHost:
		return backend, nil
	default:
		return "", fmt.Errorf("workspace backend from %s must be docker or host, got %q", source, strings.TrimSpace(raw))
	}
}
