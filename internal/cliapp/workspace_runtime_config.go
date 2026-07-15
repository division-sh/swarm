package cliapp

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func workspaceConfigValue(raw string, configured bool, key string) (string, bool, error) {
	if !configured && strings.TrimSpace(raw) == "" {
		return "", false, nil
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", true, fmt.Errorf("%s must be non-empty when configured", key)
	}
	return value, true, nil
}

func runtimeConfigWorkspaceImage(cfg *config.Config) (string, bool, error) {
	if cfg == nil {
		return "", false, nil
	}
	return workspaceConfigValue(cfg.Workspace.Image, cfg.Workspace.ImageConfigured(), "workspace.image")
}

func runtimeConfigWorkspaceDockerBin(cfg *config.Config) (string, bool, error) {
	if cfg == nil {
		return "", false, nil
	}
	return workspaceConfigValue(cfg.Workspace.DockerBin, cfg.Workspace.DockerBinConfigured(), "workspace.docker_bin")
}

func runtimeConfigWorkspaceHostRoot(cfg *config.Config) (string, bool, error) {
	if cfg == nil {
		return "", false, nil
	}
	return workspaceConfigValue(cfg.Workspace.HostRoot, cfg.Workspace.HostRootConfigured(), "workspace.host_root")
}

func runtimeConfigWorkspaceVolumesFrom(cfg *config.Config) (string, bool, error) {
	if cfg == nil {
		return "", false, nil
	}
	return workspaceConfigValue(cfg.Workspace.VolumesFrom, cfg.Workspace.VolumesFromConfigured(), "workspace.volumes_from")
}

func runtimeConfigWorkspaceNetwork(cfg *config.Config) (string, bool, error) {
	if cfg == nil {
		return "", false, nil
	}
	return workspaceConfigValue(cfg.Workspace.Network, cfg.Workspace.NetworkConfigured(), "workspace.network")
}

func workspaceImageFromRuntimeConfigOrDefault(cfg *config.Config) (string, error) {
	image, ok, err := runtimeConfigWorkspaceImage(cfg)
	if err != nil {
		return "", err
	}
	if ok {
		return image, nil
	}
	return workspace.DefaultWorkspaceImage(), nil
}

func workspaceDockerBinFromRuntimeConfigOrDefault(cfg *config.Config) (string, error) {
	dockerBin, ok, err := runtimeConfigWorkspaceDockerBin(cfg)
	if err != nil {
		return "", err
	}
	if ok {
		return dockerBin, nil
	}
	return workspace.DefaultDockerBin(), nil
}

func dockerWorkspaceConfigFromRuntimeConfig(cfg *config.Config) (workspace.DockerConfig, error) {
	out := workspace.DefaultDockerConfig()
	if image, ok, err := runtimeConfigWorkspaceImage(cfg); err != nil {
		return out, err
	} else if ok {
		out.WorkspaceImage = image
	}
	if dockerBin, ok, err := runtimeConfigWorkspaceDockerBin(cfg); err != nil {
		return out, err
	} else if ok {
		out.DockerBin = dockerBin
	}
	if network, ok, err := runtimeConfigWorkspaceNetwork(cfg); err != nil {
		return out, err
	} else if ok {
		out.WorkspaceNetwork = network
	}
	if volumesFrom, ok, err := runtimeConfigWorkspaceVolumesFrom(cfg); err != nil {
		return out, err
	} else if ok {
		out.WorkspaceVolumesFrom = volumesFrom
	}
	return out, nil
}

func hostWorkspaceConfigFromRuntimeConfig(cfg *config.Config) (workspace.HostConfig, error) {
	out := workspace.DefaultHostConfig()
	if hostRoot, ok, err := runtimeConfigWorkspaceHostRoot(cfg); err != nil {
		return out, err
	} else if ok {
		out.WorkspaceRoot = hostRoot
	}
	return out, nil
}
