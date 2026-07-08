package llm

import (
	"strings"

	"github.com/division-sh/swarm/internal/config"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func configuredWorkspaceDockerBin(cfg *config.Config) string {
	if cfg != nil {
		if dockerBin := strings.TrimSpace(cfg.Workspace.DockerBin); dockerBin != "" {
			return dockerBin
		}
	}
	return workspace.DefaultDockerBin()
}

func configuredWorkspaceImage(cfg *config.Config) string {
	if cfg != nil {
		if image := strings.TrimSpace(cfg.Workspace.Image); image != "" {
			return image
		}
	}
	return workspace.DefaultWorkspaceImage()
}
