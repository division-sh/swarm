package tools

import (
	"strings"

	"github.com/division-sh/swarm/internal/config"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func workspaceDockerBin(cfg *config.Config) string {
	if cfg != nil {
		if dockerBin := strings.TrimSpace(cfg.Workspace.DockerBin); dockerBin != "" {
			return dockerBin
		}
	}
	return workspace.DefaultDockerBin()
}
