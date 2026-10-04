package workspace

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func validateWorkspaceSourceClasses(source semanticview.Source) error {
	if source == nil {
		return fmt.Errorf("workspace semantic source is required")
	}
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		return err
	}
	return validateAgentWorkspaceClasses(source, classes)
}

func validateDockerMountAdmission(cfg DockerConfig) error {
	if strings.TrimSpace(cfg.WorkspaceVolumesFrom) != "" || strings.TrimSpace(cfg.SharedDataSource) != "" {
		return fmt.Errorf("workspace.data_source and workspace.volumes_from are unsupported")
	}
	return nil
}

func ValidateDockerSourceAdmission(cfg DockerConfig, source semanticview.Source) error {
	if err := validateDockerMountAdmission(cfg); err != nil {
		return err
	}
	if err := validateWorkspaceSourceClasses(source); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.WorkspaceImage) == "" {
		return fmt.Errorf("workspace validation failed: workspace image is required")
	}
	return nil
}

func validateHostMountAdmission(cfg HostConfig) (string, error) {
	if strings.TrimSpace(cfg.SharedDataSource) != "" {
		return "", fmt.Errorf("workspace.data_source is unsupported")
	}
	return hostWorkspaceRoot(cfg)
}

func ValidateHostSourceAdmission(cfg HostConfig, source semanticview.Source) error {
	if _, err := validateHostMountAdmission(cfg); err != nil {
		return err
	}
	return validateWorkspaceSourceClasses(source)
}
