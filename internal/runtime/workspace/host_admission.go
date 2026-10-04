package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type HostPrerequisiteInspection struct {
	RootPath         string
	ExistingAncestor string
	RequiresCreation bool
}

// InspectHostPrerequisites reads the same root boot prepares. It tests access
// with the effective process identity, not guessed permission-mode arithmetic.
func InspectHostPrerequisites(ctx context.Context, cfg HostConfig) (HostPrerequisiteInspection, error) {
	if err := ctx.Err(); err != nil {
		return HostPrerequisiteInspection{}, err
	}
	root, err := validateHostMountAdmission(cfg)
	if err != nil {
		return HostPrerequisiteInspection{}, err
	}
	current := root
	for {
		if err := ctx.Err(); err != nil {
			return HostPrerequisiteInspection{}, err
		}
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return HostPrerequisiteInspection{}, fmt.Errorf("host workspace prerequisite %s is not a directory", current)
			}
			if err := inspectHostDirectoryAccess(current); err != nil {
				return HostPrerequisiteInspection{}, fmt.Errorf("host workspace prerequisite %s is not writable/searchable: %w", current, err)
			}
			if err := ctx.Err(); err != nil {
				return HostPrerequisiteInspection{}, err
			}
			return HostPrerequisiteInspection{RootPath: root, ExistingAncestor: current, RequiresCreation: current != root}, nil
		}
		if !os.IsNotExist(err) {
			return HostPrerequisiteInspection{}, fmt.Errorf("inspect host workspace prerequisite %s: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return HostPrerequisiteInspection{}, fmt.Errorf("host workspace root has no inspectable existing ancestor")
		}
		current = parent
	}
}
