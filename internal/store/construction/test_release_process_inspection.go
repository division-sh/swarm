package construction

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// ReleaseProcessReadOnlyInspection retains an independently opened inspection
// lifetime. It cannot admit a runtime, mutate a store, or expose a SQL handle.
// Closed storage observations still validate the original native owner.
// All reads run inside the existing InspectSnapshot boundary; PostgreSQL binds
// its independent transport and both stores fence the snapshot's lifetime.
type ReleaseProcessReadOnlyInspection interface {
	pipeline.WorkflowTargetPersistenceReader
	InspectSnapshot(context.Context, func(context.Context) error) error
	Close() error
}

func OpenReleaseProcessReadOnlyInspectionForTest(backend, location string) (ReleaseProcessReadOnlyInspection, error) {
	if location == "" {
		return nil, fmt.Errorf("release-process inspection requires the original nonempty location")
	}
	switch backend {
	case "sqlite":
		selected, err := OpenSQLiteRuntimeReadOnly(location)
		if err != nil {
			return nil, err
		}
		return selected, nil
	case "postgres":
		selected, err := OpenPostgresReadOnly(location)
		if err != nil {
			return nil, err
		}
		return selected, nil
	default:
		return nil, fmt.Errorf("unsupported release-process inspection backend %q", backend)
	}
}
