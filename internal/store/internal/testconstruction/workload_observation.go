package testconstruction

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/store"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

// Reopen the fixture's original location through the native construction owner.
// This cannot bootstrap, acquire runtime possession, or recover a running pool.
func OpenIssue2564WorkloadObservationForTest(ctx context.Context, backend, location string, request private.SchemaBootstrapRequest) (*private.Issue2564WorkloadObservation, error) {
	var selected interface {
		private.Issue2564WorkloadReader
		InspectSchema(context.Context, private.SchemaBootstrapRequest) (private.SchemaInspection, error)
	}
	var err error
	switch backend {
	case "postgres":
		// H2 retains a sampler connection beside independent snapshot reads;
		// the one-connection, snapshot-scoped inspection transport cannot do both.
		selected, err = store.NewPostgresStore(location)
	case "sqlite":
		selected, err = store.NewSQLiteRuntimeStore(location)
	default:
		return nil, fmt.Errorf("unknown issue2564 observation backend %q", backend)
	}
	if err != nil {
		return nil, err
	}
	inspection, err := selected.InspectSchema(ctx, request)
	if err != nil {
		return nil, errors.Join(err, selected.Close())
	}
	if inspection.Fresh || len(inspection.MissingStateTables) != 0 {
		return nil, errors.Join(fmt.Errorf("H2 native observation requires the child's completed canonical boot"), selected.Close())
	}
	return &private.Issue2564WorkloadObservation{Reader: selected}, nil
}
