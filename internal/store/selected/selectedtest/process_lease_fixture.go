package selectedtest

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// ProcessLeaseFixture projects the original native construction owner. It does
// not bootstrap implicitly, recover a pool, or grant SQL to fixture consumers.
type ProcessLeaseFixture struct {
	store.SchemaBootstrapper
	interfaceSchemaInspection
	storetest.RunFixtureStore
	startupownership.Store
	ChannelStore
	close func() error
}

type ChannelStore interface {
	channelonboarding.Store
	channelonboarding.TeardownStore
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
}

type interfaceSchemaInspection interface {
	InspectSchema(context.Context, store.SchemaBootstrapRequest) (store.SchemaInspection, error)
}

func (f *ProcessLeaseFixture) Close() error { return f.close() }

func OpenProcessLeaseFixture(ctx context.Context, scope backendselection.Selection, dsn string) (*ProcessLeaseFixture, error) {
	if scope.Backend == backendselection.BackendSQLite {
		owner, _, err := construction.OpenSQLiteRuntimeForServing(ctx, scope.SQLitePath)
		if err != nil {
			return nil, err
		}
		return &ProcessLeaseFixture{SchemaBootstrapper: owner, interfaceSchemaInspection: owner,
			RunFixtureStore: owner, Store: owner, ChannelStore: owner, close: owner.Close}, nil
	}
	if scope.Backend == backendselection.BackendPostgres {
		owner, _, err := construction.OpenPostgresRuntime(ctx, dsn)
		if err != nil {
			return nil, err
		}
		return &ProcessLeaseFixture{SchemaBootstrapper: owner, interfaceSchemaInspection: owner,
			RunFixtureStore: owner, Store: owner, ChannelStore: owner, close: owner.Close}, nil
	}
	return nil, errors.New("process lease fixture requires an exact supported backend")
}

func OpenProcessLeaseInspection(ctx context.Context, scope backendselection.Selection, dsn string) (*selected.AdmissionInspection, error) {
	return selected.OpenAdmissionInspection(ctx, selected.AuthorityRequest{Selection: scope, PostgresDSN: dsn})
}

// Inspection consumes the same native owner's canonical singleton reader, not
// the path a test may have replaced or a second generation decoder.
func InspectProcessLeaseSnapshot(ctx context.Context, owner startupownership.Store) (startupownership.AuthorityInspection, error) {
	inspection, ok := owner.(interface {
		InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
	})
	if !ok {
		return startupownership.AuthorityInspection{}, errors.New("native process inspection port is required")
	}
	return inspection.InspectAuthority(ctx)
}
