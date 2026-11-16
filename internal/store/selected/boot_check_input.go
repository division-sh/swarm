package selected

import (
	"context"
	"errors"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/construction"
)

// BootCheckInput carries either an inspection observation or the fixed stage
// that native construction invokes. Neither grants possession or admission.
type BootCheckInput struct {
	checks    bootverify.ConstructionChecks
	sqlite    *construction.SQLiteInspectionInput
	request   store.SchemaBootstrapRequest
	schema    store.SchemaInspection
	authority startupownership.AuthorityInspection
	absent    bool
}

func ConstructionCheckInput(checks bootverify.ConstructionChecks) *BootCheckInput {
	return &BootCheckInput{checks: checks}
}

func PostgresCheckTarget(dsn string) (bootverify.ConstructionTarget, error) {
	return construction.PostgresCheckTarget(dsn)
}

func (e *AbsentSQLiteStore) BootInput(request store.SchemaBootstrapRequest) *BootCheckInput {
	if e == nil || e.input == nil {
		return nil
	}
	return &BootCheckInput{sqlite: e.input, request: request, absent: true}
}

func (s *AdmissionSnapshot) CaptureBootInput(ctx context.Context) (*BootCheckInput, error) {
	if s == nil || s.ctx == nil || s.ctx.Err() != nil {
		return nil, errors.New("selected boot inspection is not current")
	}
	input := &BootCheckInput{sqlite: s.sqlite, request: s.request, schema: s.schema}
	if !s.fresh {
		var err error
		input.authority, err = s.InspectAuthority(ctx)
		if err != nil {
			return nil, err
		}
	}
	return input, nil
}

type bootCheckReader interface {
	InspectSchema(context.Context, store.SchemaBootstrapRequest) (store.SchemaInspection, error)
	InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
}

func (i *BootCheckInput) revalidate(ctx context.Context, reader bootCheckReader) error {
	if i == nil || i.checks != nil {
		return nil
	}
	current, err := reader.InspectSchema(ctx, i.request)
	if err != nil {
		return err
	}
	if i.absent {
		if current.Fresh {
			return nil
		}
		return errors.New("selected store appeared after boot checks")
	}
	if current.Target != i.schema.Target || current.Fresh != i.schema.Fresh ||
		!sameBootCheckValue(current.Origin, i.schema.Origin) ||
		(current.MissingDeploymentTables == nil) != (i.schema.MissingDeploymentTables == nil) ||
		!slices.Equal(current.MissingDeploymentTables, i.schema.MissingDeploymentTables) {
		return errors.New("selected store schema changed after boot checks")
	}
	if current.Fresh {
		return nil
	}
	authority, err := reader.InspectAuthority(ctx)
	if err != nil {
		return err
	}
	if authority.Status != i.authority.Status || authority.Backend != i.authority.Backend ||
		!sameBootCheckValue(authority.Authority, i.authority.Authority) ||
		!sameBootCheckValue(authority.Possession, i.authority.Possession) ||
		!sameBootCheckValue(authority.Interrupted, i.authority.Interrupted) {
		return errors.New("selected store generation changed after boot checks")
	}
	return nil
}

func sameBootCheckValue[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
