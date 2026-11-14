package selected

import (
	"context"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/construction"
)

// BootCheckInput carries observations only. It neither retains possession nor
// substitutes for construction's own exclusion and process admission.
type BootCheckInput struct {
	sqlite    *construction.SQLiteInspectionInput
	request   store.SchemaBootstrapRequest
	schema    store.SchemaInspection
	authority startupownership.AuthorityInspection
	absent    bool
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
	if i == nil {
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
	if !reflect.DeepEqual(current, i.schema) {
		return errors.New("selected store schema changed after boot checks")
	}
	if current.Fresh {
		return nil
	}
	authority, err := reader.InspectAuthority(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(authority, i.authority) {
		return errors.New("selected store generation changed after boot checks")
	}
	return nil
}
