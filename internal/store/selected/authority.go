package selected

import (
	"context"
	"errors"
	"fmt"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
)

type AuthorityRequest struct {
	Selection   storebackend.Selection
	PostgresDSN string
}

type authorityInspectionPort interface {
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
}

// AuthorityInspection exposes only read-only authority inspection.
type AuthorityInspection struct {
	store authorityInspectionPort
	close closeResource
}

func OpenAuthorityInspection(ctx context.Context, req AuthorityRequest) (*AuthorityInspection, error) {
	switch req.Selection.Backend {
	case storebackend.BackendSQLite:
		selected, err := storeconstruction.OpenSQLiteRuntimeReadOnly(req.Selection.SQLitePath)
		if err != nil {
			return nil, err
		}
		if err := selected.Ping(ctx); err != nil {
			return nil, errors.Join(err, selected.Close())
		}
		return &AuthorityInspection{store: selected, close: selected}, nil
	case storebackend.BackendPostgres:
		selected, _, err := storeconstruction.OpenPostgres(req.PostgresDSN)
		if err != nil {
			return nil, err
		}
		if err := selected.Ping(ctx); err != nil {
			return nil, errors.Join(err, selected.Close())
		}
		return &AuthorityInspection{store: selected, close: selected}, nil
	default:
		return nil, errors.New("selected store backend is required")
	}
}

func (s *AuthorityInspection) InspectAuthority(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	if s == nil || s.store == nil {
		return runtimestartupownership.AuthorityInspection{}, errors.New("authority inspection store is required")
	}
	return s.store.InspectAuthority(ctx)
}

func (s *AuthorityInspection) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close.Close()
}

type authorityMaintenancePort interface {
	runtimestartupownership.AuthorityMaintenanceStore
	store.SchemaBootstrapper
}

// AuthorityMaintenance exposes the mutable repair purpose without making its
// ports reachable from read-only inspection construction.
type AuthorityMaintenance struct {
	store authorityMaintenancePort
	close closeResource
}

func OpenAuthorityMaintenance(ctx context.Context, req AuthorityRequest) (*AuthorityMaintenance, error) {
	switch req.Selection.Backend {
	case storebackend.BackendSQLite:
		selected, _, err := storeconstruction.OpenSQLiteRuntimeWithOwnershipBinding(req.Selection.SQLitePath)
		if err != nil {
			return nil, err
		}
		if err := selected.Ping(ctx); err != nil {
			return nil, errors.Join(err, selected.Close())
		}
		return &AuthorityMaintenance{store: selected, close: selected}, nil
	case storebackend.BackendPostgres:
		selected, _, err := storeconstruction.OpenPostgres(req.PostgresDSN)
		if err != nil {
			return nil, err
		}
		if err := selected.Ping(ctx); err != nil {
			return nil, errors.Join(err, selected.Close())
		}
		return &AuthorityMaintenance{store: selected, close: selected}, nil
	default:
		return nil, errors.New("selected store backend is required")
	}
}

func (s *AuthorityMaintenance) InspectAuthority(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	if s == nil || s.store == nil {
		return runtimestartupownership.AuthorityInspection{}, errors.New("authority maintenance store is required")
	}
	return s.store.InspectAuthority(ctx)
}

func (s *AuthorityMaintenance) RepairAuthority(ctx context.Context, req runtimestartupownership.AuthorityRepairRequest) (runtimestartupownership.AuthorityRepairResult, error) {
	if s == nil || s.store == nil {
		return runtimestartupownership.AuthorityRepairResult{}, errors.New("authority maintenance store is required")
	}
	return s.store.RepairAuthority(ctx, req)
}

func (s *AuthorityMaintenance) BootstrapSchema(ctx context.Context, req store.SchemaBootstrapRequest) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("authority maintenance store is required")
	}
	return s.store.BootstrapSchema(ctx, req)
}

func (s *AuthorityMaintenance) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close.Close()
}
