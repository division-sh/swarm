package selected

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type AuthorityRequest struct {
	Selection   storebackend.Selection
	PostgresDSN string
}

type authorityInspectionPort interface {
	InspectSnapshot(context.Context, func(context.Context) error) error
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
	InspectStartupRecovery(context.Context, time.Time) (effects.StartupRecoveryCounts, error)
	ProbePossession(context.Context) (runtimestartupownership.PossessionObservation, error)
}

// AuthorityInspection exposes only read-only authority inspection.
type AuthorityInspection struct {
	store authorityInspectionPort
	close closeResource
}

func OpenAuthorityInspection(ctx context.Context, req AuthorityRequest) (*AuthorityInspection, error) {
	selected, err := openInspectionStore(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AuthorityInspection{store: selected, close: selected}, nil
}

func (s *AuthorityInspection) InspectAuthority(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	if s == nil || s.store == nil {
		return runtimestartupownership.AuthorityInspection{}, errors.New("authority inspection store is required")
	}
	inspection, err := s.store.InspectAuthority(ctx)
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	return s.inspectPossession(ctx, inspection)
}

func (s *AuthorityInspection) InspectStartupStatus(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	if s == nil || s.store == nil {
		return runtimestartupownership.AuthorityInspection{}, errors.New("authority inspection store is required")
	}
	var inspection runtimestartupownership.AuthorityInspection
	err := s.store.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		inspection, err = s.store.InspectAuthority(ctx)
		if err != nil || inspection.Authority == nil {
			return err
		}
		counts, err := s.store.InspectStartupRecovery(ctx, time.Now().UTC())
		if err == nil {
			inspection.Interrupted = &counts
		}
		return err
	})
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	return s.inspectPossession(ctx, inspection)
}

func (s *AuthorityInspection) inspectPossession(ctx context.Context, inspection runtimestartupownership.AuthorityInspection) (runtimestartupownership.AuthorityInspection, error) {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	observation, err := s.store.ProbePossession(bounded)
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	inspection.Possession = &observation
	return inspection, inspection.Validate()
}

func (s *AuthorityInspection) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close.Close()
}
