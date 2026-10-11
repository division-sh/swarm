package selected

import (
	"context"
	"errors"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type AuthorityRequest struct {
	Selection   storebackend.Selection
	PostgresDSN string
}

type authorityInspectionPort interface {
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
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
	observation, err := s.store.ProbePossession(ctx)
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
