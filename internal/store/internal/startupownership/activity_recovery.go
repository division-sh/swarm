package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *StartupPostgresOwner) BindActivityJournalRole(role runtimeownership.ActivityJournalGrantBinder) error {
	native, ok := role.(*activityjournal.ActivityPostgresOwner)
	if s == nil || !ok || native == nil || s.activityJournal != nil {
		return errors.New("startup PostgreSQL activity journal must be bound exactly once")
	}
	s.activityJournal = native
	return nil
}

func (s *StartupSQLiteOwner) BindActivityJournalRole(role runtimeownership.ActivityJournalGrantBinder) error {
	native, ok := role.(*activityjournal.ActivitySQLiteOwner)
	if s == nil || !ok || native == nil || s.activityJournal != nil {
		return errors.New("startup SQLite activity journal must be bound exactly once")
	}
	s.activityJournal = native
	return nil
}

type retainedActivityJournalBinder struct {
	role        runtimeownership.ActivityJournalGrantBinder
	coordinates processbinding.LeaseCoordinates
}

func newRetainedActivityJournalBinder(authority runtimeownership.Authority, role runtimeownership.ActivityJournalGrantBinder) (runtimeownership.ActivityJournalGrantBinder, error) {
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	if role == nil {
		return nil, errors.New("retained process activity journal is not bound")
	}
	return retainedActivityJournalBinder{role: role, coordinates: authority.LeaseStamp().Coordinates()}, nil
}

func (b retainedActivityJournalBinder) BindActivityGrant(grant runtimeownership.GrantEvidence) (pipeline.ActivityAttemptJournal, error) {
	if err := grant.Validate(); err != nil {
		return nil, err
	}
	if grant.State != runtimeownership.GrantAdmitted || grant.ProcessCoordinates() != b.coordinates {
		return nil, errors.New("activity execution grant differs from its retained process session")
	}
	return b.role.BindActivityGrant(grant)
}

func (s *postgresSession) ActivityJournalBinder() (runtimeownership.ActivityJournalGrantBinder, error) {
	authority, err := s.Authority()
	if err != nil {
		return nil, err
	}
	if s.owner.activityJournal == nil {
		return nil, errors.New("retained process activity journal is not bound")
	}
	return newRetainedActivityJournalBinder(authority, s.owner.activityJournal)
}

func (s *sqliteSession) ActivityJournalBinder() (runtimeownership.ActivityJournalGrantBinder, error) {
	authority, err := s.Authority()
	if err != nil {
		return nil, err
	}
	if s.owner.activityJournal == nil {
		return nil, errors.New("retained process activity journal is not bound")
	}
	return newRetainedActivityJournalBinder(authority, s.owner.activityJournal)
}

func (s *postgresSession) ReconcileDisplacedHTTPActivities(ctx context.Context) (pipeline.ActivityRecoverySummary, error) {
	authority, err := s.Authority()
	if err != nil {
		return pipeline.ActivityRecoverySummary{}, err
	}
	role := s.owner.activityJournal
	if role == nil {
		return pipeline.ActivityRecoverySummary{}, errors.New("retained PostgreSQL activity recovery role is not bound")
	}
	commit := mutationprotocol.RunRetainedPostgres(ctx, s.lease.Session(), mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (pipeline.ActivityRecoverySummary, error) {
		cut, err := newDisplacedHTTPActivityCut(ctx, mutation, authority.LeaseStamp(), false)
		if err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		if err := role.ValidateDisplacedRecovery(); err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		return reconcileDisplacedHTTPActivities(ctx, mutation, cut, activityjournal.DialectPostgres)
	})
	result, ok := commit.Value()
	if !ok {
		return pipeline.ActivityRecoverySummary{}, commit.Err()
	}
	return result, commit.Err()
}

func (s *sqliteSession) ReconcileDisplacedHTTPActivities(ctx context.Context) (pipeline.ActivityRecoverySummary, error) {
	authority, err := s.Authority()
	if err != nil {
		return pipeline.ActivityRecoverySummary{}, err
	}
	role := s.owner.activityJournal
	if role == nil {
		return pipeline.ActivityRecoverySummary{}, errors.New("retained SQLite activity recovery role is not bound")
	}
	commit := mutationprotocol.RunSQLite(ctx, s.owner.backend, "recover displaced HTTP activities", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (pipeline.ActivityRecoverySummary, error) {
		cut, err := newDisplacedHTTPActivityCut(ctx, mutation, authority.LeaseStamp(), true)
		if err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		if err := role.ValidateDisplacedRecovery(); err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		return reconcileDisplacedHTTPActivities(ctx, mutation, cut, activityjournal.DialectSQLite)
	})
	result, ok := commit.Value()
	if !ok {
		return pipeline.ActivityRecoverySummary{}, commit.Err()
	}
	return result, commit.Err()
}
