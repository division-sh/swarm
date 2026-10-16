package startupownership

import (
	"context"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type DisplacedHTTPActivityRecovery interface {
	ReconcileDisplacedHTTPActivitiesTx(context.Context, *mutationprotocol.Attempt, DisplacedHTTPActivityCut) (pipeline.ActivityRecoverySummary, error)
}

// The original writer and the recovery adapter must be one native owner. A
// separately supplied historical reader cannot grant recovery permission.
type displacedHTTPActivityJournal interface {
	runtimeownership.ActivityJournalGrantBinder
	DisplacedHTTPActivityRecovery
}

func activityJournalRoleMissing(role displacedHTTPActivityJournal) bool {
	if role == nil {
		return true
	}
	value := reflect.ValueOf(role)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *StartupPostgresOwner) BindActivityJournalRole(role displacedHTTPActivityJournal) error {
	if s == nil || activityJournalRoleMissing(role) || s.activityJournal != nil {
		return errors.New("startup PostgreSQL activity journal must be bound exactly once")
	}
	s.activityJournal = role
	return nil
}

func (s *StartupSQLiteOwner) BindActivityJournalRole(role displacedHTTPActivityJournal) error {
	if s == nil || activityJournalRoleMissing(role) || s.activityJournal != nil {
		return errors.New("startup SQLite activity journal must be bound exactly once")
	}
	s.activityJournal = role
	return nil
}

type retainedActivityJournalBinder struct {
	role        displacedHTTPActivityJournal
	coordinates processbinding.LeaseCoordinates
}

func newRetainedActivityJournalBinder(authority runtimeownership.Authority, role displacedHTTPActivityJournal) (runtimeownership.ActivityJournalGrantBinder, error) {
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	if activityJournalRoleMissing(role) {
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
	return newRetainedActivityJournalBinder(authority, s.owner.activityJournal)
}

func (s *sqliteSession) ActivityJournalBinder() (runtimeownership.ActivityJournalGrantBinder, error) {
	authority, err := s.Authority()
	if err != nil {
		return nil, err
	}
	return newRetainedActivityJournalBinder(authority, s.owner.activityJournal)
}

func (s *postgresSession) ReconcileDisplacedHTTPActivities(ctx context.Context) (pipeline.ActivityRecoverySummary, error) {
	authority, err := s.Authority()
	if err != nil {
		return pipeline.ActivityRecoverySummary{}, err
	}
	role := s.owner.activityJournal
	if activityJournalRoleMissing(role) {
		return pipeline.ActivityRecoverySummary{}, errors.New("retained PostgreSQL activity recovery role is not bound")
	}
	commit := mutationprotocol.RunRetainedPostgres(ctx, s.lease.Session(), mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (pipeline.ActivityRecoverySummary, error) {
		cut, err := newDisplacedHTTPActivityCut(ctx, mutation, authority.LeaseStamp(), false)
		if err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		return role.ReconcileDisplacedHTTPActivitiesTx(ctx, mutation, cut)
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
	if activityJournalRoleMissing(role) {
		return pipeline.ActivityRecoverySummary{}, errors.New("retained SQLite activity recovery role is not bound")
	}
	commit := mutationprotocol.RunSQLite(ctx, s.owner.backend, "recover displaced HTTP activities", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, mutation *mutationprotocol.Attempt) (pipeline.ActivityRecoverySummary, error) {
		cut, err := newDisplacedHTTPActivityCut(ctx, mutation, authority.LeaseStamp(), true)
		if err != nil {
			return pipeline.ActivityRecoverySummary{}, err
		}
		return role.ReconcileDisplacedHTTPActivitiesTx(ctx, mutation, cut)
	})
	result, ok := commit.Value()
	if !ok {
		return pipeline.ActivityRecoverySummary{}, commit.Err()
	}
	return result, commit.Err()
}
