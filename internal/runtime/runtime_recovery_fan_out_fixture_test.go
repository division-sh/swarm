package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// The diagnostic fixture already models zero fan-out obligations. Supply its
// explicit serving roles without making every synthetic process session eligible.
type startupRecoveryFanOutSession struct {
	*runtimeTestRetainedSession
	capacity           startupownership.FanOutCapacity
	boundActivityGrant string
}

func (s *startupRecoveryFanOutSession) ActivityJournalBinder() (startupownership.ActivityJournalGrantBinder, error) {
	if err := s.ProveCurrent(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *startupRecoveryFanOutSession) BindActivityGrant(grant startupownership.GrantEvidence) (pipeline.ActivityAttemptJournal, error) {
	if err := grant.Validate(); err != nil {
		return nil, err
	}
	raw, err := canonicaljson.Bytes(grant)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || grant.State != startupownership.GrantAdmitted || s.grants[grant.GrantID] != string(raw) {
		return nil, errors.New("diagnostic fixture requires its exact admitted activity grant")
	}
	s.boundActivityGrant = string(raw)
	return startupRecoveryDiagnosticJournal{}, nil
}

func (s *startupRecoveryFanOutSession) requireActivityGrant(t *testing.T, grant startupownership.GenerationGrant) {
	t.Helper()
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Bytes(evidence)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.boundActivityGrant != string(raw) {
		t.Fatal("lifecycle fixture did not bind its own exact admitted journal grant")
	}
}

// These lifecycle-only controls never execute an HTTP activity. Supplying their
// captured role must not grant synthetic persistence or recovery permission.
type startupRecoveryDiagnosticJournal struct{}

func (startupRecoveryDiagnosticJournal) StartActivityAttempt(context.Context, pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return pipeline.ActivityAttemptRecord{}, false, errors.New("lifecycle diagnostic fixture has no HTTP activity execution")
}

func (j startupRecoveryDiagnosticJournal) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.StartActivityAttempt(ctx, record)
}

func (j startupRecoveryDiagnosticJournal) CompleteActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.StartActivityAttempt(ctx, record)
}

func (j startupRecoveryDiagnosticJournal) MarkActivityAttemptUncertain(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.StartActivityAttempt(ctx, record)
}

func (j startupRecoveryDiagnosticJournal) LoadActivityAttempt(ctx context.Context, _ string) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{})
}

func startupRecoveryFanOutSessionForTest(t testing.TB, capacity startupownership.FanOutCapacity) func(*runtimeTestRetainedSession) startupownership.RetainedSession {
	t.Helper()
	return func(session *runtimeTestRetainedSession) startupownership.RetainedSession {
		return &startupRecoveryFanOutSession{runtimeTestRetainedSession: session, capacity: capacity}
	}
}

func (s *startupRecoveryFanOutSession) FanOutServingCapacity() (startupownership.FanOutCapacity, error) {
	if err := s.ProveCurrent(context.Background()); err != nil {
		return startupownership.FanOutCapacity{}, err
	}
	return s.capacity, nil
}

func (s *startupRecoveryFanOutSession) FanOutServingStore() (startupownership.FanOutServingStore, error) {
	if err := s.ProveCurrent(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *startupRecoveryFanOutSession) NextFanOutCandidate(ctx context.Context, observations []startupownership.FanOutRegistrationObservation, _ []fanoutobligation.IntentKey) (startupownership.FanOutCandidate, bool, error) {
	if err := ctx.Err(); err != nil {
		return startupownership.FanOutCandidate{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return startupownership.FanOutCandidate{}, false, errors.New("diagnostic fixture session is released")
	}
	for _, observation := range observations {
		grant := observation.Grant
		if err := grant.Validate(); err != nil {
			return startupownership.FanOutCandidate{}, false, err
		}
		raw, err := canonicaljson.Bytes(grant)
		if err != nil {
			return startupownership.FanOutCandidate{}, false, err
		}
		if grant.State != startupownership.GrantAdmitted || s.grants[grant.GrantID] != string(raw) {
			return startupownership.FanOutCandidate{}, false, errors.New("diagnostic fixture requires its exact admitted grant")
		}
	}
	return startupownership.FanOutCandidate{}, false, nil
}

func (*startupRecoveryFanOutSession) BindFanOutGrant(startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	return nil, errors.New("startup recovery diagnostic fixture has no fan-out mutation owner")
}

func TestStartupRecoveryFanOutFixtureRefusesUnownedWork(t *testing.T) {
	session := newRuntimeTestRetainedSession(t)
	fixture := &startupRecoveryFanOutSession{runtimeTestRetainedSession: session, capacity: startupownership.SQLiteFanOutCapacity()}
	if _, present := any(session).(interface {
		FanOutServingStore() (startupownership.FanOutServingStore, error)
	}); present {
		t.Fatal("plain synthetic session gained implicit serving permission")
	}
	if _, present := any(session).(interface {
		ActivityJournalBinder() (startupownership.ActivityJournalGrantBinder, error)
	}); present {
		t.Fatal("plain synthetic session gained implicit journal binding")
	}
	if _, err := fixture.BindActivityGrant(startupownership.GrantEvidence{}); err == nil {
		t.Fatal("diagnostic fixture accepted an unowned activity grant")
	}
	if _, _, err := (startupRecoveryDiagnosticJournal{}).ClaimActivityAttemptForLoopGeneration(context.Background(), pipeline.ActivityAttemptRecord{}); err == nil {
		t.Fatal("lifecycle diagnostic fixture gained activity execution")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, found, err := fixture.NextFanOutCandidate(ctx, nil, nil); !errors.Is(err, context.Canceled) || found {
		t.Fatalf("canceled: found=%v err=%v", found, err)
	}
	if _, found, err := fixture.NextFanOutCandidate(context.Background(), []startupownership.FanOutRegistrationObservation{{}}, nil); err == nil || found {
		t.Fatalf("unowned: found=%v err=%v", found, err)
	}
	if owner, err := fixture.BindFanOutGrant(startupownership.GrantEvidence{}); err == nil || owner != nil {
		t.Fatal("fixture granted mutation authority")
	}
	if err := session.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := fixture.NextFanOutCandidate(context.Background(), nil, nil); err == nil || found {
		t.Fatalf("released: found=%v err=%v", found, err)
	}
	if _, err := fixture.FanOutServingStore(); err == nil {
		t.Fatal("released session supplied serving store")
	}
	if _, err := fixture.FanOutServingCapacity(); err == nil {
		t.Fatal("released session supplied capacity")
	}
}
