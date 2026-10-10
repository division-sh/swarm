package channelonboarding

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

type canceledSessionReadback struct {
	SessionBootstrapOwner
	phase            string
	cancel           context.CancelFunc
	readbackCanceled bool
}

func (*canceledSessionReadback) QualifySessionPlan(Candidate) error { return nil }

func (s *canceledSessionReadback) BootstrapSession(ctx context.Context, _ Operation, _ Candidate) error {
	if s.phase == "bootstrap" {
		s.cancel()
		return context.Cause(ctx)
	}
	return nil
}

func (s *canceledSessionReadback) CheckpointSessionPairing(ctx context.Context, op Operation) (Operation, bool, error) {
	s.cancel()
	if s.phase == "checkpoint" {
		return op, false, context.Cause(ctx)
	}
	return op, false, nil
}

func (s *canceledSessionReadback) ReadSessionPairing(ctx context.Context, _ Operation, _ operatorchannel.Principal) (PairingReadback, error) {
	s.readbackCanceled = ctx.Err() != nil
	return PairingReadback{Code: "must-not-disclose"}, context.Cause(ctx)
}

type canceledNativeReadiness struct {
	canceled bool
}

func (r *canceledNativeReadiness) ProjectConnectedChannelReadiness(ctx context.Context, _ Operation, _ Candidate) (ConnectedChannelReadiness, bool, error) {
	r.canceled = ctx.Err() != nil
	return ConnectedChannelReadiness{Ready: true}, true, context.Cause(ctx)
}

type cancelBeforeNativeReadbackStore struct {
	Store
	cancel context.CancelFunc
}

func (s *cancelBeforeNativeReadbackStore) GetChannelOnboarding(ctx context.Context, id string) (Operation, error) {
	op, err := s.Store.GetChannelOnboarding(ctx, id)
	s.cancel()
	return op, err
}

func TestPairedNativeResultReadinessPreservesCallerCancellation(t *testing.T) {
	now := time.Now().UTC()
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	candidate.Posture = ActivationSessionConnection
	candidate.SigningCredentialRole, candidate.Target.SigningCredentialKey = "", ""
	candidate.ConnectionHealth = "provider_connection"
	op := testSucceededOperation(candidate, now)
	selected := &cancellationTestStore{op: op}
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	readiness := &canceledNativeReadiness{}
	service := &Service{store: &cancelBeforeNativeReadbackStore{Store: selected, cancel: cancel},
		sessions: &canceledSessionReadback{}, identities: &cancellationTestIdentities{}, readiness: readiness}
	result, err := service.driveLocked(caller, op, candidate, "")
	if !errors.Is(err, context.Canceled) || !readiness.canceled {
		t.Fatalf("paired native readiness lost caller cancellation: err=%v canceled=%v", err, readiness.canceled)
	}
	if result.Readiness != nil && result.Readiness.Ready {
		t.Fatal("canceled paired observation disclosed READY authority")
	}
	if selected.op.Revision != op.Revision || selected.sawCanceledContext {
		t.Fatal("readback cancellation changed durable settlement ownership")
	}
}

func TestNativeSessionResultReadbackPreservesCallerCancellation(t *testing.T) {
	for _, phase := range []string{"bootstrap", "checkpoint", "unpaired"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Now().UTC()
			candidate := testCandidate(strings.Repeat("a", 64), "support")
			candidate.Posture = ActivationSessionConnection
			op := testSucceededOperation(candidate, now)
			op.Phase = PhaseActivatingProvider
			op.CompletedAt = time.Time{}
			store := &cancellationTestStore{op: op}
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			sessions := &canceledSessionReadback{phase: phase, cancel: cancel}
			service := &Service{store: store, sessions: sessions,
				identities: &cancellationTestIdentities{}, readiness: cancellationTestReadiness{},
				now: func() time.Time { return now }}
			result, err := service.driveLocked(caller, op, candidate, "")
			if !errors.Is(err, context.Canceled) || !sessions.readbackCanceled {
				t.Fatalf("native readback lost caller cancellation: err=%v canceled=%v", err, sessions.readbackCanceled)
			}
			if result.Pairing != nil && result.Pairing.Code != "" {
				t.Fatal("canceled native readback disclosed QR material")
			}
			if store.op.Revision != op.Revision || store.sawCanceledContext {
				t.Fatal("individual cancellation changed durable settlement ownership")
			}
		})
	}
}
