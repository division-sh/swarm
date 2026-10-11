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
	if s.phase == "readback" {
		s.cancel()
	}
	s.readbackCanceled = ctx.Err() != nil
	return PairingReadback{Code: "must-not-disclose"}, context.Cause(ctx)
}

type canceledNativeReadiness struct {
	canceled bool
	cancel   context.CancelFunc
}

func (r *canceledNativeReadiness) ProjectConnectedChannelReadiness(ctx context.Context, _ Operation, _ Candidate) (ConnectedChannelReadiness, bool, error) {
	if r.cancel != nil {
		r.cancel()
	}
	r.canceled = ctx.Err() != nil
	return ConnectedChannelReadiness{Ready: true}, true, context.Cause(ctx)
}

type cancellationObservedHistoryRead struct {
	Store
	canceled bool
}

func (s *cancellationObservedHistoryRead) ListChannelTeardowns(ctx context.Context) ([]TeardownOperation, error) {
	s.canceled = ctx.Err() != nil
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.ListChannelTeardowns(ctx)
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
	history := &cancellationObservedHistoryRead{Store: &cancelBeforeNativeReadbackStore{Store: selected, cancel: cancel}}
	service := &Service{store: history,
		sessions: &canceledSessionReadback{}, identities: &cancellationTestIdentities{}, readiness: readiness}
	result, err := service.driveLocked(caller, op, candidate, "")
	if !errors.Is(err, context.Canceled) || !history.canceled || readiness.canceled {
		t.Fatalf("paired native history did not fence disclosure at its first canceled reader: err=%v canceled=%v", err, history.canceled)
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
			history := &cancellationObservedHistoryRead{Store: store}
			service := &Service{store: history, sessions: sessions,
				identities: &cancellationTestIdentities{}, readiness: cancellationTestReadiness{},
				now: func() time.Time { return now }}
			result, err := service.driveLocked(caller, op, candidate, "")
			if !errors.Is(err, context.Canceled) || !history.canceled || sessions.readbackCanceled {
				t.Fatalf("native history did not fence disclosure at its first canceled reader: err=%v canceled=%v", err, history.canceled)
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

func TestNativeResultLateReadbackCancellationDoesNotDisclose(t *testing.T) {
	for _, phase := range []string{"readiness", "pairing"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Now().UTC()
			candidate := testCandidate(strings.Repeat("a", 64), "support")
			candidate.Posture = ActivationSessionConnection
			op := testSucceededOperation(candidate, now)
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			selected := &cancellationTestStore{op: op}
			readiness := &canceledNativeReadiness{cancel: cancel}
			sessions := &canceledSessionReadback{phase: "readback", cancel: cancel}
			if phase == "pairing" {
				op.Phase = PhaseActivatingProvider
			}
			service := &Service{store: selected, sessions: sessions, identities: &cancellationTestIdentities{}, readiness: readiness}
			result, err := service.result(caller, op, &candidate)
			observed := readiness.canceled
			if phase == "pairing" {
				observed = sessions.readbackCanceled
			}
			if !errors.Is(err, context.Canceled) || !observed ||
				result.Pairing != nil && result.Pairing.Code != "" || result.Readiness != nil && result.Readiness.Ready {
				t.Fatal("late readback cancellation was detached or disclosed QR/READY", result, err, observed)
			}
			if selected.op.Revision != op.Revision || selected.sawCanceledContext {
				t.Fatal("read-only cancellation changed durable settlement ownership")
			}
		})
	}
}
