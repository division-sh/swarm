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
	return PairingReadback{}, context.Cause(ctx)
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
			_, err := service.driveLocked(caller, op, candidate, "")
			if !errors.Is(err, context.Canceled) || !sessions.readbackCanceled {
				t.Fatalf("native readback lost caller cancellation: err=%v canceled=%v", err, sessions.readbackCanceled)
			}
			if store.op.Revision != op.Revision || store.sawCanceledContext {
				t.Fatal("individual cancellation changed durable settlement ownership")
			}
		})
	}
}
