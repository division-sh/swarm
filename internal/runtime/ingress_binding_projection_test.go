package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

type ingressBindingSequenceStore struct {
	runtimecredentials.Store
	mu        sync.Mutex
	snapshots []runtimecredentials.AtomicSnapshot
	calls     int
}

func (s *ingressBindingSequenceStore) Snapshot(context.Context, string) (runtimecredentials.AtomicSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := s.calls
	s.calls++
	if index >= len(s.snapshots) {
		index = len(s.snapshots) - 1
	}
	return s.snapshots[index], nil
}

func TestStaticEffectiveIngressProjectionObservesSharedKeyOnceAndFencesRotation(t *testing.T) {
	testIngressBindingProjectionSharedKey(t, func(
		ctx context.Context,
		base []packs.Subject,
		_ *RuntimeContextManager,
		owner *runtimecredentials.SnapshotOwner,
	) ([]packs.Subject, error) {
		return evaluateProviderTriggerCapabilitySubjects(ctx, base, owner)
	})
}

func TestManagerEffectiveIngressProjectionObservesSharedKeyOnceAndFencesRotation(t *testing.T) {
	testIngressBindingProjectionSharedKey(t, func(
		ctx context.Context,
		_ []packs.Subject,
		manager *RuntimeContextManager,
		owner *runtimecredentials.SnapshotOwner,
	) ([]packs.Subject, error) {
		return manager.EvaluatedCapabilitySubjects(ctx, owner)
	})
}

func testIngressBindingProjectionSharedKey(
	t *testing.T,
	project func(context.Context, []packs.Subject, *RuntimeContextManager, *runtimecredentials.SnapshotOwner) ([]packs.Subject, error),
) {
	t.Helper()
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	base := make([]packs.Subject, 0, 2)
	for _, target := range []StandingTarget{primary.StandingTargets[0], survivor.StandingTargets[0]} {
		subject, err := target.CapabilitySubject()
		if err != nil {
			t.Fatal(err)
		}
		base = append(base, subject)
	}
	bound := runtimecredentials.NewAtomicSnapshot(runtimecredentials.Metadata{Key: "webhook_signing.acme", Present: true}, "secret-a")
	unbound := runtimecredentials.NewAtomicSnapshot(runtimecredentials.Metadata{Key: "webhook_signing.acme"}, "")
	for _, tc := range []struct {
		name      string
		snapshots []runtimecredentials.AtomicSnapshot
		wantStale bool
	}{
		{name: "stable_shared_key", snapshots: []runtimecredentials.AtomicSnapshot{bound, bound}},
		{name: "rotation_during_projection", snapshots: []runtimecredentials.AtomicSnapshot{bound, unbound}, wantStale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &ingressBindingSequenceStore{Store: runtimecredentials.EnvStore{}, snapshots: tc.snapshots}
			owner, err := runtimecredentials.NewSnapshotOwner(store)
			if err != nil {
				t.Fatal(err)
			}
			subjects, err := project(ctx, base, manager, owner)
			var staleErr *runtimecredentials.SecretBindingProjectionStaleError
			if tc.wantStale {
				if !errors.As(err, &staleErr) {
					t.Fatalf("projection error = %v, want credential projection stale", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				ready := 0
				for _, subject := range subjects {
					if subject.Applicability == "effective" {
						if subject.Status != packs.StatusReady {
							t.Fatalf("effective subject %q status = %s, want READY", subject.ID, subject.Status)
						}
						ready++
					}
				}
				if ready != 2 {
					t.Fatalf("READY effective subjects = %d, want 2", ready)
				}
			}
			if store.calls != 2 {
				t.Fatalf("snapshot calls = %d, want one shared-key capture plus one validation", store.calls)
			}
		})
	}
}
