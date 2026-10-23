package bus

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1EventBusCarriesFrozenRootAndRejectsStaleContext(t *testing.T) {
	first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.Artifact())
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	fact := sourceartifactfixture.FactFor(first.Artifact())
	bus := &EventBus{sourceArtifactFact: fact, rootAdmission: first.Admission(), runtimeInstanceID: "rooted-bus"}
	base := correlation.WithSourceArtifactFact(context.Background(), fact)
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		valid bool
	}{
		{"absent", context.Background(), true},
		{"matching", correlation.WithRootAdmission(base, first.Admission()), true},
		{"crossed_same_content", correlation.WithRootAdmission(base, second.Admission()), false},
		{"explicit_clear", correlation.WithRootAdmission(base, sourceartifact.RootAdmission{}), false},
		{"root_without_fact", correlation.WithRootAdmission(context.Background(), first.Admission()), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := bus.AdmitSourceArtifactFact(tc.ctx)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid root context admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			root, present, err := correlation.RootAdmissionFromContext(ctx)
			if err != nil || !present || !root.Equal(first.Admission()) {
				t.Fatalf("publication lost its frozen root: %+v present=%v err=%v", root.Reference(), present, err)
			}
		})
	}
	rootless := &EventBus{sourceArtifactFact: fact, runtimeInstanceID: "rootless"}
	if _, err := rootless.AdmitSourceArtifactFact(context.Background()); err == nil {
		t.Fatal("durable source fact alone gained rooted publication context")
	}
}

func TestS1AcceptedWorkKeepsRootAndExplicitClear(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "work", sourceartifactfixture.Artifact())
	process := worklifetime.NewProcess()
	owner := newReceiverProjectionRuntimeOwner(t, process, "rooted-work")
	lease, err := owner.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Done(); err != nil {
			t.Error(err)
		}
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx := correlation.WithSourceArtifactFact(context.Background(), sourceartifactfixture.FactFor(rooted.Artifact()))
	ctx = correlation.WithRootAdmission(ctx, rooted.Admission())
	work := bindWorkContext(ctx, lease, owner)
	root, present, err := correlation.RootAdmissionFromContext(work)
	if err != nil || !present || !root.Equal(rooted.Admission()) {
		t.Fatalf("accepted-work handoff lost root: %+v err=%v", root.Reference(), err)
	}
	cleared := correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{})
	work = bindWorkContext(cleared, lease, owner)
	if _, present, err := correlation.RootAdmissionFromContext(work); err == nil || present {
		t.Fatal("accepted-work handoff repaired an explicit root clear")
	}
}
