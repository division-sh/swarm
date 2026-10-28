package startupownership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type retainedRunSourceSession struct {
	*retainedSessionProbe
	sources map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource
	err     error
	loads   int
}

func (s *retainedRunSourceSession) LoadStartupRunSources(context.Context, []agenttopology.SourceCoordinate) (map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource, error) {
	s.loads++
	return s.sources, s.err
}

func TestRetainedRunSourceCensusCarriesOnlyExactObservation(t *testing.T) {
	probe, initial := testRetainedSession(t)
	a, _ := sourceartifactfixture.Rooted(t, "source-a", sourceartifactfixture.Artifact())
	b, _ := sourceartifactfixture.Rooted(t, "source-b", startupBundleArtifactB)
	runID := uuid.NewString()
	session := &retainedRunSourceSession{retainedSessionProbe: probe, sources: map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource{a.Admission().Reference(): a, b.Admission().Reference(): b}}
	capability, err := newProcessCapability(session, time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer capability.Release(context.Background())
	plan, err := AdmitRetainedRunSources(context.Background(), capability, initial)
	if err != nil || len(plan.Sources) != 2 || len(plan.Agents) != len(initial.Agents) {
		t.Fatalf("exact retained coordinate extension: %+v %v", plan, err)
	}
	probe.plan = plan
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: a.Admission().BundleHash(), RuntimeInstanceID: probe.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx = correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(a.Artifact())), a.Admission())
	ctx, err = WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := RunSourceObservationContext(ctx, runID, b.Admission().Reference(), plan)
	if err != nil {
		t.Fatal(err)
	}
	root, present, err := correlation.RootAdmissionFromContext(observed)
	if err != nil || !present || !root.Equal(b.Admission()) {
		t.Fatalf("observation lost exact B admission: %+v %t %v", root, present, err)
	}
	parent, present, err := correlation.RootAdmissionFromContext(ctx)
	if err != nil || !present || !parent.Equal(a.Admission()) {
		t.Fatalf("observation replaced original A custody: %+v %t %v", parent, present, err)
	}
	if observed.Value(mutationAuthorityContextKey{}) != ctx.Value(mutationAuthorityContextKey{}) {
		t.Fatal("observation replaced its original grant")
	}
	if _, err := CaptureMutationAuthority(observed); err == nil {
		t.Fatal("read-only B carriage became B mutation authority under A's grant")
	}
	deadline, _ := ctx.Deadline()
	if got, _ := observed.Deadline(); got != deadline {
		t.Fatal("observation changed caller lifetime")
	}
	delete(session.sources, b.Admission().Reference())
	if again, err := AdmitRetainedRunSources(ctx, capability, initial); err != nil || again.Revision != plan.Revision || session.loads != 1 {
		t.Fatalf("census was rediscovered or mutated: %+v loads=%d err=%v", again, session.loads, err)
	}
	foreign := b.Admission().Reference()
	foreign.RootDirectory = "another-root"
	for name, check := range map[string]func() error{
		"contradictory_coordinate": func() error {
			_, err := RunSourceObservationContext(ctx, runID, foreign, plan)
			return err
		},
		"wrong_revision": func() error {
			_, err := RunSourceObservationContext(ctx, runID, b.Admission().Reference(), initial)
			return err
		},
		"cleared_parent": func() error {
			_, err := RunSourceObservationContext(correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{}), runID, b.Admission().Reference(), plan)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil {
				t.Fatal("foreign/contradictory observation admitted")
			}
		})
	}
	cancel()
	if _, err := RunSourceObservationContext(ctx, runID, b.Admission().Reference(), plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller gained observation: %v", err)
	}
}

func TestRetainedRunSourceCensusRefusesInvalidSourceAndConflictingRoot(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_hash_foreign_root", true: "invalid_rooted_source"}[missing], func(t *testing.T) {
			probe, plan := testRetainedSession(t)
			other, _ := sourceartifactfixture.Rooted(t, "another-root", sourceartifactfixture.Artifact())
			session := &retainedRunSourceSession{retainedSessionProbe: probe, sources: map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource{other.Admission().Reference(): other}}
			if missing {
				session.sources = map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource{other.Admission().Reference(): {}}
			}
			capability, err := newProcessCapability(session, time.Hour, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer capability.Release(context.Background())
			if _, err := AdmitRetainedRunSources(context.Background(), capability, plan); err == nil {
				t.Fatal("census accepted missing content or conflicting rooted source")
			}
			if err := plan.Validate(); err != nil || len(plan.Sources) != 1 {
				t.Fatal("failed census mutated its caller's source set")
			}
		})
	}
}
