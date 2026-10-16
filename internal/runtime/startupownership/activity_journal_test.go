package startupownership

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type capturedJournalBinderProbe struct {
	journal pipeline.ActivityAttemptJournal
	grants  []GrantEvidence
}

func (b *capturedJournalBinderProbe) BindActivityGrant(grant GrantEvidence) (pipeline.ActivityAttemptJournal, error) {
	b.grants = append(b.grants, grant.clone())
	return b.journal, nil
}

type capturedJournalSessionProbe struct {
	*retainedSessionProbe
	binder *capturedJournalBinderProbe
}

func (s *capturedJournalSessionProbe) ActivityJournalBinder() (ActivityJournalGrantBinder, error) {
	return s.binder, nil
}

type capturedJournalProbe struct {
	pipeline.ActivityAttemptJournal
}
type imitationActivityGrant struct{ GenerationGrant }

func TestActivityJournalCaptureUsesExactRetainedGeneration(t *testing.T) {
	base, plan := testRetainedSession(t)
	binder := &capturedJournalBinderProbe{journal: &capturedJournalProbe{}}
	session := &capturedJournalSessionProbe{retainedSessionProbe: base, binder: binder}
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{BundleHash: startupBundleHashA, RuntimeInstanceID: base.authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	prepared, _ := grant.Evidence()
	if _, err := CaptureActivityJournal(context.Background(), grant, prepared); err == nil || len(binder.grants) != 0 {
		t.Fatal("prepared grant acquired an activity journal")
	}
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	admitted, err := grant.AdmitExecution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := CaptureActivityJournal(context.Background(), grant, admitted)
	if err != nil || journal != binder.journal || len(binder.grants) != 1 || !reflect.DeepEqual(binder.grants[0], admitted) {
		t.Fatalf("capture differs from the installed session: journal=%T grants=%+v err=%v", journal, binder.grants, err)
	}
	for _, tc := range []struct {
		name     string
		grant    GenerationGrant
		evidence GrantEvidence
	}{
		{"missing", nil, admitted},
		{"imitation", imitationActivityGrant{grant}, admitted},
		{"stale_snapshot", grant, prepared},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CaptureActivityJournal(context.Background(), tc.grant, tc.evidence); err == nil || len(binder.grants) != 1 {
				t.Fatal("inexact role reached the retained binder")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CaptureActivityJournal(ctx, grant, admitted); !errors.Is(err, context.Canceled) || len(binder.grants) != 1 {
		t.Fatalf("capture lost caller cancellation: %v", err)
	}
	if err := grant.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureActivityJournal(context.Background(), grant, admitted); err == nil || len(binder.grants) != 1 {
		t.Fatal("retired grant reached the retained binder")
	}
}
