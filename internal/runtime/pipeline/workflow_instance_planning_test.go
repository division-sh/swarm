package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

type workflowPlanningFixtureReader struct {
	FlowInstanceIndexReader
	calls int
	scope FlowInstanceLookupScope
	fault error
}

func (r *workflowPlanningFixtureReader) WithFlowInstancePlanningSnapshot(ctx context.Context, scope FlowInstanceLookupScope, prepare func(context.Context, FlowInstanceIndexReader) error) error {
	r.calls++
	r.scope = scope
	if r.fault != nil {
		return r.fault
	}
	return prepare(ctx, r.FlowInstanceIndexReader)
}

func TestWorkflowPlanningCompositionKeepsExactOwnerAndRefusals(t *testing.T) {
	source, fact := instanceIndexTestSource(t, "text")
	scope, err := NewFlowInstanceLookupScope(source, fact, uuid.NewString(), []string{"worker"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root, err := flowidentity.StandingForGeneration(source, ".", scope.RunID())
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: scope.RunID(), Route: root.Route()})
	if err != nil {
		t.Fatal(err)
	}
	inner := &derivedStaticInstanceIndex{err: errors.New("original independent index error")}
	canonical := &workflowPlanningFixtureReader{FlowInstanceIndexReader: inner}
	persistence := WorkflowPersistence{store: &workflowInstanceStore{instanceIndex: canonical}}
	ctx := context.Background()
	called := 0
	consume := func(gotContext context.Context, reader FlowInstanceIndexReader) error {
		called++
		if gotContext != ctx || reader != inner {
			t.Fatal("workflow wrapper changed the canonical reader or context")
		}
		_, _, err := reader.LookupFlowInstance(gotContext, request)
		return err
	}
	if err := persistence.WithFlowInstancePlanningSnapshot(ctx, scope, consume); !errors.Is(err, inner.err) || called != 1 || canonical.calls != 1 || canonical.scope.RunID() != scope.RunID() || !canonical.scope.SourceFact().Matches(fact) {
		t.Fatalf("canonical owner/refusal changed: calls=%d/%d err=%v", called, canonical.calls, err)
	}
	canonical.fault = errors.Join(context.Canceled, errors.New("independent owner failure"))
	if err := persistence.WithFlowInstancePlanningSnapshot(ctx, scope, consume); err != canonical.fault || called != 1 {
		t.Fatalf("failed canonical owner admitted its consumer or changed causes: calls=%d err=%v", called, err)
	}
	for _, invalid := range []WorkflowPersistence{{}, {store: &workflowInstanceStore{}}} {
		if err := invalid.WithFlowInstancePlanningSnapshot(ctx, scope, consume); err == nil || called != 1 {
			t.Fatal("missing index owner admitted planning")
		}
	}
	if err := persistence.WithFlowInstancePlanningSnapshot(ctx, FlowInstanceLookupScope{}, consume); err == nil || called != 1 {
		t.Fatal("invalid planning scope reached the consumer")
	}
	if err := persistence.WithFlowInstancePlanningSnapshot(ctx, scope, nil); err == nil {
		t.Fatal("missing consumer reached the canonical owner")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := persistence.WithFlowInstancePlanningSnapshot(canceled, scope, consume); !errors.Is(err, context.Canceled) || called != 1 {
		t.Fatal("canceled planning reached the canonical owner")
	}
	// Legacy non-native readers keep their original typed read/refusal surface;
	// they acquire no SQL transaction or native construction permission here.
	persistence.store.instanceIndex = inner
	if err := persistence.WithFlowInstancePlanningSnapshot(ctx, scope, consume); !errors.Is(err, inner.err) || called != 2 {
		t.Fatalf("non-native index semantics changed: calls=%d err=%v", called, err)
	}
}
