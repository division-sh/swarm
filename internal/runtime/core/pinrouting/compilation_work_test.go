package pinrouting

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type censusCountingSource struct {
	semanticview.Source
	builds   int
	withdraw bool
}

func (s *censusCountingSource) WorkflowTimers() []runtimecontracts.WorkflowTimerContract {
	s.builds++
	return s.Source.WorkflowTimers()
}

func (s *censusCountingSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	if s.withdraw {
		return semanticview.FlowScope{}, false
	}
	return s.Source.FlowScopeByID(id)
}

func (s *censusCountingSource) FlowHasInputEvent(flowID, eventType string) bool {
	return !s.withdraw && s.Source.FlowHasInputEvent(flowID, eventType)
}

func TestResolveFlowInputProducerSharesOnlyOperationLocalCensus(t *testing.T) {
	base := operationScatterSource(t)
	source := &censusCountingSource{Source: base}
	want := ResolveFlowInputProducer(base, "workers", "item.registered")
	if len(want.Evidence) == 0 || want.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
		t.Fatalf("fixture lacks admitted input evidence: %+v", want)
	}
	for calls := 1; calls <= 2; calls++ {
		got := ResolveFlowInputProducer(source, "workers", "item.registered")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%+v want=%+v", got, want)
		}
		got.Evidence[0].Kind = "mutated returned evidence"
	}
	source.withdraw = true
	if got := ResolveFlowInputProducer(source, "workers", "item.registered"); !got.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
		t.Fatalf("later resolution reused withdrawn input: %+v", got)
	}
	if got := ResolveFlowInputProducer(nil, "", ""); !got.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
		t.Fatalf("nil source accepted: %+v", got)
	}
}

func TestCompileConnectGraphSharesOnlyOperationLocalCensus(t *testing.T) {
	base := operationScatterSource(t)
	endpoint, ok := semanticview.BuildAuthoredEventEndpointCensus(base).ResolveDeclaredInputEndpoint("workers", "item.registered").Endpoint()
	if !ok {
		t.Fatal("fixture lacks the exact workers input")
	}
	source := &censusCountingSource{Source: base}
	if _, issue := lowerTemplateInputReceiverPlan(source, semanticview.AuthoredEventEndpoint{}); issue.Failure.Empty() {
		t.Fatal("invalid endpoint was not rejected")
	}
	graph := CompileConnectGraph(source)
	if len(graph.receiverPlans) == 0 || len(graph.issues) != 0 || !reflect.DeepEqual(graph, CompileConnectGraph(base)) {
		t.Fatalf("receiver plans/issues = %d/%v", len(graph.receiverPlans), graph.issues)
	}
	plan, issue := lowerTemplateInputReceiverPlan(source, endpoint)
	matched := false
	for _, compiled := range graph.receiverPlans {
		matched = matched || reflect.DeepEqual(plan, compiled)
	}
	if !issue.Failure.Empty() || !matched {
		t.Fatalf("operation-local lowering differs from fresh private receiver registration: %v", issue)
	}
	source.withdraw = true
	if got := CompileConnectGraph(source); len(got.receiverPlans) != 0 {
		t.Fatalf("later compile reused withdrawn source: plans=%d", len(got.receiverPlans))
	}
	if _, issue := lowerTemplateInputReceiverPlan(source, endpoint); issue.Failure.Empty() {
		t.Fatal("receiver registration accepted withdrawn source")
	}
}
