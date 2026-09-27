package pinrouting

import (
	"reflect"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

func scopedCandidateGraph(t *testing.T) (CompiledConnectGraph, ConnectRoutePlan, ConnectRoutePlan) {
	t.Helper()
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}}},
		{id: "consumer", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.accepted"}, {Event: "work.alternate"}}},
	}, []runtimecontracts.FlowConnect{
		{Event: "work.ready", From: "producer", To: "consumer", Rename: "work.accepted"},
		{Event: "work.ready", From: "producer", To: "consumer", Rename: "work.alternate"},
	})
	graph := CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plans := graph.Plans()
	if len(plans) != 2 {
		t.Fatalf("compiled plans = %d, want 2", len(plans))
	}
	return graph, requireReceiverPinRoutePlan(t, plans, "work.accepted"), requireReceiverPinRoutePlan(t, plans, "work.alternate")
}

func TestCompiledGraphScopesRecipientCandidatesWithoutChangingAcceptance(t *testing.T) {
	graph, selectedPlan, alternatePlan := scopedCandidateGraph(t)
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer"}
	selected, err := NewConnectNodeRecipient(identitytest.FlowNode(t, "consumer", "shared"), "consumer")
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := NewConnectNodeRecipient(identitytest.FlowNode(t, "validation", "shared"), "validation")
	if err != nil {
		t.Fatal(err)
	}
	alternate, err := NewConnectNodeRecipient(identitytest.FlowNode(t, "consumer", "alternate"), "consumer")
	if err != nil {
		t.Fatal(err)
	}
	registrations := append(graph.AdmitReceiverRecipient("consumer", "work.accepted", selected),
		graph.AdmitReceiverRecipient("consumer", "work.accepted", unrelated)...)
	registrations = append(registrations, graph.AdmitReceiverRecipient("consumer", "work.alternate", alternate)...)
	if len(registrations) != 3 || registrations[2].receiverPin != alternatePlan.ReceiverPinIdentity() {
		t.Fatalf("admitted registrations = %#v, want selected/unrelated/alternate", registrations)
	}

	// The former full scan remains the independent acceptance oracle. Only its
	// candidate census, not the selected recipient, should change.
	fullRecipients, _, err := evaluateConnectPlanRecipients(selectedPlan, []events.RouteIdentity{target}, registrations)
	if err != nil {
		t.Fatal(err)
	}
	scoped := graph.ScopeRecipientRegistrations(selectedPlan, []events.RouteIdentity{target}, registrations)
	if len(scoped) != 1 || scoped[0].recipient != selected {
		t.Fatalf("scoped registrations = %#v, want only the exact pin and target", scoped)
	}
	evaluation := graph.EvaluateMaterializedRecipients(selectedPlan, []events.RouteIdentity{target}, registrations)
	if got := evaluation.Recipients(); !reflect.DeepEqual(got, fullRecipients) || len(got) != 1 {
		t.Fatalf("scoped recipients = %#v, full recipients = %#v", got, fullRecipients)
	}
	ledger, err := evaluation.Ledger()
	if err != nil {
		t.Fatal(err)
	}
	if plans := ledger.Plans(); len(plans) != 1 || plans[0].Resolution() != events.ConnectPlanResolved || len(plans[0].Candidates()) != 1 || plans[0].Candidates()[0].Outcome() != events.ConnectCandidateAccepted {
		t.Fatalf("scoped evaluation ledger = %#v, want one accepted candidate", plans)
	}

	// An unrelated registration does not turn an empty receiver into a
	// path-mismatch story or an invalid resolved-with-zero-candidates ledger.
	missing := graph.EvaluateMaterializedRecipients(selectedPlan, []events.RouteIdentity{target}, registrations[1:])
	if len(missing.Recipients()) != 0 {
		t.Fatalf("unrelated registrations selected recipients: %#v", missing.Recipients())
	}
	missingLedger, err := missing.Ledger()
	if err != nil {
		t.Fatal(err)
	}
	if plans := missingLedger.Plans(); len(plans) != 1 || plans[0].Resolution() != events.ConnectPlanNoRegistration || len(plans[0].Candidates()) != 0 {
		t.Fatalf("unrelated-only ledger = %#v, want no-registration", plans)
	}
}

func TestCompiledGraphScopePreservesCrossInstanceFanInObserver(t *testing.T) {
	graph := CompileConnectGraph(templatefanin.LoadSource(t, templatefanin.Options{}))
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plan := requireFanInRoutePlan(t, graph.Plans())
	receiver := plan.receiver.Readback()
	observer, err := NewConnectNodeRecipient(identitytest.FlowNode(t, templatefanin.ReceiverFlowID, "observer"), templatefanin.ReceiverFlowInstance)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := NewConnectNodeRecipient(identitytest.FlowNode(t, templatefanin.ReceiverFlowID, "other"), "elsewhere/child")
	if err != nil {
		t.Fatal(err)
	}
	registrations := append(graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), observer),
		graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), unrelated)...)
	target := events.RouteIdentity{FlowID: templatefanin.ReceiverFlowID, FlowInstance: templatefanin.ReceiverFlowInstance + "/child"}
	scoped := graph.ScopeRecipientRegistrations(plan, []events.RouteIdentity{target}, registrations)
	if len(scoped) != 1 || scoped[0].recipient != observer {
		t.Fatalf("fan-in observer scope = %#v, want declaration-scoped observer", scoped)
	}
	got := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{target}, registrations)
	if recipients := got.Recipients(); len(recipients) != 1 || recipients[0].ID() != observer.ID() {
		t.Fatalf("fan-in observer recipients = %#v", recipients)
	}
}

func TestCompiledGraphScopeRetainsMalformedSamePinCandidateForRefusal(t *testing.T) {
	graph, plan, _ := scopedCandidateGraph(t)
	recipient, err := NewConnectNodeRecipient(identitytest.FlowNode(t, "consumer", "receiver"), "")
	if err != nil {
		t.Fatal(err)
	}
	registrations := graph.AdmitReceiverRecipient("consumer", "work.accepted", recipient)
	if len(registrations) != 1 {
		t.Fatalf("registrations = %#v, want malformed-path candidate admitted to evaluation", registrations)
	}
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer"}
	if got := graph.ScopeRecipientRegistrations(plan, []events.RouteIdentity{target}, registrations); len(got) != 1 {
		t.Fatalf("malformed-path scoped registrations = %#v, want retained for refusal", got)
	}
	evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{target}, registrations)
	if got := evaluation.Recipients(); len(got) != 0 {
		t.Fatalf("malformed-path selected recipients = %#v", got)
	}
	ledger, err := evaluation.Ledger()
	if err != nil {
		t.Fatal(err)
	}
	if plans := ledger.Plans(); len(plans) != 1 || plans[0].Resolution() != events.ConnectPlanResolved || len(plans[0].Candidates()) != 1 || plans[0].Candidates()[0].Outcome() != events.ConnectCandidatePathMismatch {
		t.Fatalf("malformed-path refusal ledger = %#v", plans)
	}
}

func TestCompiledGraphDependencySelectionFollowsNestedConnectChain(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{id: "outer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "outer.ready"}}},
		{id: "middle", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "middle.ready"}}, outputs: []runtimecontracts.FlowOutputEventPin{{Event: "middle.done"}}},
		{id: "inner", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "inner.ready"}}},
		{id: "unrelated", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "other.done"}}},
	}, []runtimecontracts.FlowConnect{
		{Event: "outer.ready", From: "outer", To: "middle", Rename: "middle.ready"},
		{Event: "middle.done", From: "middle", To: "inner", Rename: "inner.ready"},
	})
	graph := CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	if plans := graph.Plans(); len(plans) != 2 {
		t.Fatalf("compiled plans = %d, want two connected edges", len(plans))
	}
	for _, tc := range []struct {
		changed  string
		context  []string
		affected []string
	}{
		{changed: "outer", context: []string{"middle", "outer"}, affected: []string{"middle"}},
		{changed: "middle", context: []string{"inner", "middle", "outer"}, affected: []string{"inner"}},
		{changed: "inner", context: []string{"middle"}},
		{changed: "unrelated"},
	} {
		t.Run(tc.changed, func(t *testing.T) {
			selected := graph.SelectRouteDependencies([]string{tc.changed}, nil)
			if !slices.Equal(selected.ContextFlowPaths, tc.context) || !slices.Equal(selected.AffectedFlowPaths, tc.affected) {
				t.Fatalf("selection = %#v, want context=%#v affected=%#v", selected, tc.context, tc.affected)
			}
		})
	}
}

func TestCompiledGraphDependencySelectionLoadsCompleteAffectedObserverContext(t *testing.T) {
	graph := CompiledConnectGraph{}
	dependencies := []RouteOwnerDependency{
		{SourceFlowPath: "producer", ReceiverFlowPath: "observer"},
		{SourceFlowPath: "other", ReceiverFlowPath: "observer"},
		{SourceFlowPath: "unrelated", ReceiverFlowPath: "elsewhere"},
	}
	for _, changed := range []string{"producer", "other"} {
		t.Run(changed, func(t *testing.T) {
			selected := graph.SelectRouteDependencies([]string{changed}, dependencies)
			if !slices.Equal(selected.AffectedFlowPaths, []string{"observer"}) ||
				!slices.Equal(selected.ContextFlowPaths, []string{"observer", "other", "producer"}) {
				t.Fatalf("selected %#v: complete observer context required without unrelated owners", selected)
			}
		})
	}
	selected := graph.SelectRouteDependencies([]string{"observer"}, dependencies)
	if len(selected.AffectedFlowPaths) != 0 || !slices.Equal(selected.ContextFlowPaths, []string{"other", "producer"}) {
		t.Fatalf("observer activation selected %#v", selected)
	}
}
