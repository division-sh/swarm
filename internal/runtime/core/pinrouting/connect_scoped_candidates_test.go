package pinrouting

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledGraphHasNoMaterializedReplacementSelector(t *testing.T) {
	if _, present := reflect.TypeOf(CompiledConnectGraph{}).MethodByName("SelectRouteDependencies"); present {
		t.Fatal("compiled graph exports retired materialized-route replacement selection")
	}
}

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

func TestCompiledGraphScopePreservesCrossInstanceObserver(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, canonicalrouting.ExampleRoot(t, canonicalrouting.FanInStream), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	graph := CompileConnectGraph(semanticview.Wrap(bundle))
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plan := requirePeriodReportRoutePlan(t, graph.Plans())
	receiver := plan.receiver.Readback()
	observer, err := NewConnectNodeRecipient(identitytest.FlowNode(t, receiver.FlowPath, "observer"), "portfolio/period-1")
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := NewConnectNodeRecipient(identitytest.FlowNode(t, receiver.FlowPath, "observer"), "portfolio/period-2")
	if err != nil {
		t.Fatal(err)
	}
	registrations := append(graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), observer),
		graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), unrelated)...)
	target := events.RouteIdentity{FlowID: receiver.FlowID, FlowInstance: observer.Path()}
	for _, instance := range []string{"operating/source-1", "operating/source-2"} {
		t.Run(instance, func(t *testing.T) {
			sourceEvent, err := AdmitSourceEvent(events.EventType(instance+"/operating.reported"), eventtest.ConcreteTemplateRoutingSource("operating", instance, ""))
			if err != nil {
				t.Fatal(err)
			}
			matching := graph.MatchingSourceEvent(sourceEvent)
			if len(matching) != 1 || matching[0].ReceiverPinIdentity() != plan.ReceiverPinIdentity() {
				t.Fatalf("source %q matching routes = %#v, want the exact shared period observer edge", instance, matching)
			}
			fullRecipients, _, err := evaluateConnectPlanRecipients(plan, []events.RouteIdentity{target}, registrations)
			if err != nil {
				t.Fatal(err)
			}
			scoped := graph.ScopeRecipientRegistrations(plan, []events.RouteIdentity{target}, registrations)
			if len(scoped) != 1 || scoped[0].recipient != observer {
				t.Fatalf("observer scope = %#v, want exact period observer without sibling leakage", scoped)
			}
			got := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{target}, registrations)
			if recipients := got.Recipients(); !reflect.DeepEqual(recipients, fullRecipients) || len(recipients) != 1 || recipients[0].Path() != observer.Path() {
				t.Fatalf("observer recipients = %#v, full scan = %#v", recipients, fullRecipients)
			}
		})
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
