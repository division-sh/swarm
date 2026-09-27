package bus

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledCandidateEvidenceDoesNotGrowWithUnrelatedTemplateChildren(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	graph := runtimepinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plans := graph.Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled plans = %d, want 1", len(plans))
	}
	plan := plans[0]
	receiver := plan.ReceiverEndpoint().Readback()
	node := testFlowNode(t, "consumer", "consumer-node")
	target := events.RouteIdentity{FlowID: receiver.FlowID, FlowInstance: "consumer/selected", EntityID: eventtest.UUID("selected-consumer")}
	var baselineBytes int
	for _, unrelatedCount := range []int{128, 512, 1362} {
		t.Run(fmt.Sprintf("unrelated_%d", unrelatedCount), func(t *testing.T) {
			selected, err := runtimepinrouting.NewConnectNodeRecipient(node, target.FlowInstance)
			if err != nil {
				t.Fatal(err)
			}
			registrations := graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), selected)
			if len(registrations) != 1 {
				t.Fatalf("selected registrations = %d, want 1", len(registrations))
			}
			for i := range unrelatedCount {
				other, err := runtimepinrouting.NewConnectNodeRecipient(node, fmt.Sprintf("consumer/other-%04d", i))
				if err != nil {
					t.Fatal(err)
				}
				registrations = append(registrations, graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), other)...)
			}
			if got := len(registrations); got != unrelatedCount+1 {
				t.Fatalf("full registrations = %d, want %d", got, unrelatedCount+1)
			}
			started := time.Now()
			scoped := graph.ScopeRecipientRegistrations(plan, []events.RouteIdentity{target}, registrations)
			if len(scoped) != 1 {
				t.Fatalf("scoped registrations = %d, want 1 from %d", len(scoped), len(registrations))
			}
			evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{target}, registrations)
			if got := evaluation.Recipients(); len(got) != 1 || got[0].ID() != selected.ID() || got[0].Path() != target.FlowInstance {
				t.Fatalf("selected recipients = %#v, want exact selected template instance", got)
			}
			ledger, err := evaluation.Ledger()
			if err != nil {
				t.Fatal(err)
			}
			if got := ledger.Plans(); len(got) != 1 || len(got[0].Candidates()) != 1 || got[0].Candidates()[0].Outcome() != events.ConnectCandidateAccepted {
				t.Fatalf("candidate ledger = %#v, want one accepted candidate", got)
			}
			settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(settlement)
			if err != nil {
				t.Fatal(err)
			}
			var decoded events.RouteSettlement
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("read persisted settlement: %v", err)
			}
			if got := decoded.Ledger().Plans(); len(got) != 1 || len(got[0].Candidates()) != 1 {
				t.Fatalf("decoded candidate ledger = %#v, want one", got)
			}
			if baselineBytes == 0 {
				baselineBytes = len(encoded)
			} else if got := len(encoded); got != baselineBytes {
				t.Fatalf("settlement bytes = %d, want cardinality-independent %d", got, baselineBytes)
			}
			t.Logf("full_registrations=%d scoped_candidates=1 settlement_bytes=%d evaluation_elapsed=%s", len(registrations), len(encoded), time.Since(started))
		})
	}
}

func TestCompiledCandidateScopePreservesNestedMultiTargetRoutes(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	graph := runtimepinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plans := graph.Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled plans = %d, want 1", len(plans))
	}
	plan := plans[0]
	receiver := plan.ReceiverEndpoint().Readback()
	node := testFlowNode(t, "consumer", "consumer-node")
	paths := []string{"consumer/outer-1/inner-1", "consumer/outer-1/inner-2"}
	targets := []events.RouteIdentity{
		{FlowID: receiver.FlowID, FlowInstance: paths[0], EntityID: eventtest.UUID("inner-one")},
		{FlowID: receiver.FlowID, FlowInstance: paths[1], EntityID: eventtest.UUID("inner-two")},
	}
	registrations := make([]runtimepinrouting.ConnectRecipientRegistration, 0, 3)
	for _, path := range []string{paths[1], "consumer/outer-2/inner-1", paths[0]} {
		recipient, err := runtimepinrouting.NewConnectNodeRecipient(node, path)
		if err != nil {
			t.Fatal(err)
		}
		registrations = append(registrations, graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), recipient)...)
	}
	for _, order := range []struct {
		name          string
		registrations []runtimepinrouting.ConnectRecipientRegistration
	}{
		{name: "older sibling first", registrations: registrations},
		{name: "newer sibling first", registrations: []runtimepinrouting.ConnectRecipientRegistration{registrations[2], registrations[1], registrations[0]}},
	} {
		t.Run(order.name, func(t *testing.T) {
			scoped := graph.ScopeRecipientRegistrations(plan, targets, order.registrations)
			if len(scoped) != 2 {
				t.Fatalf("scoped registrations = %d, want both exact nested targets", len(scoped))
			}
			evaluation := graph.EvaluateMaterializedRecipients(plan, targets, order.registrations)
			recipients := evaluation.Recipients()
			if len(recipients) != 2 {
				t.Fatalf("recipients = %#v, want both nested target paths", recipients)
			}
			gotPaths := map[string]bool{}
			for _, recipient := range recipients {
				gotPaths[recipient.Path()] = true
			}
			if !gotPaths[paths[0]] || !gotPaths[paths[1]] || len(gotPaths) != 2 {
				t.Fatalf("recipient paths = %#v, want exactly %#v", gotPaths, paths)
			}
			ledger, err := evaluation.Ledger()
			if err != nil {
				t.Fatal(err)
			}
			if plans := ledger.Plans(); len(plans) != 1 || len(plans[0].Candidates()) != 2 {
				t.Fatalf("candidate ledger = %#v, want exactly both nested targets", plans)
			}
		})
	}
}

func TestCompiledCandidateScopeDoesNotTurnBlockedPlanIntoNoRegistration(t *testing.T) {
	graph := runtimepinrouting.CompileConnectGraph(connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false))
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compiled graph issues = %#v", issues)
	}
	plans := graph.Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled plans = %d, want 1", len(plans))
	}
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/blocked", EntityID: eventtest.UUID("blocked-consumer")}
	var dispatch connectRoutePlanDispatch
	if err := (connectRoutePlanResolver{graph: graph}).appendBlockedPlanEvaluation(&dispatch, plans[0], []events.RouteIdentity{target}); err != nil {
		t.Fatal(err)
	}
	if entries := dispatch.Evaluation.Plans(); len(entries) != 1 || entries[0].Resolution() != events.ConnectPlanResolutionBlocked || len(entries[0].Candidates()) != 0 || len(entries[0].Targets()) != 1 || entries[0].Targets()[0] != target {
		t.Fatalf("blocked plan evidence = %#v, want target-preserving blocker with no candidates", entries)
	}
}
