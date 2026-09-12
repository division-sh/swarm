package bus

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedInputValidationAgentDeclarationOwnership(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopySelectedInputAgentProbe(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	routes, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	for _, tc := range []struct{ name, flow, event string }{
		{"root_input", ".", "work.ready"},
		{"ordinary_root", "", "work.ready"},
		{"ordinary_child", "child", "child/work.ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := eventtest.OperatorInjected(uuid.NewString(), events.EventType(tc.event), "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, time.Now().UTC())
			original, err = eventtest.AdmitPayload(original, tc.flow, tc.event)
			if err != nil {
				t.Fatal(err)
			}
			validation, err := RevalidateSelectedInput(source, original)
			if err != nil {
				t.Fatal(err)
			}
			subscribers := routes.ResolveIndependentPubsubForRun(runID, tc.event)
			got := validation.FilterSubscribers(subscribers)
			if len(got) != 1 || !got[0].Recipient.IsAgent() {
				t.Fatalf("exact agent input recipients: %+v", got)
			}
			good := got[0]
			wantFlow := tc.flow
			if wantFlow == "" {
				wantFlow = "."
			}
			owner, ok := semanticview.AgentDeclarationOwner(source, wantFlow, "worker")
			if !ok || good.AgentPlan.Name.Owner != owner {
				t.Fatalf("lost declaration owner: %+v", good)
			}
			otherFlow := "child"
			if wantFlow == "child" {
				otherFlow = "."
			}
			otherOwner, ok := semanticview.AgentDeclarationOwner(source, otherFlow, "worker")
			if !ok {
				t.Fatal("missing other declared agent")
			}
			for _, bad := range []struct {
				name   string
				mutate func(*Subscriber)
			}{
				{"owner_as_flow", func(s *Subscriber) { s.AgentPlan.Name.Owner = wantFlow }},
				{"foreign_owner", func(s *Subscriber) { s.AgentPlan.Name.Owner = "foreign-owner" }},
				{"other_declared_owner", func(s *Subscriber) { s.AgentPlan.Name.Owner = otherOwner }},
				{"foreign_recipient", func(s *Subscriber) { s.Recipient = events.MustAgentDeliveryRecipient("foreign") }},
				{"foreign_route", func(s *Subscriber) {
					s.AgentPlan.Route = agentidentity.RootRoute()
					if wantFlow == "." {
						s.AgentPlan.Route = agentidentity.Route{Presence: agentidentity.RoutePresent, ScopeKey: "child", InstanceID: "child", InstancePath: "child"}
					}
				}},
			} {
				t.Run(bad.name, func(t *testing.T) {
					candidate := good
					bad.mutate(&candidate)
					if validation.AllowsSubscriber(candidate) {
						t.Fatalf("accepted mismatched agent: %+v", candidate)
					}
				})
			}
			evidence, err := good.SelectedRecipient(original.Type())
			if err != nil {
				t.Fatal(err)
			}
			exact, err := validation.SelectRecipients([]forkrecipient.Evidence{evidence})
			if err != nil || !exact.AllowsSubscriber(good) {
				t.Fatalf("exact disposition lost agent: %v", err)
			}
			none, err := validation.SelectRecipients(nil)
			if err != nil || none.AllowsSubscriber(good) {
				t.Fatalf("empty disposition recreated agent: %v", err)
			}
			otherEvent := "child/work.ready"
			if wantFlow == "child" {
				otherEvent = "work.ready"
			}
			for _, other := range routes.ResolveIndependentPubsubForRun(runID, otherEvent) {
				if validation.AllowsSubscriber(other) {
					t.Fatalf("same-name other flow admitted: %+v", other)
				}
			}
		})
	}
	original := eventtest.OperatorInjected(uuid.NewString(), "templ/work.ready", "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, time.Now().UTC())
	original, err = eventtest.AdmitPayload(original, "templ", "templ/work.ready")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RevalidateSelectedInput(source, original); err == nil {
		t.Fatal("ordinary input manufactured template admission")
	}
}
