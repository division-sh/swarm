package decisioncard

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
)

func gateOfferFixture(t *testing.T, change func(*contracts.WorkflowGatePlan)) (Card, gateruntime.Activation) {
	t.Helper()
	plan := contracts.WorkflowGatePlan{FlowID: ".", Stage: "awaiting_review", Decision: "launch_review", Outcomes: map[string]contracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve", AdvancesTo: "operating", Input: map[string]contracts.WorkflowGateInputField{
			"alpha": {Type: "text", Label: "Alpha", Required: true}, "zeta": {Type: "integer", Label: "Zeta"},
		}, InputOrder: []string{"alpha", "zeta"}, Emit: contracts.EmitSpec{Event: "review.completed", Fields: map[string]contracts.ExpressionValue{"result": contracts.LiteralExpression("ready")}},
			EmitSchema: map[string]any{"type": "object", "properties": map[string]any{"result": map[string]any{"type": "string"}}, "required": []any{"result"}}},
	}}
	if change != nil {
		change(&plan)
	}
	graph := contracts.BuildWorkflowStageTopology(plan.FlowID, plan.Stage, []string{plan.Stage, "operating", "done"}, nil, nil, nil, nil, []contracts.WorkflowGatePlan{plan})
	transitions := map[string]contracts.CompiledTransition{}
	for verdict, outcome := range plan.Outcomes {
		var err error
		transitions[verdict], err = graph.AdmitTransition(contracts.WorkflowTransitionSite{DecisionID: plan.Decision, Verdict: verdict}, plan.Stage, outcome.AdvancesTo)
		if err != nil {
			t.Fatal(err)
		}
	}
	routes, err := gateruntime.FreezeRoutes(plan.Outcomes, transitions)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	activation, err := gateruntime.New("run-1", "root", "entity-1", plan.FlowID, plan.Stage, plan.Decision, "bundle-a", routes, "entry", now)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := NewStageGateAnchor(StageGateAnchor{Route: flowidentity.RouteForInstancePath("root"), FlowID: plan.FlowID, EntityID: "entity-1", Source: testRootRoutingSource("entity-1"), Stage: plan.Stage, StageActivationID: activation.ActivationID})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := FreezeSnapshot(plan.Decision, "Original title", map[string]any{"summary": "original"}, plan.Outcomes)
	if err != nil {
		t.Fatal(err)
	}
	card, err := New(Card{CardID: activation.CardID, RunID: "run-1", Anchor: anchor, ExecutionMode: "live", BundleHash: activation.BundleHash, WorkflowVersion: "a", Snapshot: snapshot, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return card, activation
}

func TestGateOfferEqualityIncludesInputsAndFullContinuation(t *testing.T) {
	card, activation := gateOfferFixture(t, nil)
	original, err := IssuedGateOffer(card, activation)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*contracts.WorkflowGatePlan){
		"decision": func(p *contracts.WorkflowGatePlan) { p.Decision = "different_review" },
		"choice": func(p *contracts.WorkflowGatePlan) {
			p.Outcomes["reject"] = contracts.WorkflowGateOutcomePlan{Verdict: "reject", AdvancesTo: "done"}
		},
		"label": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.Label = "Different"
			p.Outcomes["approve"] = o
		},
		"input_order": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.InputOrder = []string{"zeta", "alpha"}
			p.Outcomes["approve"] = o
		},
		"input_type": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			f := o.Input["alpha"]
			f.Type = "integer"
			o.Input["alpha"] = f
		},
		"input_required": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			f := o.Input["zeta"]
			f.Required = true
			o.Input["zeta"] = f
		},
		"input_label": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			f := o.Input["alpha"]
			f.Label = "Different"
			o.Input["alpha"] = f
		},
		"target": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.AdvancesTo = "done"
			p.Outcomes["approve"] = o
		},
		"event": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.Emit.Event = "review.other"
			p.Outcomes["approve"] = o
		},
		"payload": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.Emit.Fields["result"] = contracts.LiteralExpression("different")
		},
		"schema": func(p *contracts.WorkflowGatePlan) {
			o := p.Outcomes["approve"]
			o.EmitSchema["additionalProperties"] = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			changedCard, changedActivation := gateOfferFixture(t, change)
			changed, err := IssuedGateOffer(changedCard, changedActivation)
			if err != nil || original.Equal(changed) {
				t.Fatalf("changed contract accepted: equal=%t err=%v", original.Equal(changed), err)
			}
		})
	}
	card.BundleHash, activation.BundleHash = "bundle-b", "bundle-b"
	card.Snapshot.Title, card.Snapshot.Context = "New title", semanticvalue.EmptyObject()
	card.DeferredUntil = card.CreatedAt.Add(time.Hour)
	card, err = New(card)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := IssuedGateOffer(card, activation)
	if err != nil || !original.Equal(unchanged) || (GateOffer{}).Equal(GateOffer{}) {
		t.Fatalf("presentation/source changed offer or zero validated: equal=%t err=%v", original.Equal(unchanged), err)
	}
}

func TestIssuedGateOfferRefusesContradictoryEvidence(t *testing.T) {
	for name, change := range map[string]func(*Card, *gateruntime.Activation){
		"content_hash":  func(c *Card, _ *gateruntime.Activation) { c.CardContentHash = "invalid" },
		"source":        func(_ *Card, a *gateruntime.Activation) { a.BundleHash = "other" },
		"card":          func(_ *Card, a *gateruntime.Activation) { a.CardID = "00000000-0000-0000-0000-000000000001" },
		"decision":      func(_ *Card, a *gateruntime.Activation) { a.DecisionID = "other" },
		"wait":          func(_ *Card, a *gateruntime.Activation) { a.Stage = "other" },
		"missing_route": func(_ *Card, a *gateruntime.Activation) { a.RoutesJSON = "{}" },
		"route_owner": func(_ *Card, a *gateruntime.Activation) {
			a.RoutesJSON = strings.ReplaceAll(a.RoutesJSON, `"Flow":"."`, `"Flow":"other"`)
		},
		"unknown_input": func(_ *Card, a *gateruntime.Activation) {
			a.RoutesJSON = strings.Replace(a.RoutesJSON, `"cel":"","kind":"literal","literal":"ready","ref":""`, `"cel":"decision.missing","kind":"cel","literal":null,"ref":""`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			card, activation := gateOfferFixture(t, nil)
			change(&card, &activation)
			if _, err := IssuedGateOffer(card, activation); err == nil {
				t.Fatal("contradictory issued evidence accepted")
			}
		})
	}
}

func TestCompileGateOfferUsesDeclaredCurrentContract(t *testing.T) {
	load := func(comment string) semanticview.Source {
		artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("# " + comment + "\nstages:\n  waiting:\n    gate:\n      decision: review\n      outcomes:\n        approve:\n          advances_to: done\n          emit: {event: approved, fields: {result: \"ready\"}}\n  done: {final: true}\n"),
			"events.yaml":   []byte("approved:\n  result: text\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		root, _ := sourceartifactfixture.Rooted(t, comment, artifact)
		repo := filepath.Join("..", "..", "..")
		bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return semanticview.Wrap(bundle)
	}
	firstSource, secondSource := load("original"), load("comment-only")
	first, err := CompileGateOffer(firstSource, ".", "waiting")
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileGateOffer(secondSource, ".", "waiting")
	if err != nil || !first.Equal(second) {
		t.Fatalf("comment changed contract: %v", err)
	}
	for _, input := range []struct {
		source      semanticview.Source
		flow, stage string
	}{{nil, ".", "waiting"}, {firstSource, ".", "missing"}, {firstSource, "missing", "waiting"}} {
		if _, err := CompileGateOffer(input.source, input.flow, input.stage); err == nil {
			t.Fatal("missing source/wait accepted")
		}
	}
}
