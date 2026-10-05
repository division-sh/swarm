package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func a2ConstructedDescendantJoinFiles(count int, target string) map[string]string {
	files := a2ActivationJoinFiles(count)
	collector := files["orders/nodes.yaml"]
	delete(files, "orders/nodes.yaml")
	for _, path := range []string{"orders/child", "orders/child/leaf", "orders/audit", "orders/audit/leaf"} {
		files[path+"/schema.yaml"] = "name: descendant\nstages:\n  awaiting: {initial: true}\n"
		files[path+"/entities.yaml"] = "child_state:\n  final_count: {type: integer, initial: -1}\n"
	}
	files[target+"/nodes.yaml"] = collector
	files[target+"/events.yaml"] = files["orders/events.yaml"]
	files["orders/audit/leaf/schema.yaml"] = `name: review
stages:
  awaiting:
    initial: true
    gate:
      decision: child_review
      outcomes:
        approve: {advances_to: approved}
  approved: {}
`
	return files
}

func assertA2ConstructedGateSources(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, plan pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	var checked int
	for _, construction := range plan.ConstructionPlans() {
		for _, mutation := range construction.Lifecycle.GateCards {
			card, err := selected.cards.GetDecisionCard(ctx, mutation.Card.CardID)
			if err != nil {
				t.Fatalf("exact constructed gate readback: card=%#v err=%v", card, err)
			}
			actual, err := canonicaljson.Hash(card)
			if err != nil {
				t.Fatal(err)
			}
			want, err := canonicaljson.Hash(mutation.Card)
			if err != nil || actual != want {
				t.Fatalf("constructed gate changed its canonical persisted data: actual=%s want=%s err=%v", actual, want, err)
			}
			anchor, err := card.Anchor.StageGate()
			instance := construction.Identity
			if err != nil || anchor.Route != instance.Route() || anchor.FlowID != instance.TemplateID || anchor.EntityID != instance.EntityID ||
				anchor.Source.Kind() != events.RoutingSourceStaticFlow || anchor.Source.Route() != (events.RouteIdentity{FlowID: instance.TemplateID, FlowInstance: instance.InstancePath, EntityID: instance.EntityID}) {
				t.Fatalf("gate lost constructor identity: anchor=%#v instance=%#v err=%v", anchor, instance, err)
			}
			checked++
		}
	}
	if checked != 1 {
		t.Fatalf("constructed gate census=%d, want one", checked)
	}
}
