package canonicalrouting

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestA2IndependentForkAccumulatorProducerAdmitsAuthoredKey(t *testing.T) {
	for _, clearOnAdmit := range []bool{false, true} {
		name := "retained"
		if clearOnAdmit {
			name = "explicit clear"
		}
		t.Run(name, func(t *testing.T) {
			bundle := loadA2IndependentProducer(t, CopyForkLoopAccumulator(t, clearOnAdmit))
			node := identitytest.FlowNode(t, "review", "controller")
			handler := bundle.FlowTree.ByPath["review"].Nodes["controller"].EventHandlers["review.requested"]
			if handler.Accumulate == nil || handler.Accumulate.Key != "payload.token" || handler.Accumulate.Into != "reviews" || handler.Accumulate.From != "payload" {
				t.Fatalf("generated keyed accumulation changed: %#v", handler.Accumulate)
			}
			if handler.Loop == nil || handler.Loop.Admit != "revision" || handler.Loop.From != "working" || handler.AdvancesTo != "reviewing" {
				t.Fatalf("generated admission lost loop generation or business transition: %#v", handler)
			}
			if (handler.Clear != nil) != clearOnAdmit {
				t.Fatalf("explicit clear choice changed: %#v", handler.Clear)
			}
			if clearOnAdmit && (len(handler.Clear.Targets) != 1 || handler.Clear.Targets[0] != "accumulator_state") {
				t.Fatalf("explicit clear target changed: %#v", handler.Clear)
			}
			if err := accumulator.ValidateSpecForHandler(semanticview.Wrap(bundle), node, "review.requested", handler.Accumulate); err != nil {
				t.Fatalf("generated key lacks catalog admission: %v", err)
			}
		})
	}
}

func TestA2IndependentRetainedJoinProducerAdmitsMembershipAndDeadline(t *testing.T) {
	for _, separateCheckpoint := range []bool{false, true} {
		name := "outcome checkpoint"
		rootFn := CopyForkLoopRetainedJoin
		if separateCheckpoint {
			name = "separate checkpoint"
			rootFn = CopyForkLoopRetainedJoinSeparateCheckpoint
		}
		t.Run(name, func(t *testing.T) {
			bundle := loadA2IndependentProducer(t, rootFn(t))
			node := identitytest.RootNode(t, "controller")
			handler := bundle.Nodes["controller"].EventHandlers["review.requested"]
			plan, found := semanticview.WorkflowJoinPlanForHandler(semanticview.Wrap(bundle), node, "review.requested")
			if !found || plan.Spec.ID != "reviews" || plan.Spec.Stage != "working" || plan.Spec.Members.From != "state.members" || plan.Spec.Members.By != "payload.token" || plan.Spec.Output != "payload.token" {
				t.Fatalf("generated join lost exact membership/output/declaration: %#v", plan)
			}
			if plan.Spec.Deadline == nil || plan.Spec.Deadline.After != "1h" || plan.Spec.Deadline.From != contracts.JoinDeadlineFromStageEntry || plan.Spec.OnDeadline.AdvancesTo != "reviewing" || plan.Spec.OnComplete.AdvancesTo != "reviewing" {
				t.Fatalf("generated join closure changed: %#v", plan.Spec)
			}
			if handler.Loop == nil || handler.Loop.Admit != "revision" || handler.Loop.From != "working" {
				t.Fatalf("generated join lost captured loop owner: %#v", handler.Loop)
			}
			if _, found := bundle.RootEntities["work"].Fields["window"]; !found {
				t.Fatal("business generation readback field was mistaken for join grammar")
			}
			wantOutcomeEvent := "join.observed"
			if separateCheckpoint {
				wantOutcomeEvent = ""
			}
			if plan.Spec.OnComplete.Emit.EventType() != wantOutcomeEvent {
				t.Fatalf("checkpoint variant changed outcome publication: %#v", plan.Spec.OnComplete)
			}
			if separateCheckpoint {
				checkpoint := bundle.Nodes["controller"].EventHandlers["checkpoint.requested"]
				if checkpoint.Emit.EventType() != "join.observed" || checkpoint.Loop == nil || checkpoint.Loop.Admit != "revision" {
					t.Fatalf("separate checkpoint lost its ordinary authored owner: %#v", checkpoint)
				}
			}
		})
	}
}

func TestA2IndependentServedJoinProducerRetainsApprovedGrammar(t *testing.T) {
	bundle := loadA2IndependentProducer(t, CopyServedJoinProof(t))
	plan, found := semanticview.WorkflowJoinPlanForHandler(semanticview.Wrap(bundle), identitytest.RootNode(t, "join-node"), "item.completed")
	if !found || plan.Spec.Members.From != "state.expected" || plan.Spec.Members.By != "payload.member_id" || plan.Spec.Output != "payload.result" || plan.ResultType.Type != "JoinResult" {
		t.Fatalf("served standalone join lost typed membership/result: %#v", plan)
	}
	if plan.Spec.OnComplete.AdvancesTo != "ready" || plan.Spec.Deadline == nil || plan.Spec.Deadline.After != "1h" || plan.Spec.Deadline.From != contracts.JoinDeadlineFromStageEntry || plan.Spec.OnDeadline.AdvancesTo != "attention" {
		t.Fatalf("served standalone join lost approved closure: %#v", plan.Spec)
	}
}

func loadA2IndependentProducer(t *testing.T, root string) *contracts.WorkflowContractBundle {
	t.Helper()
	repo := RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load generated standalone producer: %v", err)
	}
	if findings := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("generated standalone producer has hard invalidities: %#v", findings)
	}
	return bundle
}
