package bootverify

import (
	"context"
	"fmt"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"strings"
	"testing"
)

func TestReviewer2566FinalJoinEmitOnlyStillRejects(t *testing.T) {
	for _, final := range []bool{false, true} {
		bundle := joinValidationBundle()
		handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
		handler.Join.OnComplete.AdvancesTo = ""
		handler.Join.OnDeadline.AdvancesTo = ""
		if final {
			handler.Join.Stage = "attention"
		}
		bundle.Nodes["join-node"].EventHandlers["item.completed"] = handler
		semanticviewtest.WrapRootAgents(bundle)
		if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
			t.Fatal(err)
		}
		rebuildJoinValidationTopology(bundle)
		c := newCheckerContext(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})
		findings := append(c.stateMachineCoherence(), checkJoinValidation(c)...)
		if final && len(findings) == 0 {
			t.Fatal("emit-only completion/deadline join on final attention accepted by coherence and join validation")
		}
		if !final && len(findings) != 0 {
			t.Fatalf("non-final control: %+v", findings)
		}
	}
}

func TestFinalJoinStageAdmissionDoesNotDependOnOutcomeOrDeadline(t *testing.T) {
	for _, outcome := range []string{"advance", "emit_only", "data_only"} {
		for _, deadline := range []bool{false, true} {
			for _, final := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/deadline=%t/final=%t", outcome, deadline, final), func(t *testing.T) {
					bundle := joinValidationBundle()
					handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
					if outcome != "advance" {
						handler.Join.OnComplete.AdvancesTo = ""
						handler.Join.OnDeadline.AdvancesTo = ""
					}
					if outcome == "data_only" {
						handler.Join.OnComplete.Emit = runtimecontracts.EmitSpec{}
						handler.Join.OnDeadline.Emit = runtimecontracts.EmitSpec{}
						handler.Join.OnComplete.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "expected", Value: runtimecontracts.CELExpression("join.results")}}}
						handler.Join.OnDeadline.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "expected", Value: runtimecontracts.CELExpression("join.missing")}}}
					}
					if !deadline {
						handler.Join.Deadline = nil
						handler.Join.OnDeadlineFound = false
						handler.Join.OnDeadline = runtimecontracts.HandlerRuleEntry{}
					}
					if final {
						handler.Join.Stage = "attention"
					}
					bundle.Nodes["join-node"].EventHandlers["item.completed"] = handler
					semanticviewtest.WrapRootAgents(bundle)
					if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
						t.Fatal(err)
					}
					rebuildJoinValidationTopology(bundle)
					checker := newCheckerContext(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})
					findings := append(checker.stateMachineCoherence(), checkJoinValidation(checker)...)
					if !final && len(findings) != 0 {
						t.Fatalf("lawful non-final outcome rejected: %+v", findings)
					}
					if final {
						found := false
						for _, finding := range findings {
							found = found || (finding.CheckID == "state_machine_coherence" && strings.Contains(finding.Message, "cannot execute from final stage attention"))
						}
						if !found {
							t.Fatalf("join applicability depended on its effects: %+v", findings)
						}
					}
				})
			}
		}
	}
}
