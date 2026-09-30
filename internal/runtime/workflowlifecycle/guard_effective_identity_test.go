package workflowlifecycle

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestGuardTerminationEvidenceUsesEffectiveCheckIdentity(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	graph := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "killed"}, []string{"killed"}, nil, nil, nil)
	for _, tc := range []struct {
		name   string
		checks []contracts.GuardCheck
		labels []string
		valid  bool
	}{
		{"unnamed", []contracts.GuardCheck{{Check: " false "}}, []string{"false"}, true},
		{"ordered mixed", []contracts.GuardCheck{{}, {ID: " first ", Check: "true"}, {}, {Check: " false "}}, []string{"first", "false"}, true},
		{"named registry", []contracts.GuardCheck{{ID: " registry "}}, []string{"registry"}, true},
		{"no op fabricated label", []contracts.GuardCheck{{}}, []string{"fabricated"}, false},
		{"wrong unnamed label", []contracts.GuardCheck{{Check: "false"}}, []string{"different"}, false},
		{"wrong order", []contracts.GuardCheck{{Check: "true"}, {Check: "false"}}, []string{"false", "true"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause, err := NewGuardTermination(graph, node, "work", tc.labels[len(tc.labels)-1], "ready", "killed", tc.labels)
			if err != nil {
				t.Fatal(err)
			}
			err = cause.ValidateHandlerEvidence(contracts.SystemNodeEventHandler{Guard: &contracts.GuardSpec{Checks: tc.checks, OnFail: "kill"}})
			if (err == nil) != tc.valid {
				t.Fatalf("evidence validity=%v, want %v: %v", err == nil, tc.valid, err)
			}
		})
	}
}
