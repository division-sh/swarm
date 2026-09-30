package engine

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestExecutorGuardNoOpEvaluatesNoCheck(t *testing.T) {
	for _, guard := range []contracts.GuardSpec{
		{OnFail: "kill"},
		{Checks: []contracts.GuardCheck{{}, {}}, OnFail: "kill"},
		{PolicyRef: "threshold", OnFail: "kill"},
		{ID: " ", Check: "\t", OnFail: "kill"},
	} {
		// No evaluator or registry exists: touching either would fail this proof.
		passed, labels, err := (&Executor{}).evaluateGuardSpec(&executionFrame{}, &guard)
		if err != nil || !passed || len(labels) != 0 {
			t.Fatalf("no-op evaluated or failed: guard=%+v passed=%v labels=%v err=%v", guard, passed, labels, err)
		}
	}
}
