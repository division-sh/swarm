package main

import (
	"go/ast"
	"testing"
)

var family145LifecycleBusinessCallers = map[string]int{
	"TestReleaseCompiledLifecycleJourneysBothStores":                      2,
	"runLifecycleRootForkPolicy":                                          4,
	"TestServedCompiledGateOutcomeRestartOnBothStores":                    2,
	"proveServedCompiledGateOutcomeRestart":                               2,
	"TestServedCompiledGateAdvanceOnlyOnBothStores":                       2,
	"TestServedCompiledTransitionNestedCarrierCollisionOnBothStores":      2,
	"TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores":           4,
	"TestServedCompiledLoopTransitionReplayOnBothStores":                  4,
	"TestServedCompiledGateFrozenTransitionEvidenceOnBothStores":          7,
	"TestServedCompiledTransitionRestartOnBothStores":                     2,
	"TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores":     3,
	"TestServedCompiledTransitionStaticForkEvidenceOnBothStores":          4,
	"TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores": 2,
}

func family145LifecycleBusinessRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	if family145NodeString(call.Fun) != "lifecycleStoredSnapshot" {
		return
	}
	if family145LifecycleBusinessCallers[function] == 0 || len(call.Args) != 3 || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
		t.Fatal("lifecycle snapshot caller escaped its finite original inspection owner")
	}
	counts["lifecycle-business/"+function]++
}
