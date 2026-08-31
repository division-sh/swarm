package pipeline

import "testing"

func TestClassifyStandingRestartTotalStateProduct(t *testing.T) {
	const (
		serviceID = "11111111-1111-4111-8111-111111111111"
		runID     = "22222222-2222-4222-8222-222222222222"
	)
	base := StandingRestartFact{
		ExactCurrent: true, ServiceID: serviceID, RunID: runID, Generation: 3,
		DeclarationPresent: true, EffectiveState: "active", OperatorOverride: "none", RunState: "running",
	}
	tests := []struct {
		name        string
		mutate      func(*StandingRestartFact)
		wantKind    StandingRestartDispositionKind
		wantRepair  StandingRestartRemediation
		wantGeneric bool
		wantExec    bool
	}{
		{name: "ordinary", mutate: func(f *StandingRestartFact) { *f = StandingRestartFact{} }, wantKind: StandingRestartOrdinary, wantRepair: StandingRestartNoRemediation, wantGeneric: true},
		{name: "active running", wantKind: StandingRestartActiveIntrinsic, wantRepair: StandingRestartNoRemediation, wantExec: true},
		{name: "active paused", mutate: func(f *StandingRestartFact) { f.RunState = "paused" }, wantKind: StandingRestartActiveIntrinsic, wantRepair: StandingRestartNoRemediation, wantExec: true},
		{name: "suspended", mutate: func(f *StandingRestartFact) {
			f.EffectiveState, f.OperatorOverride, f.RunState = "suspended", "suspended", "paused"
		}, wantKind: StandingRestartSuspended, wantRepair: StandingRestartResumeOrReset},
		{name: "orphaned", mutate: func(f *StandingRestartFact) {
			f.DeclarationPresent, f.EffectiveState, f.RunState = false, "orphaned", "paused"
		}, wantKind: StandingRestartOrphaned, wantRepair: StandingRestartRestoreDeclaration},
		{name: "terminal declared", mutate: func(f *StandingRestartFact) { f.RunState = "completed" }, wantKind: StandingRestartTerminalDeclared, wantRepair: StandingRestartReset},
		{name: "terminal declared cancelled", mutate: func(f *StandingRestartFact) { f.RunState = "cancelled" }, wantKind: StandingRestartTerminalDeclared, wantRepair: StandingRestartReset},
		{name: "terminal declared forked", mutate: func(f *StandingRestartFact) { f.RunState = "forked" }, wantKind: StandingRestartTerminalDeclared, wantRepair: StandingRestartReset},
		{name: "terminal orphaned", mutate: func(f *StandingRestartFact) {
			f.DeclarationPresent, f.EffectiveState, f.RunState = false, "orphaned", "failed"
		}, wantKind: StandingRestartTerminalOrphaned, wantRepair: StandingRestartRestoreThenReset},
		{name: "terminal orphaned forked", mutate: func(f *StandingRestartFact) {
			f.DeclarationPresent, f.EffectiveState, f.RunState = false, "orphaned", "forked"
		}, wantKind: StandingRestartTerminalOrphaned, wantRepair: StandingRestartRestoreThenReset},
		{name: "terminal declared wins over suspended desired state", mutate: func(f *StandingRestartFact) {
			f.EffectiveState, f.OperatorOverride, f.RunState = "suspended", "suspended", "failed"
		}, wantKind: StandingRestartTerminalDeclared, wantRepair: StandingRestartReset},
		{name: "terminal orphaned wins over orphaned desired state", mutate: func(f *StandingRestartFact) {
			f.DeclarationPresent, f.EffectiveState, f.RunState = false, "orphaned", "cancelled"
		}, wantKind: StandingRestartTerminalOrphaned, wantRepair: StandingRestartRestoreThenReset},
		{name: "running suspended invalid", mutate: func(f *StandingRestartFact) { f.EffectiveState, f.OperatorOverride = "suspended", "suspended" }, wantKind: StandingRestartInvalidCurrent, wantRepair: StandingRestartReset},
		{name: "running orphan invalid", mutate: func(f *StandingRestartFact) { f.DeclarationPresent, f.EffectiveState = false, "orphaned" }, wantKind: StandingRestartInvalidCurrent, wantRepair: StandingRestartRestoreThenReset},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fact := base
			if test.mutate != nil {
				test.mutate(&fact)
			}
			got, err := ClassifyStandingRestart(fact)
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if got.Kind != test.wantKind || got.Remediation != test.wantRepair {
				t.Fatalf("disposition = %s/%s, want %s/%s", got.Kind, got.Remediation, test.wantKind, test.wantRepair)
			}
			if got.UsesGenericRecovery() != test.wantGeneric || got.Executable() != test.wantExec {
				t.Fatalf("generic/executable = %t/%t, want %t/%t", got.UsesGenericRecovery(), got.Executable(), test.wantGeneric, test.wantExec)
			}
		})
	}
}

func TestClassifyStandingRestartRejectsUnprovableFacts(t *testing.T) {
	base := StandingRestartFact{
		ExactCurrent:       true,
		ServiceID:          "11111111-1111-4111-8111-111111111111",
		RunID:              "22222222-2222-4222-8222-222222222222",
		Generation:         1,
		DeclarationPresent: true,
		EffectiveState:     "active",
		OperatorOverride:   "none",
		RunState:           "running",
	}
	for _, mutate := range []func(*StandingRestartFact){
		func(f *StandingRestartFact) { f.RunID = "" },
		func(f *StandingRestartFact) { f.Generation = 0 },
		func(f *StandingRestartFact) { f.EffectiveState = "mystery" },
		func(f *StandingRestartFact) { f.OperatorOverride = "mystery" },
		func(f *StandingRestartFact) { f.RunState = "mystery" },
		func(f *StandingRestartFact) { f.EffectiveState = "orphaned" },
		func(f *StandingRestartFact) { f.DeclarationPresent = false },
	} {
		fact := base
		mutate(&fact)
		if _, err := ClassifyStandingRestart(fact); err == nil {
			t.Fatalf("unprovable fact unexpectedly classified: %+v", fact)
		}
	}
}
