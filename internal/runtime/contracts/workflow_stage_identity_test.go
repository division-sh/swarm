package contracts

import "testing"

func TestCompiledStageReferenceRejectsForeignAndMutableProjections(t *testing.T) {
	build := func(flow string) WorkflowStageTopology {
		return BuildWorkflowStageTopology(flow, "ready", []string{"ready", "Ready", "killed", "Killed"},
			[]string{"Ready", "killed", "Killed"}, nil, nil, nil)
	}
	root, child, anotherSource := build("."), build("child"), build(".")
	ready, err := root.ResolveStage("ready")
	if err != nil || ready.IsFinal() {
		t.Fatalf("nonterminal ready: ref=%#v err=%v", ready, err)
	}
	terminal, err := root.ResolveStage("Ready")
	if err != nil || !terminal.IsFinal() {
		t.Fatalf("terminal Ready: ref=%#v err=%v", terminal, err)
	}
	for _, foreign := range []WorkflowStageTopology{child, anotherSource} {
		if err := foreign.RequireStage(ready); err == nil {
			t.Fatalf("foreign graph %q accepted root reference", foreign.FlowID)
		}
	}
	if err := root.RequireStage(StageRef{}); err == nil {
		t.Fatal("zero stage reference accepted")
	}
	for _, unknown := range []string{"READY", "KILLED", "unknown", ""} {
		if _, err := root.ResolveStage(unknown); err == nil {
			t.Fatalf("undeclared spelling %q accepted", unknown)
		}
	}
	root.Stages[0] = "foreign"
	root.FinalStages[0] = "ready"
	root.InitialStage = "Ready"
	if got, err := root.ResolveStage("ready"); err != nil || got.IsFinal() {
		t.Fatalf("mutable projection changed private stage truth: ref=%#v err=%v", got, err)
	}
	if got, err := root.InitialStageRef(); err != nil || got.ID() != "ready" {
		t.Fatalf("mutable projection changed private initial stage: ref=%#v err=%v", got, err)
	}
	if got := root.GuardTerminationTarget(); got != "killed" {
		t.Fatalf("guard kill selected %q, want exact killed", got)
	}
	withoutKill := BuildWorkflowStageTopology(".", "ready", []string{"ready", "Killed"}, []string{"Killed"}, nil, nil, nil)
	if got := withoutKill.GuardTerminationTarget(); got != "" {
		t.Fatalf("guard kill borrowed wrong-case target %q", got)
	}
}

func TestCompiledStageClassifierRequiresExactFlowAndDeclaredStage(t *testing.T) {
	root := BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	child := BuildWorkflowStageTopology("child", "ready", []string{"ready", "Ready"}, []string{"ready"}, nil, nil, nil)
	classifier, err := NewWorkflowStageClassifier(root, map[string]WorkflowStageTopology{"child": child, "other": root})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		flow, stage     string
		terminal, known bool
	}{
		{"", "ready", false, true},
		{"", "Ready", true, true},
		{"child", "ready", true, true},
		{"child", "Ready", false, true},
		{"child", "READY", false, false},
		{"foreign", "Ready", false, false},
	} {
		terminal, known := classifier.Final(tc.flow, "", tc.stage)
		if terminal != tc.terminal || known != tc.known {
			t.Fatalf("flow=%q stage=%q: terminal=%t known=%t", tc.flow, tc.stage, terminal, known)
		}
	}
	if _, known := classifier.Final("child", "other", "ready"); known {
		t.Fatal("contradictory known flow instance was accepted")
	}
}

func TestCompiledStageClassifierAcceptsCanonicalConstructedRoot(t *testing.T) {
	root := BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	child := BuildWorkflowStageTopology("child", "ready", []string{"ready", "Ready"}, []string{"ready"}, nil, nil, nil)
	classifier, err := NewWorkflowStageClassifier(root, map[string]WorkflowStageTopology{"child": child})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "ce1b0281-3c09-40de-82c1-f089c16a987a"
	for _, tc := range []struct {
		flow, instance, stage string
		terminal, known       bool
	}{
		{".", runID, "ready", false, true},
		{".", runID, "Ready", true, true},
		{".", runID, "READY", false, false},
		{".", "child", "Ready", false, false},
		{"child", "child/key", "ready", true, true},
		{"child", ".", "ready", false, false},
		{"foreign", runID, "Ready", false, false},
	} {
		terminal, known := classifier.Final(tc.flow, tc.instance, tc.stage)
		if terminal != tc.terminal || known != tc.known {
			t.Errorf("flow=%q instance=%q stage=%q: terminal=%t known=%t, want %t/%t", tc.flow, tc.instance, tc.stage, terminal, known, tc.terminal, tc.known)
		}
	}
	if _, err := NewWorkflowStageClassifier(root, map[string]WorkflowStageTopology{".": child}); err == nil {
		t.Fatal("canonical root key accepted a foreign stage topology")
	}
}

func TestCompiledStageClassifierPreservesStatelessStoragePosture(t *testing.T) {
	root := BuildWorkflowStageTopology(".", "", nil, nil, nil, nil, nil)
	staged := BuildWorkflowStageTopology("staged", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	classifier, err := NewWorkflowStageClassifier(root, map[string]WorkflowStageTopology{"staged": staged})
	if err != nil {
		t.Fatal(err)
	}
	if terminal, known := classifier.Final("", "", "pending"); terminal || !known {
		t.Fatalf("stateless pending: terminal=%t known=%t", terminal, known)
	}
	for _, state := range []string{"Pending", "unknown", ""} {
		if _, known := classifier.Final("", "", state); known {
			t.Fatalf("stateless unknown state %q was accepted", state)
		}
	}
	if _, known := classifier.Final("staged", "", "pending"); known {
		t.Fatal("staged flow borrowed stateless pending posture")
	}
	if initial, err := root.InitialStoredStage(); err != nil || initial.ID() != "pending" || !initial.IsStatelessPosture() || initial.IsFinal() {
		t.Fatalf("stateless initial storage state = %#v, %v", initial, err)
	}
	if _, err := root.ResolveStage("pending"); err == nil {
		t.Fatal("stateless storage posture became an authored stage")
	}
}
