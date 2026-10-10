package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
)

const frozen = "1df1e21aa6c64842ed9aafd38f8086948ce1fb0e"
const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const targetDigest = "1690ca4675c7189489cf78faba03e913b1e4ac2fa3bf8953d1b8d1919e180c7c"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string          `json:"Successor,omitempty"`
	Removed                               bool            `json:"Removed,omitempty"`
	Mechanical                            json.RawMessage `json:"Mechanical,omitempty"`
}

type target struct{ File, Function string }
type transition struct{ File, Function, Before, After string }

var targets = []target{
	{"internal/serveapp/lifecycle_emitter_competing_exit_test.go", "TestServedLifecycleEmitterCompetingExitPublication"},
	{"internal/serveapp/lifecycle_release_process_test.go", "TestReleaseCompiledLifecycleJourneysBothStores"},
	{"internal/serveapp/lifecycle_transition_gate_process_test.go", "TestServedCompiledGateOutcomeRestartOnBothStores"},
	{"internal/serveapp/lifecycle_transition_gate_restart_test.go", "proveServedCompiledGateOutcomeRestart"},
	{"internal/serveapp/lifecycle_transition_gate_test.go", "TestServedCompiledGateAdvanceOnlyOnBothStores"},
	{"internal/serveapp/lifecycle_transition_nested_test.go", "TestServedCompiledTransitionNestedCarrierCollisionOnBothStores"},
	{"internal/serveapp/lifecycle_transition_node_recovery_test.go", "TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores"},
	{"internal/serveapp/lifecycle_transition_replay_test.go", "TestServedCompiledLoopTransitionReplayOnBothStores"},
	{"internal/serveapp/lifecycle_transition_restart_test.go", "TestServedCompiledGateFrozenTransitionEvidenceOnBothStores"},
	{"internal/serveapp/lifecycle_transition_restart_test.go", "TestServedCompiledTransitionRestartOnBothStores"},
	{"internal/serveapp/lifecycle_transition_served_test.go", "TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores"},
	{"internal/serveapp/lifecycle_transition_static_fork_test.go", "TestServedCompiledTransitionStaticForkEvidenceOnBothStores"},
	{"internal/serveapp/lifecycle_emitter_timer_diagnostic_test.go", "startLifecycleTimerContenderDiagnostic"},
	{"internal/serveapp/mailbox_completion_process_test.go", "mailboxCompletionProcessHarnessWithSelectionCut"},
	{"internal/runtime/pipeline/workflow_compiled_adapter_test.go", "TestCompiledTransitionPreviewExecutionAgreementOnBothStores"},
	{"internal/runtime/pipeline/a2_stage_entry_execution_test.go", "TestA2NonLoopStageReentryOnBothStores"},
	{"internal/runtime/pipeline/workflow_join_lifecycle_test.go", "TestWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStores"},
	{"internal/runtime/pipeline/workflow_join_lifecycle_test.go", "TestWorkflowJoinPersistedArrivalClassificationOnBothStores"},
}

func main() {
	b, err := exec.Command("git", "show", frozen+":"+ledger).Output()
	must(err)
	var rows []recipe
	must(json.Unmarshal(b, &rows))
	if len(rows) != 893 {
		panic("frozen recipe inventory changed")
	}
	var changes []transition
	for _, t := range targets {
		changes = append(changes, reconcileTarget(rows, t))
	}
	raw, err := json.Marshal(changes)
	must(err)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	fmt.Printf("transitions=%d digest=%s\n", len(changes), digest)
	for _, change := range changes {
		fmt.Printf("%s\t%s\n", change.File, change.Function)
	}
	if len(os.Args) > 1 && os.Args[1] == "-write" {
		if digest != targetDigest {
			panic("unreviewed rebase successor bodies")
		}
		data, err := json.MarshalIndent(rows, "", "  ")
		must(err)
		must(os.WriteFile(ledger, append(data, '\n'), 0644))
	}
}

func frozenRecipe(rows []recipe, t target) *recipe {
	var found *recipe
	for i := range rows {
		if rows[i].File != t.File || rows[i].Function != t.Function {
			continue
		}
		if found != nil {
			panic("ambiguous frozen recipe: " + t.Function)
		}
		found = &rows[i]
	}
	if found == nil || found.Removed {
		panic("missing live frozen recipe: " + t.Function)
	}
	return found
}

func reconcileTarget(rows []recipe, t target) transition {
	row := frozenRecipe(rows, t)
	before := row.After
	if row.Successor != "" {
		before = row.Successor
	}
	pin, err := parser.ParseFile(token.NewFileSet(), "pin.go", "package pin\n"+before, parser.AllErrors)
	must(err)
	name := pin.Decls[0].(*ast.FuncDecl).Name.Name
	after := currentBody(t.File, name)
	if after == before {
		panic("unchanged rebase target: " + name)
	}
	row.Successor = after
	return transition{t.File, t.Function, before, after}
}

func currentBody(path, name string) string {
	source, err := os.ReadFile(path)
	must(err)
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, parser.AllErrors)
	must(err)
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		if found != nil {
			panic("ambiguous current recipe: " + name)
		}
		found = fn
	}
	if found == nil {
		panic("missing current recipe: " + name)
	}
	return string(source[set.Position(found.Pos()).Offset:set.Position(found.End()).Offset])
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
