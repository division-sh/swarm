package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func assertFamily145DiagnosticHistoryProofs(t testing.TB, name, before, after string) {
	t.Helper()
	for _, raw := range []string{"*sql.DB", "ExecContext", "context.Background()", "authoractivityfixture.Dialect", "InsertDiagnosticDirectEventRecordForRun"} {
		if strings.Contains(after, raw) {
			t.Fatalf("diagnostic history retained %s", raw)
		}
	}
	if name == "seedSelectedExecutionDiagnosticPlatformDeadLetter" {
		beforePayload, afterPayload := sourceOutcomePreparationCalls(before, "Marshal"), sourceOutcomePreparationCalls(after, "Marshal")
		if len(beforePayload) != 1 || !reflect.DeepEqual(beforePayload, afterPayload) ||
			!strings.Contains(after, "storetest.SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, sourceRunID, diagnosticEventID, payload, at)") {
			t.Fatal("runtime diagnostic history changed original payload/owner/coordinates")
		}
		return
	}
	original := diagnosticHistoryCallArgs(before, "InsertDiagnosticDirectEventRecordForRun")
	replacement := diagnosticHistoryCallArgs(after, "SeedRunDiagnosticDeadLetterHistory")
	if len(original) != 10 || len(replacement) != 7 || original[8] != replacement[5] ||
		strings.Join(replacement[:5], ",") != "t,ctx,selected,runID,eventID" || replacement[6] != "at" ||
		!strings.Contains(after, "storetest.CaptureSelectedRunForkRevision(t, ctx, selected, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventReceipts)") {
		t.Fatal("served diagnostic history lost exact payload, coordinates or original two-family capture")
	}
}

func diagnosticHistoryCallArgs(source, name string) []string {
	var out []string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != name {
			return true
		}
		for _, arg := range call.Args {
			out = append(out, family145NodeString(arg))
		}
		return true
	})
	return out
}

func assertDiagnosticHistoryBusinessReplacement(t *testing.T) {
	t.Helper()
	const path = "internal/runtime/runforkexecution/execution_test.go"
	const name = "TestExecuteSelectedContractRunForkBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint"
	raw, err := os.ReadFile("../../../" + path)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := exec.Command("git", "show", selectedreadrole.Repair2698Before+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	current, prior := requestKeyDeclaration(t, raw, name), requestKeyDeclaration(t, signed, name)
	if current == "" || current != prior {
		t.Fatal("native source-advance replacement changed its complete settled tuple/cuts/assertions")
	}
	for _, cut := range []string{
		"storetest.InsertRevisionedExistingRunRootEventRecord(t, ctx, pg, afterEventID, sourceRunID",
		"at.Add(time.Second)", "pg.PipelineObligations()",
		"pipeline.ClaimEvent(ctx, sourceEventID, runtimepipelineobligation.PurposeRecovery)",
		`pipeline.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("source_outcome_must_not_suppress_fork"))`,
		"!result.Activation.SourceAdvancedAfterFork", "source_events_advanced_after_fork_point",
		"source_receipts_advanced_after_fork_point", "source_run_terminal_at_activation",
	} {
		if !strings.Contains(current, cut) {
			t.Fatal("native business-event/settlement owner or original source-advance oracle disappeared:", cut)
		}
	}
}
