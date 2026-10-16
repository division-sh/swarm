package main

import (
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

//go:embed family145-transitions.json
var family145TransitionBytes []byte

func assertFamily145SemanticProofs(t testing.TB, function, beforeSource, afterSource string) {
	t.Helper()
	if function == "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext" {
		before, beforeErr := runSetupAssertions(beforeSource)
		after, afterErr := runSetupAssertions(afterSource)
		var preserved []string
		transport := map[string]bool{
			`t.Fatalf("begin revision capture: %v", err)`: true,
			`t.Fatalf("capture revision: %v", err)`:       true,
			`t.Fatalf("commit revision: %v", err)`:        true,
		}
		for _, assertion := range before {
			if !transport[assertion] {
				preserved = append(preserved, assertion)
			}
		}
		if beforeErr != nil || afterErr != nil || strings.Join(preserved, "\n") != strings.Join(after, "\n") ||
			strings.Count(afterSource, "storetest.CaptureSelectedRunForkRevision(t, ctx, pg, runID, runforkrevision.AllFamilies()...)") != 1 ||
			strings.Count(afterSource, "storetest.StartPostgresRuntimeStore(t)") != 1 || strings.Contains(afterSource, "db.") {
			t.Fatal("native revision capture lost original owner/context/families or routing/history assertions")
		}
		return
	}
	if function == "proveSelectedForkPreparedPreflight" {
		if err := family145ProspectivePreflightError(beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "buildSelectedForkProofContainer" || function == "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
		if err := family145NativeBindingConstructionError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "selectedContractTestRuntimeAuthority" {
		if err := family145ComponentRuntimeAuthorityError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "selectedPreparationDatabaseSnapshot" {
		if err := family145PreparationSnapshotOwnerError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "seedSelectedOperationSource" {
		if err := family145OperationSourceCaptureError(beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if family145ActivitySetupOwner(function) {
		if err := family145ActivitySetupProofError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if family145ActivityReadOwner(function) {
		if err := family145ActivityReadChainProofError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "capture" {
		if !strings.Contains(beforeSource, "runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...)") {
			t.Fatal("activity capture lost its original complete historical family set")
		}
		if err := family145ActivitySnapshotError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "seedSelectedExecutionSourceReplayScopeMarker" || function == "seedSelectedExecutionPostForkSourceEvent" {
		assertFamily145ReplayScopeHistoryProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "seedSelectedExecutionDiagnosticPlatformDeadLetter" || function == "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter" {
		assertFamily145DiagnosticHistoryProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "assertNoForkExecutionRowsForRun" || function == "assertNoSelectedContractExecutionMutationForSource" {
		assertFamily145RefusalProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "seedSourceOutcomeThatMustNotSuppressFork" || function == "seedSelectedRuntimeOutcomeSource" {
		assertFamily145SourceOutcomeProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity" {
		assertFamily145ReplayFaultAssertions(t, beforeSource, afterSource)
		return
	}
	if family145PhysicalBusinessWitness(function) {
		before, beforeErr := family145DiagnosticBusinessAssertions(beforeSource)
		after, afterErr := family145DiagnosticBusinessAssertions(afterSource)
		if beforeErr != nil || afterErr != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
			t.Fatalf("physical observation lost a behavioral assertion: %v/%v", beforeErr, afterErr)
		}
		return
	}
	if function != "TestTerminalPanicFinalizationBothStores" {
		return
	}
	before, after := family145PanicLifecycleCalls(beforeSource), family145PanicLifecycleCalls(afterSource)
	if len(before) != 2 || strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatal("terminal panic migration changed or removed its real lifecycle proofs")
	}
}

func assertFamily145ReplayFaultAssertions(t testing.TB, beforeSource, afterSource string) {
	t.Helper()
	before, beforeErr := runSetupAssertions(beforeSource)
	after, afterErr := runSetupAssertions(afterSource)
	if beforeErr != nil || afterErr != nil {
		t.Fatalf("replay proof parse: %v/%v", beforeErr, afterErr)
	}
	remove := func(calls []string, expected string) []string {
		var kept []string
		found := 0
		for _, call := range calls {
			if call == expected {
				found++
			} else {
				kept = append(kept, call)
			}
		}
		if found != 1 {
			t.Fatalf("replay fault transport assertion %q occurs %d times, want one", expected, found)
		}
		return kept
	}
	before = remove(before, `t.Fatalf("encode original target route: %v", err)`)
	after = remove(after, `t.Fatalf("corrupt replay target route: %v", err)`)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatal("replay migration lost a behavioral assertion")
	}
}

func family145PanicLifecycleCalls(source string) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "panic.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if ok && (member.Sel.Name == "ProveTerminalPanicFinalization" || member.Sel.Name == "ProveTerminalPanicMutationFailure") {
			out = append(out, family145NodeString(call))
		}
		return true
	})
	return out
}

func family145SemanticOwner(function string) bool {
	if family145ActivityReadOwner(function) || family145ActivitySetupOwner(function) {
		return true
	}
	switch function {
	case "captureSelectedExecutionSourceRevision", "admitSelectedExecutionSourceArtifact",
		"seedSelectedExecutionStateOnlySourceRun", "seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource",
		"selectedForkExecutableMutationSnapshotForTest", "selectedContractDynamicRuntimeGlobalSnapshot",
		"assertSelectedContractDeferredWorkRejectionHasNoForkMutation",
		"postgresEventDeliveryTargetRoutes", "scanDeliveryTargetRoutes", "seedEventPublishConstructedFlow",
		"assertRunStartPersistence", "TestTerminalPanicFinalizationBothStores",
		"TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity", "gateLifecycleDiagnosticOverlap", "loadNotifyAllChildrenNumericRegistrations", "loadNotifyAllChildrenSingleEventID",
		"loadNotifyAllChildrenSemanticRejections", "assertReporterFanOutSingleChunkHistory", "assertServingMatrixHistory", "requireSourceRevisionLifecycleDiagnosticReceipts":
		return true
	case "seedSourceOutcomeThatMustNotSuppressFork", "seedSelectedRuntimeOutcomeSource":
		return true
	case "assertNoForkExecutionRowsForRun", "assertNoSelectedContractExecutionMutationForSource":
		return true
	case "seedSelectedExecutionDiagnosticPlatformDeadLetter", "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter":
		return true
	case "seedSelectedExecutionSourceReplayScopeMarker", "seedSelectedExecutionPostForkSourceEvent":
		return true
	case "capture":
		return true
	case "seedSelectedOperationSource":
		return true
	case "selectedPreparationDatabaseSnapshot":
		return true
	case "selectedContractTestRuntimeAuthority":
		return true
	case "proveSelectedForkPreparedPreflight":
		return true
	case "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext":
		return true
	case "buildSelectedForkProofContainer", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority":
		return true
	}
	return false
}

func family145PhysicalBusinessWitness(function string) bool {
	switch function {
	case "gateLifecycleDiagnosticOverlap", "loadNotifyAllChildrenNumericRegistrations", "loadNotifyAllChildrenSingleEventID",
		"loadNotifyAllChildrenSemanticRejections", "assertReporterFanOutSingleChunkHistory", "assertServingMatrixHistory", "requireSourceRevisionLifecycleDiagnosticReceipts":
		return true
	}
	return false
}

func family145DiagnosticBusinessAssertions(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "diagnostic.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var kept []ast.Stmt
		for _, stmt := range block.List {
			conditional, ok := stmt.(*ast.IfStmt)
			if ok && conditional.Init != nil && family145PhysicalTransportCheck(family145NodeString(conditional.Init)) {
				continue
			}
			kept = append(kept, stmt)
		}
		block.List = kept
		return true
	})
	return runSetupAssertions(family145NodeString(file.Decls[0]))
}

func family145PhysicalTransportCheck(initialization string) bool {
	return strings.HasPrefix(initialization, "err := rows.Scan(") || strings.HasPrefix(initialization, "err := logs.Scan(") ||
		initialization == "err := rows.Err()" || initialization == "err := logs.Err()"
}

func family145PublicationRawEscape(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "publication.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch member.Sel.Name {
		case "AdmitPostgresRuntimeStore", "AdmitSQLiteRuntimeStore", "NewPostgresStoreForTest", "DatabaseForTest", "Database", "QueryRow", "QueryRowContext", "Query", "QueryContext", "Exec", "ExecContext", "Begin", "BeginTx":
			err = fmt.Errorf("raw call %s", member.Sel.Name)
			return false
		}
		return err == nil
	})
	return err
}

func family145StageCompletionIndependenceAssertion(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "journey.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	fn, err := uniqueFunction(file, "requireStageCompletionIndependentOwner")
	if err != nil {
		return err
	}
	expected, err := canonicalFunction(`func requireStageCompletionIndependentOwner(t testing.TB, original, recovered stageCompletionJourneyStore) {
	t.Helper()
	if recovered == nil || recovered == original {
		t.Fatal("stage completion rehydration requires an independent native same-location owner")
	}
}`)
	if err != nil {
		return err
	}
	if family145NodeString(fn) != expected {
		return fmt.Errorf("stage completion independence assertion changed")
	}
	return nil
}

func family145NativeOutcomeSnapshot(fn *ast.FuncDecl) error {
	var snapshots int
	var problem error
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := family145NodeString(call.Fun)
		switch {
		case name == "storetest.CaptureRunForkSnapshot":
			snapshots++
			if len(call.Args) != 4 || family145NodeString(call) != "storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)" {
				problem = fmt.Errorf("snapshot must consume the original selected owner and coordinates")
			}
		case name == "captureSelectedExecutionSourceRevision" || strings.Contains(name, "BeginTx") ||
			strings.Contains(name, "CaptureSQLite") || name == "storetest.DatabaseForTest" || strings.HasSuffix(name, ".Commit"):
			problem = fmt.Errorf("native snapshot retained a duplicate raw transaction protocol: %s", name)
		}
		return true
	})
	if problem != nil {
		return problem
	}
	if snapshots != 1 {
		return fmt.Errorf("native snapshot multiplicity=%d, want exactly one", snapshots)
	}
	return nil
}
