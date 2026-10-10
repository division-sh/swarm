package store_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func verifyNativeAPIReadSetupDoesNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	verifyRetiredAPICardinalityHelpersStayDeleted(t)
	verifyCanonicalEventFixtureReadOwnership(t)
	for _, finding := range findings {
		if nativeAPIReadSetupAuthority(finding) {
			t.Errorf("native API read/control fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func retiredSQLiteAPICounterDeclarations(file *ast.File) []string {
	var retired []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "countSQLiteEventsByName", "countSQLiteAllEventRows", "countSQLiteAllRunRows", "countSQLiteAPIIdempotencyRows", "countSQLiteEventRowsByRunID", "countEventDeliveries":
			retired = append(retired, fn.Name.Name)
		case "mailboxWriteDBEnvelope", "mailboxWriteDBJSON", "mailboxWriteDBTime", "mailboxWriteParseDBTime":
			retired = append(retired, fn.Name.Name)
		}
	}
	return retired
}

func TestRetiredSQLiteAPICardinalityHelpersStayDeleted(t *testing.T) {
	verifyRetiredAPICardinalityHelpersStayDeleted(t)
}

func verifyRetiredAPICardinalityHelpersStayDeleted(t *testing.T) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(persistenceAuthorityRepoRoot(t), "internal", "apiv1", "*.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("enumerate owned API sources: %v", err)
	}
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		if retired := retiredSQLiteAPICounterDeclarations(file); len(retired) != 0 {
			t.Fatalf("retired API SQL/compatibility observation helper reintroduced: %s: %v", path, retired)
		}
	}
}

func TestNativeAPICensusConsumesTypedRetirementGuard(t *testing.T) {
	path := filepath.Join(persistenceAuthorityRepoRoot(t), "internal", "store", "persistence_authority_native_read_setup_test.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]int{"verifyRetiredAPICardinalityHelpersStayDeleted": 0, "verifyCanonicalEventFixtureReadOwnership": 0}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "verifyNativeAPIReadSetupDoesNotReceiveRawAuthority" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if ok {
				name, ok := call.Fun.(*ast.Ident)
				if ok {
					if _, expected := calls[name.Name]; expected {
						calls[name.Name]++
					}
				}
			}
			return true
		})
	}
	for name, count := range calls {
		if count != 1 {
			t.Fatalf("core census must invoke the exact observation guard once: %s/%d", name, count)
		}
	}
}

func TestRetiredSQLiteAPICardinalityGuardRejectsRawAndDelegatingDefinitions(t *testing.T) {
	for _, name := range []string{"countSQLiteEventsByName", "countSQLiteAllEventRows", "countSQLiteAllRunRows", "countSQLiteAPIIdempotencyRows", "countSQLiteEventRowsByRunID", "countEventDeliveries", "mailboxWriteDBEnvelope", "mailboxWriteDBJSON", "mailboxWriteDBTime", "mailboxWriteParseDBTime"} {
		for _, source := range []string{
			`package fixture;import "database/sql";func ` + name + `(db *sql.DB){_ = db}`,
			"//go:build excluded\npackage fixture;func " + name + `(owner any)int{return canonical(owner)}`,
			`package fixture;type Probe struct{};func (Probe) ` + name + `(owner any)int{return canonical(owner)}`,
		} {
			file, err := parser.ParseFile(token.NewFileSet(), "hostile.go", source, parser.AllErrors)
			if err != nil || len(retiredSQLiteAPICounterDeclarations(file)) != 1 {
				t.Fatalf("retired declaration not rejected: %s: %v", name, err)
			}
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), "ordinary.go", `package fixture;var label="countSQLiteEventsByName";func neighbor(){}`, parser.AllErrors)
	if err != nil || len(retiredSQLiteAPICounterDeclarations(file)) != 0 {
		t.Fatalf("inert text/unrelated declaration rejected: %v", err)
	}
}

func nativeAPIReadSetupAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/apiv1/operator_mailbox_test.go":
		return finding.Enclosing == "countEventsByName" || finding.Enclosing == "countAPIIdempotencyRows" || finding.Enclosing == "countEventDeliveriesForEvent" || finding.Enclosing == "countPipelineReceiptsForEvent"
	case "internal/apiv1/operator_event_publish_test.go":
		return finding.Enclosing == "assertNoEventPublishPersistence" || finding.Enclosing == "assertNoFlowScopedEventPublishPersistence" || finding.Enclosing == "countAllEventRows" || finding.Enclosing == "countAllEventDeliveries" || finding.Enclosing == "loadPipelineReceiptOutcomeAndFailure" || finding.Enclosing == "TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication"
	case "internal/apiv1/operator_event_replay_test.go":
		return finding.Enclosing == "assertReplayPersistence" || finding.Enclosing == "assertAgentReplayPersistence" || finding.Enclosing == "countOperatorReplayEvents" || finding.Enclosing == "latestEventIDByName"
	case "internal/apiv1/operator_agent_control_test.go":
		return finding.Enclosing == "countDirectiveEvents"
	case "internal/apiv1/operator_mailbox_event_fixture_test.go":
		return finding.Enclosing == "loadMailboxWritePersistedEvent"
	case "internal/apiv1/selected_store_read_supported_surface_test.go":
		return finding.Enclosing == "TestSelectedStoreRunReadHandlersExecuteAcrossBackends"
	case "internal/apiv1/operator_fan_out_test.go":
		return finding.Enclosing == "TestFanOutReadAPISelectedStores"
	case "internal/apiv1/operator_entity_test.go":
		return finding.Enclosing == "TestOperatorEntityHandlersServeContractEntityTypesFromPostgres"
	case "internal/apiv1/operator_run_control_test.go":
		return finding.Enclosing == "TestOperatorRunControlHandlersTypedResourceErrors" || finding.Enclosing == "TestOperatorRunStopDoesNotReplayCommittedTransitionAfterReconciliationFailure"
	case "internal/apiv1/operator_run_start_test.go":
		return finding.Enclosing == "TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable" || finding.Enclosing == "countAllRunRows" || finding.Enclosing == "assertNoRunStartPersistence" || finding.Enclosing == "countRunRowsByID" || finding.Enclosing == "countEventRowsByRunID"
	}
	return false
}

func TestNativeAPIEventEvidenceGuardRejectsRawHelpers(t *testing.T) {
	for _, probe := range []struct{ file, function string }{
		{"operator_mailbox_test.go", "countEventsByName"},
		{"operator_event_publish_test.go", "assertNoEventPublishPersistence"},
		{"operator_event_replay_test.go", "assertReplayPersistence"},
		{"operator_event_replay_test.go", "assertAgentReplayPersistence"},
		{"operator_event_replay_test.go", "countOperatorReplayEvents"},
		{"operator_event_replay_test.go", "latestEventIDByName"},
		{"operator_agent_control_test.go", "countDirectiveEvents"},
		{"operator_mailbox_event_fixture_test.go", "loadMailboxWritePersistedEvent"},
		{"operator_mailbox_test.go", "countAPIIdempotencyRows"},
		{"operator_mailbox_test.go", "countEventDeliveriesForEvent"},
		{"operator_mailbox_test.go", "countPipelineReceiptsForEvent"},
		{"operator_run_start_test.go", "countAllRunRows"},
		{"operator_run_start_test.go", "assertNoRunStartPersistence"},
		{"operator_run_start_test.go", "countRunRowsByID"},
		{"operator_run_start_test.go", "countEventRowsByRunID"},
		{"operator_event_publish_test.go", "countAllEventRows"},
		{"operator_event_publish_test.go", "countAllEventDeliveries"},
		{"operator_event_publish_test.go", "loadPipelineReceiptOutcomeAndFailure"},
		{"operator_event_publish_test.go", "assertNoFlowScopedEventPublishPersistence"},
		{"operator_event_publish_test.go", "TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication"},
	} {
		rejected := false
		source := `package fixture;import "database/sql";func ` + probe.function + `(db *sql.DB){_ = db}`
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/apiv1/"+probe.file, source) {
			rejected = rejected || nativeAPIReadSetupAuthority(finding)
		}
		if !rejected {
			t.Fatalf("raw API evidence helper accepted: %s", probe.function)
		}
	}
}
