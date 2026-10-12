package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

var family145GateRecoveryHelpers = map[string][]string{
	"installProposedEffectCreateFailure": {"storetest.InstallGateProposedEffectCreationFailure(testAuthorActivityContext(t, context.Background()), selected)"},
	"setGateRecoveryRouteAttempt":        {"storetest.SetGateDecisionRouteAttemptFault(testAuthorActivityContext(t, context.Background()), selected, eventID, attempt, time.Now().UTC().Add(-time.Second))"},
	"makeGateRecoveryRouteDue":           {"storetest.SetGateDecisionRouteDueFault(testAuthorActivityContext(t, context.Background()), selected, eventID, due.UTC())"},
	"assertProposedEffectOutcomeCount":   {"storetest.CountRunNamedEventStorage(testAuthorActivityContext(t, context.Background()), selected, runID, eventType)", "storetest.ReadGateRunEventNames(testAuthorActivityContext(t, context.Background()), selected, runID)", "if got != want"},
	"proposedEffectProofFailure":         {"storetest.ReadGateLatestDeadLetterFailure(testAuthorActivityContext(t, context.Background()), selected, eventID)", "errors.Is(err, sql.ErrNoRows)", "return \"<no dead letter>\"", "return err.Error()", "return failure"},
	"gateRecoveryOutcomeEventID":         {"storetest.ReadTemplateSourceNamedEventIdentity(testAuthorActivityContext(t, context.Background()), selected, \"launch.approved\", parentEventID)", "return eventID"},
	"gateRecoveryOutcomeEventCount":      {"storetest.CountGateSourceNamedEvents(testAuthorActivityContext(t, context.Background()), selected, \"launch.approved\", parentEventID)", "return count"},
	"gateRecoveryDeliveryCount":          {"storetest.CountGateEventSubscriberDeliveries(testAuthorActivityContext(t, context.Background()), selected, eventID, recipient)", "return count"},
}

var family145GateRecoveryCallers = map[string][]string{
	"TestApprovedActivityHoldsThenDispatchesExactFrozenInputOnBothStores":                  {"proposedEffectProofFailure(t, selected.events, sourceEvent.ID())", "assertProposedEffectOutcomeCount(t, selected.events, runID, wantEvent, 1)"},
	"TestApprovedActivityProposalCreationRollsBackWorkflowCardAndContinuationOnBothStores": {"installProposedEffectCreateFailure(t, selected.events)", "storetest.CountGateRunProposedEffectContinuations(ctx, selected.events, runID)", "storetest.ReadGateNodeDeliveryStatus(ctx, selected.events, event.ID(), supportNode.Key())"},
	"TestDecisionRouteObligationFairnessAdmitsNewWorkBehindFullDeferredPageOnBothStores":   {"setGateRecoveryRouteAttempt(t, selected.events, eventID, 1)", "setGateRecoveryRouteAttempt(t, selected.events, newEventID, 2)"},
	"testWorkflowGateStartupTerminalRecovery":                                              {"makeGateRecoveryRouteDue(t, tc.events, eventID, time.Now().Add(-time.Second))", "storetest.ReadRunStatusStorage(ctx, tc.events, runID)"},
	"testWorkflowGateUnavailablePinRecovery":                                               {"makeGateRecoveryRouteDue(t, tc.events, decisionEventID, time.Now().Add(-time.Second))", "gateRecoveryOutcomeEventID(t, tc.events, decisionEventID)", "gateRecoveryDeliveryCount(t, tc.events, outcomeEventID, outcomeAgent)", "gateRecoveryOutcomeEventCount(t, tc.events, decisionEventID)"},
	"TestSelectionRetryAfterRealCASConflictBothStores":                                     {"proposedEffectProofFailure(t, selected.events, event.ID())"},
}

func family145GateRecoveryProofError(function, before, after string) error {
	normalized, err := family145GateRecoveryAssertionSource(after)
	if err != nil {
		return err
	}
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(normalized)
	if err != nil {
		return err
	}
	index := 0
	for _, assertion := range got {
		if index < len(want) && assertion == want[index] {
			index++
		}
	}
	if index != len(want) {
		return fmt.Errorf("gate recovery lost original assertion: %s", want[index])
	}
	cuts, helper := family145GateRecoveryHelpers[function]
	if helper {
		if !strings.Contains(after, "selected gateRecoverySelectedStore") {
			return fmt.Errorf("gate helper retained raw-bearing container")
		}
		if err := family145DescriptorReadinessEscape([]byte("package proof\n" + after)); err != nil {
			return err
		}
	} else {
		cuts = family145GateRecoveryCallers[function]
	}
	for _, cut := range cuts {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("gate recovery lost exact helper/native-owner cut: %s", cut)
		}
	}
	return nil
}

// Only the reviewed helper's container-to-original-selected-field propagation
// is normalized for assertion comparison. No assertion condition is removed.
func family145GateRecoveryAssertionSource(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if _, reviewed := family145GateRecoveryHelpers[name.Name]; !reviewed {
			return true
		}
		if field, ok := call.Args[1].(*ast.SelectorExpr); ok && field.Sel.Name == "events" {
			call.Args[1] = field.X
		}
		return true
	})
	return family145NodeString(file.Decls[0]), nil
}

func TestFamily145GateRecoveryEntireHelperAndCallerChainRetainsAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string]string{}
	for _, row := range rows {
		cuts, helper := family145GateRecoveryHelpers[row.Function]
		if !helper {
			var caller bool
			cuts, caller = family145GateRecoveryCallers[row.Function]
			if !caller {
				continue
			}
		}
		original[row.Function] = row.Before
		if err := family145GateRecoveryProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range cuts {
			if family145GateRecoveryProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost gate helper/owner cut accepted: %s", cut)
			}
		}
		assertFamily145GateRecoveryHostileConditions(t, row.Function, row.Before, row.After)
	}
	if len(original) != len(family145GateRecoveryHelpers)+len(family145GateRecoveryCallers) {
		t.Fatalf("gate dependency inventory incomplete: %d", len(original))
	}
	assertFamily145GateRecoveryPhysicalQueries(t, original)
}

func assertFamily145GateRecoveryHostileConditions(t *testing.T, function, before, after string) {
	t.Helper()
	assertions, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		t.Fatal(err)
	}
	for _, assertion := range assertions {
		condition := strings.SplitN(assertion, " => ", 2)[0]
		hostile, err := family145BusAssertionCounterexample(after, condition)
		if err != nil {
			t.Fatal(err)
		}
		if family145GateRecoveryProofError(function, before, hostile) == nil {
			t.Fatalf("lost original gate assertion accepted: %s", condition)
		}
	}
}

func assertFamily145GateRecoveryPhysicalQueries(t *testing.T, original map[string]string) {
	t.Helper()
	for _, port := range []struct{ file, function, before string }{
		{"decisionpersistence/gate_recovery_fixture.go", "CountGateRunProposedEffectContinuationsTx", "TestApprovedActivityProposalCreationRollsBackWorkflowCardAndContinuationOnBothStores"},
		{"delivery/read_projections.go", "ReadGateNodeDeliveryStatusTx", "TestApprovedActivityProposalCreationRollsBackWorkflowCardAndContinuationOnBothStores"},
		{"delivery/read_projections.go", "ReadGateLatestDeadLetterFailureTx", "proposedEffectProofFailure"},
		{"delivery/read_projections.go", "CountGateEventSubscriberDeliveriesTx", "gateRecoveryDeliveryCount"},
		{"eventrecord/template_storage_observation.go", "ReadGateRunEventNamesTx", "assertProposedEffectOutcomeCount"},
	} {
		before := family145ConnectorSQL(t, "package proof\n"+original[port.before])
		for _, query := range family145ArtifactCallbackOwnerSQL(t, port.file, port.function) {
			found := false
			for _, candidate := range before {
				found = found || query == candidate
			}
			if !found {
				t.Fatalf("gate observation changed physical scope/NULL/order: %s: %s", port.function, query)
			}
		}
	}
	queries := family145ArtifactCallbackOwnerSQL(t, "eventrecord/template_storage_observation.go", "CountGateSourceNamedEventsTx")
	originalCount := family145ConnectorSQL(t, "package proof\n"+original["gateRecoveryOutcomeEventCount"])
	if len(queries) != 1 || len(originalCount) != 2 {
		t.Fatalf("fixed source/name cardinality inventory changed: %v/%v", queries, originalCount)
	}
	bound := strings.ReplaceAll(strings.ReplaceAll(queries[0], "$1", "'launch.approved'"), "$2", "$1")
	if bound != originalCount[0] || bound != originalCount[1] {
		t.Fatalf("fixed source/name cardinality changed: %v/%v", queries, originalCount)
	}
}

func TestFamily145GateRecoveryNamedFaultSQLMatchesOriginalTemporalCuts(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../../internal/store/internal/backend/decisionpersistence/gate_recovery_fixture.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, keyword := range []string{"UPDATE", "CREATE"} {
		owned := family145GateRecoveryStatements(t, string(data), keyword)
		for _, row := range rows {
			if row.Function != "makeGateRecoveryRouteDue" && row.Function != "setGateRecoveryRouteAttempt" && row.Function != "installProposedEffectCreateFailure" {
				continue
			}
			for _, statement := range family145GateRecoveryStatements(t, "package proof\n"+row.Before, keyword) {
				found := false
				for _, candidate := range owned {
					found = found || statement == candidate
				}
				if !found {
					t.Fatalf("named gate fault changed table/phase/columns/predicate/payload: %s", row.Function)
				}
			}
		}
	}
}

func family145GateRecoveryStatements(t *testing.T, source, keyword string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(strings.TrimSpace(value), keyword+" ") {
			query := "package proof; const query = " + strconv.Quote("SELECT "+value)
			out = append(out, strings.TrimPrefix(family145ConnectorSQL(t, query)[0], "SELECT"))
		}
		return true
	})
	return out
}
