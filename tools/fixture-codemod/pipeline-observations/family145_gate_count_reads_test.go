package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var family145GateCountFunctions = map[string]bool{
	"TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores": true,
	"TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores":            true,
	"TestA2JoinCloseLostOwnerResultRecoversOnBothStores":                  true,
	"TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores":           true,
	"footprint": true,
}

func family145GateCountRow(file, function string) bool {
	return family145GateCountFunctions[function] && (function != "footprint" || file == "internal/runtime/pipeline/a2_membership_snapshot_admission_external_test.go")
}

var family145GateCountPorts = []struct{ table, port string }{
	{"entity_state", "CountWorkflowRunFieldRows"},
	{"flow_instances", "CountRunFlowInstanceStorage"},
	{"flow_instance_runtime_readiness", "CountRunFlowRuntimeReadinessStorage"},
	{"timers", "CountRunTimerStorage"},
	{"entity_mutations", "CountRunMutationStorage"},
}

func family145NormalizeGateCountAssertions(key, source string) string {
	function := strings.SplitN(key, "|", 2)[1]
	if !family145GateCountFunctions[function] {
		return source
	}
	activation := function == "TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores"
	for _, port := range family145GateCountPorts {
		argument := "SELECT COUNT(*) FROM " + port.table + " WHERE run_id=$1"
		if activation {
			argument = port.table
		}
		source = strings.ReplaceAll(source, "count(storetest."+port.port+"(ctx, selected.events, runID))", "count("+strconv.Quote(argument)+")")
	}
	source = strings.ReplaceAll(source, "count(storetest.CountRunEventNameStorage(ctx, selected.events, runID, \"platform.join_complete\"))", "count(\"SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='platform.join_complete'\")")
	source = strings.ReplaceAll(source, "constructions()", "count(\"SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1\")")
	if activation {
		source = strings.ReplaceAll(source, "count(observation.read())", "count(table)")
	} else {
		source = strings.ReplaceAll(source, "count(observation.read())", "count(\"SELECT COUNT(*) FROM \" + table + \" WHERE run_id=$1\")")
	}
	return strings.ReplaceAll(source, "observation.table", "table")
}

func family145GateCountCalls(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var calls []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "count" {
			calls = append(calls, family145NodeString(call))
		}
		return true
	})
	return calls, nil
}

func family145GateCountProofError(key, before, after string) error {
	normalized := family145NormalizeGateCountAssertions(key, after)
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(normalized)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("run count changed original cardinality, rollback, replay or identity assertions: %s: %v", key, err)
	}
	wantCalls, err := family145GateCountCalls(before)
	if err != nil {
		return err
	}
	gotCalls, err := family145GateCountCalls(normalized)
	if err != nil || !reflect.DeepEqual(wantCalls, gotCalls) {
		return fmt.Errorf("run count changed independent call multiplicity or temporal order: %s: %v/%v", key, wantCalls, gotCalls)
	}
	return family145GateCountReaderBindings(key, after)
}

func family145GateCountReaderBindings(key, source string) error {
	function := strings.SplitN(key, "|", 2)[1]
	var cuts []string
	switch function {
	case "TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores":
		cuts = []string{
			`{"entity_state", func() (int, error) { return storetest.CountWorkflowRunFieldRows(ctx, selected.events, runID) }}`,
			`{"flow_instances", func() (int, error) { return storetest.CountRunFlowInstanceStorage(ctx, selected.events, runID) }}`,
			`accounting, err := storetest.ObserveH2ConstructionAccounting(ctx, selected.events, runID)`,
			`return accounting.Constructions, err`,
			`{"flow_instance_runtime_readiness", func() (int, error) { return storetest.CountRunFlowRuntimeReadinessStorage(ctx, selected.events, runID) }}`,
			`{"timers", func() (int, error) { return storetest.CountRunTimerStorage(ctx, selected.events, runID) }}`,
			`{"entity_mutations", func() (int, error) { return storetest.CountRunMutationStorage(ctx, selected.events, runID) }}`,
		}
	case "TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores":
		cuts = []string{
			"constructions := func() int {\n\t\t\t\tt.Helper()\n\t\t\t\taccounting, err := storetest.ObserveH2ConstructionAccounting(ctx, selected.events, runID)\n\t\t\t\treturn count(accounting.Constructions, err)\n\t\t\t}",
			`{"entity_state", func() (int, error) { return storetest.CountWorkflowRunFieldRows(ctx, selected.events, runID) }}`,
			`{"timers", func() (int, error) { return storetest.CountRunTimerStorage(ctx, selected.events, runID) }}`,
		}
	case "footprint":
		for _, port := range []string{"CountRunMutationStorage", "CountRunTimerStorage", "CountRunFanOutIntentStorage", "CountRunFanOutBarrierStorage", "CountNestedRunFanOutOutcomes", "CountA2MembershipRefusalEvents"} {
			cuts = append(cuts, "func() (int, error) { return storetest."+port+"(f.ctx, f.selected.events, f.owner.RunID) }")
		}
		cuts = append(cuts, `for index, table := range []string{"entity_mutations", "timers", "fan_out_intents", "fan_out_obligation_barriers", "fan_out_outcomes", "events"}`, "count, err := reads[index]()", "counts[index] = count", "return counts")
	default:
		cuts = []string{"count := func(n int, err error) int {"}
	}
	source = family145GateCountTokens(source)
	cursor := -1
	for _, cut := range cuts {
		cut = family145GateCountTokens(cut)
		at := strings.Index(source, cut)
		if at < 0 || at <= cursor || strings.Count(source, cut) != 1 {
			return fmt.Errorf("run count lost exact original-owner binding, scope, order or per-call destination: %s", cut)
		}
		cursor = at
	}
	return nil
}

func family145GateCountTokens(source string) string {
	var scan scanner.Scanner
	file := token.NewFileSet().AddFile("cut.go", -1, len(source))
	scan.Init(file, []byte(source), nil, 0)
	var tokens []string
	for {
		_, kind, literal := scan.Scan()
		if kind == token.EOF {
			break
		}
		if kind == token.SEMICOLON {
			continue
		}
		if literal == "" {
			literal = kind.String()
		}
		tokens = append(tokens, literal)
	}
	return strings.Join(tokens, " ")
}

func TestFamily145GateCountPhysicalOwnersRetainExactRunAndEventPredicates(t *testing.T) {
	for _, tc := range []struct{ file, function, query string }{
		{"mutationlog/selected_cleanup_observation.go", "CountRunMutationStorageTx", "SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1"},
		{"genericschedule/test_workflow_scheduler_observation.go", "CountTimerRowsForRun", "SELECT COUNT(*) FROM timers WHERE run_id=$1"},
		{"pipelinepersistence/runtime_outcome_construction_observation.go", "CountRunFlowRuntimeReadinessStorageTx", "SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1"},
		{"pipelinepersistence/nested_fan_out_observation.go", "CountRunFanOutBarrierStorageTx", "SELECT COUNT(*) FROM fan_out_obligation_barriers WHERE run_id=$1"},
		{"pipelinepersistence/owner_operations.go", "CountFanOutIntentsForRun", "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1"},
		{"pipelinepersistence/nested_fan_out_observation.go", "CountNestedRunFanOutOutcomesTx", "SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1"},
		{"eventrecord/causal_observation.go", "CountA2MembershipRefusalEventsTx", "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('platform.join_complete','platform.join_timeout','items.child','batch.completed')"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/"+tc.file, tc.function)
		queries := family145ConnectorSQL(t, "package proof\n"+physical)
		want := family145ConnectorSQL(t, "package proof\nfunc original() { _ = "+strconv.Quote(tc.query)+" }")
		if len(queries) != 1 || queries[0] != want[0] {
			t.Fatalf("%s changed original run-only physical scope or exact event filter: %v", tc.function, queries)
		}
	}
}
