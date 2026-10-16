package main

import (
	"go/scanner"
	"go/token"
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
