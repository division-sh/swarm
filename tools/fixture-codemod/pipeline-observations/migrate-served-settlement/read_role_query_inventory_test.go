package main

import (
	"go/ast"
	"go/token"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

// This is the complete helper SQL inventory, not a count inferred from the
// implementation output. Caller-only SQL remains outside the twenty-helper scope.
var selectedReadRoleQueries = map[string][]string{
	"internal/serveapp/a2_portfolio_admission_test.go/a2PortfolioAdmissionDomainCounts":            {`SELECT COUNT(*) FROM events WHERE event_name NOT LIKE 'platform.%'`},
	"internal/serveapp/fan_out_one_second_preservation_test.go/assertIssue2394TwoFullChunkHistory": {`SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`},
	"internal/serveapp/fan_out_one_second_preservation_test.go/assertIssue2394OneSecondPayloads":   {`SELECT ordinal,CAST(event_id AS TEXT),outcome_kind FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`},
	"internal/serveapp/fan_out_original_reporter_http_test.go/assertIssue2394ReporterEvents":       {`SELECT CAST(event_id AS TEXT),ordinal FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`},
	"internal/serveapp/fan_out_original_reporter_http_test.go/assertIssue2394ReporterClients":      {},
	"internal/serveapp/fork_delivery_route_evidence_test.go/readServedForkDeliveryEvidence":        {`SELECT delivery_id,event_id,route_identity,subscriber_type,subscriber_id,CAST(delivery_target_route AS TEXT),CAST(delivery_context AS TEXT),CAST(delivery_payload_projection AS TEXT),CAST(connect_execution_claim AS TEXT),CAST(receiver_materialization_plan AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY delivery_id`},
	"internal/serveapp/fork_loop_accumulator_test.go/readForkHandlerAccumulators":                  {`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`},
	"internal/serveapp/fork_loop_generation_notice_test.go/readForkLoopActivation":                 {`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`},
	"internal/serveapp/fork_receiver_execution_effect_test.go/requireForkReceiverNoPublicationOrNotice": {
		`SELECT COUNT(*) FROM mailbox WHERE source_event_id=$1`,
		`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`,
		`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`,
	},
	"internal/serveapp/fork_receiver_execution_effect_test.go/requireForkReceiverMutationOwners":  {`SELECT DISTINCT entity_id FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`},
	"internal/serveapp/fork_receiver_ownership_test.go/requireOptionalForkReceiverHeaderProgress": {`SELECT updated_at>$3 FROM flow_instances WHERE run_id=$1 AND instance_path=$2`},
	"internal/serveapp/fork_recipient_authority_test.go/logServedForkWorkflowRows":                {`SELECT instance_path,flow_template,mode,CAST(config AS TEXT),status FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`},
	"internal/serveapp/fork_recipient_authority_test.go/logServedForkHandlerDiagnostics":          {`SELECT event_id,COALESCE(CAST(run_id AS TEXT),''),CAST(payload AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at,event_id`},
	"internal/serveapp/lifecycle_emitter_competing_exit_test.go/lifecycleCompetingTimerDue":       {`SELECT fire_at FROM timers WHERE run_id = $1 AND entity_id = $2 AND task_type = 'workflow_timer'`},
	"internal/serveapp/lifecycle_transition_fork_test.go/readLifecycleStoredLoop":                 {`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`},
	"internal/serveapp/lifecycle_transition_node_recovery_test.go/readLifecycleNodeRecoveryDelivery": {
		`SELECT event_id, CAST(delivery_target_route AS TEXT), status FROM event_deliveries WHERE delivery_id=$1`,
		`SELECT claim_version, COALESCE(outcome,''), COALESCE(reason_code,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version`,
	},
	"internal/serveapp/lifecycle_transition_template_test.go/readLifecycleTemplateGate":        {`SELECT CAST(accumulator AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2 AND flow_template=$3`},
	"internal/serveapp/lifecycle_transition_template_test.go/requireLifecycleTemplateInstance": {`SELECT flow_template FROM flow_instances WHERE run_id=$1 AND instance_path=$2`},
	"internal/serveapp/lifecycle_transition_template_test.go/logLifecycleTemplateCompletionRows": {
		`SELECT event_id,event_name,CAST(payload AS TEXT),routing_source_kind,CAST(source_route AS TEXT),CAST(target_route AS TEXT),CAST(route_settlement AS TEXT) FROM events WHERE run_id=$1 AND (event_name LIKE '%work.completed' OR event_name='mailbox.card_decided') ORDER BY created_at,event_id`,
		`SELECT d.delivery_id,d.event_id,d.subscriber_id,d.status,COALESCE(d.reason_code,''),CAST(d.delivery_target_route AS TEXT) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY d.created_at,d.delivery_id`,
		`SELECT r.event_id,r.subscriber_id,r.outcome,COALESCE(r.reason_code,''),CAST(r.side_effects AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY r.processed_at`,
	},
	"internal/serveapp/selected_fork_pending_input_test.go/assertNoUnselectedTargetActivation": {`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='on_demand'`},
}

func selectedReadRoleQueryMultiset(fn *ast.FuncDecl) map[string]int {
	out := map[string]int{}
	ast.Inspect(fn, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err == nil && strings.HasPrefix(value, "SELECT ") {
				out[value]++
			}
		}
		return true
	})
	return out
}

func selectedReadRoleQueryInventoryMatches(name string, actual map[string]int) bool {
	queries, known := selectedReadRoleQueries[name]
	want := map[string]int{}
	for _, query := range queries {
		want[query]++
	}
	return known && reflect.DeepEqual(actual, want)
}

func TestSelectedReadRoleQueryInventoryMatchesHistoricalSourceAndCompletePins(t *testing.T) {
	pins, err := selectedreadrole.ReadPins("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	seen := map[string]bool{}
	for _, pin := range pins {
		if !readRoleNames[pin.Function] {
			continue
		}
		if files[pin.File] == nil {
			files[pin.File], err = exec.Command("git", "show", selectedreadrole.Predecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
		}
		before, err := servedsettlement.UniqueBody(files[pin.File], pin.Function)
		if err != nil || before != pin.Before || seen[pin.Function] {
			t.Fatal("signed source missing, repeated or changed:", pin.Function, err)
		}
		seen[pin.Function] = true
		key := pin.File + "/" + pin.Function
		if !selectedReadRoleQueryInventoryMatches(key, selectedReadRoleQueryMultiset(readRoleProofFunction(t, before))) {
			t.Fatal("original source SQL differs from the complete helper multiset:", pin.Function)
		}
		if !selectedReadRoleQueryInventoryMatches(key, selectedReadRoleQueryMultiset(readRoleProofFunction(t, pin.Before))) {
			t.Fatal("frozen predecessor SQL differs from the complete helper multiset:", pin.Function)
		}
		if len(selectedReadRoleQueryMultiset(readRoleProofFunction(t, pin.After))) != 0 {
			t.Fatal("selected helper retains direct SQL:", pin.Function)
		}
	}
	if !reflect.DeepEqual(seen, readRoleNames) || len(selectedReadRoleQueries) != len(readRoleNames) {
		t.Fatal("complete helper SQL inventory changed")
	}
}

func TestSelectedReadRoleQueryInventoryRefusesMissingExtraDuplicateAndChangedSQL(t *testing.T) {
	for name, queries := range selectedReadRoleQueries {
		for _, query := range queries {
			actual := map[string]int{}
			for _, original := range queries {
				actual[original]++
			}
			delete(actual, query)
			if selectedReadRoleQueryInventoryMatches(name, actual) {
				t.Fatal("missing query accepted:", name)
			}
			actual[query] = 2
			if selectedReadRoleQueryInventoryMatches(name, actual) {
				t.Fatal("duplicate query accepted:", name)
			}
			actual[query] = 1
			actual[query+" AND foreign_scope=1"] = 1
			if selectedReadRoleQueryInventoryMatches(name, actual) {
				t.Fatal("extra/changed predicate accepted:", name)
			}
		}
	}
	if selectedReadRoleQueryInventoryMatches("internal/serveapp/fan_out_original_reporter_http_test.go/assertIssue2394ReporterClients", map[string]int{"SELECT 1": 1}) || selectedReadRoleQueryInventoryMatches("foreign", map[string]int{}) {
		t.Fatal("zero-query helper/unknown scope accepted foreign SQL")
	}
}
