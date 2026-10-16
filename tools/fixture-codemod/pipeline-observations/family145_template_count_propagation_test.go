package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

type templateCountRule struct {
	query, port string
	arguments   int
	constants   []string
}

var templateCountRules = []templateCountRule{
	{`SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$1::uuid AND scope='subscribed'`, "CountTemplateSubscribedReplayScope", 1, nil},
	{`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1::uuid AND flow_template='operating'`, "CountRunOperatingInstances", 1, nil},
	{`SELECT COUNT(*) FROM event_deliveries d JOIN (SELECT delivery_id,claim_version,outcome,reason_code,failure,side_effects,duration_ms,completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o ON o.delivery_id=d.delivery_id AND o.claim_version=d.claim_version WHERE d.event_id=$1::uuid AND d.subscriber_type='node' AND d.subscriber_id=$2 AND d.status='delivered' AND d.settled_at IS NOT NULL AND o.outcome='delivered'`, "CountTemplateNodeDeliveredAttempt", 2, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2`, "CountTemplateNodeDelivery", 2, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2 AND settled_at IS NOT NULL AND created_at<settled_at`, "CountTemplateNodeDeliveryChronology", 2, nil},
	{`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2`, "CountTemplateNodeReceipt", 2, nil},
	{`SELECT COUNT(*) FROM events WHERE event_name='operating/inst-1/opco.ceo_ready'`, "CountEventNameStorage", 0, []string{`"operating/inst-1/opco.ceo_ready"`}},
	{`SELECT COUNT(*) FROM routing_rules WHERE flow_instance=$2 AND subscriber_type='agent' AND subscriber_id=$1 AND status='active'`, "CountTemplateActiveAgentRoutes", 2, nil},
	{`SELECT COUNT(*) FROM routing_rules WHERE flow_instance=$1 AND subscriber_id='ceo-{product_id}'`, "CountTemplatePlaceholderRoutes", 1, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='agent' AND subscriber_id=$2`, "CountTemplateAgentDelivery", 2, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id='ceo-{product_id}'`, "CountTemplatePlaceholderDelivery", 1, nil},
	{`SELECT COUNT(*) FROM events WHERE event_id=$1::uuid`, "CountTemplateEventIdentity", 1, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid`, "CountTemplateEventDeliveries", 1, nil},
	{`SELECT COUNT(*) FROM flow_instances WHERE flow_template='consumer'`, "CountTemplateInstances", 0, []string{`"consumer"`}},
	{`SELECT COUNT(*) FROM flow_instances WHERE flow_template='account'`, "CountTemplateInstances", 0, []string{`"account"`}},
	{`SELECT COUNT(*) FROM entity_state`, "CountPhysicalEntities", 0, nil},
	{`SELECT COUNT(*) FROM routing_rules`, "CountPhysicalRoutingRules", 0, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id IN ('workflow-runtime','raw-source-listener')`, "CountTemplateCarrierDeliveries", 1, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$3 AND delivery_target_route @> $2::jsonb`, "CountPostgresTemplateNodeTarget", 3, nil},
	{`SELECT COUNT(*) FROM entity_state WHERE flow_instance LIKE 'account/%'`, "CountTemplateAccountEntities", 0, nil},
	{`SELECT COUNT(*) FROM entity_state WHERE flow_instance LIKE 'account/%' AND current_state='reviewed' AND fields @> $1::jsonb`, "CountPostgresReviewedTemplateAccount", 1, nil},
	{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2 AND status='delivered' AND delivery_target_route @> $3::jsonb`, "CountPostgresTemplateDeliveredNodeTarget", 3, nil},
}

func templateSQLTokens(source string) string {
	var out strings.Builder
	quoted := false
	for _, ch := range source {
		if ch == '\'' {
			quoted = !quoted
		}
		if !quoted && strings.ContainsRune(" \t\r\n", ch) {
			continue
		}
		out.WriteRune(ch)
	}
	return out.String()
}

func templateCountCalls(source, first, second string) ([]*ast.CallExpr, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var out []*ast.CallExpr
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if ok && (name.Name == first || name.Name == second) {
			out = append(out, call)
		}
		return true
	})
	return out, nil
}

func templateCountPropagationError(function, before, after string) error {
	original, err := templateCountCalls(before, "assertRuntimeDBCount", "waitRuntimeDBCount")
	if err != nil {
		return err
	}
	current, err := templateCountCalls(after, "assertRuntimeStorageCount", "waitRuntimeStorageCount")
	if err != nil {
		return err
	}
	if len(original) != len(current) {
		return fmt.Errorf("template count-call cardinality changed")
	}
	for i, old := range original {
		if err := templateCountCallError(function, old, current[i]); err != nil {
			return err
		}
	}
	return nil
}

func templateCountCallError(function string, old, current *ast.CallExpr) error {
	if len(old.Args) < 5 {
		return fmt.Errorf("missing original query arguments")
	}
	literal, ok := old.Args[3].(*ast.BasicLit)
	if !ok {
		return fmt.Errorf("original query is not finite")
	}
	query, err := strconv.Unquote(literal.Value)
	if err != nil {
		return err
	}
	var rule *templateCountRule
	for i := range templateCountRules {
		if templateSQLTokens(query) == templateSQLTokens(templateCountRules[i].query) {
			rule = &templateCountRules[i]
			break
		}
	}
	if rule == nil || len(old.Args) != 5+rule.arguments {
		return fmt.Errorf("unaccounted template query or coordinate")
	}
	name := current.Fun.(*ast.Ident).Name
	wantIndex := 2
	if name == "waitRuntimeStorageCount" {
		wantIndex = 4
	}
	if len(current.Args) != wantIndex+2 || family145NodeString(current.Args[wantIndex]) != family145NodeString(old.Args[4]) {
		return fmt.Errorf("template asserted count changed")
	}
	callback, ok := current.Args[len(current.Args)-1].(*ast.FuncLit)
	if !ok || len(callback.Body.List) != 1 {
		return fmt.Errorf("template callback gained work or lost its read")
	}
	returned, ok := callback.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(returned.Results) != 1 {
		return fmt.Errorf("template callback has no exact returned read")
	}
	read, ok := returned.Results[0].(*ast.CallExpr)
	if !ok || family145NodeString(read.Fun) != "storetest."+rule.port {
		return fmt.Errorf("template query moved to a different scope")
	}
	expected := []string{family145NodeString(old.Args[1]), "pg"}
	if function == "assertRuntimeCommittedReplayScope" || function == "requireRuntimeInitializedOperatingPath" || function == "waitRuntimeNodeDeliveryOutcome" {
		expected[1] = "selected"
	}
	for _, arg := range old.Args[5:] {
		expected = append(expected, family145NodeString(arg))
	}
	expected = append(expected, rule.constants...)
	if len(read.Args) != len(expected) {
		return fmt.Errorf("template observation changed its coordinate count")
	}
	for i, arg := range read.Args {
		if family145NodeString(arg) != expected[i] {
			return fmt.Errorf("template observation changed coordinate%d: %s vs %s", i, family145NodeString(arg), expected[i])
		}
	}
	return nil
}
