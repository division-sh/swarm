package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ActivitySetupOwner(name string) bool {
	return name == "seedSelectedContractActivityLoop" || name == "seedSelectedContractActivityAttempt"
}

func family145ActivitySetupProofError(name, before, after string) error {
	for _, raw := range []string{"activityForkFixtureDB", "ExecContext", "Database(", "db.", "selectedContractActivityJSON"} {
		if strings.Contains(after, raw) {
			return fmt.Errorf("historical activity setup retained raw authority: %s", raw)
		}
	}
	if name == "seedSelectedContractActivityLoop" {
		want := `func seedSelectedContractActivityLoop(t *testing.T, selected any, runID, entityID, requestEventID string, activation loopruntime.Activation, at time.Time) {
			t.Helper()
			if err := storetest.SeedHistoricalActivityForkLoop(context.Background(), selected, runID, entityID, requestEventID, activation, at); err != nil {
				t.Fatalf("seed source loop history: %v", err)
			}
		}`
		actual, err := canonicalFunction(after)
		expected, wantErr := canonicalFunction(want)
		if err != nil || wantErr != nil || actual != expected {
			return fmt.Errorf("loop setup changed historical coordinates, activation, clock or selected owner")
		}
		return nil
	}
	if name != "seedSelectedContractActivityAttempt" {
		return fmt.Errorf("unclassified activity setup owner: %s", name)
	}
	for _, required := range []string{
		`requestEventID := activityidentity.RequestEventID(fact)`, `resultEventID := activityidentity.ResultEventID(fact, resultEventType)`,
		`inputHash := sha256.Sum256([]byte(` + "`" + `{"value":"x"}` + "`" + `))`,
		`resultPayload["result"] = map[string]any{"value": "recorded-result"}`,
		`runtimefailures.New(runtimefailures.Class(failureClass), failureCode, "activity-runtime", "execute_non_idempotent_http", map[string]any{"tool": "provider.connector"})`,
		`storetest.SeedHistoricalActivityForkAttempt(context.Background(), selected, pipeline.ActivityAttemptRecord{`,
	} {
		if !strings.Contains(after, required) {
			return fmt.Errorf("recorded attempt lost original fact: %s", required)
		}
	}
	if err := family145ActivityAttemptRecordError(after); err != nil {
		return err
	}
	b, be := runSetupAssertions(before)
	a, ae := runSetupAssertions(after)
	if be != nil || ae != nil || strings.Join(b, "\n") != strings.Join(a, "\n") {
		return fmt.Errorf("recorded attempt changed typed-failure or persistence assertions")
	}
	return nil
}

func family145ActivityAttemptRecordError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "attempt.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]string{
		"RequestEventID": "requestEventID", "RunID": "fact.RunID", "ExecutionMode": "executionmode.Live",
		"SourceEventID": "fact.SourceEventID", "EntityID": "fact.EntityID", "FlowInstance": `"flow_a"`, "NodeID": "fact.Owner.Key()", "HandlerEventKey": `"review.requested"`,
		"ActivityID": `"connector"`, "Tool": `"provider.connector"`, "EffectClass": `"non_idempotent_write"`, "Attempt": "1", "Status": "status",
		"SuccessEvent": `"flow_a/connector.succeeded"`, "FailureEvent": `"flow_a/connector.failed"`,
		"ResultEventID": "resultEventID", "ResultEventType": "resultEventType", "ResultPayload": "resultPayload", "Failure": "failure",
		"InputHash": `fmt.Sprintf("sha256:%x", inputHash[:])`, "Generation": "generation", "LoopStage": `"review"`,
		"StartedAt": "at", "CompletedAt": "&at", "UpdatedAt": "at",
	}
	got := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || family145NodeString(literal.Type) != "pipeline.ActivityAttemptRecord" {
			return true
		}
		for _, entry := range literal.Elts {
			if field, ok := entry.(*ast.KeyValueExpr); ok {
				got[family145NodeString(field.Key)] = family145NodeString(field.Value)
			}
		}
		return true
	})
	if len(got) != len(want) {
		return fmt.Errorf("historical journal field inventory=%d want%d", len(got), len(want))
	}
	for field, value := range want {
		if got[field] != value {
			return fmt.Errorf("historical journal field %s=%s want%s", field, got[field], value)
		}
	}
	return nil
}

func family145ActivitySetupSequenceError(before, after string) error {
	parse := func(source string) (*ast.File, error) {
		return parser.ParseFile(token.NewFileSet(), "activity.go", "package proof\n"+source, parser.AllErrors)
	}
	b, err := parse(before)
	if err != nil {
		return err
	}
	a, err := parse(after)
	if err != nil {
		return err
	}
	var original, admitted string
	var sequence []string
	ast.Inspect(b, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && family145NodeString(call.Fun) == "seedSelectedContractActivityRequest" && len(call.Args) == 5 {
			original = family145NodeString(call.Args[4])
		}
		return true
	})
	ast.Inspect(a, func(node ast.Node) bool {
		if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Lhs) == 1 && family145NodeString(assign.Lhs[0]) == "requestPayload" && len(assign.Rhs) == 1 {
			admitted = family145NodeString(assign.Rhs[0])
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := family145NodeString(call.Fun)
		switch name {
		case "fixture.seedSource":
			if len(call.Args) != 9 || family145NodeString(call.Args[8]) != "requestPayload" {
				sequence = append(sequence, "invalid-payload")
			}
			sequence = append(sequence, name)
		case "seedSelectedContractActivityLoop", "seedSelectedContractActivityAttempt":
			if len(call.Args) < 2 || family145NodeString(call.Args[1]) != "selected" {
				sequence = append(sequence, "foreign-owner")
			}
			sequence = append(sequence, name)
		case "fixture.capture":
			sequence = append(sequence, name)
		}
		return true
	})
	if original == "" || original != admitted || strings.Join(sequence, ",") != "fixture.seedSource,seedSelectedContractActivityLoop,fixture.capture,seedSelectedContractActivityAttempt" {
		return fmt.Errorf("activity setup changed request fields, selected ownership or capture-before-recorded-attempt ordering: %v", sequence)
	}
	return nil
}

func family145ActivitySourceRequestError(before, after string) error {
	want := strings.Replace(before, "source events.RoutingSource)", "source events.RoutingSource, request selectedContractActivityRequestPayload)", 1)
	want = strings.Replace(want, `json.Marshal(map[string]any{"entity_id": entityID})`, "json.Marshal(request)", 1)
	actual, err := canonicalFunction(after)
	expected, wantErr := canonicalFunction(want)
	if err != nil || wantErr != nil || actual != expected {
		return fmt.Errorf("activity request setup changed anything beyond admitting the identical complete payload once")
	}
	return nil
}
