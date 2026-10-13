package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

var catalogNamedIdentityQuery = `h\.db\.QueryRowContext\((ctx|h\.ctx), ` + "`" + `SELECT event_id FROM events WHERE run_id=\$1 AND event_name=(\$2|'[^']+')` + "`" + `, ([^)\n]+)\)\.Scan\(&([A-Za-z]+)\)`

func nativeCatalogNamedIdentitySource(source string) string {
	ifQuery := regexp.MustCompile(`if err := ` + catalogNamedIdentityQuery + `; err != nil`)
	var declarations []string
	call := func(parts []string) string {
		args := parts[3]
		if parts[2] != "$2" {
			encoded, _ := json.Marshal(strings.Trim(parts[2], "'"))
			args += ", " + string(encoded)
		}
		return "h.readRunNamedEventIdentity(" + parts[1] + ", " + args + ")"
	}
	source = ifQuery.ReplaceAllStringFunc(source, func(found string) string {
		parts := ifQuery.FindStringSubmatch(found)
		declarations = append(declarations, parts[4])
		return parts[4] + ", err := " + call(parts) + "\nif err != nil"
	})
	for _, name := range declarations {
		source = strings.Replace(source, "var "+name+" string\n", "", 1)
	}
	poll := regexp.MustCompile(`err (:=|=) ` + catalogNamedIdentityQuery)
	return poll.ReplaceAllStringFunc(source, func(found string) string {
		parts := poll.FindStringSubmatch(found)
		prefix := ""
		if parts[1] == ":=" {
			prefix = "var err error\n"
		}
		return prefix + parts[5] + ", err = " + call(parts[1:])
	})
}

func TestCatalogRunNamedIdentityRecipesPreserveEveryTemporalCutAndAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched, calls := 0, 0
	for _, row := range rows {
		if row.Family != "native-catalog-run-named-event" {
			continue
		}
		matched++
		calls += strings.Count(row.After, "h.readRunNamedEventIdentity(")
		want, err := canonicalFunction(nativeCatalogNamedIdentitySource(row.Before))
		got, afterErr := canonicalFunction(row.After)
		current, currentErr := reviewedCatalogNamedIdentityCurrentSource(row)
		currentValue, currentParseErr := canonicalFunction(current)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, actualErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || currentErr != nil || currentParseErr != nil || actualErr != nil || want != got || currentValue != value {
			t.Fatalf("run/name migration changed polling, source scope, lifecycle cut or assertion: %s/%s", row.File, row.Function)
		}
		for _, cut := range []string{"h.readRunNamedEventIdentity", "catalogRuntimeRunID"} {
			mutant := strings.Replace(row.After, cut, "unreviewed", 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("changed identity scope/owner accepted: %s", cut)
			}
		}
	}
	if matched != 10 || calls != 11 {
		t.Fatalf("finite run/name consumers=%d calls=%d want10/11", matched, calls)
	}
	bridge := selectedCausalObservationBody(t, "internal/runtime/cataloge2e/named_event_storage_test.go", "readRunNamedEventIdentity")
	for _, cut := range []string{"h.catalogOperatorEventLister()", "storetest.ReadRunNamedEventIdentityStorage(ctx, reader, runID, eventName)", "return \"\", err"} {
		if !strings.Contains(bridge, cut) {
			t.Fatalf("shared catalog read lost original selected owner: %s", cut)
		}
	}
}

func reviewedCatalogNamedIdentityCurrentSource(row recipe) (string, error) {
	if row.Successor == "" {
		return row.After, nil
	}
	// These two owner repairs were reviewed at134a521a8; their original run/name
	// transformation remains pinned in Before/After.
	for _, pin := range []struct{ file, function, digest string }{
		{"internal/runtime/cataloge2e/receiver_materialization_geometry_test.go", "TestReceiverMaterializationSourceLocalTargetlessAgentControlBothStores", "ba3016484746604e9e3999fc799b1948c71a0d65e67ee16a2e5d900193a53eea"},
		{"internal/runtime/cataloge2e/receiver_materialization_selected_test.go", "runReceiverMaterializationSelectedForkExecution", "135922df7ee8e651ae20a84d14e3543c34ab3d02a7e6308605ff6ab5f5552cdb"},
	} {
		if row.File == pin.file && row.Function == pin.function {
			if !historicalCurrentSourcePinned(row.Successor, pin.digest) {
				return "", fmt.Errorf("reviewed receiver construction/selector successor changed")
			}
			return row.Successor, nil
		}
	}
	if row.File != "internal/runtime/cataloge2e/guard_historical_fork_test.go" || row.Function != "TestGuardTerminationHistoricalForkPreservesCauseBothStores" {
		return "", fmt.Errorf("unreviewed named-event successor: %s/%s", row.File, row.Function)
	}
	before := "\t\t\t\t\tSlug: metadata.Slug, Name: metadata.Name, WorkflowName: guarded.WorkflowName,"
	after := "\t\t\t\t\tParentInstance: metadata.ParentInstance, InstanceKey: metadata.InstanceKey, Mode: metadata.Mode,\n\t\t\t\t\tSlug: metadata.Slug, Name: metadata.Name, WorkflowName: metadata.FlowTemplate,"
	if strings.Count(row.After, before) != 1 || row.Successor != strings.Replace(row.After, before, after, 1) {
		return "", fmt.Errorf("named-event successor changed beyond reviewed captured header facts")
	}
	return row.Successor, nil
}

func TestCatalogRunNamedIdentitySuccessorRejectsUnreviewedHeaderBindings(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected recipe
	for _, row := range rows {
		if row.Function == "TestGuardTerminationHistoricalForkPreservesCauseBothStores" {
			selected = row
		}
	}
	if selected.Successor == "" {
		t.Fatal("missing reviewed captured-header successor")
	}
	if _, err := reviewedCatalogNamedIdentityCurrentSource(selected); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"metadata.ParentInstance", "metadata.InstanceKey", "metadata.Mode", "metadata.FlowTemplate", "check.requested", "reflect.DeepEqual", "h.readRunNamedEventIdentity", "catalogRuntimeRunID"} {
		t.Run(cut, func(t *testing.T) {
			mutant := selected
			mutant.Successor = strings.Replace(mutant.Successor, cut, "unreviewed", 1)
			if mutant.Successor == selected.Successor {
				t.Fatal("negative control did not change its captured binding")
			}
			if _, err := reviewedCatalogNamedIdentityCurrentSource(mutant); err == nil {
				t.Fatal("unreviewed successor accepted")
			}
		})
	}
	selected.File = "internal/runtime/cataloge2e/unreviewed_test.go"
	if _, err := reviewedCatalogNamedIdentityCurrentSource(selected); err == nil {
		t.Fatal("foreign successor gained reviewed authority")
	}
}

func TestR7ReviewedFixtureSuccessorInventoryIsExact(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	// The selected-input134a521a8 endpoint is retained as history; its exact
	// reviewed C05/C4 successor alone is current. The other three pins stay current.
	for _, pin := range []struct{ file, function, digest string }{
		{"internal/runtime/cataloge2e/guard_historical_fork_test.go", "TestGuardTerminationHistoricalForkPreservesCauseBothStores", "b2e7ab7337a58d13ab1837b7b4f31b3dd9d489a291e5650e8f5760eb6f0bdcc8"},
		{"internal/runtime/cataloge2e/receiver_materialization_geometry_test.go", "TestReceiverMaterializationSourceLocalTargetlessAgentControlBothStores", "ba3016484746604e9e3999fc799b1948c71a0d65e67ee16a2e5d900193a53eea"},
		{"internal/runtime/cataloge2e/receiver_materialization_selected_test.go", "runReceiverMaterializationSelectedForkExecution", "135922df7ee8e651ae20a84d14e3543c34ab3d02a7e6308605ff6ab5f5552cdb"},
		{"internal/runtime/runforkexecution/constructed_agent_input_execution_test.go", "proveSelectedConstructedAgentInput", "49bf1fb2c3444f00ea8a75d3e1352aae24aa300a687c96a1f2a249220ea14d7f"},
	} {
		matched := 0
		for _, row := range rows {
			if row.File != pin.file || row.Function != pin.function {
				continue
			}
			matched++
			if row.Function == "proveSelectedConstructedAgentInput" {
				assertSelectedAgentInputHistoricalEndpoint2617(t)
				if !residualComparatorCurrent2617(t, row, selectedCausalObservationBody(t, row.File, row.Function)) {
					t.Fatal("selected-input current endpoint diverged from the finite C05/C4 pin")
				}
				continue
			}
			if row.Successor == "" || !historicalCurrentSourcePinned(row.Successor, pin.digest) {
				t.Fatalf("missing or changed reviewed R7 successor: %s/%s", row.File, row.Function)
			}
			mutant := strings.Replace(row.Successor, "{", "{ unreviewed();", 1)
			if historicalCurrentSourcePinned(mutant, pin.digest) {
				t.Fatal("unreviewed successor work accepted")
			}
		}
		if matched != 1 {
			t.Fatalf("reviewed successor identity multiplicity=%d, want1", matched)
		}
	}
}

func TestCatalogRunNamedIdentityOracleRejectsLostWorkloadAndPolling(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row recipe
	for _, candidate := range rows {
		if candidate.Family == "native-catalog-run-named-event" && candidate.Function == "TestGuardTerminationHistoricalForkPreservesCauseBothStores" {
			row = candidate
		}
	}
	if row.Function == "" {
		t.Fatal("missing finite historical fork identity recipe")
	}
	want, err := canonicalFunction(nativeCatalogNamedIdentitySource(row.Before))
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"check.requested", "score_check", "10*time.Second", "fork.ExecutedEventCount != 1", "reflect.DeepEqual", "guarded.TransitionHistory"} {
		mutant := strings.Replace(row.After, cut, "unreviewed", 1)
		changed, mutantErr := canonicalFunction(mutant)
		if mutant == row.After || (mutantErr == nil && changed == want) {
			t.Fatalf("lost historical fork workload accepted: %s", cut)
		}
	}
}
