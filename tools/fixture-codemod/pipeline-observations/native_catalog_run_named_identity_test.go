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
	current := row.Successor
	for _, pair := range [][2]string{
		{`preserved = entity.CurrentState == "killed"`, `preserved = entity.CurrentState != nil && *entity.CurrentState == "killed"`},
		{"\t\t\t\tfields, err := json.Marshal(entity.Fields)", "\t\t\t\tif entity.CurrentState == nil {\n\t\t\t\t\tt.Fatalf(\"historical guard cause lacks its actual lifecycle stage: %+v\", entity)\n\t\t\t\t}\n\t\t\t\tfields, err := json.Marshal(entity.Fields)"},
		{"CurrentState: entity.CurrentState, Config: metadata.FlowConfig, Fields: fields,", "CurrentState: *entity.CurrentState, Config: metadata.FlowConfig, Fields: fields,"},
	} {
		if strings.Count(current, pair[0]) != 1 {
			return "", fmt.Errorf("missing exact Q10 historical lifecycle binding %q", pair[0])
		}
		current = strings.Replace(current, pair[0], pair[1], 1)
	}
	return current, nil
}

func TestQ10CatalogHistorySuccessorRetainsExactPresenceAndCause(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected recipe
	matched := 0
	for _, row := range rows {
		if row.File == "internal/runtime/cataloge2e/guard_historical_fork_test.go" && row.Function == "TestGuardTerminationHistoricalForkPreservesCauseBothStores" {
			selected = row
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("historical guard successor multiplicity=%d, want1", matched)
	}
	current, err := reviewedCatalogNamedIdentityCurrentSource(selected)
	if err != nil {
		t.Fatal(err)
	}
	guard := selected
	guard.Before, guard.After, guard.Successor = current, current, ""
	for _, cut := range []string{
		"entity.CurrentState != nil && ", "*entity.CurrentState == \"killed\"",
		"if entity.CurrentState == nil", "CurrentState: *entity.CurrentState",
		"metadata.ParentInstance", "metadata.InstanceKey", "metadata.Mode", "metadata.FlowTemplate",
		"reflect.DeepEqual(instance.TransitionHistory, guarded.TransitionHistory)",
		"fork.ExecutedEventCount != 1", "len(plan.Entities) != 2", "h.readRunNamedEventIdentity", "catalogRuntimeRunID",
	} {
		t.Run(cut, func(t *testing.T) {
			mutant := strings.Replace(current, cut, "unreviewed", 1)
			if mutant == current {
				t.Fatal("negative control did not change the exact current binding")
			}
			if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
				t.Fatal("changed lifecycle presence, cause or native fork assertion accepted")
			}
		})
	}
	if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+selected.Successor), guard); err == nil || changed {
		t.Fatal("historical scalar source accepted as the current optional-lifecycle endpoint")
	}
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
