package main

import (
	"encoding/json"
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
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, actualErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || actualErr != nil || want != got || got != value {
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
