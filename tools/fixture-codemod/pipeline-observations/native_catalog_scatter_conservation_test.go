package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const nativeScatterCountsShape = `func scatterGatherCounts(t testing.TB,h *runtimeHarness,ctx context.Context) map[string]int {
t.Helper()
reader,err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
counts,err := storetest.ReadScatterGatherPhysicalCounts(ctx,reader,catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
return map[string]int{"entity_state":counts.Entities,"timers":counts.Timers,"fan_out_intents":counts.FanOutIntents,"domain_events":counts.DomainEvents}
}`

func scatterCountsConsumerPreserved(source string) bool {
	want, err := canonicalFunction(nativeScatterCountsShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogScatterConservationKeepsAllFourKeysScopeAndRefusal(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-scatter-conservation-observation" {
			continue
		}
		count++
		if !scatterCountsConsumerPreserved(row.After) {
			t.Fatal("conservation changed original four keys, run, context, owner or errors")
		}
		for _, pair := range [][2]string{
			{"if err != nil", "if false"}, {"ctx, reader, catalogRuntimeRunID", "ctx, reader, otherRunID"},
			{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
			{"counts.Entities", "counts.DomainEvents"}, {"counts.Timers", "0"},
			{"counts.FanOutIntents", "counts.Timers"}, {"\"domain_events\"", "\"events\""},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After || scatterCountsConsumerPreserved(mutant) {
				t.Fatalf("weakened conservation mapping accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("scatter count recipes=%d, want one exact consumer", count)
	}
}

func TestNativeCatalogScatterConservationOwnersRetainOriginalPhysicalQueries(t *testing.T) {
	path := "internal/store/internal/backend/pipelinepersistence/test_scatter_gather_physical_counts.go"
	source := selectedCausalObservationBody(t, path, "readScatterGatherPhysicalCountsTx")
	want := []string{
		`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`,
		`SELECT COUNT(*) FROM timers WHERE run_id=$1`,
		`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`,
		`SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','batch.opened') OR event_name LIKE 'workers/%/item.reported')`,
	}
	for i := range want {
		want[i] = strings.Join(strings.Fields(want[i]), "")
	}
	if got := eventDeliveryDiagnosticSQL(t, source); !reflect.DeepEqual(got, want) {
		t.Fatalf("scatter/gather changed physical scope, retained history or domain-event predicate: %q", got)
	}
	previous := -1
	for _, field := range []string{"Entities", "Timers", "FanOutIntents", "DomainEvents"} {
		at := strings.Index(source, ".Scan(&out."+field+")")
		if at <= previous {
			t.Fatalf("scatter/gather count order or field %s changed", field)
		}
		previous = at
	}
	if strings.Count(source, "return ScatterGatherPhysicalCounts{}, err") != 4 {
		t.Fatal("scatter/gather lost zero-on-error after a physical read")
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", path))
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"func (s *PipelinePostgresOwner) ReadScatterGatherPhysicalCountsForTest", "func (s *PipelineSQLiteOwner) ReadScatterGatherPhysicalCountsForTest"} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("scatter/gather lost original selected owner %s", fragment)
		}
	}
	for fragment, wantCount := range map[string]int{
		"s.backend.RunReadTransaction(ctx":                  2,
		"readScatterGatherPhysicalCountsTx(ctx, tx, runID)": 2,
		"return ScatterGatherPhysicalCounts{}, err":         6,
	} {
		if got := strings.Count(string(data), fragment); got != wantCount {
			t.Fatalf("scatter/gather lost one snapshot or zero-on-error per owner: %s count=%d", fragment, got)
		}
	}
}

func TestNativeCatalogScatterConservationSnapshotUsesAllCanonicalOwners(t *testing.T) {
	source := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_event_support.go", "ReadScatterGatherPhysicalCountsForTest")
	const shape = `func ReadScatterGatherPhysicalCountsForTest(ctx context.Context,selected any,runID string) (ScatterGatherPhysicalCounts,error) {
if err := validateChannelObservationOwner(selected); err != nil { return ScatterGatherPhysicalCounts{},err }
switch store := selected.(type) {
case *PostgresStore: return store.pipelinePostgresOwner.ReadScatterGatherPhysicalCountsForTest(ctx,runID)
case *SQLiteRuntimeStore: return store.pipelineSQLiteOwner.ReadScatterGatherPhysicalCountsForTest(ctx,runID)
default: return ScatterGatherPhysicalCounts{},fmt.Errorf("scatter/gather counts require the original native owner")
}
}`
	want, err := canonicalFunction(shape)
	got, parseErr := canonicalFunction(source)
	if err != nil || parseErr != nil || want != got {
		t.Fatal("conservation escaped one selected read, owner/field mapping or complete late-error refusal")
	}
}
