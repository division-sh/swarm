package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
	for _, owner := range []struct{ path, function, query string }{
		{"pipelinepersistence/workflow_projection_observation.go", "CountWorkflowFieldRowsForRun", `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`},
		{"genericschedule/test_workflow_scheduler_observation.go", "CountTimerRowsForRun", `SELECT COUNT(*) FROM timers WHERE run_id=$1`},
		{"pipelinepersistence/owner_operations.go", "CountFanOutIntentsForRun", `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`},
		{"eventrecord/causal_observation.go", "CountScatterGatherDomainEvents", `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','batch.opened') OR event_name LIKE 'workers/%/item.reported')`},
	} {
		path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", owner.path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueFunction(file, owner.function)
		if err != nil {
			t.Fatal(err)
		}
		source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		if causalDeliveryQueryLiteral(t, source) != strings.Join(strings.Fields(owner.query), "") {
			t.Fatalf("%s changed physical scope, retained history or domain-event predicate", owner.function)
		}
	}
}

func TestNativeCatalogScatterConservationSnapshotUsesAllCanonicalOwners(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "runtimepersistence", "test_event_support.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "ReadScatterGatherPhysicalCountsForTest")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	const shape = `func ReadScatterGatherPhysicalCountsForTest(ctx context.Context,selected any,runID string) (ScatterGatherPhysicalCounts,error) {
if err := validateChannelObservationOwner(selected); err != nil { return ScatterGatherPhysicalCounts{},err }
var out ScatterGatherPhysicalCounts
err := readServedDeliveryObservation(ctx,selected,func(ctx context.Context,tx *sql.Tx) error {
var err error
if out.Entities,err = pipelinepersistence.CountWorkflowFieldRowsForRun(ctx,tx,runID); err != nil { return err }
if out.Timers,err = genericschedule.CountTimerRowsForRun(ctx,tx,runID); err != nil { return err }
if out.FanOutIntents,err = pipelinepersistence.CountFanOutIntentsForRun(ctx,tx,runID); err != nil { return err }
out.DomainEvents,err = eventrecord.CountScatterGatherDomainEvents(ctx,tx,runID)
return err
})
if err != nil { return ScatterGatherPhysicalCounts{},err }
return out,nil
}`
	want, err := canonicalFunction(shape)
	got, parseErr := canonicalFunction(source)
	if err != nil || parseErr != nil || want != got {
		t.Fatal("conservation escaped one selected read, owner/field mapping or complete late-error refusal")
	}
}
