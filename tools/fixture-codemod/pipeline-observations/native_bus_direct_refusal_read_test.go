package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func busDirectRefusalAfterShape(name string) string {
	if name == "TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerPostgres" {
		return `func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerPostgres(t *testing.T) {
pg := storetest.StartPostgresRuntimeStore(t)
assertEventBusDiagnosticDirectRefusal(t,pg,func(eventID string)(int,error){
_,found,err := storetest.ReadCanonicalEventRecord(context.Background(),pg,eventID)
count := 0
if found { count = 1 }
return count,err
})
}`
	}
	return `func TestEventBusRejectsDiagnosticDirectEventsThroughEveryPublishOwnerSQLite(t *testing.T) {
sqliteStore := storetest.StartSQLiteRuntimeStore(t)
assertEventBusDiagnosticDirectRefusal(t,sqliteStore,func(eventID string)(int,error){
_,found,err := storetest.ReadCanonicalEventRecord(context.Background(),sqliteStore,eventID)
count := 0
if found { count = 1 }
return count,err
})
}`
}

func busDirectRefusalPreserved(name, source string) bool {
	want, err := canonicalFunction(busDirectRefusalAfterShape(name))
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeBusDirectRefusalRetainsOriginalOwnersIdentityAndErrors(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-direct-refusal-canonical-event" {
			continue
		}
		matched++
		if !busDirectRefusalPreserved(row.Function, row.After) {
			t.Fatal("bus refusal witness lost original store, exact event, presence or read error")
		}
		for _, pair := range [][2]string{
			{"eventID)", "otherEventID)"}, {"if found", "if false"},
			{"count = 1", "count = 0"}, {"return count, err", "return count, nil"},
			{"context.Background(), pg,", "context.Background(), foreignStore,"},
			{"context.Background(), sqliteStore,", "context.Background(), foreignStore,"},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After {
				continue // Backend-specific owner spelling is absent on its sibling.
			}
			if busDirectRefusalPreserved(row.Function, mutant) {
				t.Fatalf("weakened direct-event refusal admitted: %v", pair)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("bus direct refusal recipes=%d, want2", matched)
	}
}
