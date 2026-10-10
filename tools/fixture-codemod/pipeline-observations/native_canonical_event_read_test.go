package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativeCanonicalEventReadShape(t *testing.T, source string) bool {
	t.Helper()
	want := `func LoadCanonicalEventRecord(t testing.TB, ctx context.Context, selectedStore any, eventID string) events.Event {
		t.Helper()
		event, found, err := private.ReadCanonicalEventRecordForTest(ctx, selectedStore, eventID)
		if err != nil || !found {
			t.Fatalf("load canonical event record %s: found=%v err=%v", eventID, found, err)
		}
		return event
	}`
	return formattedNativeReadNode(projectionShapeFunction(t, source)) == formattedNativeReadNode(projectionShapeFunction(t, want))
}

func TestNativeCanonicalEventReadPreservesExactKeyDecoderAndFailClosedEvidence(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-canonical-event-read")
	for _, before := range []string{"eventrecordpostgres.Load(ctx, DatabaseForTest(selected), eventID)", "eventrecordsqlite.Load(ctx, DatabaseForTest(selected), eventID)", "record.Decode()", "return admitted.Event()"} {
		if !strings.Contains(row.Before, before) {
			t.Fatalf("missing predecessor contract %s", before)
		}
	}
	if !nativeCanonicalEventReadShape(t, row.After) {
		t.Fatal("changed event read coordinates, refusal or selected owner")
	}
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "runtimepersistence", "test_event_support.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, contract := range []string{"validateChannelObservationOwner(selected)", "readServedDeliveryObservation(ctx, selected,", "eventrecordpostgres.Load(ctx, tx, eventID)", "eventrecordsqlite.Load(ctx, tx, eventID)", "record.Decode()", "return admitted.Event(), true, nil"} {
		if !strings.Contains(string(source), contract) {
			t.Fatalf("canonical decoder/original read owner lost %s", contract)
		}
	}
}

func TestNativeCanonicalEventReadRejectsChangedKeyOwnerAndRefusal(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-canonical-event-read")
	for _, probe := range []struct{ from, to string }{
		{"ctx, selectedStore, eventID", "ctx, anotherOwner, eventID"},
		{"ctx, selectedStore, eventID", "ctx, selectedStore, anotherEventID"},
		{"err != nil || !found", "err != nil && !found"},
		{"return event", "return reconstructedEvent"},
	} {
		changed := strings.Replace(row.After, probe.from, probe.to, 1)
		if changed == row.After || nativeCanonicalEventReadShape(t, changed) {
			t.Fatalf("event read regression accepted: %s", probe.from)
		}
	}
}
