package main

import (
	"go/ast"
	"go/parser"
	"go/token"
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

func canonicalReadMethod(t *testing.T, path, owner string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var selected string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ReadCanonicalEventRecordForTest" || fn.Recv == nil {
			continue
		}
		if formattedNativeReadNode(fn.Recv.List[0].Type) != "*"+owner {
			continue
		}
		if selected != "" {
			t.Fatalf("duplicate canonical read method on %s", owner)
		}
		selected = formattedNativeReadNode(fn)
	}
	if selected == "" {
		t.Fatalf("missing canonical read method on %s", owner)
	}
	return selected
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
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	facade, err := uniqueFunction(file, "ReadCanonicalEventRecordForTest")
	if err != nil {
		t.Fatal(err)
	}
	for _, contract := range []string{"validateChannelObservationOwner(selected)", "store.eventPostgresOwner.ReadCanonicalEventRecordForTest(ctx, eventID)", "store.eventSQLiteOwner.ReadCanonicalEventRecordForTest(ctx, eventID)", "return empty, false, err"} {
		if !strings.Contains(formattedNativeReadNode(facade), contract) {
			t.Fatalf("canonical facade owner/delegation lost %s", contract)
		}
	}
	nativePath := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "eventpersistence", "test_canonical_event_observation.go")
	for _, owner := range []struct{ name, dialect string }{
		{"EventPostgresOwner", "eventrecordpostgres"},
		{"EventSQLiteOwner", "eventrecordsqlite"},
	} {
		method := canonicalReadMethod(t, nativePath, owner.name)
		for _, contract := range []string{"s.backend.RunReadTransaction(ctx,", owner.dialect + ".Load(ctx, tx, eventID)", "if err != nil || !found", "record.Decode()", "return empty, false, fmt.Errorf", "return admitted.Event(), true, nil"} {
			if !strings.Contains(method, contract) {
				t.Fatalf("%s canonical decoder/original read owner lost %s", owner.name, contract)
			}
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
