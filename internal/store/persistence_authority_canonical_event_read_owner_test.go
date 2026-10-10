package store_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

const canonicalRequiredEventReadShape = `func LoadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, error) {
	event, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
	if err != nil { return events.Event{}, err }
	if !found { return events.Event{}, fmt.Errorf("canonical event record %s is missing", eventID) }
	return event, nil
}`

const canonicalOptionalEventReadShape = `func ReadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil { return events.Event{}, false, err }
	var record eventrecord.Record
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		_, postgres := selected.(*PostgresStore)
		record, found, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil || !found { return events.Event{}, false, err }
	admitted, err := record.Decode()
	if err != nil { return events.Event{}, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err) }
	return admitted.Event(), true, nil
}`

func canonicalEventReadFunctionShapes(t *testing.T, source string) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "event.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, duplicate := result[fn.Name.Name]; duplicate {
			t.Fatalf("duplicate canonical read declaration: %s", fn.Name.Name)
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
			t.Fatal(err)
		}
		result[fn.Name.Name] = out.String()
	}
	return result
}

func verifyCanonicalEventFixtureReadOwnership(t *testing.T) {
	t.Helper()
	path := filepath.Join(persistenceAuthorityRepoRoot(t), "internal", "store", "internal", "runtimepersistence", "test_event_support.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{canonicalRequiredEventReadShape, canonicalOptionalEventReadShape} {
		expected := canonicalEventReadFunctionShapes(t, "package fixture;"+source)
		for name, want := range expected {
			found := 0
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != name {
					continue
				}
				found++
				var out bytes.Buffer
				if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
					t.Fatal(err)
				}
				if out.String() != want {
					t.Fatalf("canonical read ownership/presence/context/key/decoder changed: %s", name)
				}
			}
			if found != 1 {
				t.Fatalf("canonical read entry missing or duplicated: %s/%d", name, found)
			}
		}
	}
}

func TestCanonicalEventFixtureReadOwnerGuardRejectsIndependentReadsAndLostRefusals(t *testing.T) {
	for _, probe := range []struct{ source, from, to string }{
		{canonicalRequiredEventReadShape, "ReadCanonicalEventRecordForTest(ctx, selected, eventID)", "independentRead(ctx, selected, eventID)"},
		{canonicalRequiredEventReadShape, "if !found", "if found"},
		{canonicalOptionalEventReadShape, "loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)", "loadCanonicalFixtureRecordTx(ctx, tx, postgres, otherEventID)"},
		{canonicalOptionalEventReadShape, "readServedDeliveryObservation(ctx, selected,", "readServedDeliveryObservation(otherContext, selected,"},
		{canonicalOptionalEventReadShape, "record.Decode()", "alternateDecoder(record)"},
		{canonicalOptionalEventReadShape, "return events.Event{}, false, err", "return events.Event{}, true, nil"},
	} {
		broken := strings.Replace(probe.source, probe.from, probe.to, 1)
		if broken == probe.source {
			t.Fatalf("canonical ownership adversary stopped matching: %s", probe.from)
		}
		before := canonicalEventReadFunctionShapes(t, "package fixture;"+probe.source)
		after := canonicalEventReadFunctionShapes(t, "package fixture;"+broken)
		for name, shape := range before {
			if after[name] == shape {
				t.Fatalf("independent/lost-refusal interpreter admitted: %s", probe.from)
			}
		}
	}
}
