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
	var empty events.Event
	event, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
	if err != nil { return empty, err }
	if !found { return empty, fmt.Errorf("canonical event record %s is missing", eventID) }
	return event, nil
}`

const canonicalOptionalEventReadShape = `func ReadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, bool, error) {
	var empty events.Event
	if err := validateChannelObservationOwner(selected); err != nil { return empty, false, err }
	switch store := selected.(type) {
	case *PostgresStore: return store.eventPostgresOwner.ReadCanonicalEventRecordForTest(ctx, eventID)
	case *SQLiteRuntimeStore: return store.eventSQLiteOwner.ReadCanonicalEventRecordForTest(ctx, eventID)
	default: return empty, false, fmt.Errorf("canonical event read requires the original native owner")
	}
}`

const canonicalNativeEventReadShape = `func (s *OWNER) ReadCanonicalEventRecordForTest(ctx context.Context, eventID string) (events.Event, bool, error) {
	var empty events.Event
	var record eventrecord.Record
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		record, found, err = DIALECT.Load(ctx, tx, eventID)
		return err
	})
	if err != nil || !found { return empty, false, err }
	admitted, err := record.Decode()
	if err != nil { return empty, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err) }
	return admitted.Event(), true, nil
}`

func nativeCanonicalEventReadShape(owner, dialect string) string {
	return strings.NewReplacer("OWNER", owner, "DIALECT", dialect).Replace(canonicalNativeEventReadShape)
}

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
		key := fn.Name.Name
		if fn.Recv != nil {
			var receiver bytes.Buffer
			if err := format.Node(&receiver, token.NewFileSet(), fn.Recv.List[0].Type); err != nil {
				t.Fatal(err)
			}
			key += ":" + receiver.String()
		}
		if _, duplicate := result[key]; duplicate {
			t.Fatalf("duplicate canonical read declaration: %s", key)
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
			t.Fatal(err)
		}
		result[key] = out.String()
	}
	return result
}

func verifyCanonicalEventFixtureReadOwnership(t *testing.T) {
	t.Helper()
	root := persistenceAuthorityRepoRoot(t)
	for _, target := range []struct{ path, source string }{
		{filepath.Join(root, "internal", "store", "internal", "runtimepersistence", "test_event_support.go"), canonicalRequiredEventReadShape},
		{filepath.Join(root, "internal", "store", "internal", "runtimepersistence", "test_event_support.go"), canonicalOptionalEventReadShape},
		{filepath.Join(root, "internal", "store", "internal", "backend", "eventpersistence", "test_canonical_event_observation.go"), nativeCanonicalEventReadShape("EventPostgresOwner", "eventrecordpostgres")},
		{filepath.Join(root, "internal", "store", "internal", "backend", "eventpersistence", "test_canonical_event_observation.go"), nativeCanonicalEventReadShape("EventSQLiteOwner", "eventrecordsqlite")},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), target.path, nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		source := target.source
		expected := canonicalEventReadFunctionShapes(t, "package fixture;"+source)
		for key, want := range expected {
			found := 0
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != strings.Split(key, ":")[0] {
					continue
				}
				actualKey := fn.Name.Name
				if fn.Recv != nil {
					var receiver bytes.Buffer
					if err := format.Node(&receiver, token.NewFileSet(), fn.Recv.List[0].Type); err != nil {
						t.Fatal(err)
					}
					actualKey += ":" + receiver.String()
				}
				if actualKey != key {
					continue
				}
				found++
				var out bytes.Buffer
				if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
					t.Fatal(err)
				}
				if out.String() != want {
					t.Fatalf("canonical read ownership/presence/context/key/decoder changed: %s", key)
				}
			}
			if found != 1 {
				t.Fatalf("canonical read entry missing or duplicated: %s/%d", key, found)
			}
		}
	}
}

func TestCanonicalEventFixtureReadOwnerGuardRejectsIndependentReadsAndLostRefusals(t *testing.T) {
	for _, probe := range []struct{ source, from, to string }{
		{canonicalRequiredEventReadShape, "ReadCanonicalEventRecordForTest(ctx, selected, eventID)", "independentRead(ctx, selected, eventID)"},
		{canonicalRequiredEventReadShape, "if !found", "if found"},
		{canonicalOptionalEventReadShape, "validateChannelObservationOwner(selected)", "validateChannelObservationOwner(otherOwner)"},
		{canonicalOptionalEventReadShape, "ReadCanonicalEventRecordForTest(ctx, eventID)", "ReadCanonicalEventRecordForTest(ctx, otherEventID)"},
		{canonicalOptionalEventReadShape, "store.eventPostgresOwner", "store.deliveryPostgresOwner"},
		{canonicalOptionalEventReadShape, "store.eventSQLiteOwner", "store.deliverySQLiteOwner"},
		{canonicalOptionalEventReadShape, "return empty, false, err", "return empty, true, nil"},
		{nativeCanonicalEventReadShape("EventPostgresOwner", "eventrecordpostgres"), "eventrecordpostgres.Load(ctx, tx, eventID)", "eventrecordpostgres.Load(ctx, tx, otherEventID)"},
		{nativeCanonicalEventReadShape("EventSQLiteOwner", "eventrecordsqlite"), "eventrecordsqlite.Load(ctx, tx, eventID)", "eventrecordsqlite.Load(ctx, tx, otherEventID)"},
		{nativeCanonicalEventReadShape("EventPostgresOwner", "eventrecordpostgres"), "RunReadTransaction(ctx,", "RunReadTransaction(otherContext,"},
		{nativeCanonicalEventReadShape("EventSQLiteOwner", "eventrecordsqlite"), "record.Decode()", "alternateDecoder(record)"},
		{nativeCanonicalEventReadShape("EventPostgresOwner", "eventrecordpostgres"), "if err != nil || !found", "if err != nil && !found"},
		{nativeCanonicalEventReadShape("EventSQLiteOwner", "eventrecordsqlite"), "return empty, false, err", "return empty, true, nil"},
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
