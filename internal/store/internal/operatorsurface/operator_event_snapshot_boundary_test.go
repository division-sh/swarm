package operatorsurface

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Source-location census only, not an authority or transaction-identity guard.
// Names below locate the four consumers excluded from #2363 and their reviewed
// helpers. This intentionally makes no assertion about resolved call targets,
// transaction identity, shadowing, or transitive pool escapes. The both-store
// runtime snapshot tests supply the interleaving and failure-release evidence.
func TestOperatorEventSnapshotConsumerCensus(t *testing.T) {
	for _, backend := range []struct {
		receiver, file string
	}{
		{"ObservabilityPostgres", "operator_observability_read_surface.go"},
		{"ObservabilitySQLite", "sqlite_runtime_observability.go"},
	} {
		deliveryReader, deadLetterReader := "loadOperatorEventDeliveries", "loadOperatorEventDeadLetters"
		if backend.receiver == "ObservabilitySQLite" {
			deliveryReader, deadLetterReader = "sqliteOperatorEventDeliveries", "sqliteOperatorEventDeadLetters"
		}
		for _, method := range []string{"ListOperatorEvents", "LoadOperatorEvent"} {
			operatorSnapshotMethod(t, backend.file, backend.receiver, method)
			t.Logf("consumer census: %s.%s in %s", backend.receiver, method, backend.file)
		}
		for _, helper := range []struct {
			file, method string
		}{
			{backend.file, "listOperatorEvents"},
			{backend.file, "loadOperatorEvent"},
			{backend.file, deliveryReader},
			{backend.file, deadLetterReader},
			{"operator_event_batch.go", "loadOperatorEventBatch"},
			{"delivery_projection.go", "deliverySnapshotsForEvent"},
		} {
			operatorSnapshotMethod(t, helper.file, backend.receiver, helper.method)
			t.Logf("helper census: %s.%s in %s", backend.receiver, helper.method, helper.file)
		}
	}
}

func operatorSnapshotMethod(t *testing.T, path, receiver, method string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != method || fn.Recv == nil {
			continue
		}
		if pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
			if name, ok := pointer.X.(*ast.Ident); ok && name.Name == receiver {
				return fn
			}
		}
	}
	t.Fatalf("missing consumer %s.%s in %s", receiver, method, path)
	return nil
}
