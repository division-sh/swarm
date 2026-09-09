package schemastore

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestExpectedPlatformShapeUsesExactPlansAndIsolatedResults(t *testing.T) {
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		t.Run(string(dialect), func(t *testing.T) {
			memo := platformShapeMemo{dialect: dialect}
			plans := []SchemaTableDDL{{TableName: "example", SchemaKind: "test", ColumnCount: 2, Statements: []string{
				"CREATE TABLE IF NOT EXISTS example (id TEXT PRIMARY KEY, attempt INTEGER CHECK (attempt >= 0), UNIQUE (attempt))",
				"CREATE INDEX IF NOT EXISTS example_attempt ON example (attempt)",
			}}}
			want, err := expectedSchemaShape(plans, dialect)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				got, err := memo.expected(plans)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("expected shape = %#v, %v", got, err)
				}
				table := got.Tables["example"]
				table.Columns["attempt"].Checks[0] = "poison"
				table.Constraints[0] = "poison"
				clear(table.Indexes)
				clear(table.Columns)
				clear(got.Tables)
			}
			plans[0].Statements[0] = strings.ReplaceAll(plans[0].Statements[0], "attempt >= 0", "attempt >= 7")
			changed, err := memo.expected(plans)
			if err != nil || reflect.DeepEqual(changed, want) {
				t.Fatalf("changed plan reused stale shape: %v", err)
			}
			plans[0].Statements[0] = "INVALID SQL"
			if _, err := memo.expected(plans); err == nil {
				t.Fatal("invalid changed plan reused admitted shape")
			}
			plans[0].Statements[0] = "CREATE TABLE IF NOT EXISTS example (id TEXT PRIMARY KEY)"
			want, err = expectedSchemaShape(plans, dialect)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := memo.expected(plans)
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Errorf("concurrent expected shape mismatch: %v", err)
					}
					clear(got.Tables)
				}()
			}
			wg.Wait()
		})
	}
}

func TestExpectedPlatformShapeDoesNotRetainOversizedPlans(t *testing.T) {
	memo := platformShapeMemo{dialect: SchemaDialectPostgres}
	plans := []SchemaTableDDL{{TableName: "example", Statements: []string{
		"CREATE TABLE IF NOT EXISTS example (id TEXT DEFAULT '" + strings.Repeat("x", 1<<20) + "')",
	}}}
	if _, err := memo.expected(plans); err != nil {
		t.Fatal(err)
	}
	if memo.plans != nil || memo.shape.Tables != nil {
		t.Fatal("oversized plan retained")
	}
}

func BenchmarkExpectedPlatformShape(b *testing.B) {
	plans := []SchemaTableDDL{{TableName: "example", Statements: []string{
		"CREATE TABLE IF NOT EXISTS example (id TEXT PRIMARY KEY, attempt INTEGER CHECK (attempt >= 0))",
	}}}
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		b.Run(string(dialect), func(b *testing.B) {
			memo := platformShapeMemo{dialect: dialect}
			if _, err := memo.expected(plans); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := memo.expected(plans); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
