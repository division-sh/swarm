package schemastore

import (
	"strings"
	"testing"
)

func TestSQLitePredicateReplacementsPreserveExactTranslation(t *testing.T) {
	for _, replacement := range sqlitePredicateReplacements {
		t.Run(replacement[0], func(t *testing.T) {
			got, err := sqliteRenderPredicate(replacement[0])
			if err != nil || got != replacement[1] {
				t.Fatalf("render = %q, %v; want %q", got, err, replacement[1])
			}
		})
	}
	for _, column := range []string{"lifecycle_bundle_hash", "current_bundle_hash", "authority_bundle_hash", "bundle_hash"} {
		input := column + " ~ '^bundle-v2:sha256:[0-9a-f]{64}$'"
		got, err := sqliteRenderPredicate(input + " AND " + input)
		want := sqliteBundleHashPredicate(column) + " AND " + sqliteBundleHashPredicate(column)
		if err != nil || got != want {
			t.Fatalf("overlapping column/repeated replacement: %q, %v; want %q", got, err, want)
		}
	}
	if _, err := sqliteRenderPredicate("id ~ '^bad$'"); err == nil || !strings.Contains(err.Error(), "Postgres regex") {
		t.Fatalf("unsupported predicate accepted: %v", err)
	}
}

func BenchmarkSQLiteRenderUnchangedPredicate(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		got, err := sqliteRenderPredicate("attempt >= 0")
		if err != nil || got != "attempt >= 0" {
			b.Fatalf("%q, %v", got, err)
		}
	}
}
