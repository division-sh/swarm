package main

import (
	"encoding/json"
	"strings"
	"testing"
)

var nativeSQLiteConstructionShapes = map[string]string{
	"StartSQLiteRuntimeStorePair":        "func StartSQLiteRuntimeStorePair(t testing.TB) (*store.SQLiteRuntimeStore, *store.SQLiteRuntimeStore) {\n\tt.Helper()\n\tprimary, reopen := StartSQLiteRuntimeStoreWithReopen(t, context.Background())\n\treconstructed := reopen()\n\tif _, err := os.Stat(primary.Path()); err != nil {\n\t\tt.Fatalf(\"sqlite runtime store did not create file-backed db at %s: %v\", primary.Path(), err)\n\t}\n\treturn primary, reconstructed\n}",
	"StartSQLiteRuntimeStoreWithContext": "func StartSQLiteRuntimeStoreWithContext(t testing.TB, ctx context.Context) *store.SQLiteRuntimeStore {\n\tt.Helper()\n\tsqliteStore, _ := StartSQLiteRuntimeStoreWithReopen(t, ctx)\n\tif _, err := os.Stat(sqliteStore.Path()); err != nil {\n\t\tt.Fatalf(\"sqlite runtime store did not create file-backed db at %s: %v\", sqliteStore.Path(), err)\n\t}\n\treturn sqliteStore\n}",
}

func TestNativeSQLiteConstructionConsumesSingleBootstrapAndCleanupOwner(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-sqlite-construction-owner" {
			continue
		}
		matched++
		want, err := canonicalFunction(nativeSQLiteConstructionShapes[row.Function])
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || got != actual {
			t.Fatalf("SQLite constructor lost shared ownership: %s", row.Function)
		}
		for _, retired := range []string{"store.NewSQLiteRuntimeStore(", ".BootstrapSchema(", "t.Cleanup(", "bindTestPayloadAdmitter("} {
			if strings.Contains(row.After, retired) {
				t.Fatalf("duplicate construction owner survives: %s", retired)
			}
		}
		for _, pair := range [][2]string{
			{"StartSQLiteRuntimeStoreWithReopen(t,", "StartSQLiteRuntimeStoreWithReopen(otherTest,"},
			{"os.Stat(", "ignoreStat("},
			{"; err != nil {", "; false {"},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			value, err := canonicalFunction(mutant)
			if mutant == row.After || (err == nil && value == want) {
				t.Fatalf("foreign owner or suppressed failure admitted: %v", pair)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("SQLite constructor recipes=%d,want2", matched)
	}
	owner := selectedCausalObservationBody(t, "internal/store/storetest/runtime_reopen.go", "StartSQLiteRuntimeStoreWithReopen")
	for _, fact := range []string{"t.Cleanup(", "owners = append(owners, selected)", "selected.BootstrapSchema(ctx, request)", "bindTestPayloadAdmitter(selected)", "owners[i].Close()", "return open(), open"} {
		if !strings.Contains(owner, fact) {
			t.Fatalf("canonical construction owner lost %s", fact)
		}
	}
}
