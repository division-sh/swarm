package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeCatalogClockRecipePreservesExactObservationCut(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-observation-clock" {
			continue
		}
		count++
		if row.File != "internal/runtime/cataloge2e/runtime_harness_test.go" || row.Function != "catalogHarnessStartBoundary" {
			t.Fatal("unreviewed catalog clock recipe")
		}
		old := "var out time.Time\n\tif err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `SELECT NOW()`).Scan(&out); err != nil {"
		fresh := "out, err := storetest.ReadPostgresObservationTime(testAuthorActivityContext(context.Background()), pg)\n\tif err != nil {"
		if strings.Count(row.Before, old) != 1 || strings.Count(row.Before, "db *sql.DB") != 1 {
			t.Fatal("catalog clock query or native input differs from the audited boundary")
		}
		expected, err := canonicalFunction(strings.Replace(strings.Replace(row.Before, "db *sql.DB", "pg *store.PostgresStore", 1), old, fresh, 1))
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("catalog clock, SQLite branch or overlap cut changed: %v / %v", err, parseErr)
		}
		for _, pair := range [][2]string{
			{"ReadPostgresObservationTime(testAuthorActivityContext(context.Background()), pg)", "ReadPostgresObservationTime(context.Background(), foreignStore)"},
			{"backend == catalogBackendSQLite", "backend == catalogBackendPostgres"},
			{"out.UTC()", "time.Now().UTC()"},
			{"dbTime.Before(appTime)", "dbTime.After(appTime)"},
			{"-1 * time.Second", "time.Second"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After {
				t.Fatalf("negative control did not change source: %v", pair)
			}
			actual, err := canonicalFunction(changed)
			if err == nil && actual == expected {
				t.Fatalf("foreign owner, clock fallback or changed cut accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("catalog clock recipe count=%d, want one shared observation owner", count)
	}
}
