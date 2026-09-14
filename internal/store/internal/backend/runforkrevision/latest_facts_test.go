package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	_ "modernc.org/sqlite"
)

// Retain the pre-optimization query as an independent row-set oracle.
const originalLatestFactsQuery = `SELECT r.family,r.fact_key,r.fact,r.present
FROM run_fork_fact_revisions r WHERE r.run_id=$1 AND NOT EXISTS (
SELECT 1 FROM run_fork_fact_revisions newer WHERE newer.run_id=r.run_id
AND newer.family=r.family AND newer.fact_key=r.fact_key AND newer.revision>r.revision)`

func revisionQueryFixture(t testing.TB, db *sql.DB, keys, revisions int) {
	t.Helper()
	// Deliberately omit the closed-family CHECK so the read-owner corruption
	// check is exercised independently of the schema constraint.
	_, err := db.Exec(`CREATE TABLE run_fork_fact_revisions (
run_id TEXT NOT NULL, family TEXT NOT NULL, fact_key TEXT NOT NULL,
revision BIGINT NOT NULL, fact TEXT NOT NULL, present BOOLEAN NOT NULL,
PRIMARY KEY(run_id,family,fact_key,revision))`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO run_fork_fact_revisions VALUES ($1,$2,$3,$4,$5,$6)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, run := range []string{"selected", "foreign"} {
		for _, family := range AllFamilies() {
			for key := 0; key < keys; key++ {
				for revision := 1; revision <= revisions; revision++ {
					body := fmt.Sprintf(`{"revision":%d,"value":%q}`, revision, strings.Repeat("evidence", 32))
					if _, err := stmt.Exec(run, string(family), fmt.Sprint(key), revision, body, (key+revision)%3 != 0); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func originalLatestFacts(t testing.TB, tx *sql.Tx, run string) ledgerFactsByFamily {
	t.Helper()
	rows, err := tx.Query(originalLatestFactsQuery, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := ledgerFactsByFamily{}
	for rows.Next() {
		var family Family
		var key string
		var fact ledgerFact
		if err := rows.Scan(&family, &key, &fact.fact, &fact.present); err != nil {
			t.Fatal(err)
		}
		if result[family] == nil {
			result[family] = map[string]ledgerFact{}
		}
		result[family][key] = fact
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestLatestFactsQueryPreservesAllFamiliesAndTombstones(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "sqlite" {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "revision.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
			} else {
				_, db, _ = testutil.StartEmptyPostgres(t)
			}
			revisionQueryFixture(t, db, 4, 5)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var adapter ledgerAdapter = &sqliteAdapter{tx}
			if backend == "postgres" {
				adapter = &postgresAdapter{tx}
			}
			for _, run := range []string{"selected", "foreign", "absent"} {
				want := originalLatestFacts(t, tx, run)
				got, err := adapter.latestFacts(context.Background(), run)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: got=%v want=%v err=%v", run, got, want, err)
				}
				if run != "absent" && len(got) != len(AllFamilies()) {
					t.Fatal("family omitted")
				}
				for _, family := range AllFamilies() {
					if run == "absent" {
						continue
					}
					if len(got[family]) != 4 {
						t.Fatalf("%s missing latest keys", family)
					}
					for key := 0; key < 4; key++ {
						fact := got[family][fmt.Sprint(key)]
						wantBody := fmt.Sprintf(`{"revision":5,"value":%q}`, strings.Repeat("evidence", 32))
						if string(fact.fact) != wantBody || fact.present != ((key+5)%3 != 0) {
							t.Fatalf("%s/%d lost latest body or tombstone", family, key)
						}
					}
				}
			}
			if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ('selected','unknown','hostile',9,'{}',false)`); err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.latestFacts(context.Background(), "selected"); err == nil || !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("unknown tombstoned family escaped validation: %v", err)
			}
		})
	}
}

func BenchmarkLatestFactsSQLite(b *testing.B) {
	db, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "revision.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	revisionQueryFixture(b, db, 40, 8)
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		facts, err := (&sqliteAdapter{tx}).latestFacts(context.Background(), "selected")
		if err != nil || len(facts) != len(AllFamilies()) {
			b.Fatalf("%v %d", err, len(facts))
		}
	}
}

func TestLatestFactsSQLiteQueryPlan(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	revisionQueryFixture(t, db, 2, 3)
	for name, query := range map[string]string{"original": originalLatestFactsQuery, "candidate": latestFactsQuery} {
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, "selected")
		if err != nil {
			t.Fatal(err)
		}
		var exactSeek bool
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			t.Log(name, detail)
			if name == "candidate" {
				if strings.Contains(detail, "CORRELATED") {
					t.Error("latest-fact query probes each historical revision")
				}
				if strings.Contains(detail, "SEARCH r ") && strings.Contains(detail, "run_id=? AND family=? AND fact_key=? AND revision=?") {
					exactSeek = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if name == "candidate" && !exactSeek {
			t.Fatal("latest fact hydration is not an exact primary-key lookup")
		}
	}
}
