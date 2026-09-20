package runforkrevision

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

var exactSeekRuns = []string{
	"01234567-1234-1234-1234-012345678901",
	"01234567-1234-1234-1234-012345678902",
}

func exactSeekDB(t *testing.T, backend string) *sql.DB {
	t.Helper()
	if backend == "postgres" {
		_, db, _ := testutil.StartEmptyPostgres(t)
		return db
	}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "seeks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func exactSeekRef(t testing.TB, run string, family Family, i int) FactRef {
	t.Helper()
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprint(i))).String()
	var ref FactRef
	var err error
	if family == FamilyFanOutObligations {
		ref, err = FanOutOutcomeFact(fanoutobligation.IntentKey{
			RunID: run, TriggeringDeliveryID: id,
			ElementRef: contracts.FanOutElementRef{FlowPath: "parent/child", Family: "fan_out", SemanticPath: `nodes["quoted'|node"].rules[0]`},
		}, i)
	} else {
		if family == FamilyReplyContexts {
			id = fmt.Sprintf("opaque'|reply-$1-%d", i)
		}
		ref, err = NewFactRef(family, id)
	}
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func exactSeekFixture(t testing.TB, db *sql.DB, families []Family, keys, revisions int, primary bool) {
	t.Helper()
	constraint := ""
	if primary {
		constraint = ", PRIMARY KEY(run_id,family,fact_key,revision)"
	}
	// UUID/JSONB are supported by both native engines; SQLite applies its own
	// scalar affinities. A constraint-free variant admits hostile duplicate rows.
	if _, err := db.Exec(`CREATE TABLE run_fork_fact_revisions (
	 run_id UUID NOT NULL, family TEXT NOT NULL, fact_key TEXT NOT NULL,
	 revision BIGINT NOT NULL, fact JSONB NOT NULL, present BOOLEAN` + constraint + `)`); err != nil {
		t.Fatal(err)
	}
	if !primary {
		if _, err := db.Exec(`CREATE INDEX exact_seek_coordinates ON run_fork_fact_revisions(run_id,family,fact_key,revision)`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE INDEX idx_run_fork_fact_revision_snapshot ON run_fork_fact_revisions(run_id,revision,family,fact_key)`); err != nil {
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
	for _, run := range exactSeekRuns {
		for _, family := range families {
			for i := 0; i < keys; i++ {
				ref := exactSeekRef(t, run, family, i)
				for revision := 1; revision <= revisions; revision++ {
					fact, err := json.Marshal(map[string]any{"run_id": run, "event_id": ref.key, "entity_id": ref.key, "revision": revision, "value": "exact evidence"})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stmt.Exec(run, string(family), ref.key, revision, string(fact), (i+revision)%3 != 0); err != nil {
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

func exactSeekChange(t testing.TB, run string, families []Family, keys int) declaredChange {
	t.Helper()
	change := declaredChange{runID: run, families: append([]Family(nil), families...), exact: map[Family][]FactRef{}}
	for _, family := range families {
		for i := 0; i < keys; i++ {
			change.exact[family] = append(change.exact[family], exactSeekRef(t, run, family, i))
		}
	}
	return change
}

// Independent pre-change grouped-MAX SQL oracle. Tests use admitted coordinates;
// production's shared validation is separately checked below.
func originalAffectedFactQuery(change declaredChange) (string, []any, map[Family]bool) {
	args := []any{change.runID}
	wanted := map[Family]bool{}
	var selections []string
	for _, family := range change.families {
		wanted[family] = true
		args = append(args, string(family))
		condition := fmt.Sprintf("family=$%d", len(args))
		if refs, exact := change.exact[family]; exact {
			var binds []string
			for _, ref := range refs {
				args = append(args, ref.key)
				binds = append(binds, fmt.Sprintf("$%d", len(args)))
			}
			condition += " AND fact_key IN (" + strings.Join(binds, ",") + ")"
		}
		selections = append(selections, "("+condition+")")
	}
	return `SELECT r.family,r.fact_key,r.fact,r.present
		FROM (SELECT family,fact_key,MAX(revision) AS revision FROM run_fork_fact_revisions
		WHERE run_id=$1 AND (` + strings.Join(selections, " OR ") + `)
		GROUP BY family,fact_key) latest CROSS JOIN run_fork_fact_revisions r
		WHERE r.run_id=$1 AND r.family=latest.family
		AND r.fact_key=latest.fact_key AND r.revision=latest.revision`, args, wanted
}

type exactSeekQuery struct {
	query string
	args  []any
}

type exactSeekRecorder struct {
	queryer
	calls []exactSeekQuery
}

func (r *exactSeekRecorder) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.calls = append(r.calls, exactSeekQuery{query, append([]any(nil), args...)})
	return r.queryer.QueryContext(ctx, query, args...)
}

func assertExactSeekDifferential(t *testing.T, ctx context.Context, q queryer, change declaredChange) ledgerFactsByFamily {
	t.Helper()
	query, args, wanted := originalAffectedFactQuery(change)
	want, oldErr := readLedgerFacts(ctx, q, query, args, wanted)
	got, err := readSelectedLatestFacts(ctx, q, change)
	if (oldErr == nil) != (err == nil) || !reflect.DeepEqual(got, want) {
		t.Fatalf("differential: new=%v old=%v newError=%v oldError=%v", got, want, err, oldErr)
	}
	if oldErr != nil && oldErr.Error() != err.Error() {
		t.Fatalf("error changed: new=%v old=%v", err, oldErr)
	}
	return got
}

func TestExactRevisionSeeksDifferentialBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := exactSeekDB(t, backend)
			exactSeekFixture(t, db, AllFamilies(), 4, 3, true)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ctx := context.Background()
			for _, run := range append(append([]string(nil), exactSeekRuns...), uuid.NewString()) {
				change := exactSeekChange(t, run, AllFamilies(), 5) // Last coordinate is absent.
				change.families = append(change.families, FamilyEvents)
				change.exact[FamilyEvents] = append(change.exact[FamilyEvents], change.exact[FamilyEvents][0])
				got := assertExactSeekDifferential(t, ctx, tx, change)
				if run == exactSeekRuns[0] || run == exactSeekRuns[1] {
					for _, family := range AllFamilies() {
						if len(got[family]) != 4 {
							t.Fatalf("family %s lost keys or added absent/duplicate coordinates", family)
						}
						for i := 0; i < 4; i++ {
							fact := got[family][exactSeekRef(t, run, family, i).key]
							var body struct {
								RunID    string `json:"run_id"`
								Revision int    `json:"revision"`
							}
							if err := json.Unmarshal(fact.fact, &body); err != nil || body.RunID != run || body.Revision != 3 || fact.present != ((i+3)%3 != 0) {
								t.Fatalf("latest fact or tombstone changed: %s/%d %+v err=%v", family, i, fact, err)
							}
						}
					}
				} else if len(got) != 0 {
					t.Fatal("absent run returned facts")
				}
			}
			change := exactSeekChange(t, exactSeekRuns[0], []Family{FamilyEvents, FamilyEntityMetadata}, 2)
			before := assertExactSeekDifferential(t, ctx, tx, change)
			ref := change.exact[FamilyEvents][0]
			if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,$2,$3,4,'{}',true)`, change.runID, string(FamilyEvents), ref.key); err != nil {
				t.Fatal(err)
			}
			after := assertExactSeekDifferential(t, ctx, tx, change)
			if reflect.DeepEqual(before, after) || !after[FamilyEvents][ref.key].present {
				t.Fatal("new latest revision was cached or tombstone remained")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if restored := assertExactSeekDifferential(t, ctx, db, change); !reflect.DeepEqual(restored, before) {
				t.Fatal("rolled back revision survived")
			}
		})
	}
}

func TestExactRevisionSeeksBatchAndBroadBoundary(t *testing.T) {
	db := exactSeekDB(t, "sqlite")
	families := []Family{FamilyEvents, FamilyEntityMetadata}
	exactSeekFixture(t, db, families, exactFactReadBatch+1, 2, true)
	for _, broad := range []bool{false, true} {
		change := exactSeekChange(t, exactSeekRuns[0], families, exactFactReadBatch+1)
		if broad {
			delete(change.exact, FamilyEvents)
		}
		recorder := &exactSeekRecorder{queryer: db}
		got, err := readSelectedLatestFacts(context.Background(), recorder, change)
		old := &exactSeekRecorder{queryer: db}
		want, oldErr := readAffectedLatestFactsWithSeeks(context.Background(), old, change, false)
		if err != nil || oldErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("broad=%v err=%v old=%v", broad, err, oldErr)
		}
		if len(recorder.calls) != len(old.calls) {
			t.Fatal("batch count changed")
		}
		for i, call := range recorder.calls {
			if len(call.args) > exactFactReadBatch+2 {
				t.Fatalf("unbounded batch: %d parameters", len(call.args))
			}
			if broad {
				if !reflect.DeepEqual(call, old.calls[i]) {
					t.Fatal("mixed-broad request changed a physical query or arguments")
				}
			} else if !strings.Contains(call.query, "WITH requested") {
				t.Fatal("all-exact batch did not use bounded seeks")
			}
		}
	}
	// The whole-family reader must retain its original all-family validation.
	if _, err := db.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,'unknown','hostile',1,'{}',false)`, exactSeekRuns[0]); err != nil {
		t.Fatal(err)
	}
	change := declaredChange{runID: exactSeekRuns[0], families: []Family{FamilyEvents}}
	if _, err := readSelectedLatestFacts(context.Background(), db, change); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("broad reader lost unrelated-family corruption check: %v", err)
	}
}

func TestExactRevisionSeeksCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := exactSeekDB(t, backend)
			exactSeekFixture(t, db, []Family{FamilyEvents}, 2, 2, false)
			change := exactSeekChange(t, exactSeekRuns[0], []Family{FamilyEvents}, 2)
			key := change.exact[FamilyEvents][0].key
			for _, mutation := range []string{
				`INSERT INTO run_fork_fact_revisions SELECT * FROM run_fork_fact_revisions WHERE run_id=$1 AND fact_key=$2 AND revision=2`,
				`UPDATE run_fork_fact_revisions SET present=NULL WHERE run_id=$1 AND fact_key=$2 AND revision=2`,
			} {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(mutation, change.runID, key); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				assertExactSeekDifferential(t, context.Background(), tx, change)
				if _, err := readSelectedLatestFacts(context.Background(), tx, change); err == nil {
					t.Fatal("hostile ledger accepted")
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`UPDATE run_fork_fact_revisions SET fact='{}',present=true WHERE run_id=$1 AND fact_key=$2 AND revision=2`, change.runID, key); err != nil {
				t.Fatal(err)
			}
			facts := assertExactSeekDifferential(t, context.Background(), db, change)
			if err := validateExactCapture(change.runID, FamilyEvents, change.exact[FamilyEvents], nil, facts[FamilyEvents]); err == nil {
				t.Fatal("selected malformed canonical key escaped existing validation")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := readSelectedLatestFacts(ctx, db, change); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation changed: %v", err)
			}
		})
	}
}

func TestExactRevisionSeeksValidationPrecedesQuery(t *testing.T) {
	valid := exactSeekChange(t, exactSeekRuns[0], []Family{FamilyEvents}, 1)
	foreign := exactSeekRef(t, exactSeekRuns[1], FamilyFanOutObligations, 0)
	wrongKey := valid.exact[FamilyEvents][0]
	wrongKey.key = uuid.NewString()
	for _, change := range []declaredChange{
		{runID: valid.runID, families: []Family{"unknown"}, exact: map[Family][]FactRef{"unknown": {}}},
		{runID: valid.runID, families: valid.families, exact: map[Family][]FactRef{FamilyEvents: {}}},
		{runID: valid.runID, families: valid.families, exact: map[Family][]FactRef{FamilyEvents: {FactRef{}}}},
		{runID: valid.runID, families: valid.families, exact: map[Family][]FactRef{FamilyEvents: {wrongKey}}},
		{runID: valid.runID, families: valid.families, exact: map[Family][]FactRef{FamilyEvents: {exactSeekRef(t, valid.runID, FamilyEntityMetadata, 0)}}},
		{runID: valid.runID, families: []Family{FamilyFanOutObligations}, exact: map[Family][]FactRef{FamilyFanOutObligations: {foreign}}},
	} {
		old, optimized := &exactSeekRecorder{}, &exactSeekRecorder{}
		_, oldErr := readAffectedLatestFactsWithSeeks(context.Background(), old, change, false)
		_, err := readSelectedLatestFacts(context.Background(), optimized, change)
		if err == nil || oldErr == nil || err.Error() != oldErr.Error() || len(old.calls) != 0 || len(optimized.calls) != 0 {
			t.Fatalf("validation changed: new=%v old=%v calls=%d/%d", err, oldErr, len(optimized.calls), len(old.calls))
		}
	}
}

func TestExactRevisionSeeksPlanBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := exactSeekDB(t, backend)
			families := []Family{FamilyEvents, FamilyEntityMetadata}
			exactSeekFixture(t, db, families, 4, 3, true)
			change := exactSeekChange(t, exactSeekRuns[0], families, 2)
			recorder := &exactSeekRecorder{queryer: db}
			if _, err := readSelectedLatestFacts(context.Background(), recorder, change); err != nil {
				t.Fatal(err)
			}
			oldQuery, oldArgs, _ := originalAffectedFactQuery(change)
			for _, tc := range []struct {
				name string
				call exactSeekQuery
			}{
				{"grouped", exactSeekQuery{oldQuery, oldArgs}}, {"seeks", recorder.calls[0]},
			} {
				prefix := "EXPLAIN "
				if backend == "sqlite" {
					prefix = "EXPLAIN QUERY PLAN "
				}
				rows, err := db.Query(prefix+tc.call.query, tc.call.args...)
				if err != nil {
					t.Fatal(err)
				}
				var plan []string
				for rows.Next() {
					var detail string
					if backend == "sqlite" {
						var id, parent, unused int
						err = rows.Scan(&id, &parent, &unused, &detail)
					} else {
						err = rows.Scan(&detail)
					}
					if err != nil {
						rows.Close()
						t.Fatal(err)
					}
					plan = append(plan, detail)
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					t.Fatal(err)
				}
				joined := strings.Join(plan, "\n")
				t.Logf("%s native %s plan:\n%s", backend, tc.name, joined)
				if backend == "sqlite" && tc.name == "seeks" && (!strings.Contains(joined, "run_id=? AND family=? AND fact_key=?") || strings.Contains(joined, "SCAN run_fork_fact_revisions")) {
					t.Fatalf("exact latest read did not seek coordinate index: %s", joined)
				}
			}
		})
	}
}

func BenchmarkExactRevisionSeeksSQLiteHistory(b *testing.B) {
	db, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "history.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	families := []Family{FamilyEvents, FamilyEntityMetadata}
	exactSeekFixture(b, db, families, 512, 32, true)
	for _, keys := range []int{1, 16, 64} {
		change := exactSeekChange(b, exactSeekRuns[0], families, keys)
		for _, seek := range []bool{false, true} {
			b.Run(fmt.Sprintf("keys_%d/seeks_%v", keys*len(families), seek), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var facts ledgerFactsByFamily
					var err error
					if seek {
						facts, err = readSelectedLatestFacts(context.Background(), db, change)
					} else {
						facts, err = readAffectedLatestFactsWithSeeks(context.Background(), db, change, false)
					}
					if err != nil || len(facts[FamilyEvents]) != keys || len(facts[FamilyEntityMetadata]) != keys {
						b.Fatalf("facts=%d err=%v", len(facts), err)
					}
				}
			})
		}
	}
}
