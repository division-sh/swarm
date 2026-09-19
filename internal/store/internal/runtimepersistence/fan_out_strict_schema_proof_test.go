package runtimepersistence

import (
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestFanOutRetiredSchemaShapesRejectedUnchangedBothStores(t *testing.T) {
	for _, dialect := range []SchemaDialect{SchemaDialectSQLite, SchemaDialectPostgres} {
		for _, shape := range []struct {
			name                   string
			duration, missingRetry bool
		}{
			{"retired_duration", true, false},
			{"missing_retry_pair", false, true},
			{"exact_pre_2394", true, true},
		} {
			for _, populated := range []bool{false, true} {
				cell := "empty"
				if populated {
					cell = "populated"
				}
				t.Run(string(dialect)+"/"+shape.name+"/"+cell, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					request := canonicalSchemaBootstrapTestRequest(t)
					db, bootstrap := legacyShapeTestStore(t, dialect, request)
					if populated {
						var selected authorActivityReceiptStore
						if dialect == SchemaDialectSQLite {
							selected = NewSQLiteRuntimeStoreForTest(db)
						} else {
							selected = admitTestPostgresStore(t, db)
						}
						seedDeclaredForkFanOutFixture(t, string(dialect), authorActivityReceiptFixture{db: db, store: selected}, 3, time.Now().UTC())
					}
					var plan SchemaTableDDL
					for _, candidate := range request.PlatformPlans {
						if candidate.TableName == "fan_out_intents" {
							plan = candidate
						}
					}
					if plan.TableName == "" {
						t.Fatal("canonical schema omitted fan_out_intents")
					}
					installRetiredFanOutShape(t, db, dialect, plan, shape.duration, shape.missingRetry)
					beforeColumns := selectedStoreTableColumns(t, dialect, db, "fan_out_intents")
					beforeRows := selectedStoreTableRowCount(t, db, "fan_out_intents")
					wantRows := 0
					if populated {
						wantRows = 1
					}
					if beforeRows != wantRows || beforeColumns["last_chunk_ms"] != shape.duration || beforeColumns["retry_ready_at"] == shape.missingRetry || beforeColumns["retry_failure"] == shape.missingRetry {
						t.Fatalf("fixture is not the requested exact shape: columns=%v rows=%d", beforeColumns, beforeRows)
					}
					var beforeCapsule, beforeSource string
					if populated {
						if err := db.QueryRowContext(ctx, `SELECT CAST(capsule AS TEXT),CAST(source_event_id AS TEXT) FROM fan_out_intents WHERE cardinality=3 AND cursor=0 AND status='open'`).Scan(&beforeCapsule, &beforeSource); err != nil {
							t.Fatal(err)
						}
					}
					request.StatePlans = generatedProbeStatePlans()
					var incompatible *SchemaCompatibilityError
					if err := bootstrap(ctx, request); !errors.As(err, &incompatible) {
						t.Fatalf("retired fan-out shape admission = %v, want SchemaCompatibilityError", err)
					}
					wantDrift := []string{"fan_out_intents"}
					if shape.duration {
						wantDrift = append(wantDrift, "last_chunk_ms")
					}
					if shape.missingRetry {
						wantDrift = append(wantDrift, "retry_ready_at", "retry_failure")
					}
					for _, want := range wantDrift {
						if !strings.Contains(strings.Join(incompatible.Drift, ";"), want) {
							t.Fatalf("missing exact drift %q: %v", want, incompatible.Drift)
						}
					}
					if after := selectedStoreTableColumns(t, dialect, db, "fan_out_intents"); !reflect.DeepEqual(after, beforeColumns) {
						t.Fatalf("rejected bootstrap rewrote columns: before=%v after=%v", beforeColumns, after)
					}
					if after := selectedStoreTableRowCount(t, db, "fan_out_intents"); after != beforeRows {
						t.Fatalf("rejected bootstrap changed rows: before=%d after=%d", beforeRows, after)
					}
					assertSchemaTableExists(t, dialect, db, "generated_probe_state", false)
					if populated {
						var capsule, source string
						if err := db.QueryRowContext(ctx, `SELECT CAST(capsule AS TEXT),CAST(source_event_id AS TEXT) FROM fan_out_intents WHERE cardinality=3 AND cursor=0 AND status='open'`).Scan(&capsule, &source); err != nil || capsule != beforeCapsule || source != beforeSource {
							t.Fatalf("rejected bootstrap changed source/progress/capsule: source=%s capsule=%s err=%v", source, capsule, err)
						}
						if shape.duration {
							var duration int
							if err := db.QueryRowContext(ctx, `SELECT last_chunk_ms FROM fan_out_intents`).Scan(&duration); err != nil || duration != 73 {
								t.Fatalf("retired duration was rewritten: %d %v", duration, err)
							}
						}
					}
				})
			}
		}
	}
}

// Build only the retired fan-out table. All other canonical tables, checks,
// indexes and seeded source/trigger facts remain under the existing harness.
func installRetiredFanOutShape(t *testing.T, db *sql.DB, dialect SchemaDialect, plan SchemaTableDDL, duration, missingRetry bool) {
	t.Helper()
	ctx := testAuthorActivityContext()
	columns := selectedStoreTableColumns(t, dialect, db, "fan_out_intents")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	exec := func(statement string) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("install retired fan-out shape: %s: %v", statement, err)
		}
	}
	if dialect == SchemaDialectPostgres {
		if missingRetry {
			exec(`ALTER TABLE fan_out_intents DROP COLUMN retry_ready_at CASCADE`)
			exec(`ALTER TABLE fan_out_intents DROP COLUMN retry_failure`)
		}
		if duration {
			exec(`ALTER TABLE fan_out_intents ADD COLUMN last_chunk_ms BIGINT NOT NULL DEFAULT 0 CHECK (last_chunk_ms >= 0)`)
			exec(`UPDATE fan_out_intents SET last_chunk_ms=73`)
		}
	} else {
		// SQLite cannot drop a column referenced by the old paired CHECK.
		// Rebuild this test precondition transactionally, preserving inbound FK
		// names by dropping (not renaming) the original before the final rename.
		statements, err := SQLiteStatementsForPlan(plan)
		if err != nil || len(statements) == 0 {
			t.Fatalf("canonical SQLite fan-out DDL: %v", err)
		}
		lines := strings.Split(statements[0], "\n")
		var kept []string
		removed, added := 0, 0
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if missingRetry && (strings.HasPrefix(trimmed, `"retry_ready_at" `) || strings.HasPrefix(trimmed, `"retry_failure" `) || strings.HasPrefix(trimmed, "CHECK ((retry_ready_at ")) {
				removed++
				continue
			}
			if duration && strings.HasPrefix(trimmed, `"capsule" `) {
				kept = append(kept, "last_chunk_ms INTEGER NOT NULL DEFAULT 0 CHECK (last_chunk_ms >= 0),")
				added++
			}
			kept = append(kept, line)
		}
		if (missingRetry && removed != 3) || (duration && added != 1) {
			t.Fatalf("retired shape transform was not exact: removed=%d added=%d DDL=%s", removed, added, statements[0])
		}
		exec(strings.Replace(strings.Join(kept, "\n"), "fan_out_intents", "fan_out_intents_retired_fixture", 1))
		var names []string
		for name := range columns {
			if !missingRetry || (name != "retry_ready_at" && name != "retry_failure") {
				names = append(names, QuoteIdent(name))
			}
		}
		sort.Strings(names)
		destination, source := strings.Join(names, ","), strings.Join(names, ",")
		if duration {
			destination += ",last_chunk_ms"
			source += ",73"
		}
		exec(`INSERT INTO fan_out_intents_retired_fixture (` + destination + `) SELECT ` + source + ` FROM fan_out_intents`)
		exec(`DROP TABLE fan_out_intents`)
		exec(`ALTER TABLE fan_out_intents_retired_fixture RENAME TO fan_out_intents`)
		for _, statement := range statements[1:] {
			exec(statement)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
