package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedBranchDivergenceExactPointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
				for _, status := range []string{"running", "completed"} {
					t.Run(string(kind)+"/"+status, func(t *testing.T) {
						db, value := selectedBranchDivergenceTestDatabase(t, backend, kind, status)
						ctx := context.Background()
						write := func(t *testing.T, value runfork.RunForkSelectedContractBranchDivergence, want bool) {
							t.Helper()
							tx, err := db.BeginTx(ctx, nil)
							if err != nil {
								t.Fatal(err)
							}
							defer tx.Rollback()
							if backend == "postgres" {
								err = insertRunForkSelectedContractBranchDivergence(ctx, tx, value)
							} else {
								err = insertSQLiteRunForkSelectedContractBranchDivergence(ctx, tx, value)
							}
							if !want {
								if err == nil {
									t.Fatal("invalid branch evidence accepted")
								}
								return
							}
							if err != nil {
								t.Fatal(err)
							}
							if err := tx.Commit(); err != nil {
								t.Fatal(err)
							}
						}
						for _, invalid := range []struct {
							name string
							edit func(*runfork.RunForkSelectedContractBranchDivergence)
						}{
							{"missing_point", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.ForkPoint = runfork.RunForkPoint{} }},
							{"zero_revision", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.ForkPoint.Revision = 0 }},
							{"foreign_source", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.SourceRunID = uuid.NewString() }},
							{"crossed_child", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.ForkRunID = uuid.NewString() }},
							{"wrong_revision", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.ForkPoint.Revision++ }},
							{"foreign_event", func(v *runfork.RunForkSelectedContractBranchDivergence) {
								v.ForkPoint.EventID = uuid.NewString()
								v.ForkEventID = v.ForkPoint.EventID
							}},
							{"contradictory_projection", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.ForkEventID = uuid.NewString() }},
							{"source_freeze", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.SourceFrozen = true }},
							{"source_rewind", func(v *runfork.RunForkSelectedContractBranchDivergence) { v.SourceRunStatusAfterActivation = "paused" }},
						} {
							t.Run(invalid.name, func(t *testing.T) {
								bad := value
								invalid.edit(&bad)
								write(t, bad, false)
								var count int
								if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences`).Scan(&count); err != nil || count != 0 {
									t.Fatalf("rejected evidence left rows=%d err=%v", count, err)
								}
							})
						}
						for _, corruption := range []struct {
							name, corrupt, restore string
							args                   []any
						}{
							{"binding_revision", `UPDATE run_fork_selected_contract_bindings SET fork_revision=fork_revision+1 WHERE fork_run_id=$1`, `UPDATE run_fork_selected_contract_bindings SET fork_revision=fork_revision-1 WHERE fork_run_id=$1`, []any{value.ForkRunID}},
							{"child_revision", `UPDATE runs SET forked_from_revision=forked_from_revision+1 WHERE run_id=$1`, `UPDATE runs SET forked_from_revision=forked_from_revision-1 WHERE run_id=$1`, []any{value.ForkRunID}},
							{"missing_committed_revision", `UPDATE run_fork_revisions SET revision=revision+1 WHERE run_id=$1`, `UPDATE run_fork_revisions SET revision=revision-1 WHERE run_id=$1`, []any{value.SourceRunID}},
							{"foreign_event_ledger", `UPDATE run_fork_fact_revisions SET run_id=$2 WHERE run_id=$1`, `UPDATE run_fork_fact_revisions SET run_id=$1 WHERE run_id=$2`, []any{value.SourceRunID, uuid.NewString()}},
						} {
							if (corruption.name == "foreign_event_ledger" && kind != runfork.RunForkPointEvent) ||
								(corruption.name == "missing_committed_revision" && kind == runfork.RunForkPointEvent) {
								continue
							}
							t.Run(corruption.name, func(t *testing.T) {
								if _, err := db.Exec(corruption.corrupt, corruption.args...); err != nil {
									t.Fatal(err)
								}
								defer func() {
									if _, err := db.Exec(corruption.restore, corruption.args...); err != nil {
										t.Error(err)
									}
								}()
								write(t, value, false)
								var count int
								if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences`).Scan(&count); err != nil || count != 0 {
									t.Fatalf("corrupt lineage left rows=%d err=%v", count, err)
								}
							})
						}
						write(t, value, true)
						retry := value
						retry.CreatedAt = retry.CreatedAt.Add(time.Hour)
						write(t, retry, true)
						conflict := value
						conflict.SourceAdvancedFacts = []string{"other_source_fact"}
						write(t, conflict, false)
						var pointKind, sourceID, sourceStatus, createdAt string
						var revision int64
						var eventID sql.NullString
						if err := db.QueryRow(`SELECT fork_point_kind,fork_revision,CAST(fork_event_id AS TEXT),
							CAST(source_run_id AS TEXT),source_run_status_after_activation,CAST(created_at AS TEXT)
							FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id=$1`, value.ForkRunID).
							Scan(&pointKind, &revision, &eventID, &sourceID, &sourceStatus, &createdAt); err != nil {
							t.Fatal(err)
						}
						stamp, _, err := sqliteTimeValue(createdAt)
						if err != nil || !stamp.Equal(value.CreatedAt) || pointKind != string(kind) || revision != value.ForkPoint.Revision ||
							eventID.String != value.ForkPoint.EventID || eventID.Valid != (kind == runfork.RunForkPointEvent) || sourceID != value.SourceRunID || sourceStatus != status {
							t.Fatalf("first committed evidence changed: point=%s/r%d/%+v source=%s/%s time=%s err=%v", pointKind, revision, eventID, sourceID, sourceStatus, createdAt, err)
						}
						var originalStatus string
						if err := db.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, value.SourceRunID).Scan(&originalStatus); err != nil || originalStatus != status {
							t.Fatalf("source mutated: status=%s err=%v", originalStatus, err)
						}
					})
				}
			}
		})
	}
}

// This adapter proof uses a deliberately minimal ledger. Full construction,
// activation and public effect recovery have separate real-owner controls.
func selectedBranchDivergenceTestDatabase(t *testing.T, backend string, kind runfork.RunForkPointKind, status string) (*sql.DB, runfork.RunForkSelectedContractBranchDivergence) {
	t.Helper()
	db := forkOperationTestDatabase(t, backend)
	idType, factsType := "TEXT", "TEXT"
	if backend == "postgres" {
		idType, factsType = "UUID", "TEXT[]"
	}
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id ID_TYPE PRIMARY KEY, bundle_hash TEXT NOT NULL, origin_kind TEXT, status TEXT,
			forked_from_run_id ID_TYPE, forked_from_point_kind TEXT, forked_from_revision BIGINT, forked_from_event_id ID_TYPE)`,
		`CREATE TABLE run_fork_selected_contract_bindings (binding_id ID_TYPE PRIMARY KEY, fork_run_id ID_TYPE,
			source_run_id ID_TYPE, fork_point_kind TEXT, fork_revision BIGINT, fork_event_id ID_TYPE,
			mode TEXT, bundle_hash TEXT, created_at TEXT)`,
		`CREATE TABLE run_fork_revisions (run_id ID_TYPE, revision BIGINT, PRIMARY KEY(run_id,revision))`,
		`CREATE TABLE run_fork_fact_revisions (run_id ID_TYPE, family TEXT, fact_key TEXT, revision BIGINT, fact TEXT, present BOOLEAN)`,
		`CREATE TABLE run_fork_selected_contract_branch_divergences (fork_run_id ID_TYPE PRIMARY KEY, source_run_id ID_TYPE,
			fork_point_kind TEXT, fork_revision BIGINT, fork_event_id ID_TYPE, owner TEXT, policy TEXT,
			source_run_status_at_activation TEXT, source_run_status_after_activation TEXT, source_frozen BOOLEAN,
			source_advanced_facts FACTS_TYPE, created_at TEXT)`,
	} {
		ddl = strings.ReplaceAll(strings.ReplaceAll(ddl, "ID_TYPE", idType), "FACTS_TYPE", factsType)
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	point := runfork.RunForkPoint{Kind: kind, Revision: 4}
	if kind == runfork.RunForkPointEvent {
		point.EventID = uuid.NewString()
	}
	value := runfork.RunForkSelectedContractBranchDivergence{
		Owner: runfork.RunForkSelectedContractBranchDivergenceOwner, Policy: runfork.RunForkSelectedContractSourceAdvancedBranchPolicy,
		SourceRunID: uuid.NewString(), ForkRunID: uuid.NewString(), ForkPoint: point, ForkEventID: point.EventID,
		SourceRunStatusAtActivation: status, SourceRunStatusAfterActivation: status,
		SourceAdvancedFacts: []string{"source_entity_mutations_advanced_after_fork_point"}, CreatedAt: time.Date(2026, 10, 2, 19, 0, 0, 0, time.UTC),
	}
	for _, command := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO runs (run_id,bundle_hash,origin_kind,status) VALUES ($1,$2,'deployment',$3)`, []any{value.SourceRunID, strings.Repeat("a", 64), status}},
		{`INSERT INTO runs (run_id,bundle_hash,origin_kind,status,forked_from_run_id,forked_from_point_kind,forked_from_revision,forked_from_event_id) VALUES ($1,$2,'fork_materialization','running',$3,$4,$5,$6)`, []any{value.ForkRunID, strings.Repeat("a", 64), value.SourceRunID, kind, point.Revision, nullableForkEventID(point)}},
		{`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,$3,$4,$5,$6,'selected_contracts',NULL,$7)`, []any{uuid.NewString(), value.ForkRunID, value.SourceRunID, kind, point.Revision, nullableForkEventID(point), value.CreatedAt.Format(time.RFC3339Nano)}},
		{`INSERT INTO run_fork_revisions VALUES ($1,$2)`, []any{value.SourceRunID, point.Revision}},
	} {
		if _, err := db.Exec(command.query, command.args...); err != nil {
			t.Fatal(err)
		}
	}
	if kind == runfork.RunForkPointEvent {
		fact := fmt.Sprintf(`{"event_id":%q,"event_name":"test.ready","payload_base64":"e30="}`, point.EventID)
		if _, err := db.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,'events',$2,$3,$4,TRUE)`, value.SourceRunID, point.EventID, point.Revision, fact); err != nil {
			t.Fatal(err)
		}
	}
	return db, value
}
