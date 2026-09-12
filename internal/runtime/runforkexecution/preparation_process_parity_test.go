package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedForkPreparationProcessBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var db *sql.DB
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			repo := runForkExecutionRepoRoot(t)
			fixtureLoader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
			loaded, err := fixtureLoader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, eventID := uuid.NewString(), uuid.NewString()
			seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceRun, eventID, uuid.NewString())
			request := SelectedContractExecutionRequest{
				Owner: owner, SourceRunID: sourceRun, At: eventID,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				SourceLoader:      SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(SourceArtifactSelectedContractSourceStore)},
				AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
			}
			before := selectedPreparationDatabaseSnapshot(t, db, backend)
			baseline := process.ActiveCount()
			for _, test := range []struct {
				name string
				cap  startupownership.ProcessCapability
			}{
				{"missing", nil},
				{"foreign_acquisition", crossedSelectedProcessCapability{ProcessCapability: capability, cross: func(a *startupownership.Authority) { a.AcquisitionID = uuid.NewString() }}},
				{"foreign_generation", crossedSelectedProcessCapability{ProcessCapability: capability, cross: func(a *startupownership.Authority) { a.AuthorityGeneration++ }}},
			} {
				t.Run(test.name, func(t *testing.T) {
					attempt := request
					attempt.AgentRuntime.ProcessCapability = test.cap
					prepared, err := owner.Prepare(ctx, attempt)
					if prepared != nil || err == nil || !strings.Contains(err.Error(), "process capability") {
						if prepared != nil {
							_ = prepared.Close()
						}
						t.Fatalf("wrong preparation disposition: prepared=%v err=%v", prepared, err)
					}
					if !reflect.DeepEqual(before, selectedPreparationDatabaseSnapshot(t, db, backend)) || process.ActiveCount() != baseline {
						t.Fatal("rejected preparation changed persisted facts or leaked process ownership")
					}
				})
			}
			prepared, err := owner.Prepare(ctx, request)
			if err != nil {
				t.Fatalf("exact process preparation: %v", err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, selectedPreparationDatabaseSnapshot(t, db, backend)) || process.ActiveCount() != baseline {
				t.Fatal("non-executable preparation mutated the store or retained process work")
			}
		})
	}
}

func selectedPreparationDatabaseSnapshot(t testing.TB, db *sql.DB, backend string) map[string][]string {
	t.Helper()
	opts := &sql.TxOptions{ReadOnly: true}
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if backend == "postgres" {
		opts.Isolation = sql.LevelRepeatableRead
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`
	}
	tx, err := db.BeginTx(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	out := make(map[string][]string, len(tables))
	for _, table := range tables {
		rows, err := tx.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		out[table] = []string{strings.Join(columns, ",")}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for i, v := range values {
				if raw, ok := v.([]byte); ok {
					values[i] = string(raw)
				}
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(encoded))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(out[table][1:])
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return out
}
