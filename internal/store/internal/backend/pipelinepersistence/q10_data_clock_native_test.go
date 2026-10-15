package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

// Native writer/paired-reader proofs over the actual fresh schema. These are
// not full engine, served, historical-ledger or replay qualification receipts.
func TestQ10DataClockWriteTriggersBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(q10Dialect(postgres), func(t *testing.T) {
			db := q10FreshDatabase(t, postgres)
			ctx := context.Background()
			at := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
			runID, entityID, foreignRunID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if postgres {
				materializePostgresStatementRuns(t, db, false, runID, foreignRunID)
			} else {
				materializeSQLiteStatementRuns(t, db, false, runID, foreignRunID)
			}
			record := pipeline.WorkflowEngineStateRecord{
				Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute("work", "work", "work")},
				EntityID: entityID, EntityType: "task", WorkflowName: "work", Mode: "static", Status: "active", StageDefined: true,
				CurrentState: "ready", Fields: []byte(`{"count":1}`), Gates: []byte(`{}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`), Config: []byte(`{}`),
				CreatedAt: at, UpdatedAt: at, EnteredStageAt: at,
			}
			commit := func(create bool) {
				t.Helper()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := commitWorkflowHeaderAndFields(ctx, tx, postgres, record, workflowEngineStateDecision{createState: create}); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			read := func() pipeline.WorkflowTargetPersistenceRecord {
				t.Helper()
				value, err := loadWorkflowTargetPersistence(ctx, db, record.Identity, identity.NormalizeEntityID(entityID), !postgres)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			commit(true)
			born := read()
			if born.Presence != pipeline.WorkflowTargetPersistenceComplete || !born.State.CreatedAt.Equal(at) || !born.State.UpdatedAt.Equal(at) || born.Lifecycle.State.Revision != 1 {
				t.Fatalf("fresh paired owner/data clocks: %+v", born)
			}
			for index, change := range []struct {
				name, fields string
				changesData  bool
			}{
				{"lifecycle_only", `{"count":1}`, false},
				{"integer_to_double", `{"count":1.0}`, true},
				{"absent_to_null", `{"count":1.0,"note":null}`, true},
				{"gate_only", `{"count":1.0,"note":null}`, false},
				{"representation_only", `{"note":null,"count":1.000}`, false},
				{"slug_only", `{"count":1.0,"note":null}`, true},
				{"name_only", `{"count":1.0,"note":null}`, true},
				{"bookkeeping_only", `{"count":1.0,"note":null}`, false},
				{"clear_last_fields", `{}`, true},
			} {
				t.Run(change.name, func(t *testing.T) {
					before := read()
					record.ExpectedState, record.ExpectedRevision = record.CurrentState, before.Lifecycle.State.Revision
					record.CurrentState = "review"
					record.UpdatedAt = at.Add(time.Duration(index+1) * time.Minute)
					record.Fields = []byte(change.fields)
					if change.name == "gate_only" {
						record.Gates = []byte(`{"approved":true}`)
					}
					if change.name == "slug_only" {
						record.Slug = "renamed-task"
					}
					if change.name == "name_only" {
						record.Name = "Renamed task"
					}
					if change.name == "bookkeeping_only" {
						record.Bookkeeping = []byte(`{"processed":true}`)
					}
					commit(false)
					after := read()
					wantUpdate := before.State.UpdatedAt
					if change.changesData {
						wantUpdate = record.UpdatedAt
					}
					if !after.State.CreatedAt.Equal(at) || !after.State.UpdatedAt.Equal(wantUpdate) || after.Lifecycle.State.Revision != before.Lifecycle.State.Revision+1 || !after.Lifecycle.State.UpdatedAt.Equal(record.UpdatedAt) {
						t.Fatalf("data/lifecycle clock trigger: before=%+v after=%+v", before, after)
					}
					if after.State.Slug != record.Slug || after.State.Name != record.Name || !after.Lifecycle.State.EnteredStageAt.Equal(at) {
						t.Fatalf("display/entry preservation: %+v", after)
					}
				})
			}
			for _, refusal := range []string{"stale_revision", "wrong_stage", "backwards_data_clock"} {
				t.Run(refusal, func(t *testing.T) {
					before, original := read(), record
					record.ExpectedRevision, record.ExpectedState = before.Lifecycle.State.Revision, record.CurrentState
					switch refusal {
					case "stale_revision":
						record.ExpectedRevision--
					case "wrong_stage":
						record.ExpectedState = "not-the-current-stage"
					case "backwards_data_clock":
						record.UpdatedAt = before.State.UpdatedAt.Add(-time.Second)
						record.Fields = []byte(`{"count":2}`)
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					_, mutationErr := commitWorkflowHeaderAndFields(ctx, tx, postgres, record, workflowEngineStateDecision{})
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					if mutationErr == nil || !reflect.DeepEqual(before, read()) {
						t.Fatalf("refusal changed the committed pair: %v", mutationErr)
					}
					if refusal != "backwards_data_clock" {
						failure, ok := failures.As(mutationErr)
						if !ok || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" || !failure.Failure.Retryable {
							t.Fatalf("CAS refusal lost retryable identity: %v", mutationErr)
						}
					}
					record = original
				})
			}
			for _, hostile := range []string{"missing", "foreign_type", "foreign_run", "foreign_entity", "foreign_path", "null_fields", "array_fields", "zero_creation_clock", "zero_update_clock", "reversed_data_clocks"} {
				t.Run(hostile+"_unchanged", func(t *testing.T) {
					before := read()
					original := record
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					query := `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`
					args := []any{runID, entityID}
					column := ""
					var value any
					switch hostile {
					case "foreign_type":
						column, value = "entity_type", "foreign"
					case "foreign_run":
						column, value = "run_id", foreignRunID
					case "foreign_entity":
						column, value = "entity_id", uuid.NewString()
					case "foreign_path":
						column, value = "flow_instance", "foreign/path"
					case "null_fields":
						column, value = "fields", "null"
					case "array_fields":
						column, value = "fields", "[]"
					case "zero_creation_clock":
						column, value = "created_at", time.Time{}
					case "zero_update_clock":
						column, value = "updated_at", time.Time{}
					case "reversed_data_clocks":
						column, value = "updated_at", at.Add(-time.Second)
					}
					if column != "" {
						query = `UPDATE entity_state SET ` + column + `=$3 WHERE run_id=$1 AND entity_id=$2`
						args = append(args, value)
					}
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						_ = tx.Rollback()
						t.Fatal(err)
					}
					record.ExpectedRevision, record.ExpectedState = before.Lifecycle.State.Revision, record.CurrentState
					record.UpdatedAt = record.UpdatedAt.Add(time.Minute)
					_, mutationErr := commitWorkflowHeaderAndFields(ctx, tx, postgres, record, workflowEngineStateDecision{})
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					if mutationErr == nil {
						t.Fatal("unchanged data bypassed required-companion admission")
					}
					if !reflect.DeepEqual(before, read()) {
						t.Fatalf("unchanged-data refusal leaked header or data effects: %v", mutationErr)
					}
					record = original
				})
			}
		})
	}
}

func q10FreshDatabase(t *testing.T, postgres bool) *sql.DB {
	t.Helper()
	path := contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot())
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(source.Document(path).Root())
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	var db *sql.DB
	var owner schemastore.SchemaBootstrapper
	if postgres {
		_, db, _ = testutil.StartEmptyPostgres(t)
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err = schemastore.NewPostgres(backend)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		file := filepath.Join(t.TempDir(), "q10.db")
		db, err = sql.Open("sqlite", file)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err = schemastore.NewSQLiteWithBackend(backend, file)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.BootstrapSchema(context.Background(), schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "q10-proof", PlatformVersion: "q10-proof", CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	return db
}
