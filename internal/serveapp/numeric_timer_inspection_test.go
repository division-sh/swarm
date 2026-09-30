package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// This read-only child is supplementary persistence proof, not a public timer
// surface. Its parent invokes it only after the retained serve child has joined.
func TestOwnedNumericTimerInspection(t *testing.T) {
	raw := os.Getenv("SWARM_NUMERIC_TIMER_INSPECTION")
	if raw == "" {
		t.Skip("supplementary inspection child; invoked by the numeric lifecycle proof")
	}
	var request struct {
		ConfigPath, RunID, Flow, After, Output string
		Entities                               map[string]string
	}
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	if request.RunID == "" || request.Output == "" || len(request.Entities) != 100 {
		t.Fatal("exact numeric inspection request required")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: request.ConfigPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var db *sql.DB
	var reader pipeline.WorkflowTimerActivationPersistence
	if loaded.Config.Store.Backend == "sqlite" {
		db, err = sql.Open("sqlite", loaded.Config.Store.SQLite.Path)
		if err != nil {
			t.Fatal(err)
		}
		reader = storetest.AdmitSQLiteRuntimeStore(t, db)
	} else {
		dsn, err := postgresDSNFromConfig(ctx, loaded.Config.Database)
		if err != nil {
			t.Fatal(err)
		}
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		reader = storetest.AdmitPostgresRuntimeStore(t, db)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	activations, err := reader.ListWorkflowTimerActivations(ctx, request.RunID, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(activations) != len(request.Entities) {
		t.Fatalf("typed timer inventory=%d, want %d", len(activations), len(request.Entities))
	}
	after, err := time.ParseDuration(request.After)
	if err != nil {
		t.Fatal(err)
	}
	byEntity := map[string]pipeline.WorkflowTimerActivation{}
	for _, activation := range activations {
		if err := activation.Validate(); err != nil {
			t.Fatal(err)
		}
		if activation.RunID != request.RunID || activation.Status != "active" || activation.Recurring || activation.Ref.Cause != timeridentity.WorkflowTimerActivationCauseInitial ||
			activation.FireAt.Sub(activation.CreatedAt) != after || activation.Route.ScopeKey != request.Flow || byEntity[activation.EntityID].EntityID != "" || request.Entities[activation.EntityID] != activation.Route.InstancePath {
			t.Fatalf("wrong initial exact numeric timer: %+v", activation)
		}
		byEntity[activation.EntityID] = activation
	}
	body, err := json.Marshal(byEntity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(request.Output, body, 0600); err != nil {
		t.Fatal(err)
	}
}
