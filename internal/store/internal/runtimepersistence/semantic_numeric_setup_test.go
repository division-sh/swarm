package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestDirectScenarioSetupSemanticNumericParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var db *sql.DB
			var setup interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
			}
			if backend == "sqlite" {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				requireDefaultSourceArtifactForTest(t, ctx, s)
				setup, db = s, s.backend.ConstructionHandle()
			} else {
				_, db, _ = testutil.StartPostgres(t)
				s := newTestPostgresStore(t, db)
				requireDefaultSourceArtifactForTest(t, ctx, s)
				setup = s
			}
			run, entity := uuid.NewString(), uuid.NewString()
			req := pipeline.ScenarioSetupRequest{RunID: run, CreatedAt: time.Now().UTC(), Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "subject", EntityID: entity, FlowInstance: "operating", EntityType: "product", CurrentState: "waiting"}}}
			for _, carrier := range []any{int(5), float64(5), json.Number("5.0"), json.Number("5e0")} {
				req.Entities[0].Fields = map[string]any{"score": carrier, "nested": []any{carrier, 7.5}}
				if _, err := setup.SetupScenarioEntities(ctx, req); err != nil {
					t.Fatal(err)
				}
				var fields string
				if err := db.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT) FROM entity_state WHERE CAST(run_id AS TEXT)=$1 AND CAST(entity_id AS TEXT)=$2`, run, entity).Scan(&fields); err != nil {
					t.Fatal(err)
				}
				var decoded map[string]any
				if err := canonicaljson.DecodePreservingNumberLexemes([]byte(fields), &decoded); err != nil {
					t.Fatal(err)
				}
				projected, err := workflowexpr.ProjectCELValue(decoded)
				if err != nil {
					t.Fatal(err)
				}
				p := projected.(map[string]any)
				if p["score"] != int64(5) || p["nested"].([]any)[0] != int64(5) || p["nested"].([]any)[1] != float64(7.5) {
					t.Fatalf("setup numeric fields %#v", p)
				}
			}
			req.Entities[0].Fields["score"] = 6
			if _, err := setup.SetupScenarioEntities(ctx, req); err == nil {
				t.Fatal("changed fields accepted")
			}
		})
	}
}
