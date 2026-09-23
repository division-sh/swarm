package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type scenarioSetupAckSelectedStore interface {
	TestSetupStore
	APIIdempotencyStore
	RunBundleContextStore
	sourceartifactfixture.Writer
}

type scenarioSetupPostCommitFaultStore struct {
	TestSetupStore
	fault error
	calls int
}

func (s *scenarioSetupPostCommitFaultStore) SetupScenarioEntities(ctx context.Context, req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error) {
	s.calls++
	result, err := s.TestSetupStore.SetupScenarioEntities(ctx, req)
	if err != nil {
		return result, err
	}
	return result, s.fault
}

func TestOperatorTestSetupAcknowledgedCleanupErrorCompletesIdempotencyBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (scenarioSetupAckSelectedStore, *sql.DB)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (scenarioSetupAckSelectedStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
				return selected, storetest.DatabaseForTest(selected)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (scenarioSetupAckSelectedStore, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected, db := backend.open(t)
			bundle := testSetupValidationBundle(t)
			source := semanticview.Wrap(bundle)
			fact := sourceartifactfixture.RequireArtifact(t, context.Background(), selected, bundle.SourceArtifact)
			ctx := testAuthorActivityContextForSource(context.Background(), fact)
			fault := errors.New("scenario setup postcommit cleanup failed")
			setup := &scenarioSetupPostCommitFaultStore{TestSetupStore: selected, fault: fault}
			now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
			opts := TestSetupHandlerOptions{
				Setup:            setup,
				Idempotency:      selected,
				RunBundleContext: selected,
				SourceArtifact:   bundleScopedFailingEventPublisher{fact: fact},
				Source:           source,
			}
			runID, entityID := uuid.NewString(), uuid.NewString()
			req := Request{
				Method:       testSetupEntitiesMethod,
				ActorTokenID: "setup-ack-test",
				RequestHash:  "setup-ack-request-hash",
				Params: map[string]any{
					"bundle_hash":     bundle.SourceArtifact.BundleHash(),
					"run_id":          runID,
					"idempotency_key": "setup-ack-key",
					"entities":        []any{validTestSetupEntity(entityID, "waiting", "seeded", true)},
				},
			}

			first, err := executeTestSetupEntities(ctx, req, opts, now)
			if !errors.Is(err, fault) {
				t.Fatalf("first setup error = %v, want postcommit diagnostic", err)
			}
			committed, ok := first.(testSetupEntitiesResult)
			if !ok || committed.RunID != runID || len(committed.Entities) != 1 || committed.Entities[0].EntityID != entityID {
				t.Fatalf("acknowledged setup result = %#v, want committed run/entity", first)
			}
			assertScenarioSetupAckRows(t, db, runID, entityID)
			if setup.calls != 1 {
				t.Fatalf("setup calls after acknowledged fault = %d, want 1", setup.calls)
			}

			replayed, err := executeTestSetupEntities(ctx, req, opts, now)
			if err != nil {
				t.Fatalf("idempotent replay error = %v", err)
			}
			got, ok := replayed.(testSetupEntitiesResult)
			if !ok || got.RunID != committed.RunID || len(got.Entities) != 1 || got.Entities[0] != committed.Entities[0] {
				t.Fatalf("idempotent replay result = %#v, want %#v", replayed, committed)
			}
			if setup.calls != 1 {
				t.Fatalf("setup calls after replay = %d, want no duplicate domain write", setup.calls)
			}
			assertScenarioSetupAckRows(t, db, runID, entityID)
		})
	}
}

func assertScenarioSetupAckRows(t *testing.T, db *sql.DB, runID, entityID string) {
	t.Helper()
	for _, check := range []struct {
		name  string
		query string
		args  []any
		want  int
	}{
		{"run", `SELECT COUNT(*) FROM runs WHERE run_id = $1`, []any{runID}, 1},
		{"entity", `SELECT COUNT(*) FROM entity_state WHERE run_id = $1 AND entity_id = $2`, []any{runID, entityID}, 1},
		{"entity mutations", `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1 AND entity_id = $2 AND writer_id = 'test.setup_entities'`, []any{runID, entityID}, 3},
		{"idempotency completion", `SELECT COUNT(*) FROM api_idempotency WHERE resource_id = $1`, []any{runID}, 1},
	} {
		var got int
		if err := db.QueryRow(check.query, check.args...).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", check.name, err)
		}
		if got != check.want {
			t.Fatalf("%s rows = %d, want %d", check.name, got, check.want)
		}
	}
	var response string
	if err := db.QueryRow(`SELECT response FROM api_idempotency WHERE resource_id = $1`, runID).Scan(&response); err != nil {
		t.Fatalf("load idempotency completion: %v", err)
	}
	var completed testSetupEntitiesResult
	if err := json.Unmarshal([]byte(response), &completed); err != nil {
		t.Fatalf("decode idempotency completion: %v", err)
	}
	if completed.RunID != runID || len(completed.Entities) != 1 || completed.Entities[0].EntityID != entityID {
		t.Fatalf("stored idempotency completion = %#v, want committed run/entity", completed)
	}
}
