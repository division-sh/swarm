package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
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

// API acknowledgement controls intentionally exercise field-only import, not
// supported runtime construction. The public served path uses the selected bus.
type scenarioSetupImportPublisher struct {
	bundleScopedFailingEventPublisher
	TestSetupStore
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
		open func(*testing.T) scenarioSetupAckSelectedStore
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) scenarioSetupAckSelectedStore {
				return storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) scenarioSetupAckSelectedStore {
				return storetest.StartPostgresRuntimeStore(t)
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			bundle := testSetupValidationBundle(t)
			source := semanticview.Wrap(bundle)
			fact := sourceartifactfixture.RequireArtifact(t, context.Background(), selected, bundle.SourceArtifact)
			ctx := testAuthorActivityContextForSource(context.Background(), fact)
			fault := errors.New("scenario setup postcommit cleanup failed")
			setup := &scenarioSetupPostCommitFaultStore{TestSetupStore: selected, fault: fault}
			now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
			opts := TestSetupHandlerOptions{
				Idempotency:      selected,
				RunBundleContext: selected,
				SourceArtifact: scenarioSetupImportPublisher{
					bundleScopedFailingEventPublisher: bundleScopedFailingEventPublisher{fact: fact},
					TestSetupStore:                    setup,
				},
				Source: source,
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
			assertScenarioSetupAckRows(t, selected, runID, entityID)
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
			assertScenarioSetupAckRows(t, selected, runID, entityID)
		})
	}
}

func assertScenarioSetupAckRows(t *testing.T, selected any, runID, entityID string) {
	t.Helper()
	observed, err := storetest.ReadScenarioSetupAckStorage(context.Background(), selected, runID, entityID)
	if err != nil {
		t.Fatalf("read exact scenario setup acknowledgment: %v", err)
	}
	for _, check := range []struct {
		name string
		got  int
		want int
	}{
		{"run", observed.Runs, 1},
		{"entity", observed.Entities, 1},
		{"entity mutations", observed.SetupMutations, 3},
		{"idempotency completion", observed.Completions, 1},
	} {
		if check.got != check.want {
			t.Fatalf("%s rows = %d, want %d", check.name, check.got, check.want)
		}
	}
	var completed testSetupEntitiesResult
	if err := json.Unmarshal(observed.Response, &completed); err != nil {
		t.Fatalf("decode idempotency completion: %v", err)
	}
	if completed.RunID != runID || len(completed.Entities) != 1 || completed.Entities[0].EntityID != entityID {
		t.Fatalf("stored idempotency completion = %#v, want committed run/entity", completed)
	}
}
