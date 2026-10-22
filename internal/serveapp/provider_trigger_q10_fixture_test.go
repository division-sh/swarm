package serveapp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Exercises the opt-in provider fixtures without a CLI, live credentials or
// external requests. This is native SQLite setup proof, not a webhook journey.
func TestQ10ProviderTriggerFixtureKeepsHeaderOwnedLifecycle(t *testing.T) {
	for _, provider := range []string{"shopify", "typeform"} {
		t.Run(provider, func(t *testing.T) {
			run, entity := uuid.NewString(), uuid.NewString()
			ctx := correlation.WithRunID(context.Background(), run)
			selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			seedProviderTriggerSmokeRuntime(t, ctx, selected, run, entity, "provider-fixture", "customer-a", provider, "fixture-only-secret", "fixture-subscriber")
			evidence, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, selected, run, entity)
			if err != nil {
				t.Fatal(err)
			}
			if evidence.EntityType != "default" || evidence.CurrentState == nil || *evidence.CurrentState != "active" || evidence.Revision == nil || *evidence.Revision != 1 || len(evidence.Mutations) != 0 {
				t.Fatalf("provider fixture changed header/mutation ownership: %+v", evidence)
			}
			for _, raw := range []json.RawMessage{evidence.Fields, evidence.Gates, evidence.Bookkeeping, evidence.Accumulator} {
				var object map[string]any
				if err := json.Unmarshal(raw, &object); err != nil || object == nil || len(object) != 0 {
					t.Fatalf("provider fixture changed a persisted empty object: %s err=%v", raw, err)
				}
			}
		})
	}
}
