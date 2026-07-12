package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestValidatePersistedStandingOwnershipIsPredecessorOwnedAcrossBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var store *WorkflowInstanceStore
			if backend == "sqlite" {
				db = newSQLiteWorkflowInstanceStoreTestDB(t)
				store = newSQLiteWorkflowInstanceStoreForTest(t, db)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				store = NewWorkflowInstanceStore(db)
			}
			existing := runtimecorrelation.BundleSourceFact{
				BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), BundleSource: "persisted",
			}.Normalized()
			seedPersistedStandingOwner(t, db, backend == "sqlite", existing)

			if err := store.ValidatePersistedStandingOwnership(context.Background(), []runtimecorrelation.BundleSourceFact{existing}); err != nil {
				t.Fatalf("same admitted source: %v", err)
			}
			candidate := runtimecorrelation.BundleSourceFact{
				BundleHash: "bundle-v1:sha256:" + strings.Repeat("b", 64), BundleSource: "persisted",
			}
			for _, manifestation := range []string{"declaration removed", "flow renamed", "package identity renamed", "standing changed to non-standing"} {
				t.Run(manifestation, func(t *testing.T) {
					err := store.ValidatePersistedStandingOwnership(context.Background(), []runtimecorrelation.BundleSourceFact{candidate})
					if err == nil || !strings.Contains(err.Error(), "outside candidate bundle set") || !strings.Contains(err.Error(), existing.BundleHash) || !strings.Contains(err.Error(), "explicit reset/migration") {
						t.Fatalf("changed-source error = %v", err)
					}
				})
			}
		})
	}
}

func seedPersistedStandingOwner(t *testing.T, db *sql.DB, sqlite bool, fact runtimecorrelation.BundleSourceFact) {
	t.Helper()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	if sqlite {
		if _, err := db.Exec(`INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES (?, 'running', ?, ?)`, runID, fact.BundleHash, fact.BundleSource); err != nil {
			t.Fatalf("seed standing run: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO flow_instances (instance_id, flow_template, mode, config, status) VALUES ('service/a', 'telegram-chat', 'static', '{}', 'active')`); err != nil {
			t.Fatalf("seed standing instance: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state, fields) VALUES (?, ?, 'service/a', 'active', '{"activation":"standing","package_key":"."}')`, runID, entityID); err != nil {
			t.Fatalf("seed standing entity: %v", err)
		}
		return
	}
	if _, err := db.Exec(`INSERT INTO runs (run_id, status, bundle_hash, bundle_source) VALUES ($1::uuid, 'running', $2, $3)`, runID, fact.BundleHash, fact.BundleSource); err != nil {
		t.Fatalf("seed standing run: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO flow_instances (instance_id, flow_template, mode, config, status) VALUES ('service/a', 'telegram-chat', 'static', '{}'::jsonb, 'active')`); err != nil {
		t.Fatalf("seed standing instance: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO entity_state (run_id, entity_id, flow_instance, current_state, fields) VALUES ($1::uuid, $2::uuid, 'service/a', 'active', '{"activation":"standing","package_key":"."}'::jsonb)`, runID, entityID); err != nil {
		t.Fatalf("seed standing entity: %v", err)
	}
}
