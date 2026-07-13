package runtime_test

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	storepkg "github.com/division-sh/swarm/internal/store"
)

type boundedProviderCredentialStore struct{}

func (boundedProviderCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	return key, key != "", nil
}
func (boundedProviderCredentialStore) Set(context.Context, string, string) error { return nil }
func (boundedProviderCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (boundedProviderCredentialStore) Delete(context.Context, string) error      { return nil }

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	root := filepath.Join("..", "..", "packs", "provider-triggers")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	registry, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", dirs, nil)
	if err != nil {
		t.Fatalf("load provider trigger registry: %v", err)
	}
	return registry
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *runtimepkg.RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...runtimepkg.InboundPersistence) *runtimepkg.InboundGateway {
	t.Helper()
	if bus != nil {
		bus.SetProviderOutputAuthorizationVerifier(testProviderTriggerCatalog(t))
	}
	gateway := runtimepkg.NewInboundGateway(bus, logger, shutdownAdmissionClosed, stores...)
	gateway.SetCredentialStore(boundedProviderCredentialStore{})
	return gateway
}

// handleBoundedProviderDelivery exercises provider parsing through the real
// standing-service inbound publication operation.
func handleBoundedProviderDelivery(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, store runtimepkg.InboundPersistence, w http.ResponseWriter, r *http.Request, runID, entityID, provider, signingSecret string) {
	t.Helper()
	_ = bus
	plan, err := testProviderTriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: entityID, Provider: provider, SigningSecret: signingSecret,
	})
	if err != nil {
		t.Fatalf("compile provider admission: %v", err)
	}
	target := ensureBoundedStandingTarget(t, r.Context(), store, runID, entityID, provider)
	target.Provider = provider
	target.SigningSecret = signingSecret
	target.AdmissionPlan = plan
	gateway.HandleResolvedWebhook(w, r, target, nil)
}

func ensureBoundedStandingTarget(t *testing.T, ctx context.Context, persistence runtimepkg.InboundPersistence, runID, entityID, provider string) runtimepkg.InboundTarget {
	t.Helper()
	packageKey := "test.provider." + strings.ToLower(strings.TrimSpace(provider))
	flowID := "bounded-inbound"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	const bundleHash = "bundle-v1:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const bundleSource = "ephemeral"
	var flowInstance string

	switch selected := persistence.(type) {
	case *storepkg.PostgresStore:
		if err := selected.DB.QueryRowContext(ctx, `SELECT flow_instance FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runID, entityID).Scan(&flowInstance); err != nil {
			t.Fatalf("load postgres bounded provider flow instance: %v", err)
		}
		insertPostgresStandingFixture(t, ctx, selected.DB, serviceID, packageKey, flowID, flowInstance, entityID, runID, bundleHash, bundleSource)
	case *storepkg.SQLiteRuntimeStore:
		if err := selected.DB.QueryRowContext(ctx, `SELECT flow_instance FROM entity_state WHERE run_id = ? AND entity_id = ?`, runID, entityID).Scan(&flowInstance); err != nil {
			t.Fatalf("load sqlite bounded provider flow instance: %v", err)
		}
		insertSQLiteStandingFixture(t, ctx, selected.DB, serviceID, packageKey, flowID, flowInstance, entityID, runID, bundleHash, bundleSource)
	default:
		t.Fatalf("unsupported bounded provider persistence %T", persistence)
	}

	return runtimepkg.InboundTarget{
		BundleHash: bundleHash, ServiceID: serviceID, PackageKey: packageKey,
		FlowID: flowID, RunID: runID, Generation: 1, PublicationSequence: 1,
		InstanceID: flowInstance, FlowInstance: flowInstance, EntityID: entityID,
		EntitySlug: entityID, Alias: entityID,
	}
}

func insertPostgresStandingFixture(t *testing.T, ctx context.Context, db *sql.DB, serviceID, packageKey, flowID, instanceID, entityID, runID, bundleHash, bundleSource string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_services (
			service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
			operator_override, effective_state, current_bundle_hash, current_bundle_source,
			revision_sequence, current_generation, current_run_id, publication_state,
			publication_sequence, created_at, updated_at
		) VALUES ($1::uuid, $2, $3, $4, $5::uuid, TRUE, 'none', 'active', $6, $7, 1, 1, $8::uuid, 'published', 1, now(), now())
		ON CONFLICT (service_id) DO NOTHING
	`, serviceID, packageKey, flowID, instanceID, entityID, bundleHash, bundleSource, runID); err != nil {
		t.Fatalf("seed postgres standing service: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at)
		VALUES ($1::uuid, 1, $2::uuid, $3, $4, now())
		ON CONFLICT (service_id, generation) DO NOTHING
	`, serviceID, runID, bundleHash, bundleSource); err != nil {
		t.Fatalf("seed postgres standing generation: %v", err)
	}
}

func insertSQLiteStandingFixture(t *testing.T, ctx context.Context, db *sql.DB, serviceID, packageKey, flowID, instanceID, entityID, runID, bundleHash, bundleSource string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_services (
			service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
			operator_override, effective_state, current_bundle_hash, current_bundle_source,
			revision_sequence, current_generation, current_run_id, publication_state,
			publication_sequence, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, TRUE, 'none', 'active', ?, ?, 1, 1, ?, 'published', 1, ?, ?)
		ON CONFLICT(service_id) DO NOTHING
	`, serviceID, packageKey, flowID, instanceID, entityID, bundleHash, bundleSource, runID, now, now); err != nil {
		t.Fatalf("seed sqlite standing service: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at)
		VALUES (?, 1, ?, ?, ?, ?)
		ON CONFLICT(service_id, generation) DO NOTHING
	`, serviceID, runID, bundleHash, bundleSource, now); err != nil {
		t.Fatalf("seed sqlite standing generation: %v", err)
	}
}
