package storetest

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/platform"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// StartSQLiteRuntimeStore creates a file-backed SQLite runtime store with the
// canonical platform schema for backend-neutral tests.
func StartSQLiteRuntimeStore(t testing.TB) *store.SQLiteRuntimeStore {
	t.Helper()
	return StartSQLiteRuntimeStoreWithContext(t, context.Background())
}

// StartSQLiteRuntimeStorePair returns independently constructed store handles
// over one canonical file-backed database. It is used to prove behavior that
// must survive process-local store reconstruction.
func StartSQLiteRuntimeStorePair(t testing.TB) (*store.SQLiteRuntimeStore, *store.SQLiteRuntimeStore) {
	t.Helper()
	primary, reopen := StartSQLiteRuntimeStoreWithReopen(t, context.Background())
	reconstructed := reopen()
	if _, err := os.Stat(primary.Path()); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", primary.Path(), err)
	}
	return primary, reconstructed
}

func StartSQLiteRuntimeStoreWithContext(t testing.TB, ctx context.Context) *store.SQLiteRuntimeStore {
	t.Helper()
	sqliteStore, _ := StartSQLiteRuntimeStoreWithReopen(t, ctx)
	if _, err := os.Stat(sqliteStore.Path()); err != nil {
		t.Fatalf("sqlite runtime store did not create file-backed db at %s: %v", sqliteStore.Path(), err)
	}
	return sqliteStore
}

// AdmitPostgresRuntimeStore runs the production bootstrap against a canonical
// PostgreSQL test database and returns the admitted store object.
func AdmitPostgresRuntimeStore(t testing.TB, db *sql.DB) *store.PostgresStore {
	t.Helper()
	postgresStore := NewPostgresStoreForTest(db)
	BootstrapPostgresRuntimeStore(t, postgresStore)
	bindTestPayloadAdmitter(postgresStore)
	return postgresStore
}

// AdmitSQLiteRuntimeStore runs the production bootstrap against an existing
// SQLite test database and returns the admitted store object.
func AdmitSQLiteRuntimeStore(t testing.TB, db *sql.DB) *store.SQLiteRuntimeStore {
	t.Helper()
	sqliteStore := NewSQLiteRuntimeStoreForTest(db)
	platformSpec, plans := canonicalPlatformPlans(t)
	if err := sqliteStore.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    "storetest",
			PlatformVersion: platformSpec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	bindTestPayloadAdmitter(sqliteStore)
	return sqliteStore
}

func bindTestPayloadAdmitter(selected interface {
	SetEventPayloadAdmitter(runtimebus.PayloadAdmitter)
}) {
	selected.SetEventPayloadAdmitter(func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
	})
}

// Database exposes the exact test-owned SQL handle for hostile fixture setup
// and persistence readback. Runtime code must use typed selected-store ports.
func Database(selected any) *sql.DB {
	return private.DatabaseForTest(selected)
}

func DatabaseForTest(selected any) *sql.DB {
	return Database(selected)
}

func SetResetFinalReceiptFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetResetFinalReceiptFaultForTest(ctx, selected, enabled)
}

func SetResetPlanReceiptFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetResetPlanReceiptFaultForTest(ctx, selected, enabled)
}

func commitPersistedEventDeliveryFixture(ctx context.Context, selected any, eventID, runID string, routes []events.DeliveryRoute) error {
	return private.CommitPersistedEventDeliveryFixtureForTest(ctx, selected, eventID, runID, routes)
}

func NewPostgresStoreForTest(db *sql.DB) *store.PostgresStore {
	return private.NewPostgresStoreForTest(db)
}

func NewSQLiteRuntimeStoreForTest(db *sql.DB) *store.SQLiteRuntimeStore {
	return private.NewSQLiteRuntimeStoreForTest(db)
}

// BootstrapPostgresRuntimeStore admits an existing PostgreSQL store through
// the same production bootstrap used at serve startup.
func BootstrapPostgresRuntimeStore(t testing.TB, postgresStore *store.PostgresStore) {
	t.Helper()
	platformSpec, plans := canonicalPlatformPlans(t)
	if err := postgresStore.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    "storetest",
			PlatformVersion: platformSpec.Platform.Version,
			CreatedAt:       time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
}

func canonicalPlatformPlans(t testing.TB) (runtimecontracts.PlatformSpecDocument, []store.SchemaTableDDL) {
	t.Helper()
	var platformSpec runtimecontracts.PlatformSpecDocument
	source, err := yamlsource.Load(platform.PlatformSpecYAML())
	if err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	platformSpec, err = runtimecontracts.AdmitPlatformSpecValue(source.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	return platformSpec, plans
}
