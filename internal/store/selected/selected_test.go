package selected

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type closeProbe struct {
	closed int
	err    error
}

func (p *closeProbe) Close() error {
	p.closed++
	return p.err
}

type pingProbe struct{ err error }

func (p pingProbe) Ping(context.Context) error { return p.err }

func TestValidateOpenedRuntimeClosesExactResourceAfterPingFailure(t *testing.T) {
	pingErr := errors.New("ping failed")
	closeErr := errors.New("close failed")
	resource := &closeProbe{err: closeErr}
	owner := &Owner{
		lifetime: lifecycle{resource: resource, state: ownershipUnactivated},
		required: requiredPorts{pinger: pingProbe{err: pingErr}},
	}
	_, err := validateOpenedRuntime(context.Background(), owner)
	if !errors.Is(err, pingErr) || !errors.Is(err, closeErr) {
		t.Fatalf("validateOpenedRuntime error = %v, want joined ping and close failures", err)
	}
	if resource.closed != 1 {
		t.Fatalf("close count = %d, want 1", resource.closed)
	}
}

func TestPostgresFailedPingClosesTheConstructedPool(t *testing.T) {
	dsn, _, _ := testutil.StartEmptyPostgres(t)
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL sandbox DSN: %v", err)
	}
	connection, err = connection.WithDatabase("swarm_missing_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	if err != nil {
		t.Fatalf("select missing PostgreSQL database: %v", err)
	}
	missingDSN, err := connection.String()
	if err != nil {
		t.Fatalf("serialize missing PostgreSQL database DSN: %v", err)
	}

	owner, err := openPostgresRuntime(missingDSN, 0)
	if err != nil {
		t.Fatalf("construct PostgreSQL owner before Ping: %v", err)
	}
	pinger, pingerOK := owner.required.pinger.(*private.PostgresStore)
	resource, resourceOK := owner.lifetime.resource.(*private.PostgresStore)
	if !pingerOK || !resourceOK || pinger != resource {
		t.Fatalf("Ping and close resources differ: pinger=%T resource=%T", owner.required.pinger, owner.lifetime.resource)
	}
	if _, err := validateOpenedRuntime(context.Background(), owner); err == nil {
		t.Fatal("missing PostgreSQL database unexpectedly passed Ping")
	}
	if err := pinger.Ping(context.Background()); err == nil || err.Error() != "sql: database is closed" {
		t.Fatalf("Ping after failed construction = %v, want closed database", err)
	}
}

func TestOwnerUnactivatedCloseIsDirectAndIdempotent(t *testing.T) {
	resource := &closeProbe{}
	owner := &Owner{lifetime: lifecycle{resource: resource, state: ownershipUnactivated}}
	if err := owner.CloseUnactivated(); err != nil {
		t.Fatalf("CloseUnactivated: %v", err)
	}
	if err := owner.CloseUnactivated(); err != nil {
		t.Fatalf("duplicate CloseUnactivated: %v", err)
	}
	if resource.closed != 1 {
		t.Fatalf("close count = %d, want 1", resource.closed)
	}
}

func TestOwnerUnactivatedCloseFailureRetainsCloseAuthority(t *testing.T) {
	resource := &closeProbe{err: errors.New("close journal")}
	owner := &Owner{lifetime: lifecycle{resource: resource, state: ownershipUnactivated}}
	if err := owner.CloseUnactivated(); err == nil || !strings.Contains(err.Error(), "close journal") {
		t.Fatalf("CloseUnactivated error = %v, want close failure", err)
	}
	resource.err = nil
	if err := owner.CloseUnactivated(); err != nil {
		t.Fatalf("retry CloseUnactivated: %v", err)
	}
	if resource.closed != 2 {
		t.Fatalf("close attempts = %d, want 2", resource.closed)
	}
}

func TestOwnerActivatedCloseRequiresExactProcessReceipt(t *testing.T) {
	resource := &closeProbe{}
	owner := &Owner{lifetime: lifecycle{resource: resource, state: ownershipUnactivated}}
	process := worklifetime.NewProcess()
	other := worklifetime.NewProcess()
	if err := owner.Activate(process); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := owner.CloseUnactivated(); err == nil || !strings.Contains(err.Error(), "join receipt") {
		t.Fatalf("CloseUnactivated error = %v, want receipt requirement", err)
	}
	other.Retire()
	wrong, err := other.Join(context.Background())
	if err != nil {
		t.Fatalf("join other process: %v", err)
	}
	if err := owner.CloseActivated(wrong); err == nil {
		t.Fatal("CloseActivated accepted another process receipt")
	}
	if resource.closed != 0 {
		t.Fatalf("close count after wrong receipt = %d, want 0", resource.closed)
	}
	process.Retire()
	receipt, err := process.Join(context.Background())
	if err != nil {
		t.Fatalf("join process: %v", err)
	}
	if err := owner.CloseActivated(receipt); err != nil {
		t.Fatalf("CloseActivated: %v", err)
	}
	if resource.closed != 1 {
		t.Fatalf("close count = %d, want 1", resource.closed)
	}
}

func TestOpenRuntimeSQLiteResolvesRequiredAndOptionalProductsOnce(t *testing.T) {
	owner, err := OpenRuntime(context.Background(), RuntimeRequest{
		Selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite")},
	})
	if err != nil {
		t.Fatalf("OpenRuntime: %v", err)
	}
	t.Cleanup(func() { _ = owner.CloseUnactivated() })
	if owner.Schema() == nil || owner.OperatorChannels() == nil || owner.TestSetup() == nil || owner.RunBundleAvailability() == nil || owner.ServeBundleIngestWriter() == nil {
		t.Fatal("SQLite required selected-store projections are incomplete")
	}
	if _, available := owner.BundleCatalog(); !available {
		t.Fatal("SQLite bundle catalog read must be available")
	}
	if _, available := owner.ConversationFork(); !available {
		t.Fatal("SQLite conversation fork must be available")
	}
	if _, available := owner.BundleRegisterWriter(); !available {
		t.Fatal("SQLite public bundle-register writer must be available")
	}
	if _, available := owner.RunFork(); !available {
		t.Fatal("SQLite run fork must be available")
	}
	for name, available := range map[string]bool{
		"bundle delete":     func() bool { _, ok := owner.BundleDelete(); return ok }(),
		"destructive reset": func() bool { _, ok := owner.DestructiveReset(); return ok }(),
		"startup recovery":  func() bool { _, ok := owner.StartupRecovery(); return ok }(),
	} {
		if available {
			t.Fatalf("SQLite %s unexpectedly available", name)
		}
	}
}

func TestOpenRuntimePostgresResolvesRequiredAndOptionalBundleWritersOnce(t *testing.T) {
	dsn, _, _ := testutil.StartEmptyPostgres(t)
	owner, err := OpenRuntime(context.Background(), RuntimeRequest{
		Selection:   storebackend.Selection{Backend: storebackend.BackendPostgres},
		PostgresDSN: dsn,
	})
	if err != nil {
		t.Fatalf("OpenRuntime: %v", err)
	}
	t.Cleanup(func() { _ = owner.CloseUnactivated() })
	if owner.ServeBundleIngestWriter() == nil || owner.RunBundleAvailability() == nil {
		t.Fatal("PostgreSQL required selected-store projections are incomplete")
	}
	if _, available := owner.BundleRegisterWriter(); !available {
		t.Fatal("PostgreSQL public bundle-register writer must be available")
	}
}
