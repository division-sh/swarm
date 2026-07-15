package serveapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestSelectedStoreBundleRoleLedgerCoversStoreBundleFields(t *testing.T) {
	typ := reflect.TypeOf(storeBundle{})
	fields := make(map[string]struct{}, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		fields[typ.Field(i).Name] = struct{}{}
	}

	seen := map[string]struct{}{}
	for _, entry := range selectedStoreBundleRoleLedger() {
		if strings.TrimSpace(entry.Name) == "" {
			t.Fatal("selected store role ledger contains empty field name")
		}
		if _, ok := fields[entry.Name]; !ok {
			t.Fatalf("selected store role ledger names unknown storeBundle field %s", entry.Name)
		}
		if _, ok := seen[entry.Name]; ok {
			t.Fatalf("selected store role ledger classifies %s more than once", entry.Name)
		}
		seen[entry.Name] = struct{}{}
		if entry.Classification == "" {
			t.Fatalf("selected store role ledger field %s missing classification", entry.Name)
		}
		if strings.TrimSpace(entry.Reason) == "" {
			t.Fatalf("selected store role ledger field %s missing reason", entry.Name)
		}
		if strings.TrimSpace(entry.SpecRef) == "" && entry.Issue == 0 {
			t.Fatalf("selected store role ledger field %s missing live spec or issue ref", entry.Name)
		}
		if entry.RequiredOn == 0 && entry.ForbiddenOn == 0 {
			t.Fatalf("selected store role ledger field %s must declare required or forbidden backend coverage", entry.Name)
		}
	}

	var missing []string
	for name := range fields {
		if _, ok := seen[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("selected store role ledger missing storeBundle fields: %s", strings.Join(missing, ", "))
	}
}

func TestValidateSelectedStoreBundleRolesAcceptsSQLiteBuildStores(t *testing.T) {
	stores := buildSQLiteSelectedStoreBundleForRoleTest(t)
	if err := validateSelectedStoreBundleRoles(storebackend.BackendSQLite, stores); err != nil {
		t.Fatalf("validate selected sqlite store roles: %v", err)
	}
	if stores.RuntimeSQLDB != nil {
		t.Fatalf("sqlite RuntimeSQLDB = %#v, want nil raw runtime SQL handle", stores.RuntimeSQLDB)
	}
}

func TestValidateSelectedStoreBundleRolesAcceptsPostgresSelectedBundle(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open inert sql handle for selected postgres role projection: %v", err)
	}
	t.Cleanup(func() { closeDB(db) })

	stores := selectedPostgresStoreBundle(&store.PostgresStore{DB: db}, &config.Config{})
	if err := validateSelectedStoreBundleRoles(storebackend.BackendPostgres, stores); err != nil {
		t.Fatalf("validate selected postgres store roles: %v", err)
	}
}

func TestValidateSelectedStoreBundleRolesFailsClosedForMissingRequiredCoreRole(t *testing.T) {
	stores := buildSQLiteSelectedStoreBundleForRoleTest(t)
	stores.RuntimeLogStore = nil

	err := validateSelectedStoreBundleRoles(storebackend.BackendSQLite, stores)
	if err == nil {
		t.Fatal("expected selected role validation to fail for missing RuntimeLogStore")
	}
	if !strings.Contains(err.Error(), "RuntimeLogStore") {
		t.Fatalf("selected role validation error = %v, want RuntimeLogStore named", err)
	}
}

func TestValidateSelectedStoreBundleRolesFailsClosedForSQLiteRawRuntimeSQL(t *testing.T) {
	stores := buildSQLiteSelectedStoreBundleForRoleTest(t)
	stores.RuntimeSQLDB = stores.SQLDB

	err := validateSelectedStoreBundleRoles(storebackend.BackendSQLite, stores)
	if err == nil {
		t.Fatal("expected selected role validation to reject SQLite raw runtime SQL handle")
	}
	if !strings.Contains(err.Error(), "RuntimeSQLDB") {
		t.Fatalf("selected role validation error = %v, want RuntimeSQLDB named", err)
	}
}

func buildSQLiteSelectedStoreBundleForRoleTest(t *testing.T) storeBundle {
	t.Helper()
	stores, err := buildStores(context.Background(), storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceFlag,
		SQLitePath:       filepath.Join(t.TempDir(), "dev.db"),
		SQLitePathSource: storebackend.SourceRolloutDefault,
	}, &config.Config{})
	if err != nil {
		t.Fatalf("buildStores(sqlite): %v", err)
	}
	t.Cleanup(func() { closeDB(stores.SQLDB) })
	return stores
}
