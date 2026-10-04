package cliapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestVerifyPossessionPublicFormatsAndPortableBothStores(t *testing.T) {
	for _, backend := range []backendselection.Backend{backendselection.BackendSQLite, backendselection.BackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			configText := "llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n"
			var owner interface {
				store.SchemaBootstrapper
				startupownership.Store
				Close() error
			}
			var db *sql.DB
			if backend == backendselection.BackendSQLite {
				path := filepath.Join(t.TempDir(), "runtime.db")
				selected, handle, err := construction.OpenSQLiteRuntimeWithOwnershipBinding(path)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
				configText += fmt.Sprintf("store: {backend: sqlite, sqlite: {path: %q}}\n", path)
			} else {
				dsn, _, _ := testutil.StartEmptyPostgres(t)
				selected, handle, err := construction.OpenPostgres(dsn)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
				connection, err := testpostgres.ParseConnection(dsn)
				if err != nil {
					t.Fatal(err)
				}
				parameters := connection.Parameters()
				t.Setenv("SWARM_VERIFY_POSSESSION_PASSWORD", parameters.Password)
				configText += fmt.Sprintf("store: {backend: postgres}\ndatabase: {host: %q, port: %d, name: %q, user: %q, password_env: SWARM_VERIFY_POSSESSION_PASSWORD, sslmode: %q}\n",
					parameters.Host, parameters.Port, parameters.Database, parameters.User, parameters.SSLMode)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := owner.BootstrapSchema(ctx, schema); err != nil {
				t.Fatal(err)
			}
			capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
				OwnerID: "public-possession-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := capability.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			var before int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_startup_authority_facts").Scan(&before); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, configText))
			for _, mode := range []string{"json", "text", "quiet"} {
				args := []string{"verify", root, "--config", configPath}
				if mode != "text" {
					args = append(args, "--"+mode)
				}
				var out, diagnostic bytes.Buffer
				code := executeRootCommand(ctx, root, args, &out, &diagnostic)
				if code != cliExitConflict {
					t.Fatalf("%s did not preserve occupied-coordinate exit: %d stdout=%s stderr=%s", mode, code, &out, &diagnostic)
				}
				if mode != "json" {
					if !strings.Contains(out.String(), "failed: startup_process_possession") || strings.TrimSpace(out.String()) == "ok" {
						t.Fatalf("%s hid the possession refusal: %s", mode, &out)
					}
					continue
				}
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, observation := range result.Observations {
					if observation.CheckID == "startup_process_possession" {
						found = observation.Status == bootverify.AdmissionFailed && observation.FailureClass == failures.ClassLifecycleConflict
					}
				}
				if !found || result.OK || result.AdmissionComplete {
					t.Fatalf("public JSON erased a held owner: %+v", result)
				}
			}
			var out, diagnostic bytes.Buffer
			if code := executeRootCommand(ctx, root, []string{"verify", root, "--config", configPath, "--portable", "--json"}, &out, &diagnostic); code != 0 {
				t.Fatalf("portable validation observed deployment possession: %d %s %s", code, &out, &diagnostic)
			}
			var portable verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &portable); err != nil {
				t.Fatal(err)
			}
			for _, observation := range portable.Observations {
				if observation.CheckID == "startup_process_possession" && observation.Status != bootverify.AdmissionNotRun {
					t.Fatalf("portable mode probed occupied deployment state: %+v", observation)
				}
			}
			var after int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_startup_authority_facts").Scan(&after); err != nil || before != after {
				t.Fatalf("public observation changed/released the owner: %d -> %d %v", before, after, err)
			}
			if _, err := capability.Evidence(); err != nil {
				t.Fatalf("verification released the actual owner's capability: %v", err)
			}
		})
	}
}
