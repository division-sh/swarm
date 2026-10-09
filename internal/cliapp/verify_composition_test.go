package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type verifyCompositionBootStore interface {
	store.SchemaBootstrapper
	storetest.RunFixtureStore
	startupownership.Store
	InspectSchema(context.Context, store.SchemaBootstrapRequest) (store.SchemaInspection, error)
	Close() error
}

func newVerifyCompositionBootStore(t *testing.T, backend, root string, schema store.SchemaBootstrapRequest) (verifyCompositionBootStore, string) {
	t.Helper()
	var owner verifyCompositionBootStore
	var configText string
	if backend == "sqlite" {
		path := filepath.Join(t.TempDir(), "runtime.db")
		selected, _, err := construction.OpenSQLiteRuntimeWithOwnershipBinding(path)
		if err != nil {
			t.Fatal(err)
		}
		owner = selected
		configText = fmt.Sprintf("store: {backend: sqlite, sqlite: {path: %q}}\n", path)
	} else {
		dsn, _, _ := testutil.StartEmptyPostgres(t)
		selected, _, err := construction.OpenPostgres(dsn)
		if err != nil {
			t.Fatal(err)
		}
		owner = selected
		connection, err := testpostgres.ParseConnection(dsn)
		if err != nil {
			t.Fatal(err)
		}
		p := connection.Parameters()
		t.Setenv("SWARM_VERIFY_COMPOSITION_PASSWORD", p.Password)
		configText = fmt.Sprintf("store: {backend: postgres}\ndatabase: {host: %q, port: %d, name: %q, user: %q, password_env: SWARM_VERIFY_COMPOSITION_PASSWORD, sslmode: %q}\n", p.Host, p.Port, p.Database, p.User, p.SSLMode)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	if err := owner.BootstrapSchema(ctx, schema); err != nil {
		t.Fatal(err)
	}
	capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "composition-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	return owner, configText
}

func TestVerifyPublicListenerSetMatchesBootAndReleasesBindings(t *testing.T) {
	for _, mode := range []string{"same_fixed", "wildcard_specific", "specific_wildcard", "distinct_fixed", "ephemeral"} {
		t.Run(mode, func(t *testing.T) {
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
			_, configText := newVerifyCompositionBootStore(t, "sqlite", root, schema)
			freeAddress := func() string {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				addr := listener.Addr().String()
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				return addr
			}
			api, mcp := freeAddress(), ""
			mcp = api
			_, port, err := net.SplitHostPort(api)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "wildcard_specific":
				api = "0.0.0.0:" + port
			case "specific_wildcard":
				mcp = "0.0.0.0:" + port
			case "distinct_fixed":
				mcp = freeAddress()
			case "ephemeral":
				api, mcp = "127.0.0.1:0", "127.0.0.1:0"
			}
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, configText+fmt.Sprintf("llm: {backend: anthropic}\nserve: {api_listen_addr: %q, mcp_listen_addr: %q, api_token_file: %q}\n", api, mcp, writeCLIAPITokenFile(t, "composition-token")))
			var out, diagnostic bytes.Buffer
			code := executeRootCommand(context.Background(), root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic)
			var result verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatalf("public result: %v output=%s stderr=%s", err, &out, &diagnostic)
			}
			bootAPI, err := ListenServeHTTPListener("api", api)
			if err != nil {
				t.Fatalf("verify leaked its API listener: %v", err)
			}
			bootMCP, bootErr := ListenServeHTTPListener("mcp", mcp)
			if bootMCP != nil {
				if err := bootMCP.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := bootAPI.Close(); err != nil {
				t.Fatal(err)
			}
			conflict := mode == "same_fixed" || mode == "wildcard_specific" || mode == "specific_wildcard"
			if (bootErr != nil) != conflict || (code != 0) != conflict || result.OK == conflict || result.AdmissionComplete == conflict {
				t.Fatalf("verify/boot composition mismatch: mode=%s boot=%v code=%d result=%+v stderr=%s", mode, bootErr, code, result, &diagnostic)
			}
			for _, addr := range []string{api, mcp} {
				listener, err := ListenServeHTTPListener("after-cleanup", addr)
				if err != nil {
					t.Fatalf("composition retained %s: %v", addr, err)
				}
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVerifyPublicExistingStoreReportsGeneratedPreparationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			path := filepath.Join(root, "nodes.yaml")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			edited := strings.Replace(string(body), "item-handler:\n", "item-handler:\n  state_table: composition_item_state\n  state_schema:\n    fields:\n      item_id: string\n", 1)
			if edited == string(body) {
				t.Fatal("generated-state fixture mutation missed its node")
			}
			if err := os.WriteFile(path, []byte(edited), 0600); err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil || len(plans.State) != 1 {
				t.Fatalf("generated plans=%+v err=%v", plans.State, err)
			}
			platformOnly, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform, nil)
			if err != nil {
				t.Fatal(err)
			}
			complete, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
			if err != nil {
				t.Fatal(err)
			}
			owner, configText := newVerifyCompositionBootStore(t, backend, root, platformOnly)
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, configText+"llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n")
			ctx := context.Background()
			for _, prepared := range []bool{false, true} {
				before, err := owner.InspectSchema(ctx, complete)
				if err != nil || before.Fresh || (len(before.MissingStateTables) == 0) != prepared {
					t.Fatalf("boot inspection: %+v %v", before, err)
				}
				var out, diagnostic bytes.Buffer
				code := executeRootCommand(ctx, root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic)
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil || code != 0 || !result.OK || !result.AdmissionComplete {
					t.Fatalf("preparable state refused: code=%d result=%+v err=%v stderr=%s", code, result, err, &diagnostic)
				}
				found := false
				for _, obligation := range result.ExecutionObligations {
					if obligation.ID == "store_schema_preparation" {
						found = true
						if obligation.Owner != "internal/store.SchemaBootstrapper.BootstrapSchema" || !strings.Contains(obligation.Reason, "composition_item_state") {
							t.Fatalf("missing exact boot preparation: %+v", obligation)
						}
					}
				}
				if found == prepared {
					t.Fatalf("preparation obligation=%v, boot prepared=%v", found, prepared)
				}
				after, err := owner.InspectSchema(ctx, complete)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("verify changed schema: before=%+v after=%+v err=%v", before, after, err)
				}
				for _, observation := range result.Observations {
					if observation.CheckID == "selected_store_schema" && observation.Status != bootverify.AdmissionPassed {
						t.Fatalf("preparation mistaken for invalidity: %+v", observation)
					}
				}
				if !prepared {
					if err := owner.BootstrapSchema(ctx, complete); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
