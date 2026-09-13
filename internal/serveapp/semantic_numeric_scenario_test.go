package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedSemanticNumericScenarioModes(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"authored", "generated", "authored_derive", "automatic_derived"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := canonicalrouting.WriteNovelDerivedScenarioBundle(t)
				files := map[string]string{
					"fulfillment/events.yaml": `fulfillment.requested:
  value: integer
  fraction: numeric
fulfillment.completed:
  value: integer
  fraction: numeric
  explicit_double: numeric
`,
					"fulfillment/nodes.yaml": `complete-request:
  execution_type: system_node
  subscribes_to: [fulfillment.requested]
  produces: [fulfillment.completed]
  event_handlers:
    fulfillment.requested:
      emit:
        event: fulfillment.completed
        fields:
          value: {cel: 'payload.value + 1'}
          fraction: {cel: 'double(payload.fraction) + 0.5'}
          explicit_double: {cel: 'double(payload.value) + 1.0'}
collector:
  execution_type: system_node
  subscribes_to: [fulfillment.completed]
  event_handlers:
    fulfillment.completed: {}
`,
				}
				scenario := "name: numeric " + mode + "\n"
				switch mode {
				case "authored":
					scenario += "steps:\n  - publish: fulfillment.requested\n    payload: {value: 7.0, fraction: 7.5}\n"
				case "generated":
					scenario += "steps:\n  - publish: fulfillment.requested\n    payload: generate\n"
				case "authored_derive":
					scenario += "derive:\n  flow: fulfillment\n  input: fulfillment.requested\n  payload:\n    generate: true\n"
				}
				if mode != "automatic_derived" {
					files["fulfillment/tests/numeric.yaml"] = scenario + "expect:\n  events:\n    include: [fulfillment/fulfillment.completed]\n  no_dead_letters: true\n"
				}
				for relative, raw := range files {
					path := filepath.Join(root, relative)
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
						t.Fatal(err)
					}
				}
				rt := startSemanticNumericScenarioRuntime(t, backend, root)
				args := []string{"test", root, "--api-server", strings.TrimSuffix(rt.Endpoint, "/v1/rpc"), "--config", writeServeRuntimeTestConfig(t), "--timeout", "10s", "--poll-interval", "10ms"}
				if mode == "automatic_derived" {
					args = append(args, "--derive", "fulfillment", "--input", "fulfillment.requested")
				}
				var stdout, stderr bytes.Buffer
				if code := executeCLIFrom(context.Background(), repoRootForTest(), args, &stdout, &stderr, nil); code != 0 {
					t.Fatalf("numeric scenario code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
				}
				if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
					t.Fatalf("scenario not executed: %s", stdout.String())
				}
				var listed struct {
					Runs []operatorread.RunHeader `json:"runs"`
				}
				requireServedJSONRPCResult(t, rt.Endpoint, "run.list", map[string]any{"bundle_hash": rt.BundleHash}, &listed)
				var matched []operatorread.RunHeader
				for _, run := range listed.Runs {
					if run.Origin.EventType() == "fulfillment/fulfillment.requested" {
						matched = append(matched, run)
					}
				}
				if len(matched) != 1 {
					t.Fatalf("expected one scenario run: %#v", listed)
				}
				run := matched[0].RunID
				read := func(event string) map[string]any {
					t.Helper()
					var raw string
					if err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name=$2`, run, event).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var value map[string]any
					if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &value); err != nil {
						t.Fatal(err)
					}
					projected, err := workflowexpr.ProjectCELValue(value)
					if err != nil {
						t.Fatal(err)
					}
					return projected.(map[string]any)
				}
				in, out := read("fulfillment/fulfillment.requested"), read("fulfillment/fulfillment.completed")
				integer, ok := in["value"].(int64)
				if !ok || out["value"] != integer+1 || out["explicit_double"] != float64(integer)+1 {
					t.Fatalf("numeric execution input=%#v output=%#v", in, out)
				}
				semantic, err := canonicaljson.FromGo(in["fraction"])
				if err != nil {
					t.Fatal(err)
				}
				var fraction float64
				raw, err := canonicaljson.Bytes(semantic)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &fraction); err != nil {
					t.Fatal(err)
				}
				if out["fraction"] != fraction+0.5 {
					t.Fatalf("fraction input=%#v output=%#v", in, out)
				}
			})
		}
	}
}

func startSemanticNumericScenarioRuntime(t *testing.T, backend servedparity.Backend, root string) servedControlProofRuntime {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	var db *sql.DB
	opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	dialect := "sqlite"
	if backend == servedparity.BackendExplicitPostgres {
		dialect = "postgres"
		_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		opts.StoreMode, opts.StoreModeSet = dialect, true
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, dialect, "")
	} else {
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, dialect, filepath.Join(t.TempDir(), "numeric.db"))
	}
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
	endpoint, rt := startServedEventPublishFollowUpRuntime(t, opts)
	return servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: dialect, BundleHash: servedEventPublishFixtureBundleHash(t, root), Runtime: rt}
}
