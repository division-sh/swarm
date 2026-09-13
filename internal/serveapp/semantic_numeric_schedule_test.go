package serveapp

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestGenericScheduleSemanticPayloadExecutionParity(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := semanticNumericIngressFixture(t)
			eventFile := filepath.Join(root, "events.yaml")
			raw, err := os.ReadFile(eventFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(eventFile, []byte(strings.Replace(string(raw), "source: external", "source: platform schedule", 1)), 0600); err != nil {
				t.Fatal(err)
			}
			nodeFile := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodeFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(nodeFile, []byte(strings.Replace(string(nodes), "      create_entity: true\n", "", 1)), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: numeric-schedule\ninitial_state: waiting\nterminal_states: [done]\nstates: [waiting, done]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			var db *sql.DB
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			dialect := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				dialect = "postgres"
				_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode, opts.StoreModeSet = "postgres", true
			} else {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "schedule.db"))
			}
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
			start := func() (*serveRuntimeTestProcess, servedControlProofRuntime) {
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				return process, servedControlProofRuntime{Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc", DB: db, Backend: dialect, BundleHash: servedEventPublishFixtureBundleHash(t, root), Runtime: servedTestProcessRuntime(t, process)}
			}
			process, rt := start()
			run := uuid.NewString()
			var setup any
			requireServedJSONRPCResult(t, rt.Endpoint, "test.setup_entities", map[string]any{"bundle_hash": rt.BundleHash, "run_id": run, "idempotency_key": uuid.NewString(),
				"entities": []any{map[string]any{"alias": "subject", "entity_id": run, "entity_type": "widget", "current_state": "waiting", "fields": map[string]any{"score": 0}}}}, &setup)
			route, err := events.NewRootRoutingSource(run)
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{RunID: run, OwnerID: "runtime", OwnerKind: genericschedule.OwnerAgent,
				AgentIdentity: agentidentitytest.RootRuntimeForRun(t, run, "runtime", "numeric-schedule"), ScheduleKey: "numeric", TaskID: "numeric", EventType: "numeric.requested", EntityID: run,
				RoutingSource: route, ExecutionMode: executionmode.Live, Due: genericschedule.AbsoluteDue(time.Now().UTC().Add(100 * time.Millisecond).Truncate(time.Microsecond))}
			var activationID, fingerprint string
			admit := func(number string) {
				t.Helper()
				value, err := canonicaljson.Decode([]byte(`{"value":` + number + `,"nested":{"numbers":[` + number + `],"fraction":7.5}}`))
				if err != nil {
					t.Fatal(err)
				}
				command.Payload = value
				result, err := rt.Runtime.GenericSchedules.Admit(servedControlProofAuthorActivityContext(t, rt), command)
				if err != nil {
					t.Fatal(err)
				}
				if activationID == "" {
					activationID, fingerprint = result.Activation.ID, result.Activation.ImmutableHash
				} else if result.Activation.ID != activationID || result.Activation.ImmutableHash != fingerprint {
					t.Fatalf("equivalent schedule changed identity: %#v", result)
				}
			}
			admit("7")
			admit("7.0")
			semanticNumericOutput(t, rt.Endpoint, rt.DB, run)
			if code := process.stop(); code != 0 {
				t.Fatalf("stop=%d", code)
			}
			process, rt = start()
			admit("7e0")
			semanticNumericOutput(t, rt.Endpoint, rt.DB, run)
			if code := process.stop(); code != 0 {
				t.Fatalf("restart stop=%d", code)
			}
		})
	}
}
