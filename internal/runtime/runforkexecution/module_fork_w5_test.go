package runforkexecution

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestW5ToolsPinnedModuleRunsThroughSelectedForkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var selected any
			var capabilityStore startupownership.Store
			var owner SelectedContractExecutionOwner
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, capabilityStore, db, owner = s, s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, capabilityStore, owner = s, s, selectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			repo := runForkExecutionRepoRoot(t)
			root := filepath.Join(t.TempDir(), "contracts")
			if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"))); err != nil {
				t.Fatal(err)
			}
			bytes, err := os.ReadFile(filepath.Join(repo, "internal/runtime/computemodule/testdata/structured_renderer.wasm"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(root, "modules"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "modules/renderer.wasm"), bytes, 0o644); err != nil {
				t.Fatal(err)
			}
			files := map[string]string{
				"events.yaml": "item.received:\n  component: text\n  owner: text\n  language: text\n  files: list<text>\nitem.processed:\n  content: text\n",
				"nodes.yaml":  "test-node:\n  event_handlers:\n    item.received:\n      rules:\n        render_bundle:\n          compute_module:\n            module: renderer\n            input: {component: payload.component, owner: payload.owner, language: payload.language, files: payload.files}\n            into: computed.rendered_bundle\n        complete:\n          else: true\n          advances_to: done\n          emit:\n            event: item.processed\n            fields: {content: '${computed.rendered_bundle.content}'}\n",
				"tools.yaml":  fmt.Sprintf("renderer:\n  handler_type: wasm\n  path: modules/renderer.wasm\n  abi: core-json-v1\n  entry: compute\n  digest: sha256:%x\n  input_schema:\n    type: object\n    required: [component, owner, language, files]\n    properties:\n      component: {type: string}\n      owner: {type: string}\n      language: {type: string}\n      files: {type: array, items: {type: string}}\n  output_schema:\n    type: object\n    required: [content, format, line_count]\n    properties:\n      content: {type: string}\n      format: {type: string}\n      line_count: {type: integer}\n  limits: {gas: 5000000, memory_pages: 17, output_bytes: 1024}\n", sha256.Sum256(bytes)),
			}
			for name, body := range files {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
			if err != nil {
				t.Fatal(err)
			}
			runID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			input := eventtest.OperatorInjectedWithRoutingSource(eventID, "item.received", "operator", "", []byte(`{"component":"api","owner":"platform","language":"go","files":["main.go"]}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Unix(1700002200, 0).UTC())
			input, err = eventtest.AdmitPayload(input, ".", "item.received")
			if err != nil {
				t.Fatal(err)
			}
			seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, runID, eventID, entityID, input)
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: runID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
				AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: selectedContractTestProcessCapability(t, ctx, capabilityStore)},
			})
			if err != nil {
				t.Fatalf("selected module execution: %v", err)
			}
			var payload []byte
			if err := db.QueryRow(`SELECT payload_bytes FROM events WHERE run_id=$1 AND event_name='item.processed'`, result.Materialization.ForkRunID).Scan(&payload); err != nil {
				t.Fatalf("persisted selected module output: %v", err)
			}
			var output map[string]string
			if err := json.Unmarshal(payload, &output); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output["content"], "component: api") {
				t.Fatalf("selected module output missing: %#v", output)
			}
		})
	}
}
