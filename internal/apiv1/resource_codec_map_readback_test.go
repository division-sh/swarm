package apiv1

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestResourceCodecCompiledMapImportReceiptAndPinnedPublicAgentReadbackBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		root := t.TempDir()
		for name, text := range map[string]string{
			"schema.yaml":         "name: codec-map-readback\n",
			"support/schema.yaml": "name: support\n",
			"support/types.yaml":  "types:\n  CodecProfile:\n    note: integer?\n",
			"support/events.yaml": "records.loaded:\n  records: map[text]CodecProfile\n",
			"support/agents.yaml": "reader:\n  id: codec-map-reader\n  role: reader\n  intent: {inline: Read exact run data.}\n  data_access:\n    - data: support/records.loaded\n",
		} {
			path := filepath.Join(root, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		repo, err := filepath.Abs(filepath.Join("..", ".."))
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := contracts.BuildDurableDataCatalog(bundle)
		if err != nil || len(catalog.Declarations) != 1 {
			t.Fatalf("compiled catalog=%+v error=%v", catalog, err)
		}
		declaration := catalog.Declarations[0]
		ctx := testAuthorActivityContextForSource(context.Background(), mustAPITestSourceArtifactFact(catalog.BundleHash))
		storetest.RequireBundleDataCatalog(t, ctx, fixture.primary, bundle)
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorDataHandlers(DataHandlerOptions{Store: fixture.primary})})
		call := func(method string, params map[string]any) map[string]any {
			t.Helper()
			status, response, body := callReadOnlyProbeRPC(t, handler, method, params, "Bearer "+testToken)
			if status != 200 || response.Error != nil {
				t.Fatalf("%s status=%d error=%+v body=%s", method, status, response.Error, body)
			}
			return asMap(t, response.Result)
		}
		const input = `{"records":{"primary":{"note":null}}}`
		const canonical = "{\"records\":{\"primary\":{}}}\n"
		sourceID := uuid.NewString()
		params := map[string]any{
			"source_invocation_id": sourceID, "bundle_hash": catalog.BundleHash,
			"declaration": dataRunDeclaration(declaration.Ref), "expected_head": map[string]any{"state": "absent"},
			"input": map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte(input))},
		}
		imported := call("data.import", params)
		version := stringValue(t, asMap(t, imported["candidate"])["version_id"], "version")
		if imported["outcome"] != "accepted" {
			t.Fatalf("import=%+v", imported)
		}
		if replay := call("data.import", params); !reflect.DeepEqual(replay, imported) {
			t.Fatalf("public replay=%+v, want=%+v", replay, imported)
		}
		receipt, err := fixture.reconstructed.LoadDataSourceOperation(ctx, sourceID)
		if err != nil || receipt.Result.Outcome != "accepted" || string(receipt.Result.Candidate.VersionID) != version {
			t.Fatalf("retained receipt=%+v error=%v", receipt, err)
		}
		command := durabledata.SourceCommand{Operation: "import", SourceInvocationID: sourceID, Actor: actorTokenID(testToken),
			BundleHash: catalog.BundleHash, Declaration: declaration.Ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte(input)}
		if err := receipt.Evaluation.ValidateRecord(command, receipt); err != nil {
			t.Fatalf("retained canonical source evaluation: %v", err)
		}
		selector := map[string]any{"kind": "version", "version_id": version}
		row := call("data.show", map[string]any{"view": "row", "declaration": dataRunDeclaration(declaration.Ref), "selector": selector, "row_selector": map[string]any{"position": 1}})
		value, err := json.Marshal(row["value"])
		if err != nil || string(value)+"\n" != canonical || row["ordinal"] != float64(1) {
			t.Fatalf("public row=%+v value=%s error=%v", row, value, err)
		}
		exported := call("data.show", map[string]any{"view": "export_chunk", "declaration": dataRunDeclaration(declaration.Ref), "selector": selector, "page": map[string]any{}})
		exportedBytes, err := base64.StdEncoding.DecodeString(stringValue(t, exported["chunk_base64"], "export content"))
		if err != nil || string(exportedBytes) != canonical {
			t.Fatalf("export=%+v payload=%s error=%v", exported, exportedBytes, err)
		}
		_, retained, err := fixture.reconstructed.ResolveDataVersionPayload(ctx, declaration.Ref, durabledata.VersionSelector{Kind: "version", VersionID: durabledata.VersionID(version)})
		if err != nil || !bytes.Equal(retained.CanonicalJSONL, []byte(canonical)) {
			t.Fatalf("retained payload=%+v error=%v", retained, err)
		}
		head := durabledata.VersionHead(retained.VersionID)
		base := durabledata.SourceEvaluationBase{State: "version", Head: durabledata.HeadResult{Before: head, After: head, Revision: 1},
			Manifest: &retained.Manifest, CanonicalSchema: retained.CanonicalSchema, CanonicalJSONL: []byte(input + "\n")}
		if _, err := base.Validate(declaration.Ref); err == nil {
			t.Fatal("same-meaning noncanonical retained bytes were accepted")
		}
		sourceRun, targetRun := uuid.NewString(), uuid.NewString()
		for _, runID := range []string{sourceRun, targetRun} {
			storetest.RequireRun(t, ctx, fixture.primary, storetest.RunFixture{RunID: runID, State: runlifecycle.StateRunning,
				Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC()})
		}
		pins, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, sourceRun, targetRun, catalog.BundleHash,
			[]durabledata.ExplicitPin{{Declaration: declaration.Ref, VersionID: retained.VersionID}}, false)
		if err != nil || len(pins) != 1 || pins[0].RunID != targetRun || pins[0].VersionID != retained.VersionID {
			t.Fatalf("canonical pin owner=%+v error=%v", pins, err)
		}
		source := semanticview.Wrap(bundle)
		actor := models.AgentConfig{ID: "codec-map-reader", Role: "reader", FlowID: "support"}
		admitted, ok := semanticview.ResolveAgentDeclaration(source, actor)
		if !ok {
			t.Fatal("loaded actor declaration absent")
		}
		plan, err := semanticview.ScopedAgentNamePlan(source, admitted)
		if err != nil {
			t.Fatal(err)
		}
		actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, "support", "codec-map", "support/codec-map")
		access, ok := fixture.reconstructed.(durabledata.ResourceAccessStore)
		if !ok {
			t.Fatal("selected store lacks resource access owner")
		}
		executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{WorkflowSource: source, DataAccessStore: access})
		toolCtx := models.WithActor(runtimecorrelation.WithRunID(ctx, targetRun), actor)
		read, err := executor.Execute(toolCtx, "read_flow_data", map[string]any{"kind": "resource_row", "declaration": declaration.Ref, "position": 1})
		if err != nil {
			t.Fatal(err)
		}
		result, ok := read.(map[string]any)
		if !ok || result["version_id"] != retained.VersionID {
			t.Fatalf("pinned agent read=%+v", read)
		}
		toolRow, ok := result["row"].(map[string]any)
		if !ok {
			t.Fatalf("pinned agent row=%+v", result)
		}
		value, err = json.Marshal(toolRow["value"])
		if err != nil || string(value)+"\n" != canonical || toolRow["ordinal"] != uint64(1) {
			t.Fatalf("pinned agent payload=%s row=%+v error=%v", value, toolRow, err)
		}
	})
}
