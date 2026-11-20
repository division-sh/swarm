package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v46"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestComputeModuleSecretInputNativeBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			path := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", path)
			file, err := credentials.NewFileStore(path)
			if err != nil {
				t.Fatal(err)
			}
			const key = "module_boundary_key"
			values := []string{"A-private-native-canary", "B-private-native-canary"}
			if err := file.Set(t.Context(), key, values[0]); err != nil {
				t.Fatal(err)
			}
			root := computeSecretNativeSource(t, key)
			process, fixture := startServedBoundaryFixture(t, backend, root)
			uses := credentials.BuildRequirementUses(fixture.Runtime.Options.WorkflowModule.SemanticSource())[key]
			worker := identitytest.RootNode(t, "worker")
			if len(uses) != 1 || uses[0].Kind != "compute_module" || uses[0].Role != "private_key" || uses[0].FlowID != "." || uses[0].Name != worker.Key()+"/compute.requested/inspect_key/rules[0]: compute_module" {
				t.Fatalf("exact compute requirement was lost: %#v", uses)
			}
			published := map[string]string{}
			publish := func(label, document string) servedEventPublishRPCResult {
				runID := seedServedPublicationRun(t, fixture.Runtime)
				result := requireServedEventPublishRPCResult(t, fixture.Endpoint, map[string]any{
					"event_name": "compute.requested", "bundle_hash": fixture.BundleHash, "run_id": runID,
					"payload": map[string]any{"document": document}, "idempotency_key": label,
				})
				if result.RunID != runID || result.EventID == "" || result.NewRunCreated {
					t.Fatal("publication did not acknowledge the exact fixture run")
				}
				published[result.EventID] = result.RunID
				return result
			}
			for index, tc := range []struct {
				value, document string
				want            bool
			}{
				{values[0], "x", true}, {values[1], "x", false}, {values[0], "y", false},
			} {
				if err := file.Set(t.Context(), key, tc.value); err != nil {
					t.Fatal(err)
				}
				started := publish(fmt.Sprintf("secret-%d", index), tc.document)
				readback := computeSecretNativeEntity(t, fixture.Endpoint, started.RunID, "done")
				if readback.Fields["seen"] != tc.want {
					t.Fatalf("guest did not consume the current key: fields=%#v", readback.Fields)
				}
				var public json.RawMessage
				requireServedJSONRPCResult(t, fixture.Endpoint, "run.get", map[string]any{"run_id": started.RunID}, &public)
				for _, canary := range values {
					if strings.Contains(string(public), canary) {
						t.Fatal("public run readback exposed injected input")
					}
				}
			}
			if err := file.Delete(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			failed := publish("secret-missing", "x")
			deadline := time.Now().Add(15 * time.Second)
			for {
				var event operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, fixture.Endpoint, "event.get", map[string]any{"event_id": failed.EventID}, &event)
				matches := 0
				for _, delivery := range event.Deliveries {
					if delivery.SubscriberType == "node" && delivery.SubscriberID == worker.Key() && delivery.Status == "dead_letter" {
						matches++
					}
				}
				if event.EventID != failed.EventID || event.RunID != failed.RunID || matches > 1 {
					t.Fatal("failed recipient identity changed")
				}
				if matches == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("private invocation did not reach its exact terminal recipient")
				}
				time.Sleep(50 * time.Millisecond)
			}
			computeSecretNativeEntity(t, fixture.Endpoint, failed.RunID, "waiting")
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := fixture.Runtime.Bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			var traces []computemodule.ReplayEnvelope
			completionCount := 0
			events := computeSecretNativeEvents(t, fixture.Endpoint)
			for eventID, runID := range published {
				if event, found := events[eventID]; !found || event.RunID != runID {
					t.Fatal("native census missed a fixture publication")
				}
			}
			for _, event := range events {
				if event.RunID == failed.RunID && event.EventName == "compute.completed" {
					completionCount++
				}
				encoded, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				for _, canary := range values {
					if strings.Contains(string(encoded), canary) {
						t.Fatal("native journal/failure/log retained injected input")
					}
				}
				detail, _ := event.Payload["details"].(map[string]any)
				if detail["action"] == computemodule.ReplayEvidenceAction {
					loaded, err := computemodule.DecodeReplayEvidenceDetail(detail)
					if err != nil {
						t.Fatal(err)
					}
					traces = append(traces, loaded...)
				}
			}
			if completionCount != 0 {
				t.Fatal("failed private invocation published a partial result")
			}
			groups := map[string]map[string]bool{}
			for _, trace := range traces {
				if groups[trace.InputHash] == nil {
					groups[trace.InputHash] = map[string]bool{}
				}
				groups[trace.InputHash][trace.OutputHash] = true
			}
			if len(traces) != 3 || len(groups) != 2 {
				t.Fatal("native replay input identity did not separate ordinary input from key rotation")
			}
			var equalInputsDifferentOutputs bool
			for _, outputs := range groups {
				equalInputsDifferentOutputs = equalInputsDifferentOutputs || len(outputs) == 2
			}
			if !equalInputsDifferentOutputs {
				t.Fatal("normal output evidence did not retain the changed-key result")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("native boundary shutdown did not join", code)
			}
			for _, canary := range values {
				if strings.Contains(process.outputString(), canary) {
					t.Fatal("serve --verbose exposed injected input")
				}
			}
		})
	}
}

func computeSecretNativeEntity(t *testing.T, endpoint, runID, stage string) operatorread.OperatorEntityFull {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var page operatorread.OperatorEntityListResult
		requireServedJSONRPCResult(t, endpoint, "entity.list", map[string]any{"run_id": runID, "limit": 500}, &page)
		if page.NextCursor != "" || len(page.Entities) > 1 {
			t.Fatal("entity census did not exhaust the exact single-root flow")
		}
		if len(page.Entities) == 1 {
			entity := page.Entities[0]
			if entity.RunID != runID {
				t.Fatal("entity escaped the exact run")
			}
			if entity.CurrentState != nil && *entity.CurrentState == stage {
				var full operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entity.EntityID}, &full)
				if full.Entity.EntityID != entity.EntityID || full.Entity.RunID != runID || full.Entity.CurrentState == nil || *full.Entity.CurrentState != stage {
					t.Fatal("entity full view changed identity/state")
				}
				return full
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("exact run never reached %s", stage)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func computeSecretNativeEvents(t *testing.T, endpoint string) map[string]operatorread.OperatorEventFull {
	t.Helper()
	var runs struct {
		Runs       []operatorread.RunHeader `json:"runs"`
		NextCursor string                   `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 500}, &runs)
	if runs.NextCursor != "" || len(runs.Runs) >= 500 {
		t.Fatal("run census not exhausted")
	}
	events := map[string]operatorread.OperatorEventFull{}
	for _, run := range runs.Runs {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": run.RunID}, "limit": 1000}, &page)
		if page.NextCursor != "" || len(page.Events) >= 1000 {
			t.Fatal("event census not exhausted")
		}
		for _, summary := range page.Events {
			var full operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": summary.EventID}, &full)
			if full.EventID != summary.EventID || full.RunID != run.RunID {
				t.Fatal("event full view changed identity/run")
			}
			events[full.EventID] = full
		}
	}
	var logs operatorread.OperatorRuntimeLogListResult
	requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"limit": 1000}, &logs)
	if logs.NextCursor != "" || len(logs.Logs) >= 1000 {
		t.Fatal("runtime-log census not exhausted")
	}
	for _, log := range logs.Logs {
		var full operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": log.LogID}, &full)
		if full.EventID != log.LogID || full.RunID != log.RunID || full.EventName != "platform.runtime_log" {
			t.Fatal("runtime-log raw event identity changed")
		}
		events[full.EventID] = full
	}
	return events
}

func computeSecretNativeSource(t *testing.T, key string) string {
	t.Helper()
	root := t.TempDir()
	// Both the ordinary document and named key determine this non-signing result.
	// The output contains only a boolean, not either input value.
	offset := len(`{"document":"x","private_key":"`)
	wasm, err := wasmtime.Wat2Wasm(fmt.Sprintf(`(module
  (memory (export "memory") 1 1)
  (data (i32.const 1024) "{\22seen\22:true}")
  (data (i32.const 2048) "{\22seen\22:false}")
  (func (export "alloc") (param i32) (result i32) i32.const 4096)
  (func (export "compute") (param $p i32) (param i32) (result i64)
    local.get $p i32.const %d i32.add i32.load8_u i32.const 120 i32.eq
    local.get $p i32.const %d i32.add i32.load8_u i32.const 65 i32.eq
    i32.and
    if (result i64)
      i64.const %d
    else
      i64.const %d
    end))`, len(`{"document":"`), offset, uint64(1024)<<32|uint64(len(`{"seen":true}`)), uint64(2048)<<32|uint64(len(`{"seen":false}`))))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":   "stages:\n  waiting: {}\n  done: {final: true}\ninputs: [compute.requested]\noutputs: [compute.completed]\n",
		"state.yaml":    "seen: boolean\n",
		"events.yaml":   "compute.requested:\n  document: text\ncompute.completed:\n  seen: boolean\n",
		"nodes.yaml": fmt.Sprintf(`worker:
  compute.requested:
    rules:
      - compute_module:
          module: inspect_key
          input: {document: payload.document}
          secret_input: {private_key: %s}
          into: computed.result
      - emit:
          event: compute.completed
          fields: {seen: computed.result.seen}
sink:
  compute.completed:
    data_accumulation:
      writes:
        - target_field: seen
          value: payload.seen
    advances_to: done
`, key),
		"tools.yaml": fmt.Sprintf(`inspect_key:
  handler_type: wasm
  path: modules/guest.wasm
  abi: core-json-v1
  entry: compute
  digest: %s
  input_schema:
    type: object
    additionalProperties: false
    required: [document, private_key]
    properties:
      document: {type: string, enum: [x, y]}
      private_key: {type: string}
  output_schema:
    type: object
    additionalProperties: false
    required: [seen]
    properties:
      seen: {type: boolean}
  limits: {gas: 100000, memory_pages: 1, output_bytes: 128}
`, computemodule.HashBytes(wasm)),
	} {
		writeStandingCandidateFile(t, filepath.Join(root, name), body)
	}
	if err := os.MkdirAll(filepath.Join(root, "modules"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "modules", "guest.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}
