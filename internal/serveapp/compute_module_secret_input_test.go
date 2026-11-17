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
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/store/storetest"
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
			db := storetest.DatabaseForTest(fixture.Selected.RuntimeDeps().EventStore)
			uses := credentials.BuildRequirementUses(fixture.Runtime.Options.WorkflowModule.SemanticSource())[key]
			if len(uses) != 1 || uses[0].Kind != "compute_module" || uses[0].Role != "private_key" || uses[0].FlowID != "." || !strings.Contains(uses[0].Name, "/compute.requested/inspect_key/") {
				t.Fatalf("exact compute requirement was lost: %#v", uses)
			}
			publish := func(label string) servedEventPublishRPCResult {
				return requireServedEventPublishRPCResult(t, fixture.Endpoint, map[string]any{
					"event_name": "compute.requested", "bundle_hash": fixture.BundleHash,
					"payload": map[string]any{"document": "x"}, "idempotency_key": label,
				})
			}
			for index, value := range values {
				if err := file.Set(t.Context(), key, value); err != nil {
					t.Fatal(err)
				}
				started := publish(fmt.Sprintf("secret-%d", index))
				entity := requireServedEventPublishEntityState(t, db, backend, started.RunID, "", "done")
				var readback operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, fixture.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": entity}, &readback)
				if readback.Fields["seen"] != (index == 0) {
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
			failed := publish("secret-missing")
			waitServedEventPublishDeliveryStatusCount(t, db, backend, failed.EventID, "node", "worker", "dead_letter", 1)
			query := "SELECT COUNT(*) FROM events WHERE event_name='compute.completed' AND CAST(run_id AS TEXT)=?"
			if backend == "postgres" {
				query = strings.Replace(query, "?", "$1", 1)
			}
			var count int
			if err := db.QueryRowContext(t.Context(), query, failed.RunID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("failed private invocation published a partial result")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := fixture.Runtime.Bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				"SELECT CAST(payload AS TEXT) FROM events",
				"SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE failure IS NOT NULL",
			} {
				rows, err := db.QueryContext(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
					var encoded string
					if err := rows.Scan(&encoded); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					for _, canary := range values {
						if strings.Contains(encoded, canary) {
							rows.Close()
							t.Fatal("native journal/failure/log retained injected input")
						}
					}
				}
				err = rows.Err()
				if closeErr := rows.Close(); err != nil || closeErr != nil {
					t.Fatalf("read/close native evidence: %v / %v", err, closeErr)
				}
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

func computeSecretNativeSource(t *testing.T, key string) string {
	t.Helper()
	root := t.TempDir()
	// A first-party, non-signing guest reads the first byte of the named input.
	// Its output deliberately contains only a boolean, not the input value.
	offset := len(`{"document":"x","private_key":"`)
	wasm, err := wasmtime.Wat2Wasm(fmt.Sprintf(`(module
  (memory (export "memory") 1 1)
  (data (i32.const 1024) "{\22seen\22:true}")
  (data (i32.const 2048) "{\22seen\22:false}")
  (func (export "alloc") (param i32) (result i32) i32.const 4096)
  (func (export "compute") (param $p i32) (param i32) (result i64)
    local.get $p i32.const %d i32.add i32.load8_u i32.const 65 i32.eq
    if (result i64)
      i64.const %d
    else
      i64.const %d
    end))`, offset, uint64(1024)<<32|uint64(len(`{"seen":true}`)), uint64(2048)<<32|uint64(len(`{"seen":false}`))))
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
  path: guest.wasm
  abi: core-json-v1
  entry: compute
  digest: %s
  input_schema:
    type: object
    additionalProperties: false
    required: [document, private_key]
    properties:
      document: {type: string}
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
	if err := os.WriteFile(filepath.Join(root, "guest.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}
