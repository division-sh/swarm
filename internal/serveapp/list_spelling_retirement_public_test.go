package serveapp

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestListSpellingRetirementPublicBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := t.TempDir()
			for name, body := range listRetirementPublicSource() {
				write2376SourceFile(t, root, name, body)
			}
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "list.sqlite"))
			if backend == "postgres" {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			for _, args := range [][]string{
				{"verify", ".", "--portable", "--json", "--config", opts.ConfigPath},
				{"describe", ".", "--json"},
			} {
				var out, errOut bytes.Buffer
				if code := executeCLIFrom(context.Background(), root, args, &out, &errOut, nil); code != 0 {
					t.Fatalf("%s rejected the teaching replacement: code=%d\n%s\n%s", args[0], code, &out, &errOut)
				}
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			defer func() { process.stop() }()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			item := map[string]any{"name": "one", "tags": []any{"a", "b"}}
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "proof.append", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
				"payload": item, "idempotency_key": "list-first",
			})
			entityID := waitListRetirementFields(t, endpoint, seed.RunID, []any{item})
			if code := process.stop(); code != 0 {
				t.Fatalf("first owner stop=%d", code)
			}
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			if got := waitListRetirementFields(t, endpoint, seed.RunID, []any{item}); got != entityID {
				t.Fatalf("restart changed native membership: %s -> %s", entityID, got)
			}
			second := map[string]any{"name": "two", "tags": []any{"c"}}
			requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "proof.append", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": second, "idempotency_key": "list-second",
			})
			waitListRetirementFields(t, endpoint, seed.RunID, []any{item, second})
		})
	}
}

func listRetirementPublicSource() map[string]string {
	return map[string]string{
		"schema.yaml":   "name: list-retirement\nstages:\n  ready: {}\n  done: {final: true}\npins:\n  inputs: [proof.append, proof.finish]\n",
		"types.yaml":    "types:\n  ProbeItem:\n    name: text\n    tags: [text]\n",
		"entities.yaml": "probe:\n  entries: {type: '[ProbeItem]', initial: []}\n  snapshots:\n    type: '[ProbeItem]'\n    materialize_from: collector.observed\n",
		"events.yaml":   "proof.append:\n  name: text\n  tags: [text]\nproof.finish:\n",
		"nodes.yaml": `collector:
  execution_type: system_node
  state_schema:
    fields:
      observed: [ProbeItem]
  event_handlers:
    proof.append:
      accumulate:
        into: observed
        key: payload.name
      data_accumulation:
        writes:
          - op: append
            target: entity.entries
            value: payload
    proof.finish:
      advances_to: done
`,
	}
}

func TestListSpellingRetirementPublicAdmission(t *testing.T) {
	for _, field := range []struct{ file, original, prefix, element string }{
		{"types.yaml", "    tags: [text]", "    tags: ", "text"},
		{"entities.yaml", "  entries: {type: '[ProbeItem]', initial: []}", "  entries: ", "ProbeItem"},
		{"events.yaml", "  tags: [text]", "  tags: ", "text"},
		{"nodes.yaml", "      observed: [ProbeItem]", "      observed: ", "ProbeItem"},
	} {
		for _, form := range []struct {
			name, declaration string
			admit             bool
		}{
			{"plain", "[T]", true},
			{"explicit_type", "{type: '[T]'}", true},
			{"quoted_bracket", "'[T]'", false},
			{"quoted_singleton", "['T']", false},
			{"angle", "list<T>", false},
			{"long", "{type: list, of: T}", false},
		} {
			t.Run(field.file+"/"+form.name, func(t *testing.T) {
				root := t.TempDir()
				for file, body := range listRetirementPublicSource() {
					if file == field.file {
						body = strings.Replace(body, field.original, field.prefix+strings.ReplaceAll(form.declaration, "T", field.element), 1)
					}
					write2376SourceFile(t, root, file, body)
				}
				for _, command := range []string{"verify", "describe"} {
					args := []string{command, ".", "--json"}
					if command == "verify" {
						args = append(args, "--portable")
					}
					var out, errOut bytes.Buffer
					code := executeCLIFrom(context.Background(), root, args, &out, &errOut, nil)
					diagnostic := out.String() + errOut.String()
					if (code == 0) != form.admit || (!form.admit && (!strings.Contains(diagnostic, "[T]") || !strings.Contains(diagnostic, field.file))) {
						t.Fatalf("%s admission=%d want admit=%v and canonical teaching: %s / %s", command, code, form.admit, &out, &errOut)
					}
				}
			})
		}
	}
}

func waitListRetirementFields(t *testing.T, endpoint, runID string, want []any) string {
	t.Helper()
	var observed operatorread.OperatorEntityFull
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		var result operatorread.OperatorEntityListResult
		requireServedJSONRPCResult(t, endpoint, "entity.list", map[string]any{"run_id": runID}, &result)
		if len(result.Entities) == 1 {
			entityID := result.Entities[0].EntityID
			requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &observed)
			if reflect.DeepEqual(observed.Fields["entries"], want) && reflect.DeepEqual(observed.Fields["snapshots"], want) {
				return entityID
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("native append/projection/readback did not match exact declared values: got=%#v want=%#v", observed, want)
	return ""
}
