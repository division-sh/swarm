package releasee2e

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEventCatalogFactoringCompiledReaders(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	config, env := clockReleaseConfig(t, root, "sqlite")
	source := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), source)
	path := filepath.Join(source, "events.yaml")
	writeReleaseFile(t, path, "task.requested:\n")
	for _, args := range [][]string{{"verify", "--portable", "--json"}, {"describe"}} {
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
		if result.err != nil || (args[0] == "describe" && !strings.Contains(result.output, "(no fields)")) {
			t.Fatalf("compiled bare-event reader: %v\n%s", result.err, result.output)
		}
	}
	for _, body := range []string{"task.requested: {}\n", "task.requested:\n  required: [id]\n  id: uuid\n"} {
		writeReleaseFile(t, path, body)
		for _, args := range [][]string{{"verify", "--portable", "--json"}, {"describe", "--json"}} {
			result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
			if result.err == nil || (!strings.Contains(result.output, "an event with no fields is declared bare") && !strings.Contains(result.output, "reserved for required-by-default field semantics")) {
				t.Fatalf("compiled event grammar refusal: %v\n%s", result.err, result.output)
			}
		}
	}
}

func TestEventCatalogFactoringRefinedPublicationBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store public publication", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			source := filepath.Join(base, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), source)
			writeReleaseFile(t, filepath.Join(source, "events.yaml"), "task.requested:\n  id: {type: text, pattern: '^[a-z]+$', length: {min: 2, max: 8}}\n  note: text?\n")
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public serve: %v\n%s", err, process.output.String())
			}
			hash := goldenServedBundleHash(t, process.rpc, "live")
			payload := map[string]any{"id": "valid"}
			var published struct {
				RunID string `json:"run_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "task.requested", "payload": payload, "emitter": "releasee2e", "idempotency_key": "refined-valid"}, &published); err != nil || published.RunID == "" {
				t.Fatalf("valid refined publication: %+v %v", published, err)
			}
			waitForGoldenTerminalRun(t, process, store, published.RunID, 30*time.Second)
			events, err := listGoldenEvents(ctx, process.rpc, published.RunID)
			if err != nil {
				t.Fatal(err)
			}
			assertGoldenExactPayload(t, goldenSingleNamedEvent(t, events, "task.requested"), payload)
			var before, after json.RawMessage
			if err := process.rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash}, &before); err != nil {
				t.Fatal(err)
			}
			for index, invalid := range []map[string]any{{"id": "X"}, {"id": "x"}, {"id": "toooolong"}, {"note": "missing id"}} {
				var result json.RawMessage
				if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "task.requested", "payload": invalid, "emitter": "releasee2e", "idempotency_key": "refused-" + strconv.Itoa(index)}, &result); err == nil || !strings.Contains(err.Error(), "PAYLOAD_VALIDATION_FAILED") {
					t.Fatalf("invalid refined publication did not reach payload refusal: %v %s %v", invalid, result, err)
				}
			}
			if err := process.rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash}, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("refused payload created a partial run: before=%s after=%s", before, after)
			}
			afterEvents, err := listGoldenEvents(ctx, process.rpc, published.RunID)
			if err != nil || !reflect.DeepEqual(events, afterEvents) {
				t.Fatalf("refused payload changed stored events: %v", err)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}
