package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogTypeDetachmentPublicRestartBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			source := filepath.Join(base, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), source)
			writeReleaseFile(t, filepath.Join(source, "events.yaml"), "task.requested:\n  id: {type: text, pattern: '^[a-z]+$', length: {min: 2, max: 8}, equal_to: peer}\n  peer: text\n  score: {type: numeric, range: {min: 1.5, max: 9.5}}\n  note: text?\n")
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			var description string
			for _, command := range []string{"verify", "describe"} {
				args := []string{command, "--json", source, "--config", config}
				if command == "verify" {
					args = append(args, "--portable")
				}
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, args...)
				if result.err != nil {
					t.Fatalf("public %s: %v\n%s", command, result.err, result.output)
				}
				if command == "describe" {
					description = result.output
				}
			}
			start := func() *releaseServeProcess {
				process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := process.waitReady(ctx); err != nil {
					t.Fatalf("public boot: %v\n%s", err, process.output.String())
				}
				return process
			}
			process := start()
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			hash := goldenServedBundleHash(t, process.rpc, "live")
			payload := map[string]any{"id": "valid", "peer": "valid", "score": 3.5}
			var published struct {
				RunID string `json:"run_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "task.requested", "payload": payload, "emitter": "releasee2e", "idempotency_key": "detached-valid"}, &published); err != nil || published.RunID == "" {
				t.Fatalf("public admitted payload: %+v %v", published, err)
			}
			waitForGoldenTerminalRun(t, process, store, published.RunID, 30*time.Second)
			before, err := listGoldenEvents(ctx, process.rpc, published.RunID)
			if err != nil {
				t.Fatal(err)
			}
			assertGoldenExactPayload(t, goldenSingleNamedEvent(t, before, "task.requested"), payload)
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = start()
			if got := goldenServedBundleHash(t, process.rpc, "live"); got != hash {
				t.Fatalf("restart changed source identity: %s != %s", got, hash)
			}
			after, err := listGoldenEvents(ctx, process.rpc, published.RunID)
			left, _ := json.Marshal(before)
			right, _ := json.Marshal(after)
			if err != nil || !bytes.Equal(left, right) {
				t.Fatalf("public restart changed retained events: %v", err)
			}
			for index, bad := range []map[string]any{
				{"id": "x", "peer": "x", "score": 3.5},
				{"id": "toooolong", "peer": "toooolong", "score": 3.5},
				{"id": "valid", "peer": "valid", "score": 1.0},
				{"id": "valid", "peer": "valid", "score": 10.0},
				{"id": "valid", "peer": "different", "score": 3.5},
			} {
				var result json.RawMessage
				if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "task.requested", "payload": bad, "emitter": "releasee2e", "idempotency_key": "detached-refused-" + string(rune('a'+index))}, &result); err == nil || !strings.Contains(err.Error(), "PAYLOAD_VALIDATION_FAILED") {
					t.Fatalf("restart changed refusal: payload=%v err=%v", bad, err)
				}
			}
			result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "describe", "--json", source, "--config", config)
			if result.err != nil || result.output != description {
				t.Fatalf("restart changed public structural description: %v", result.err)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}
