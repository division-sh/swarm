package releasee2e

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEntityProgressivePresencePublicServeRestartSQLitePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the both-store presence proof", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(releaseRoot, backend)
			contracts := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/conformance/entity-progressive-presence"), contracts)
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(root, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(root, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, root, store.passwordEnv, 0)
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", contracts, "--config", config, "--json")
			var verified struct {
				OK bool `json:"ok"`
			}
			if err := json.Unmarshal([]byte(verify.output), &verified); verify.err != nil || err != nil || !verified.OK {
				t.Fatalf("verify: %v, decode: %v\n%s", verify.err, err, verify.output)
			}
			start := func() *releaseServeProcess {
				// Public live serve, not the mock lifecycle binary. No agents or
				// external providers are needed by this deterministic workload.
				process := startReleaseServe(t, releaseProcessSpec{
					BinaryPath: binary, WorkingDir: root, Source: contracts, ConfigPath: config,
					Store: backend, APIPort: freeReleaseTCPPort(t), TokenFile: token, Token: goldenAPIToken, Env: env,
				})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := process.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				goldenServedBundleHash(t, process.rpc, "live")
				return process
			}
			process := start()
			hash := goldenServedBundleHash(t, process.rpc, "live")
			runID := publishPresenceEvent(t, process.rpc, hash, "", "work.opened", map[string]any{})
			initial := map[string]any{"attempt_count": float64(0)}
			entityID := waitForPresenceEntity(t, process, runID, "assess", initial)
			publishPresenceEvent(t, process.rpc, hash, runID, "work.annotated", map[string]any{"review_note": ""})
			emptyNote := map[string]any{"attempt_count": float64(0), "review_note": ""}
			if got := waitForPresenceEntity(t, process, runID, "assess", emptyNote); got != entityID {
				t.Fatalf("annotation changed owner: %s != %s", got, entityID)
			}
			if err := process.killAndWait(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = start()
			if got := waitForPresenceEntity(t, process, runID, "assess", emptyNote); got != entityID {
				t.Fatalf("restart changed owner: %s != %s", got, entityID)
			}
			publishPresenceEvent(t, process.rpc, hash, runID, "work.assessed", map[string]any{"business_brief": "An assessed brief"})
			completed := map[string]any{"attempt_count": float64(1), "business_brief": "An assessed brief"}
			if got := waitForPresenceEntity(t, process, runID, "done", completed); got != entityID {
				t.Fatalf("completion changed owner: %s != %s", got, entityID)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			events, err := listGoldenEvents(ctx, process.rpc, runID)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			done := goldenSingleNamedEvent(t, events, "work.completed")
			assertGoldenExactPayload(t, done, map[string]any{
				"business_brief": "An assessed brief", "attempt_count": float64(1), "note_supplied": false,
			})
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = start()
			if got := waitForPresenceEntity(t, process, runID, "done", completed); got != entityID {
				t.Fatalf("completed restart changed owner: %s != %s", got, entityID)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func publishPresenceEvent(t *testing.T, rpc *releaseRPCClient, hash, runID, name string, payload map[string]any) string {
	t.Helper()
	params := map[string]any{
		"bundle_hash": hash, "event_name": name, "payload": payload,
		"emitter": "releasee2e", "idempotency_key": "presence-" + name,
	}
	if runID != "" {
		params["run_id"] = runID
	}
	var result struct {
		EventID string `json:"event_id"`
		RunID   string `json:"run_id"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rpc.call(ctx, "event.publish", params, &result); err != nil {
		t.Fatal(err)
	}
	if result.EventID == "" || result.RunID == "" || (runID != "" && result.RunID != runID) {
		t.Fatalf("publish %s returned invalid identity: %#v", name, result)
	}
	return result.RunID
}

func waitForPresenceEntity(t *testing.T, process *releaseServeProcess, runID, stage string, fields map[string]any) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last any
	for {
		var list struct {
			Entities []struct {
				EntityID string `json:"entity_id"`
			} `json:"entities"`
		}
		if err := process.rpc.call(ctx, "entity.list", map[string]any{"run_id": runID, "type": "work"}, &list); err != nil {
			t.Fatal(err)
		}
		if len(list.Entities) > 1 {
			t.Fatalf("unexpected entity multiplication: %#v", list)
		}
		if len(list.Entities) == 1 {
			var entity struct {
				Entity struct {
					CurrentState string `json:"current_state"`
				} `json:"entity"`
				Fields map[string]any `json:"fields"`
			}
			id := list.Entities[0].EntityID
			if err := process.rpc.call(ctx, "entity.get", map[string]any{"run_id": runID, "entity_id": id}, &entity); err != nil {
				t.Fatal(err)
			}
			last = entity
			// Exact maps prove omission, not null or fabricated zero values.
			if entity.Entity.CurrentState == stage && reflect.DeepEqual(entity.Fields, fields) {
				return id
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("entity did not reach %s %#v; last=%#v\n%s", stage, fields, last, process.output.String())
		case <-ticker.C:
		}
	}
}
