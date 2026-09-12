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

	"github.com/division-sh/swarm/internal/apiv1"
)

func TestEntityProgressivePresencePublicServeRestartSQLitePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the both-store presence proof", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	for _, variant := range []string{"progressive", "last-field"} {
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(variant+"/"+backend, func(t *testing.T) {
				root := filepath.Join(releaseRoot, variant, backend)
				contracts := filepath.Join(root, "contracts")
				copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/conformance/entity-progressive-presence"), contracts)
				if variant == "last-field" {
					writePresenceLastFieldFixture(t, contracts)
				}
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
				if variant == "last-field" {
					initial = map[string]any{"review_note": "seeded"}
				}
				entityID := waitForPresenceEntity(t, process, runID, "assess", initial)
				publishPresenceEvent(t, process.rpc, hash, runID, "work.annotated", map[string]any{"review_note": ""})
				emptyNote := map[string]any{"attempt_count": float64(0), "review_note": ""}
				if variant == "last-field" {
					emptyNote = map[string]any{"review_note": ""}
				}
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
				// Retry the same public ingress after restart. It must not create a
				// second occurrence or reinitialize the current entity.
				publishPresenceEvent(t, process.rpc, hash, runID, "work.annotated", map[string]any{"review_note": ""})
				publishPresenceEvent(t, process.rpc, hash, runID, "work.assessed", map[string]any{"business_brief": "An assessed brief"})
				completed := map[string]any{"attempt_count": float64(1), "business_brief": "An assessed brief"}
				if variant == "last-field" {
					completed = map[string]any{}
				}
				if got := waitForPresenceEntity(t, process, runID, "done", completed); got != entityID {
					t.Fatalf("completion changed owner: %s != %s", got, entityID)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				events, err := listGoldenEvents(ctx, process.rpc, runID)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if countGoldenEvents(events, "work.annotated") != 1 {
					t.Fatal("idempotent ingress retry created another annotation")
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
				read := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
					"entity", "view", entityID, "--run-id", runID, "--json", "--config", config,
					"--api-server", process.apiBase, "--api-token-file", token)
				var visible struct {
					Fields map[string]any `json:"fields"`
				}
				if err := json.Unmarshal([]byte(read.output), &visible); read.err != nil || err != nil || !reflect.DeepEqual(visible.Fields, completed) {
					t.Fatalf("CLI entity JSON changed sparse fields: command=%v decode=%v\n%s", read.err, err, read.output)
				}
				waitForGoldenTerminalRun(t, process, store, runID, 30*time.Second)
				// The frontier was published after clear committed. Fork reconstructs
				// that history and executes the real completed handler in a new run.
				params := map[string]any{"source_run_id": runID, "fork_event_id": done.EventID, "allow_source_freeze": true, "idempotency_key": "presence-fork"}
				var fork apiv1.RunForkExecutionResult
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				err = process.rpc.call(ctx, "run.fork", params, &fork)
				cancel()
				if err != nil || fork.ForkRunID == "" || fork.ForkRunID == runID || fork.ExecutedEventCount < 1 {
					t.Fatalf("reconstruct and execute fork: result=%+v err=%v", fork, err)
				}
				waitForPresenceEntity(t, process, fork.ForkRunID, "done", completed)
				var retried apiv1.RunForkExecutionResult
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				err = process.rpc.call(ctx, "run.fork", params, &retried)
				cancel()
				if err != nil || !reflect.DeepEqual(fork, retried) {
					t.Fatalf("fork retry changed outcome: first=%+v retry=%+v err=%v", fork, retried, err)
				}
				waitForPresenceEntity(t, process, runID, "done", completed)
				if err := process.stopAndWait(10 * time.Second); err != nil {
					t.Fatal(err)
				}
				process = start()
				waitForPresenceEntity(t, process, fork.ForkRunID, "done", completed)
				waitForPresenceEntity(t, process, runID, "done", completed)
				if err := process.stopAndWait(10 * time.Second); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func writePresenceLastFieldFixture(t *testing.T, root string) {
	t.Helper()
	writeReleaseFile(t, filepath.Join(root, "entities.yaml"), "work:\n  review_note:\n    type: text?\n    initial: seeded\n")
	writeReleaseFile(t, filepath.Join(root, "nodes.yaml"), `owner:
  execution_type: system_node
  subscribes_to: [work.opened, work.annotated, work.assessed, work.consume, work.completed]
  produces: [work.consume, work.completed]
  event_handlers:
    work.opened:
      create_entity: true
      advances_to: assess
    work.annotated:
      guard: {check: "_entity.current_state == 'assess'"}
      data_accumulation:
        writes: [review_note]
    work.assessed:
      guard: {check: "_entity.current_state == 'assess'"}
      advances_to: consume
      emit: {event: work.consume}
    work.consume:
      guard: {check: "_entity.current_state == 'consume'"}
      data_accumulation:
        writes:
          - op: clear
            target: entity.review_note
      emit:
        event: work.completed
        fields:
          business_brief: "'An assessed brief'"
          attempt_count: "1"
          note_supplied: has(entity.review_note)
    work.completed:
      guard: {check: "_entity.current_state == 'consume' && !payload.note_supplied && !has(entity.review_note)"}
      advances_to: done
`)
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
