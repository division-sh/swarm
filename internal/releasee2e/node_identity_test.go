package releasee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Fixed public contract expectations, independent of the implementation codec.
var nodeIdentityScopes = map[string]struct{ Key, Request string }{
	".":           {"Lg.d29ya2Vy", "entry.root"},
	"left":        {"bGVmdA.d29ya2Vy", "entry.left"},
	"right":       {"cmlnaHQ.d29ya2Vy", "entry.right"},
	"left/nested": {"bGVmdC9uZXN0ZWQ.d29ya2Vy", "entry.nested"},
}

func TestNodeIdentityCanonicalMapKeySQLitePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store node identity proof", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			base := filepath.Join(releaseRoot, backend)
			root := filepath.Join(base, "bundle")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/node_identity"), root)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			assertGoldenProcessHasNoExternalExecutables(t, env)
			activityRoot := filepath.Join(base, "activity-control")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/node_identity_activity"), activityRoot)
			// This control only verifies authored activity identity; it never serves
			// or dispatches the fixture's HTTP tool. The runtime proof stays mock-only.
			writeReleaseFile(t, config, strings.Replace(goldenRuntimeConfig(store), "execution_posture: mock_only", "execution_posture: live", 1))
			activity := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "verify", activityRoot, "--config", config, "--json")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			if activity.err != nil {
				t.Fatalf("legitimate activity ID rejected: %v\n%s", activity.err, activity.output)
			}
			verified := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "verify", root, "--config", config, "--json")
			if verified.err != nil {
				t.Fatalf("verify: %v\n%s", verified.err, verified.output)
			}
			graph := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "describe", root, "--graph", "--config", config)
			if graph.err != nil || !strings.Contains(graph.output, "entry.root") {
				t.Fatalf("graph: %v\n%s", graph.err, graph.output)
			}
			for _, scope := range nodeIdentityScopes {
				if !strings.Contains(graph.output, scope.Key) {
					t.Fatalf("graph omits scoped node %s:\n%s", scope.Key, graph.output)
				}
			}
			start := func() *releaseServeProcess {
				p := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: root, ConfigPath: config, Store: backend, APIPort: freeReleaseTCPPort(t), MCPPort: freeReleaseTCPPort(t), TokenFile: token, Token: goldenAPIToken, Env: env})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return p
			}
			process := start()
			hash := goldenServedBundleHash(t, process.rpc)
			persisted := map[string][]goldenEvent{}
			for _, scope := range []string{".", "left", "right", "left/nested"} {
				run, events := executeNodeIdentityWork(t, process, hash, scope, "before-"+scope)
				persisted[run] = events
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = start()
			if goldenServedBundleHash(t, process.rpc) != hash {
				t.Fatal("restart changed selected artifact")
			}
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			for run, want := range persisted {
				got, err := listGoldenEvents(ctx, process.rpc, run)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("restart readback: %v\nwant %#v\ngot %#v", err, want, got)
				}
			}
			for _, scope := range []string{".", "left", "right", "left/nested"} {
				_, events := executeNodeIdentityWork(t, process, hash, scope, "after-"+scope)
				for _, event := range events {
					if !strings.HasSuffix(event.EventName, "work.processed") {
						continue
					}
					read := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "event", "view", event.EventID, "--config", config, "--api-server", process.apiBase, "--api-token-file", token)
					if read.err != nil || !strings.Contains(read.output, event.EventID) || !strings.Contains(read.output, "node/"+nodeIdentityScopes[scope].Key) {
						t.Fatalf("CLI readback: %v\n%s", read.err, read.output)
					}
				}
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			for _, scope := range []string{".", "left/nested"} {
				file := filepath.Join(root, scope, "nodes.yaml")
				original, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				for _, value := range []string{"worker", "other", "worker-{instance_id}", `""`, "null"} {
					writeReleaseFile(t, file, strings.Replace(string(original), "worker:\n", "worker:\n  id: "+value+"\n", 1))
					for _, command := range []string{"verify", "serve"} {
						result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, command, root, "--config", config)
						if result.err == nil || !strings.Contains(result.output, "node.id is retired; the map key is the identity.") || !strings.Contains(result.output, "nodes.yaml") {
							t.Fatalf("%s %s %s: %v\n%s", scope, value, command, result.err, result.output)
						}
					}
				}
				writeReleaseFile(t, file, string(original))
			}
		})
	}
}

func executeNodeIdentityWork(t *testing.T, process *releaseServeProcess, hash, scope, marker string) (string, []goldenEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
	defer cancel()
	prefix := ""
	if scope != "." {
		prefix = scope + "/"
	}
	var admitted struct {
		RunID string `json:"run_id"`
	}
	expected, ok := nodeIdentityScopes[scope]
	if !ok {
		t.Fatalf("unknown fixture scope %q", scope)
	}
	if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": prefix + expected.Request, "payload": map[string]any{"marker": marker}, "emitter": "releasee2e", "idempotency_key": marker}, &admitted); err != nil {
		t.Fatal(err)
	}
	if admitted.RunID == "" {
		t.Fatal("no admitted run")
	}
	var observed []goldenEvent
	var diagnosis goldenDiagnosis
	err := pollReleaseCondition(ctx, 20*time.Millisecond, func() (bool, error) {
		var err error
		observed, err = listGoldenEvents(ctx, process.rpc, admitted.RunID)
		if err != nil {
			return false, err
		}
		if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": admitted.RunID}, &diagnosis); err != nil {
			return false, err
		}
		if len(diagnosis.FailedDeliveries) != 0 {
			return false, fmt.Errorf("failed deliveries: %s", diagnosis.FailedDeliveries)
		}
		return countGoldenEvents(observed, prefix+"work.processed") == 1 && diagnosis.Run.Status == "completed" && diagnosis.TestQuiescence.Ready, nil
	})
	if err != nil {
		diagnosisJSON, _ := json.Marshal(diagnosis)
		eventsJSON, _ := json.Marshal(observed)
		logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer logCancel()
		var logs json.RawMessage
		logErr := process.rpc.call(logCtx, "runtime.logs", map[string]any{"run_id": admitted.RunID, "limit": 100}, &logs)
		t.Logf("runtime logs: %s (%v)", logs, logErr)
		t.Fatalf("execution %s: %v; diagnosis=%s events=%s\n%s", scope, err, diagnosisJSON, eventsJSON, process.output.String())
	}
	count := 0
	for _, event := range observed {
		if event.EventName != prefix+expected.Request && event.EventName != prefix+"work.processed" && !(scope == "." && event.EventName == "work.routed") {
			continue
		}
		count++
		if event.Payload["marker"] != marker {
			t.Fatalf("payload leaked from another execution: %#v", event)
		}
		if strings.HasSuffix(event.EventName, "work.processed") && event.Payload["origin"] != scope {
			t.Fatalf("wrong scoped handler: %#v", event)
		}
		want := map[string]bool{expected.Key: true}
		if scope == "." && event.EventName == "work.routed" {
			want = map[string]bool{nodeIdentityScopes["left"].Key: true}
		}
		if len(event.Deliveries) != len(want) {
			t.Fatalf("delivery count: %#v; want %v", event, want)
		}
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "node" || !want[delivery.SubscriberID] || !delivery.Terminal {
				t.Fatalf("exact settled node identity: %#v; want %v", event, want)
			}
			delete(want, delivery.SubscriberID)
		}
		var read goldenEvent
		if err := process.rpc.call(ctx, "event.get", map[string]any{"event_id": event.EventID}, &read); err != nil || !reflect.DeepEqual(read, event) {
			t.Fatalf("event.get/list parity: %v\n%#v\n%#v", err, event, read)
		}
	}
	wantCount := 2
	if scope == "." {
		wantCount = 3
	}
	if count != wantCount {
		t.Fatalf("handler execution count = %d", count)
	}
	for other := range nodeIdentityScopes {
		if other == scope {
			continue
		}
		name := "work.processed"
		if other != "." {
			name = other + "/" + name
		}
		if countGoldenEvents(observed, name) != 0 {
			t.Fatalf("unselected scope %s executed the request", other)
		}
	}
	if scope == "." {
		if countGoldenEvents(observed, "left/work.observed") != 1 {
			t.Fatalf("connected child handler did not emit: %#v", observed)
		}
		for _, event := range observed {
			if event.EventName == "left/work.observed" && (event.Payload["origin"] != "left" || event.Payload["marker"] != marker) {
				t.Fatalf("child result did not come from exact selected handler: %#v", event)
			}
		}
	}
	observed, err = listGoldenEvents(ctx, process.rpc, admitted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return admitted.RunID, observed
}
