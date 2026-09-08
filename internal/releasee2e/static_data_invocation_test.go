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

func TestDurableDataInvocationInvarianceSQLitePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the two-store invocation proof", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	var baseline map[string]map[string]any
	var baselineHash string
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(releaseRoot, backend)
			root := filepath.Join(base, "bundle")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/static_data_invocation"), root)
			outside := filepath.Join(base, "outside")
			if err := os.Mkdir(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, target := range map[string]string{"alias": root, "chain": "alias", "ancestor": base, "outside/link": root} {
				if err := os.Symlink(target, filepath.Join(base, name)); err != nil {
					t.Fatal(err)
				}
			}
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
			cells := []struct{ name, cwd, operand string }{
				{"relative-inside", base, "bundle"}, {"absolute-inside", base, root},
				{"relative-outside", outside, "../bundle"}, {"absolute-outside", outside, root},
				{"omitted", root, ""}, {"dot", root, "."},
				{"symlink-cwd", filepath.Join(base, "alias"), "."},
				{"alias", base, "alias"}, {"chain", base, "chain"}, {"ancestor", base, "ancestor/bundle"},
				{"relative-alias-parent", base, "outside/link/../bundle"},
				{"absolute-alias-parent", outside, outside + "/link/../bundle"},
			}
			for _, cell := range cells {
				t.Run(cell.name, func(t *testing.T) {
					args := []string{"verify", "--config", config, "--json"}
					if cell.operand != "" {
						args = append(args, cell.operand)
					}
					verified := runReleaseCommand(t, goldenStartupTimeout, cell.cwd, env, "", binary, args...)
					var result struct {
						OK bool `json:"ok"`
					}
					if err := json.Unmarshal([]byte(verified.output), &result); verified.err != nil || err != nil || !result.OK {
						t.Fatalf("verify: %v; decode: %v\n%s", verified.err, err, verified.output)
					}
					process := startReleaseServe(t, releaseProcessSpec{
						BinaryPath: binary, WorkingDir: cell.cwd, Source: cell.operand, ConfigPath: config,
						Store: backend, APIPort: freeReleaseTCPPort(t), MCPPort: freeReleaseTCPPort(t), TokenFile: token, Token: goldenAPIToken, Env: env,
					})
					ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
					defer cancel()
					if err := process.waitReady(ctx); err != nil {
						t.Fatal(err)
					}
					hash := goldenServedBundleHash(t, process.rpc)
					var identity struct {
						SourceArtifacts []struct {
							BundleHash string `json:"bundle_hash"`
						} `json:"source_artifacts"`
					}
					if err := process.rpc.call(ctx, "runtime.identity", map[string]any{}, &identity); err != nil {
						t.Fatal(err)
					}
					if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash != hash {
						t.Fatalf("runtime.identity differs from health: %#v", identity)
					}
					if baselineHash == "" {
						baselineHash = hash
					}
					if hash != baselineHash {
						t.Fatalf("hash = %s, want %s", hash, baselineHash)
					}
					observed := runStaticDataRead(t, process, hash, cell.name, "")
					if baseline == nil {
						baseline = observed
					}
					if !reflect.DeepEqual(observed, baseline) {
						t.Fatalf("readback differs: %#v; baseline %#v", observed, baseline)
					}
					foreign := observed["registry/child.completed"]["static_id"].(string)
					runStaticDataRead(t, process, hash, cell.name+"-foreign", foreign)
					healthy := runStaticDataRead(t, process, hash, cell.name+"-healthy", "")
					if !reflect.DeepEqual(healthy, baseline) {
						t.Fatalf("own-ID healthy read after foreign denial differs: %#v", healthy)
					}
					if err := process.stopAndWait(10 * time.Second); err != nil {
						t.Fatalf("stop: %v\n%s", err, process.output.String())
					}
				})
			}
			// Every geometry also rejects a referenced file that no longer exists.
			if err := os.Remove(filepath.Join(root, "data/resume.md")); err != nil {
				t.Fatal(err)
			}
			for _, cell := range cells {
				t.Run("missing/"+cell.name, func(t *testing.T) {
					for _, command := range []string{"verify", "serve"} {
						args := []string{command, "--config", config}
						if cell.operand != "" {
							args = append(args, cell.operand)
						}
						result := runReleaseCommand(t, goldenStartupTimeout, cell.cwd, env, "", binary, args...)
						if result.err == nil || !strings.Contains(result.output, "resume.md") {
							t.Fatalf("%s missing data accepted: %v\n%s", command, result.err, result.output)
						}
					}
				})
			}
			writeReleaseFile(t, filepath.Join(root, "data/resume.md"), "Root resume: exact admitted bytes.\n")
			writeReleaseFile(t, filepath.Join(base, "file-root"), "not a directory")
			for name, target := range map[string]string{"broken-root": "absent", "cyclic-root": "cyclic-root"} {
				if err := os.Symlink(target, filepath.Join(base, name)); err != nil {
					t.Fatal(err)
				}
			}
			for _, operand := range []string{"file-root", "broken-root", "cyclic-root"} {
				t.Run(operand, func(t *testing.T) {
					for _, command := range []string{"verify", "serve"} {
						result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, command, operand, "--config", config)
						if result.err == nil || !strings.Contains(result.output, "source") || !strings.Contains(result.output, operand) {
							t.Fatalf("%s invalid root: %v\n%s", command, result.err, result.output)
						}
					}
				})
			}
			for _, kind := range []string{"included-file-link", "included-directory-link"} {
				t.Run(kind, func(t *testing.T) {
					link := filepath.Join(root, "data", "escape.md")
					target := filepath.Join(root, "data/resume.md")
					if kind == "included-directory-link" {
						link = filepath.Join(root, "linked")
						target = filepath.Join(root, "registry")
					}
					if err := os.Symlink(target, link); err != nil {
						t.Fatal(err)
					}
					defer os.Remove(link)
					for _, command := range []string{"verify", "serve"} {
						result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, command, "alias", "--config", config)
						if result.err == nil || !strings.Contains(result.output, "symlink") {
							t.Fatalf("%s included-link admission: %v\n%s", command, result.err, result.output)
						}
					}
				})
			}
		})
	}
}

func runStaticDataRead(t *testing.T, process *releaseServeProcess, hash, key, foreign string) map[string]map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
	defer cancel()
	var admitted struct {
		RunID  string `json:"run_id"`
		NewRun bool   `json:"new_run_created"`
	}
	if err := process.rpc.call(ctx, "event.publish", map[string]any{
		"bundle_hash": hash, "event_name": "read.requested", "payload": map[string]any{"foreign_id": foreign, "request_key": key}, "emitter": "releasee2e", "idempotency_key": key,
	}, &admitted); err != nil {
		t.Fatal(err)
	}
	if admitted.RunID == "" || !admitted.NewRun {
		t.Fatalf("run admission: %#v", admitted)
	}
	observed := map[string]map[string]any{}
	var last goldenDiagnosis
	err := pollReleaseCondition(ctx, 20*time.Millisecond, func() (bool, error) {
		events, err := listGoldenEvents(ctx, process.rpc, admitted.RunID)
		if err != nil {
			return false, err
		}
		for _, event := range events {
			if event.EventName == "read.completed" {
				observed[event.EventName] = event.Payload
			}
			if strings.HasSuffix(event.EventName, "/child.completed") {
				observed["registry/child.completed"] = event.Payload
			}
		}
		if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": admitted.RunID}, &last); err != nil {
			return false, err
		}
		if foreign != "" && len(last.FailedDeliveries) > 0 {
			if len(observed) != 0 {
				return false, fmt.Errorf("foreign ID leaked content: %#v", observed)
			}
			return last.TestQuiescence.Ready, nil
		}
		if last.Run.Status == "failed" || len(last.FailedDeliveries) > 0 {
			diagnosis, _ := json.Marshal(last)
			return false, fmt.Errorf("run failed: %s", diagnosis)
		}
		return len(observed) == 2 && last.Run.Status == "completed" && last.TestQuiescence.Ready, nil
	})
	if err != nil {
		diagnosis, _ := json.Marshal(last)
		var logs json.RawMessage
		logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer logCancel()
		logErr := process.rpc.call(logCtx, "runtime.logs", map[string]any{"run_id": admitted.RunID, "limit": 100}, &logs)
		t.Logf("runtime logs: %s (%v)", logs, logErr)
		t.Fatalf("actual tool read/public result: %v; observed=%#v diagnosis=%s\n%s", err, observed, diagnosis, process.output.String())
	}
	var execution struct {
		Logs []struct {
			Action  string `json:"action"`
			Details struct {
				ToolName      string `json:"tool_name"`
				ResultSummary string `json:"result_summary"`
			} `json:"details"`
		} `json:"logs"`
	}
	if err := process.rpc.call(ctx, "runtime.logs", map[string]any{"run_id": admitted.RunID, "component": "tool-executor", "limit": 100}, &execution); err != nil {
		t.Fatal(err)
	}
	reads := map[string]map[string]any{}
	for _, log := range execution.Logs {
		if log.Details.ToolName != "read_flow_data" {
			continue
		}
		if foreign != "" {
			t.Fatalf("foreign ID passed generated-schema admission and reached execution: %#v", log)
		}
		if log.Action != "tool_execution_succeeded" {
			t.Fatalf("static read failed: %#v", log)
		}
		var result map[string]any
		if err := json.Unmarshal([]byte(log.Details.ResultSummary), &result); err != nil {
			t.Fatal(err)
		}
		path, ok := result["flow_path"].(string)
		if !ok || reads[path] != nil {
			t.Fatalf("duplicate or missing read owner: %#v", result)
		}
		reads[path] = result
	}
	if foreign != "" {
		if len(last.FailedDeliveries) != 1 {
			t.Fatalf("foreign-ID failures: %s", last.FailedDeliveries)
		}
		var rejected struct {
			EventName      string `json:"event_name"`
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
			Failure        struct {
				Class     string `json:"class"`
				Component string `json:"component"`
				Detail    struct {
					Code string `json:"code"`
				} `json:"detail"`
			} `json:"failure"`
		}
		if err := json.Unmarshal(last.FailedDeliveries[0], &rejected); err != nil {
			t.Fatal(err)
		}
		if rejected.EventName != "read.work" || rejected.SubscriberType != "agent" || rejected.SubscriberID != "reader" {
			t.Fatalf("foreign-ID failed at wrong gate: %s", last.FailedDeliveries[0])
		}
		if rejected.Failure.Class != "platform.internal_failure" || rejected.Failure.Component != "agent-manager" || rejected.Failure.Detail.Code != "unclassified_runtime_error" {
			t.Fatalf("foreign-ID failure changed class: %s", last.FailedDeliveries[0])
		}
		var stopped struct {
			OK bool `json:"ok"`
		}
		if err := process.rpc.call(ctx, "run.stop", map[string]any{"run_id": admitted.RunID, "idempotency_key": key + "-stop"}, &stopped); err != nil || !stopped.OK {
			t.Fatalf("stop negative-control run: %v %#v", err, stopped)
		}
		return observed
	}
	root, child := observed["read.completed"], observed["registry/child.completed"]
	for path, event := range map[string]map[string]any{".": root, "registry": child} {
		if read := reads[path]; read == nil || read["static_id"] != event["static_id"] || read["content"] != event["content"] || read["relative_path"] != "resume.md" {
			t.Fatalf("public event did not report the actual %s tool result: reads=%#v event=%#v", path, reads, event)
		}
	}
	if root["content"] != "Root resume: exact admitted bytes.\n" || child["content"] != "Child resume: different admitted bytes.\n" || root["static_id"] == child["static_id"] {
		t.Fatalf("root/child identity or bytes: %#v", observed)
	}
	return observed
}
