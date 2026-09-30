package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGoldenNumericDataScatterParkRefusalBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store whole-input refusal", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	lifecycle := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			selected := goldenSQLiteStore(root)
			if backend == "postgres" {
				selected = goldenPostgresStore(t, dsn)
			}
			project := filepath.Join(root, "yaml-project")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), filepath.Join(project, "contracts"))
			loadNumericFeedCorpus(t, filepath.Join(project, "contracts"))
			writeReleaseFile(t, filepath.Join(project, ".swarm/swarm.yaml"), goldenRuntimeConfig(selected))
			writeReleaseFile(t, filepath.Join(project, "api-token"), goldenAPIToken+"\n")
			writeReleaseFile(t, filepath.Join(root, "home/.config/swarm/swarm.yaml"), "connection:\n  api_token_file: api-token\n")
			env := goldenProcessEnv(t, root, selected.passwordEnv, 0)
			verified := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary,
				"verify", "contracts", "--config", ".swarm/swarm.yaml", "--json")
			if verified.err != nil {
				t.Fatalf("admitted refusal source: %v\n%s", verified.err, verified.output)
			}
			p := startReleaseServe(t, releaseProcessSpec{
				BinaryPath: binary, InternalMockLifecycleBinary: lifecycle, WorkingDir: project,
				Source: "contracts", ConfigPath: ".swarm/swarm.yaml", Store: backend,
				TokenFile: "api-token", Token: goldenAPIToken, Env: env,
			})
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			if err := p.waitReady(ctx); err != nil {
				t.Fatal(err)
			}
			hash := goldenServedBundleHash(t, p.rpc, "mock_only")
			before := captureNumericRefusalInventory(t, ctx, p.rpc, hash)
			data, err := os.ReadFile(filepath.Join(project, "contracts/data/items.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
			if len(lines) != 100 {
				t.Fatal("refusal must retain 99 valid predecessor rows")
			}
			for _, defect := range []string{"invalid_numeric", "required_null", "duplicate_key"} {
				t.Run(defect, func(t *testing.T) {
					var last map[string]any
					if err := json.Unmarshal(lines[99], &last); err != nil {
						t.Fatal(err)
					}
					switch defect {
					case "invalid_numeric":
						last["score"] = "not-a-number"
					case "required_null":
						last["openings"] = nil
					case "duplicate_key":
						var first map[string]any
						if err := json.Unmarshal(lines[0], &first); err != nil {
							t.Fatal(err)
						}
						last["item_id"] = first["item_id"]
					}
					mutated, err := json.Marshal(last)
					if err != nil {
						t.Fatal(err)
					}
					candidate := append(bytes.Join(lines[:99], []byte("\n")), '\n')
					candidate = append(candidate, mutated...)
					writeReleaseFile(t, filepath.Join(project, "bad.jsonl"), string(candidate)+"\n")
					runID := uuid.NewString()
					result := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary,
						"run", "start", "--connect", p.apiBase, "--bundle-hash", hash,
						"--run-id", runID, "--idempotency-key", "numeric-refusal-"+runID,
						"--data", "item.registered=bad.jsonl", "--no-follow")
					if result.err == nil || !strings.Contains(result.output, "RUN_DATA_REJECTED") {
						t.Fatalf("whole-input row-100 refusal: %v\n%s", result.err, result.output)
					}
					assertNumericRow100Defect(t, ctx, p.rpc, runID, defect)
					var missing any
					for _, method := range []string{"run.get", "run.fan_out.list"} {
						if err := p.rpc.call(ctx, method, map[string]any{"run_id": runID}, &missing); err == nil || !strings.Contains(err.Error(), "RUN_NOT_FOUND") {
							t.Fatalf("rejected input created %s domain: result=%+v error=%v", method, missing, err)
						}
					}
					if got := captureNumericRefusalInventory(t, ctx, p.rpc, hash); !reflect.DeepEqual(got, before) {
						t.Fatalf("rejected %s changed public version/head/run facts: before=%s after=%s", defect, before, got)
					}
				})
			}
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatalf("refusal child shutdown: %v\n%s", err, p.output.String())
			}
		})
	}
}

func assertNumericRow100Defect(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID, defect string) {
	t.Helper()
	var page struct {
		Items []struct {
			Defect struct {
				Row int `json:"row"`
			} `json:"defect"`
		} `json:"items"`
		Continuation struct {
			State string `json:"state"`
		} `json:"continuation"`
	}
	if err := rpc.call(ctx, "data.show", map[string]any{
		"view": "operation", "detail": "child_defects", "operation_ref": map[string]any{"kind": "run_creation", "run_id": runID},
		"page": map[string]any{"limit": 100},
	}, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Continuation.State != "end" || page.Items[0].Defect.Row != 100 {
		t.Fatalf("exact late-row %s defect: %+v", defect, page)
	}
	var binding struct {
		Items        []json.RawMessage `json:"items"`
		Continuation struct {
			State string `json:"state"`
		} `json:"continuation"`
	}
	if err := rpc.call(ctx, "data.show", map[string]any{
		"view": "operation", "detail": "run_binding", "operation_ref": map[string]any{"kind": "run_creation", "run_id": runID},
		"page": map[string]any{"limit": 100},
	}, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Items == nil || len(binding.Items) != 0 || binding.Continuation.State != "end" {
		t.Fatalf("rejected input retained an accepted run/pin binding: %+v", binding)
	}
}

func captureNumericRefusalInventory(t *testing.T, ctx context.Context, rpc *releaseRPCClient, hash string) map[string]json.RawMessage {
	t.Helper()
	result := map[string]json.RawMessage{}
	for _, view := range []string{"versions", "head_history"} {
		var page struct {
			Items        []json.RawMessage `json:"items"`
			Continuation struct {
				State string `json:"state"`
			} `json:"continuation"`
		}
		if err := rpc.call(ctx, "data.show", map[string]any{
			"view": view, "declaration": map[string]any{"flow_path": ".", "event": "item.registered"},
			"page": map[string]any{"limit": 100},
		}, &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != 0 || page.Continuation.State != "end" {
			t.Fatalf("refusal admitted %s: %+v", view, page)
		}
		body, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		result[view] = body
	}
	var runs json.RawMessage
	if err := rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash, "limit": 100}, &runs); err != nil {
		t.Fatal(err)
	}
	result["runs"] = runs
	return result
}
