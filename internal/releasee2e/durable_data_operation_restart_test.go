package releasee2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

// The retained host uses the existing internal MockOnly composition. The CLI
// and authenticated API are real compiled/public surfaces, not RPC doubles.
func TestDurableDataOperationAggregatePublicRestartBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store receipt restart proof", goldenPostgresEnv)
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
			project := filepath.Join(root, "project")
			contracts := filepath.Join(project, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), contracts)
			raw, err := os.ReadFile(filepath.Join(contracts, "data/items.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			rows := strings.Split(strings.TrimSpace(string(raw)), "\n")
			writeReleaseFile(t, filepath.Join(project, "input.jsonl"), rows[2]+"\n")
			writeReleaseFile(t, filepath.Join(project, "invalid.jsonl"), "{\"unknown\":true}\n")
			writeReleaseFile(t, filepath.Join(project, ".swarm/swarm.yaml"), goldenRuntimeConfig(selected))
			writeReleaseFile(t, filepath.Join(project, "api-token"), goldenAPIToken+"\n")
			writeReleaseFile(t, filepath.Join(root, "home/.config/swarm/swarm.yaml"), "connection:\n  api_token_file: api-token\n")
			env := goldenProcessEnv(t, root, selected.passwordEnv, 0)
			assertGoldenProcessHasNoExternalExecutables(t, env)
			start := func() *releaseServeProcess {
				p := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, InternalMockLifecycleBinary: lifecycle,
					WorkingDir: project, Source: "contracts", ConfigPath: ".swarm/swarm.yaml", Store: backend,
					TokenFile: "api-token", Token: goldenAPIToken, Env: env, ShutdownGrace: goldenShutdownGrace})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := start()
			hash := goldenServedBundleHash(t, p.rpc, "mock_only")
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			operations, sources := createReceiptRestartSources(t, ctx, p.rpc, hash, rows)
			createdID, rejectedID := uuid.NewString(), uuid.NewString()
			create := func(id, path string, accepted bool) {
				result := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary,
					"run", "start", "--connect", p.apiBase, "--bundle-hash", hash, "--run-id", id,
					"--idempotency-key", "receipt-"+id, "--data", "item.registered="+path, "--no-follow")
				if accepted && (result.err != nil || !strings.Contains(result.output, "run_id="+id)) {
					t.Fatalf("compiled creation/reconstruction: %v\n%s", result.err, result.output)
				}
				if !accepted && (result.err == nil || !strings.Contains(result.output, "RUN_DATA_REJECTED")) {
					t.Fatalf("compiled rejection/reconstruction: %v\n%s", result.err, result.output)
				}
			}
			create(createdID, "input.jsonl", true)
			create(rejectedID, "invalid.jsonl", false)
			for _, id := range []string{createdID, rejectedID} {
				operations = append(operations, receiptRestartRef("run_creation", id))
			}
			// The source owner advances the head after CLI creation. Replay must
			// reconstruct the original request, not substitute this newer head.
			var current durabledata.VersionSummary
			if err := p.rpc.call(ctx, "data.show", map[string]any{"view": "version", "declaration": receiptRestartDeclaration(),
				"selector": map[string]any{"kind": "head"}}, &current); err != nil {
				t.Fatal(err)
			}
			latest := receiptRestartSource(t, ctx, p.rpc, hash, "import", durabledata.VersionHead(current.VersionID), rows[3]+"\n", "accepted")
			operations = append(operations, receiptRestartRef("source", latest.SourceInvocationID))
			prunes := createReceiptRestartPrunes(t, ctx, p.rpc, sources, latest, createdID)
			operations = append(operations, prunes...)
			before := captureReceiptRestartOperations(t, ctx, p.rpc, operations)
			var pruned durabledata.VersionSummary
			if err := p.rpc.call(ctx, "data.show", map[string]any{"view": "version", "declaration": receiptRestartDeclaration(),
				"selector": map[string]any{"kind": "version_id", "version_id": sources[0].Candidate.VersionID}}, &pruned); err != nil || pruned.PayloadState != "pruned" || pruned.Manifest.RowCount != 1 || pruned.VersionID != sources[0].Candidate.VersionID {
				t.Fatalf("pruned payload lost permanent version metadata: %+v error=%v", pruned, err)
			}
			rematerialized := receiptRestartSource(t, ctx, p.rpc, hash, "import", latest.Head.After, rows[0]+"\n", "accepted")
			if rematerialized.Candidate.VersionID != sources[0].Candidate.VersionID {
				t.Fatal("rematerialization did not restore the exact pruned version")
			}
			if after := captureReceiptRestartOperations(t, ctx, p.rpc, operations); !reflect.DeepEqual(before, after) {
				t.Fatalf("rematerialization changed permanent receipts: before=%s after=%s", before, after)
			}
			assertReceiptRestartBinding(t, before, createdID, rejectedID)
			assertReceiptRestartAuth(t, ctx, p.rpc, operations[0])
			if err := p.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("retained host shutdown: %v\n%s", err, p.output.String())
			}
			p = start()
			if after := captureReceiptRestartOperations(t, ctx, p.rpc, operations); !reflect.DeepEqual(before, after) {
				t.Fatalf("retained restart changed permanent operation facts: before=%s after=%s", before, after)
			}
			create(createdID, "input.jsonl", true)
			create(rejectedID, "invalid.jsonl", false)
			for _, source := range sources {
				input := rows[0] + "\n"
				if source.Outcome == "validation_rejected" {
					input = "{\"unknown\":true}\n"
				} else if source.SourceInvocationID == sources[1].SourceInvocationID {
					input = rows[1] + "\n"
				}
				params := receiptRestartSourceParams(hash, source.SourceInvocationID, source.ExpectedHead, input)
				var replay durabledata.SourceOperationResult
				if err := p.rpc.call(ctx, "data."+source.Operation, params, &replay); err != nil || !reflect.DeepEqual(source, replay) {
					t.Fatalf("permanent source replay: before=%+v after=%+v error=%v", source, replay, err)
				}
			}
			replayReceiptRestartPrunes(t, ctx, p.rpc, prunes)
			if after := captureReceiptRestartOperations(t, ctx, p.rpc, operations); !reflect.DeepEqual(before, after) {
				t.Fatalf("fresh compiled CLI/permanent replay changed receipts: before=%s after=%s", before, after)
			}
			var head durabledata.VersionSummary
			if err := p.rpc.call(ctx, "data.show", map[string]any{"view": "version", "declaration": receiptRestartDeclaration(),
				"selector": map[string]any{"kind": "head"}}, &head); err != nil || head.VersionID != rematerialized.Candidate.VersionID {
				t.Fatalf("replay moved current head: %+v error=%v", head, err)
			}
			if err := p.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("restarted host shutdown: %v\n%s", err, p.output.String())
			}
		})
	}
}

func receiptRestartDeclaration() map[string]any {
	return map[string]any{"flow_path": ".", "event": "item.registered"}
}

func receiptRestartRef(kind, id string) map[string]any {
	field := map[string]string{"source": "source_invocation_id", "prune": "prune_invocation_id", "run_creation": "run_id"}[kind]
	return map[string]any{"kind": kind, field: id}
}

func receiptRestartSourceParams(hash, id string, expected durabledata.ExpectedHead, input string) map[string]any {
	return map[string]any{"source_invocation_id": id, "bundle_hash": hash, "declaration": receiptRestartDeclaration(),
		"expected_head": expected, "input": map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte(input))}}
}

func receiptRestartSource(t *testing.T, ctx context.Context, rpc *releaseRPCClient, hash, operation string, expected durabledata.ExpectedHead, input, outcome string) durabledata.SourceOperationResult {
	t.Helper()
	var result durabledata.SourceOperationResult
	if err := rpc.call(ctx, "data."+operation, receiptRestartSourceParams(hash, uuid.NewString(), expected, input), &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != outcome || result.Operation != operation || result.BundleHash != hash {
		t.Fatalf("source %s/%s: %+v", operation, outcome, result)
	}
	return result
}

func createReceiptRestartSources(t *testing.T, ctx context.Context, rpc *releaseRPCClient, hash string, rows []string) ([]map[string]any, []durabledata.SourceOperationResult) {
	t.Helper()
	first := receiptRestartSource(t, ctx, rpc, hash, "import", durabledata.AbsentHead(), rows[0]+"\n", "accepted")
	second := receiptRestartSource(t, ctx, rpc, hash, "import", first.Head.After, rows[1]+"\n", "accepted")
	sources := []durabledata.SourceOperationResult{first, second}
	for _, operation := range []string{"check", "import"} {
		if operation == "check" {
			sources = append(sources, receiptRestartSource(t, ctx, rpc, hash, operation, second.Head.After, rows[0]+"\n", "accepted"))
		}
		sources = append(sources, receiptRestartSource(t, ctx, rpc, hash, operation, second.Head.After, "{\"unknown\":true}\n", "validation_rejected"))
		sources = append(sources, receiptRestartSource(t, ctx, rpc, hash, operation, durabledata.AbsentHead(), rows[0]+"\n", "head_conflict"))
	}
	refs := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		refs = append(refs, receiptRestartRef("source", source.SourceInvocationID))
	}
	return refs, sources
}

func createReceiptRestartPrunes(t *testing.T, ctx context.Context, rpc *releaseRPCClient, sources []durabledata.SourceOperationResult, latest durabledata.SourceOperationResult, runID string) []map[string]any {
	t.Helper()
	var binding durabledata.PageResult[durabledata.RunCreationDataItem]
	readReceiptRestartDetail(t, ctx, rpc, receiptRestartRef("run_creation", runID), "run_binding", &binding)
	if len(binding.Items) != 2 || binding.Items[0].Pin == nil {
		t.Fatalf("created run must retain exact pin/import group: %+v", binding)
	}
	refs := []map[string]any{}
	for _, test := range []struct {
		version durabledata.VersionID
		head    durabledata.ExpectedHead
		outcome string
	}{
		{sources[0].Candidate.VersionID, latest.Head.After, "pruned"},
		{sources[0].Candidate.VersionID, latest.Head.After, "already_pruned"},
		{sources[0].Candidate.VersionID, durabledata.AbsentHead(), "head_conflict"},
		{latest.Candidate.VersionID, latest.Head.After, "refused_current"},
		{binding.Items[0].Pin.VersionID, latest.Head.After, "refused_pinned"},
		{durabledata.VersionID("resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), latest.Head.After, "rejected"},
	} {
		var result durabledata.PruneOperationResult
		params := map[string]any{"prune_invocation_id": uuid.NewString(), "declaration": receiptRestartDeclaration(),
			"version_id": test.version, "expected_head": test.head}
		if err := rpc.call(ctx, "data.prune", params, &result); err != nil || result.Outcome != test.outcome {
			t.Fatalf("prune %s: %+v error=%v", test.outcome, result, err)
		}
		var replay durabledata.PruneOperationResult
		if err := rpc.call(ctx, "data.prune", params, &replay); err != nil || !reflect.DeepEqual(result, replay) {
			t.Fatalf("prune permanent replay: before=%+v after=%+v error=%v", result, replay, err)
		}
		refs = append(refs, receiptRestartRef("prune", result.PruneInvocationID))
	}
	return refs
}

func readReceiptRestartDetail(t *testing.T, ctx context.Context, rpc *releaseRPCClient, ref map[string]any, detail string, result any) {
	t.Helper()
	params := map[string]any{"view": "operation", "operation_ref": ref, "detail": detail}
	if detail != "summary" && detail != "request_binding" {
		params["page"] = map[string]any{"limit": 100}
	}
	if err := rpc.call(ctx, "data.show", params, result); err != nil {
		t.Fatal(err)
	}
}

func captureReceiptRestartOperations(t *testing.T, ctx context.Context, rpc *releaseRPCClient, refs []map[string]any) map[string]json.RawMessage {
	t.Helper()
	result := map[string]json.RawMessage{}
	for index, ref := range refs {
		var summary durabledata.OperationSummary
		readReceiptRestartDetail(t, ctx, rpc, ref, "summary", &summary)
		details := []string{"summary"}
		switch ref["kind"] {
		case "source":
			details = append(details, "defects", "delta_added", "delta_removed", "delta_changed")
		case "run_creation":
			details = append(details, "request_binding", "child_evaluations", "child_defects", "run_binding")
		case "prune":
			if summary.Prune.Defects != nil {
				details = append(details, "defects")
			}
			if summary.Prune.Pins != nil {
				details = append(details, "pins")
			}
		}
		for _, detail := range details {
			var body json.RawMessage
			readReceiptRestartDetail(t, ctx, rpc, ref, detail, &body)
			if len(body) == 0 || string(body) == "null" {
				t.Fatalf("missing permanent operation %d/%s", index, detail)
			}
			result[fmt.Sprintf("%d/%s", index, detail)] = body
		}
	}
	return result
}

func replayReceiptRestartPrunes(t *testing.T, ctx context.Context, rpc *releaseRPCClient, refs []map[string]any) {
	t.Helper()
	for _, ref := range refs {
		var original durabledata.OperationSummary
		readReceiptRestartDetail(t, ctx, rpc, ref, "summary", &original)
		if original.Prune == nil {
			t.Fatalf("missing permanent prune: %+v", original)
		}
		var replay durabledata.PruneOperationResult
		params := map[string]any{"prune_invocation_id": original.Prune.PruneInvocationID, "declaration": receiptRestartDeclaration(),
			"version_id": original.Prune.VersionID, "expected_head": original.Prune.ExpectedHead}
		if err := rpc.call(ctx, "data.prune", params, &replay); err != nil || !reflect.DeepEqual(*original.Prune, replay) {
			t.Fatalf("retained prune replay re-evaluated mutable facts: before=%+v after=%+v error=%v", original.Prune, replay, err)
		}
	}
}

func assertReceiptRestartBinding(t *testing.T, records map[string]json.RawMessage, createdID, rejectedID string) {
	t.Helper()
	found := map[string]bool{}
	for key, raw := range records {
		if !strings.HasSuffix(key, "/request_binding") {
			continue
		}
		var binding durabledata.RunCreationRequestBinding
		if err := json.Unmarshal(raw, &binding); err != nil || binding.Validate() != nil || len(binding.Imports) != 1 {
			t.Fatalf("exact permanent request binding: %s error=%v", raw, err)
		}
		if strings.Contains(string(raw), "content_base64") || strings.Contains(string(raw), "actor") || strings.Contains(string(raw), "unknown") {
			t.Fatalf("request binding leaked payload or actor: %s", raw)
		}
		found[binding.RunID] = true
	}
	if !found[createdID] || !found[rejectedID] {
		t.Fatalf("missing created/rejected permanent request bindings: %+v", found)
	}
}

func assertReceiptRestartAuth(t *testing.T, ctx context.Context, rpc *releaseRPCClient, ref map[string]any) {
	t.Helper()
	unauthenticated := *rpc
	unauthenticated.token, unauthenticated.onFailure = "invalid-token", nil
	var result any
	if err := unauthenticated.call(ctx, "data.show", map[string]any{"view": "operation", "operation_ref": ref, "detail": "summary"}, &result); err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("operation readback did not refuse public authentication: %v", err)
	}
}
