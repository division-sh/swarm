package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Artifact admission below is private setup. This proves the public fork and
// readback boundary, not the publication-to-fork journey owned by #2376/#2322.
func TestSelectedForkPublicChangedTargetExecutionBothStores(t *testing.T) {
	proveSelectedForkPublicChangedTargetExecutionBothStores(t, selectedForkProofOptions{})
}

func TestSelectedForkPublicChangedTargetAfterResetBothStores(t *testing.T) {
	proveSelectedForkPublicChangedTargetExecutionBothStores(t, selectedForkProofOptions{reset: true})
}

func TestSelectedForkPublicNativeMCPReadBothStores(t *testing.T) {
	proveSelectedForkPublicChangedTargetExecutionBothStores(t, selectedForkProofOptions{nativeRead: true})
}

type selectedForkProofOptions struct {
	reset       bool
	nativeRead  bool
	docker      bool
	gatewayLoss bool
}

func proveSelectedForkPublicChangedTargetExecutionBothStores(t *testing.T, options selectedForkProofOptions) {
	reset, nativeRead := options.reset, options.nativeRead
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			supervisors := make(chan *processLifecycleSupervisor, 1)
			if reset {
				prior := buildSelectedAPICapabilities
				var once sync.Once
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					once.Do(func() { supervisors <- req.RuntimeSupervisor })
					return prior(owner, req)
				}
				t.Cleanup(func() { buildSelectedAPICapabilities = prior })
			}
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			writeSelectedForkAgentProofFixture(t, root, "loaded-decoy", "[receiver.closed]", "SOURCE CONFIGURATION MUST NOT EXECUTE IN THE FORK", "return {'text': 'Source-only delivery.', 'usage': {'input_tokens': 2, 'output_tokens': 2}}")
			rt, gatewayFault := startSelectedForkTransportProofRuntime(t, backend, root, options)
			if reset {
				supervisor := <-supervisors
				predecessor := supervisor.selected
				params := map[string]any{"include_source_artifacts": false, "idempotency_key": "selected-reset"}
				response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
				if response.Error != nil {
					t.Fatalf("retained reset: %+v", response.Error)
				}
				supervisor.operationMu.Lock()
				successor := supervisor.selected
				supervisor.operationMu.Unlock()
				if predecessor == nil || successor == nil || predecessor == successor {
					t.Fatal("reset reused selected family instead of constructing a successor")
				}
				if _, _, err := predecessor.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: "late-predecessor"}); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("predecessor stop did not remain retired: %v", err)
				}
				expireWorkspaceProofResetTransportCache(t, selected.Idempotency(), selected.RuntimeDeps().EventStore)
				replay := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
				if replay.Error != nil || !reflect.DeepEqual(response.Result, replay.Result) {
					t.Fatalf("reset outcome replay: %+v", replay)
				}
				supervisor.operationMu.Lock()
				same := supervisor.selected == successor
				supervisor.operationMu.Unlock()
				if !same {
					t.Fatal("completed reset retry replaced its selected successor")
				}
				rt.Runtime = supervisor.CurrentRuntime()
				rt.Events = selected.RuntimeDeps().EventStore
				rt.Lifecycle = selected.RuntimeDeps().ManagerPersistenceRoles.LifecycleState
				rt.Observability = selected.Observability()
			}
			declarations := semanticview.AgentDeclarations(rt.Runtime.Options.WorkflowModule.SemanticSource())
			if len(declarations) != 1 || declarations[0].LocalID != "same-name" || declarations[0].Entry.Role != "loaded-decoy" {
				t.Fatalf("loaded source declaration is not the same-name decoy: %+v", declarations)
			}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-seed",
			})
			if nativeRead {
				t.Cleanup(func() {
					if !t.Failed() {
						return
					}
					snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected.RuntimeDeps().EventStore)
					if err != nil {
						t.Logf("selected failure storage snapshot: %v", err)
					} else {
						// Keep every child's physical rows, including a failed preparation
						// whose public operation never returned a fork run identity.
						for table, rows := range snapshot {
							t.Logf("selected failure storage %s: columns=%v rows=%v", table, rows.Columns, rows.Rows)
						}
					}
					rows, err := storetest.ReadWorkspaceEffectFailures(context.Background(), selected.RuntimeDeps().EventStore)
					if err != nil {
						t.Logf("selected effect failure evidence: %v", err)
						return
					}
					for _, row := range rows {
						t.Logf("selected effect: state=%s failure=%s", row.State, row.Failure)
					}
				})
			}
			waitWorkspaceProofPipelineHandoff(t, servedWorkspaceProofRuntime{Events: selected.RuntimeDeps().EventStore}, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-request",
			})
			waitWorkspaceProofPipelineHandoff(t, servedWorkspaceProofRuntime{Events: selected.RuntimeDeps().EventStore}, seed.RunID)
			frontier := requireWorkspaceProofWorkReadyEvent(t, rt.Endpoint, selected.RuntimeDeps().EventStore, seed.RunID)
			sourceBefore := readWorkspaceProofSourceDomain(t, selected.RuntimeDeps().EventStore, seed.RunID)
			targetRoot := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			body := "return {'text': 'Observed delivery.', 'usage': {'input_tokens': 1, 'output_tokens': 1}}"
			if nativeRead {
				data := filepath.Join(targetRoot, "consumer", "data")
				if err := os.MkdirAll(data, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(data, "selected-only.txt"), []byte("selected-only-mcp-resource\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				body = `results = input["tool_results"] or []
    if not results:
        tool = [tool for tool in input["tools"] if tool["name"] == "read_flow_data"][0]
        static_id = tool["schema"]["properties"]["static_id"]["enum"][0]
        return {"calls": [{"name": "read_flow_data", "arguments": {"kind": "static_file", "static_id": static_id}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    assert "selected-only-mcp-resource" in str(results), results
    return {"text": "selected-only-mcp-resource verified", "usage": {"input_tokens": 1, "output_tokens": 1}}`
			}
			writeSelectedForkAgentProofFixture(t, targetRoot, "observer", "[work.ready]", "Observe the explicitly delivered closure event.", body)
			if nativeRead {
				path := filepath.Join(targetRoot, "consumer", "agents.yaml")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(data, []byte("  flow_data_access: [selected-only.txt]\n")...), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			target := loadWorkflowValidationBundleAt(t, targetRoot)
			fact, err := prepareServeSourceArtifact(servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash), selected.SourceArtifactWriter(), target)
			if err != nil || fact.BundleHash() == rt.BundleHash {
				t.Fatalf("admit distinct target fixture: hash=%q err=%v", fact.BundleHash(), err)
			}
			params := map[string]any{
				"source_run_id": seed.RunID, "fork_event_id": frontier, "bundle_hash": fact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": "public-selected-changed-target",
			}
			if options.gatewayLoss {
				requireSelectedForkGatewayLossBeforeModel(t, rt.Endpoint, selected.RuntimeDeps().EventStore, params, gatewayFault, seed.RunID)
				if !reflect.DeepEqual(sourceBefore, readWorkspaceProofSourceDomain(t, selected.RuntimeDeps().EventStore, seed.RunID)) {
					t.Fatal("failed selected target changed source domain")
				}
				return
			}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("public fork lost execution/source evidence: %+v", fork)
			}
			childEvent := requireWorkspaceProofWorkReadyEvent(t, rt.Endpoint, selected.RuntimeDeps().EventStore, fork.ForkRunID)
			childHash, err := storetest.ReadSelectedForkRunBundleHash(context.Background(), selected.RuntimeDeps().EventStore, fork.ForkRunID)
			if err != nil || childHash != fact.BundleHash() {
				t.Fatalf("public fork selected a loaded source instead of target: hash=%q err=%v", childHash, err)
			}
			rows := readWorkspaceProofReceiverRows(t, rt.Events, fork.ForkRunID)
			requireSelectedForkMixedBusinessMutation(t, rt.Endpoint, selected.RuntimeDeps().EventStore, fork.ForkRunID, childEvent, rows["consumer"].ID)
			if nativeRead {
				requireSelectedForkNativeMCPRead(t, selected.RuntimeDeps().EventStore, fork.ForkRunID, childEvent)
			}
			diagnostics, err := storetest.ReadSelectedForkLifecycleDiagnosticStorage(context.Background(), selected.RuntimeDeps().EventStore, fork.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			if len(diagnostics.Receipts) == 0 {
				t.Fatal("selected agent lifecycle produced no diagnostic receipts")
			}
			for _, row := range diagnostics.Receipts {
				id := row.OutboxID
				var receipt map[string]any
				if len(row.Projection) != 0 {
					if err := json.Unmarshal(row.Projection, &receipt); err != nil {
						t.Fatal(err)
					}
				}
				if receipt == nil {
					t.Fatalf("selected execution returned before diagnostic %s projection", id)
				}
				requireWorkspaceProofDiagnosticEventCount(t, diagnostics.Logs, id, fork.ForkRunID, 1)
				payload, _ := receipt["payload"].(map[string]any)
				details, _ := payload["details"].(map[string]any)
				if receipt["run_id"] != fork.ForkRunID || receipt["parent_event_id"] != "" || receipt["lineage_disposition"] != "parentless" ||
					details["runtime_lineage_run_id"] != fork.ForkRunID || details["runtime_lineage_selected_fork_context"] != true ||
					details["runtime_lineage_owner"] != runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner {
					t.Fatalf("selected lifecycle diagnostic %s lost its exact parentless producer authority: %+v", id, receipt)
				}
			}
			if !reflect.DeepEqual(sourceBefore, readWorkspaceProofSourceDomain(t, selected.RuntimeDeps().EventStore, seed.RunID)) {
				t.Fatal("public selected execution changed source domain")
			}
			completionCount := 1
			if nativeRead {
				completionCount = 2
			}
			requireSelectedForkPublicControlBoundary(t, rt.Endpoint, selected.RuntimeDeps().EventStore, rt.BundleHash, fork.ForkRunID, childEvent, true, completionCount)
			t.Run("terminal_public_readback", func(t *testing.T) {
				requireSelectedForkDeclaredAgentReads(t, rt.Endpoint, selected.RuntimeDeps().EventStore, rt.BundleHash, fork.ForkRunID, completionCount)
				if !reflect.DeepEqual(sourceBefore, readWorkspaceProofSourceDomain(t, selected.RuntimeDeps().EventStore, seed.RunID)) {
					t.Fatal("terminal selected readback changed source domain")
				}
			})
		})
	}
}

func readWorkspaceProofSourceDomain(t *testing.T, owner runtimebus.EventStore, runID string) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkSourceDomain(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func requireSelectedForkNativeMCPRead(t *testing.T, owner runtimebus.EventStore, runID, eventID string) {
	t.Helper()
	rows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), owner, runID, "same-name")
	if err != nil {
		t.Fatal(err)
	}
	count, sessionID := 0, ""
	for _, row := range rows {
		if row.TriggerEventID != eventID || row.ExecutionMode != "mock" {
			continue
		}
		callsRaw, responseRaw, emittedRaw, currentSession := row.ToolCalls, row.ResponsePayload, row.EmittedEvents, row.SessionID
		if count == 0 {
			sessionID = currentSession
		}
		if sessionID == "" || currentSession != sessionID {
			t.Fatalf("selected transport changed session between completions: %q -> %q", sessionID, currentSession)
		}
		var calls []llm.ToolCall
		if err := json.Unmarshal([]byte(callsRaw), &calls); err != nil {
			t.Fatal(err)
		}
		switch count {
		case 0:
			if len(calls) != 1 || calls[0].Name != "read_flow_data" {
				t.Fatalf("selected native tool-call cardinality/authority: %s", callsRaw)
			}
			arguments, ok := calls[0].Arguments.(map[string]any)
			if !ok || arguments["kind"] != "static_file" {
				t.Fatalf("selected call lost its typed static-resource arguments: %s", callsRaw)
			}
		case 1:
			var response struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal([]byte(responseRaw), &response); err != nil || response.Text != "selected-only-mcp-resource verified" || len(calls) != 0 {
				t.Fatalf("selected child did not consume the target resource result: %s, calls=%s err=%v", responseRaw, callsRaw, err)
			}
		default:
			t.Fatal("selected transport persisted an extra completion")
		}
		var emitted []json.RawMessage
		if err := json.Unmarshal([]byte(emittedRaw), &emitted); err != nil || len(emitted) != 0 {
			t.Fatalf("read-only selected transport widened publication: %s, err=%v", emittedRaw, err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("selected native turn persisted %d completions, want exactly two", count)
	}
}

func requireSelectedForkExecutionRPCResult(t *testing.T, endpoint string, params map[string]any) apiv1.RunForkExecutionResult {
	t.Helper()
	// Full preparation/execution uses the same bounded operation budget as the
	// dev-scratch and pre-audit fork controls, including under race instrumentation.
	started := time.Now()
	response := requestServedJSONRPCWithTimeout(t, endpoint, "run.fork", params, 30*time.Second)
	t.Logf("run.fork preparation and execution completed in %s", time.Since(started))
	if response.Error != nil {
		t.Fatalf("run.fork error = %#v", response.Error)
	}
	var result apiv1.RunForkExecutionResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode run.fork result: %v", err)
	}
	return result
}
