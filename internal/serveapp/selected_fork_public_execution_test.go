package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
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
			{
				prior := buildSelectedAPICapabilities
				var once sync.Once
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					once.Do(func() {
						supervisors <- req.RuntimeSupervisor
					})
					caps, err := prior(owner, req)
					if prepare := caps.PrepareRunRuntime; prepare != nil {
						caps.PrepareRunRuntime = func(ctx context.Context, runID string) error {
							err := prepare(ctx, runID)
							if err != nil {
								t.Logf("W4 continue runtime preparation for %s: %v", runID, err)
							}
							return err
						}
					}
					if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); err == nil && ok {
						execute := executor.ExecuteSelectedContractRunFork
						executor.ExecuteSelectedContractRunFork = func(ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
							result, err := execute(ctx, request)
							if err != nil {
								t.Logf("selected changed-target owner error: %v", err)
							}
							return result, err
						}
						caps.RunFork = executor
					}
					return caps, err
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
			publication := newExactOriginPublicationProbe(t)
			writeSelectedForkAgentProofFixture(t, root, "loaded-decoy", "[receiver.closed]", "SOURCE CONFIGURATION MUST NOT EXECUTE IN THE FORK", "return {'text': 'Source-only delivery.', 'usage': {'input_tokens': 2, 'output_tokens': 2}}")
			rt, gatewayFault := startSelectedForkTransportProofRuntime(t, backend, root, options)
			supervisor := <-supervisors
			if reset {
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
				if _, _, err := predecessor.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: uuid.NewString()}); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("predecessor stop did not remain retired: %v", err)
				}
				expireWorkspaceProofResetTransportCache(t, selected.Idempotency(), selected.RuntimeDeps().EventStore, supervisor.processCapability)
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
				rt.WorkflowTargets = rt.Events.(pipeline.WorkflowTargetPersistenceReader)
				rt.Inbound = selected.RuntimeDeps().InboundStore
				rt.FlowRuns = rt.Events.(runlifecycle.LandingRunReader)
			}
			declarations := semanticview.AgentDeclarations(rt.Runtime.Options.WorkflowModule.SemanticSource())
			if len(declarations) != 1 || declarations[0].LocalID != "same-name" || declarations[0].Entry.Role != "loaded-decoy" {
				t.Fatalf("loaded source declaration is not the same-name decoy: %+v", declarations)
			}
			sourceBundle, present := semanticview.Bundle(rt.Runtime.Options.WorkflowModule.SemanticSource())
			if !present {
				t.Fatal("public selected fixture requires its captured source bundle")
			}
			sourceRoot, err := sourceBundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			sourceCoordinate, err := agenttopology.NewSourceCoordinate(sourceRoot.Admission())
			if err != nil {
				t.Fatal(err)
			}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-seed",
			})
			rootEvent := storetest.LoadCanonicalEventRecord(t, t.Context(), rt.Events, seed.EventID)
			if rootEvent.RunID() != seed.RunID || rootEvent.ExecutionMode() != rt.Runtime.ExecutionPosture.RootMode() {
				t.Fatalf("selected source changed its exact root mode: run=%s mode=%s", rootEvent.RunID(), rootEvent.ExecutionMode())
			}
			if nativeRead {
				if err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {
					t.Fatal(err)
				}
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
			requested := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-request",
			})
			waitWorkspaceProofPipelineHandoff(t, servedWorkspaceProofRuntime{Events: selected.RuntimeDeps().EventStore}, seed.RunID)
			frontier := requireWorkspaceProofWorkReadyEvent(t, rt.Endpoint, selected.RuntimeDeps().EventStore, seed.RunID)
			if err := waitExactOriginPublications(t.Context(), publication, seed.EventID, requested.EventID); err != nil {
				t.Fatal(err)
			}
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
			if replay := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params); !reflect.DeepEqual(fork, replay) {
				t.Fatalf("synchronous fork ACK changed on replay: first=%+v replay=%+v", fork, replay)
			}
			waitWorkspaceProofPipelineReceipt(t, rt.Events, fork.ForkRunID, activityidentity.ForkLineageEventID(fork.ForkRunID, frontier), "success")
			waitServedRunDeliveryQuiescence(t, func(ctx context.Context, runID string) (deliverylifecycle.RunSummary, error) {
				return storetest.ReadServedRunDeliverySummary(ctx, rt.Events, runID)
			}, fork.ForkRunID)
			waitWorkspaceProofPipelineHandoff(t, rt, fork.ForkRunID)
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
			ordinaryGrant, err := supervisor.CurrentRuntime().CurrentStartupGrantEvidence()
			if err != nil || ordinaryGrant.SelectedFork != nil {
				t.Fatalf("handoff lost its ordinary generation: %+v %v", ordinaryGrant, err)
			}
			selectedDiagnostics, ordinaryDiagnostics := 0, 0
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
				if receipt["run_id"] != fork.ForkRunID || receipt["parent_event_id"] != "" || receipt["lineage_disposition"] != "parentless" {
					t.Fatalf("selected lifecycle diagnostic %s lost its exact parentless producer authority: %+v", id, receipt)
				}
				provenance, _ := details["diagnostic_provenance"].(map[string]any)
				origin, _ := provenance["origin"].(map[string]any)
				switch origin["owner"] {
				case string(manager.LifecycleDiagnosticSelectedFork):
					selectedDiagnostics++
					if details["runtime_lineage_run_id"] != fork.ForkRunID || details["runtime_lineage_selected_fork_context"] != true || details["runtime_lineage_owner"] != runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner {
						t.Fatalf("selected diagnostic %s lost its exact owner: %+v", id, receipt)
					}
				case string(manager.LifecycleDiagnosticNormal):
					ordinaryDiagnostics++
					binding, _ := details["process_binding"].(map[string]any)
					if binding["generation_grant_id"] != ordinaryGrant.GrantID || binding["bundle_hash"] != fact.BundleHash() || details["runtime_lineage_selected_fork_context"] == true {
						t.Fatalf("ordinary diagnostic %s borrowed selected authority: %+v", id, receipt)
					}
				default:
					t.Fatalf("lifecycle diagnostic %s has no exact execution owner: %+v", id, receipt)
				}
			}
			if selectedDiagnostics == 0 || ordinaryDiagnostics == 0 {
				t.Fatalf("handoff did not retain both diagnostic owners: selected=%d ordinary=%d", selectedDiagnostics, ordinaryDiagnostics)
			}
			requireForkSourceSelfReleaseConservation(t, selected.RuntimeDeps().EventStore, seed.RunID, sourceBefore)
			completionCount := 1
			if nativeRead {
				completionCount = 2
			}
			requireForkLivePublicControls(t, rt.Endpoint, supervisor, selected.RuntimeDeps().EventStore, seed.RunID, sourceCoordinate, fork.ForkRunID, childHash)
			t.Run("live_public_readback", func(t *testing.T) {
				requireSelectedForkDeclaredAgentReads(t, rt.Endpoint, selected.RuntimeDeps().EventStore, rt.BundleHash, fork.ForkRunID, completionCount)
				requireSelectedForkMixedBusinessMutation(t, rt.Endpoint, selected.RuntimeDeps().EventStore, fork.ForkRunID, childEvent, rows["consumer"].ID)
			})
		})
	}
}

func requireForkSourceSelfReleaseConservation(t *testing.T, owner runtimebus.EventStore, runID string, before map[string]storetest.SelectedForkStorageTableSnapshot) {
	t.Helper()
	after := readWorkspaceProofSourceDomain(t, owner, runID)
	eventsBefore, eventsAfter := before["events"], after["events"]
	if len(eventsAfter.Rows) != len(eventsBefore.Rows)+1 || !reflect.DeepEqual(eventsBefore.Columns, eventsAfter.Columns) {
		t.Fatal("source handoff must add exactly one lifecycle receipt event")
	}
	var added []string
	for _, row := range eventsAfter.Rows {
		if !slices.Contains(eventsBefore.Rows, row) {
			added = append(added, row)
		}
	}
	if len(added) != 1 {
		t.Fatal("source handoff replaced an original event or added extra events")
	}
	decoded, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: eventsAfter.Columns, Rows: added})
	if err != nil {
		t.Fatal(err)
	}
	text := func(column string) string {
		value, err := decoded[0][column].Text()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(text("payload")), &payload); err != nil {
		t.Fatal(err)
	}
	details, _ := payload["details"].(map[string]any)
	if text("run_id") != runID || text("event_name") != "platform.runtime_log" || text("event_class") != "diagnostic_direct" ||
		details["action"] != "platform.agent_lifecycle_transition" ||
		details["previous_phase"] != "running" || details["phase"] != "registered" || details["run_mode"] != "stopped" ||
		details["previous_generation"] != details["generation"] || details["previous_epoch"] != details["runtime_epoch"] {
		t.Fatalf("source handoff added something other than its exact self-release: %+v", payload)
	}
	operations, err := storetest.DecodeSelectedForkSnapshotRows(readWorkspaceProofApplication(t, owner)["agent_lifecycle_operations"])
	if err != nil {
		t.Fatal(err)
	}
	matchedOperation := false
	for _, operation := range operations {
		id, err := operation["operation_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if id != details["operation_id"] {
			continue
		}
		kind, kindErr := operation["operation_kind"].Text()
		actor, actorErr := operation["agent_id"].Text()
		run, runErr := operation["run_id"].Text()
		if kindErr != nil || actorErr != nil || runErr != nil || kind != "self_release" || actor != details["agent_id"] || run != runID {
			t.Fatal("source lifecycle receipt is not its actor's native self-release operation")
		}
		matchedOperation = true
	}
	if !matchedOperation {
		t.Fatal("source self-release has no native operation")
	}
	diagnostics, err := storetest.ReadSelectedForkLifecycleDiagnosticStorage(t.Context(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	matched, original := 0, false
	for _, row := range diagnostics.Receipts {
		var receipt struct {
			EventID string         `json:"event_id"`
			RunID   string         `json:"run_id"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(row.Projection, &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.EventID == text("event_id") {
			if receipt.RunID != runID || details["outbox_id"] != row.OutboxID || !reflect.DeepEqual(receipt.Payload, payload) {
				t.Fatalf("source self-release differs from its native receipt: %+v", receipt)
			}
			matched++
		}
		prior, _ := receipt.Payload["details"].(map[string]any)
		if receipt.RunID == runID && prior["phase"] == "running" && prior["generation"] != nil &&
			reflect.DeepEqual(prior["generation"], details["generation"]) &&
			reflect.DeepEqual(prior["agent_identity"], details["agent_identity"]) && prior["agent_identity"] != nil &&
			reflect.DeepEqual(prior["process_binding"], details["process_binding"]) && prior["process_binding"] != nil {
			for _, encoded := range eventsBefore.Rows {
				rows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: eventsBefore.Columns, Rows: []string{encoded}})
				if err != nil {
					t.Fatal(err)
				}
				id, err := rows[0]["event_id"].Text()
				if err != nil {
					t.Fatal(err)
				}
				original = original || id == receipt.EventID
			}
		}
	}
	if matched != 1 || !original {
		t.Fatal("source self-release lacks its receipt or changed the original actor generation/process binding")
	}
	eventsAfter.Rows = slices.DeleteFunc(slices.Clone(eventsAfter.Rows), func(row string) bool { return row == added[0] })
	after["events"] = eventsAfter
	if !reflect.DeepEqual(before, after) {
		t.Fatal("source handoff changed original business rows or another source table")
	}
}

func requireForkLivePublicControls(t *testing.T, endpoint string, supervisor *processLifecycleSupervisor, owner runtimebus.EventStore, sourceRunID string, sourceCoordinate agenttopology.SourceCoordinate, childRunID, childHash string) {
	t.Helper()
	sourceHash := sourceCoordinate.BundleHash
	status := func(runID, want string) {
		var result struct {
			Run struct{ Status string } `json:"run"`
		}
		requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
		if result.Run.Status != want {
			t.Fatalf("run %s status=%s, want %s", runID, result.Run.Status, want)
		}
	}
	control := func(method, runID string) {
		var result struct{ OK bool }
		requireServedJSONRPCResult(t, endpoint, method, map[string]any{"run_id": runID, "idempotency_key": uuid.NewString()}, &result)
		if !result.OK {
			t.Fatalf("%s did not acknowledge %s", method, runID)
		}
	}
	conflict := func(runID, liveID, hash string) {
		before := readWorkspaceProofApplication(t, owner)
		err := requireServedJSONRPCError(t, endpoint, "run.continue", map[string]any{"run_id": runID, "idempotency_key": uuid.NewString()})
		if err.Data["code"] != apiv1.RunFlowAlreadyLiveCode {
			t.Fatalf("continue escaped live-run selection: %+v", err)
		}
		details, _ := err.Data["details"].(map[string]any)
		if details["live_run_id"] != liveID || !reflect.DeepEqual(before, readWorkspaceProofApplication(t, owner)) {
			t.Fatalf("conflicting continue changed state or lost the live sibling: %+v", err)
		}
		grant, grantErr := supervisor.CurrentRuntime().CurrentStartupGrantEvidence()
		if grantErr != nil || grant.BundleHash != hash {
			t.Fatalf("conflicting continue replaced the live bundle: %+v %v", grant, grantErr)
		}
	}
	status(sourceRunID, "paused")
	status(childRunID, "live")
	// Refusing removal of a bundle with live work must not retire its service.
	current, found, err := supervisor.processCapability.CurrentSourceSet(t.Context())
	if err != nil || !found {
		t.Fatalf("read live predecessor: found=%t err=%v", found, err)
	}
	target, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{sourceCoordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	beforeRuntime := supervisor.CurrentRuntime()
	grant, err := beforeRuntime.CurrentStartupGrantEvidence()
	if err != nil {
		t.Fatal(err)
	}
	supervisor.operationMu.Lock()
	_, removalErr := supervisor.replaceLiveSourceSet(t.Context(), supervisor.runtimeContexts, current, target, sourceHash, grant.RuntimeInstanceID)
	supervisor.operationMu.Unlock()
	if !errors.Is(removalErr, runcontrol.ErrNotPaused) {
		t.Fatalf("source removal did not retain its live-work refusal: %v", removalErr)
	}
	use, lookup, err := supervisor.runtimeContexts.AcquireBundleHash(t.Context(), childHash)
	if err != nil || !lookup.Loaded() || use == nil {
		t.Fatalf("refused source removal retired the serving runtime: lookup=%+v err=%v", lookup, err)
	}
	if use.Runtime() != beforeRuntime {
		t.Error("pre-shutdown refusal replaced the live runtime")
	}
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	unchanged, found, err := supervisor.processCapability.CurrentSourceSet(t.Context())
	if err != nil || !found || unchanged.Revision != current.Revision {
		t.Fatalf("refused source removal changed committed membership: %+v found=%t err=%v", unchanged, found, err)
	}
	conflict(sourceRunID, childRunID, childHash)
	control("run.pause", childRunID)
	status(childRunID, "paused")
	control("run.continue", sourceRunID)
	status(sourceRunID, "live")
	status(childRunID, "paused")
	conflict(childRunID, sourceRunID, sourceHash)
	control("run.pause", sourceRunID)
	control("run.continue", childRunID)
	status(sourceRunID, "paused")
	status(childRunID, "live")
	stop := map[string]any{"run_id": sourceRunID, "idempotency_key": "W4-unloaded-source-stop"}
	var stopped, replayed map[string]any
	requireServedJSONRPCResult(t, endpoint, "run.stop", stop, &stopped)
	requireServedJSONRPCResult(t, endpoint, "run.stop", stop, &replayed)
	if stopped["ok"] != true || !reflect.DeepEqual(stopped, replayed) {
		t.Fatalf("paused unloaded stop lost its canonical replay: first=%v replay=%v", stopped, replayed)
	}
	status(sourceRunID, "cancelled")
	status(childRunID, "live")
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
	event := storetest.LoadCanonicalEventRecord(t, context.Background(), owner, eventID)
	if event.RunID() != runID || event.ExecutionMode() != executionmode.Live {
		t.Fatalf("public MCP turn event run=%s mode=%s, want run=%s mode=live", event.RunID(), event.ExecutionMode(), runID)
	}
	rows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), owner, runID, "same-name")
	if err != nil {
		t.Fatal(err)
	}
	count, sessionID := 0, ""
	for _, row := range rows {
		if row.TriggerEventID != eventID || row.ExecutionMode != string(executionmode.Live) {
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
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal([]byte(responseRaw), &response); err != nil || len(response.Content) != 1 || response.Content[0].Type != "text" || response.Content[0].Text != "selected-only-mcp-resource verified" || len(calls) != 0 {
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
