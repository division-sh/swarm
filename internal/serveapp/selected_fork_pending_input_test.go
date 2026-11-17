package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/servedparity"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

type competingForkContinueReader struct {
	runlifecycle.LiveRunReader
	runlifecycle.LandingRunReader
	runcontrol.Store
	runID   string
	reads   int
	outcome runcontrol.StoreTransition
	err     error
}

type servedPostCutForkProof struct {
	Endpoint              string
	BundleHash            string
	ReadRunDeliveries     servedRunDeliveryReader
	ListWorkflowInstances func(context.Context, string) ([]pipeline.WorkflowInstance, error)
	CountRunNamedEvent    func(context.Context, string, string) (int, error)
	ReadCanonicalEvent    func(context.Context, string) (events.Event, bool, error)
	BeginSourceDispatch   func(context.Context, string) (func() error, error)
}

func startServedPostCutForkProof(t *testing.T, backend servedparity.Backend, sourceRoot string) servedPostCutForkProof {
	t.Helper()
	var reader receiverProofStateReader
	var deliveryReader servedRunDeliveryReader
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
		reader = requireReceiverProofStateReader(t, p.deps.EventStore)
		deliveryReader = p.deps.DeliveryStore.SummarizeRun
	})
	bundleHash := servedEventPublishFixtureBundleHash(t, sourceRoot)
	var endpoint string
	var runtime *runtimepkg.Runtime
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
		configPath := writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath)
		endpoint, runtime = startOwnedLifecycleFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath: configPath, SourceRoot: sourceRoot,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, Verbose: true,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}, executionposture.Live)
	case servedparity.BackendExplicitPostgres:
		configureCompletion2270PostgresFixture(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		configPath := writeServeRuntimeTestConfig(t)
		endpoint, runtime = startOwnedLifecycleFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath: configPath, SourceRoot: sourceRoot,
			StoreMode: "postgres", StoreModeSet: true,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
			SelfCheck: true, Verbose: true,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		}, executionposture.Live)
	default:
		t.Fatalf("unknown post-cut proof backend %q", backend)
	}
	if reader == nil || deliveryReader == nil || runtime == nil {
		t.Fatal("post-cut proof requires its selected reader, delivery owner and runtime")
	}
	return servedPostCutForkProof{
		Endpoint: endpoint, BundleHash: bundleHash,
		ReadRunDeliveries:     deliveryReader,
		ListWorkflowInstances: reader.ListWorkflowInstances,
		CountRunNamedEvent: func(ctx context.Context, runID, name string) (int, error) {
			return storetest.CountRunNamedEventStorage(ctx, reader, runID, name)
		},
		ReadCanonicalEvent: func(ctx context.Context, eventID string) (events.Event, bool, error) {
			return storetest.ReadCanonicalEventRecord(ctx, reader, eventID)
		},
		BeginSourceDispatch: func(ctx context.Context, runID string) (func() error, error) {
			lease, err := runtime.WorkOccurrence().BeginDispatch(correlation.WithRunID(ctx, runID))
			if err != nil {
				return nil, err
			}
			return lease.Done, nil
		},
	}
}

func TestServedForkWaitKeepsPostCutPublicationOnOriginalBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputOriginal)
			proof := startServedPostCutForkProof(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": proof.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "post-cut-seed",
			})
			waitServedRunDeliveryQuiescence(t, proof.ReadRunDeliveries, seed.RunID)
			requirePendingInputStateCount(t, proof.ListWorkflowInstances, seed.RunID, "ready", 2)
			requireServedOKJSONRPC(t, proof.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "post-cut-pause"})
			heldDone, err := proof.BeginSourceDispatch(t.Context(), seed.RunID)
			if err != nil {
				t.Fatalf("admit source work before fork wait: %v", err)
			}
			released := false
			defer func() {
				if !released {
					_ = heldDone()
				}
			}()
			forkDone := make(chan servedJSONRPCEnvelope, 1)
			go func() {
				defer close(forkDone)
				forkDone <- requestServedJSONRPCWithTimeout(t, proof.Endpoint, "run.fork", map[string]any{
					"source_run_id": seed.RunID, "fork_event_id": seed.EventID, "idempotency_key": "post-cut-fork",
				}, 15*time.Second)
			}()
			deadline := time.Now().Add(5 * time.Second)
			for {
				probeDone, err := proof.BeginSourceDispatch(t.Context(), seed.RunID)
				if errors.Is(err, worklifetime.ErrAdmissionFenced) {
					break
				}
				if err != nil {
					t.Fatalf("read source fork admission fence: %v", err)
				}
				if err := probeDone(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-forkDone:
					t.Fatal("fork returned before joining its admitted source work")
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("fork never fenced the paused source run")
				}
				time.Sleep(time.Millisecond)
			}
			point := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
				"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "post-cut"}, "idempotency_key": "post-cut-input",
			})
			if point.RunID != seed.RunID || point.NewRunCreated || len(point.Deliveries) != 2 {
				t.Fatalf("post-cut publication did not stay in the paused original: %+v", point)
			}
			requirePendingInputStateCount(t, proof.ListWorkflowInstances, seed.RunID, "ready", 2)
			if err := heldDone(); err != nil {
				t.Fatal(err)
			}
			released = true
			var response servedJSONRPCEnvelope
			select {
			case response = <-forkDone:
			case <-time.After(10 * time.Second):
				t.Fatal("fork did not join its source work and return")
			}
			if response.Error != nil {
				t.Fatalf("post-cut fork: %+v", response.Error)
			}
			var fork apiv1.RunForkExecutionResult
			if err := json.Unmarshal(response.Result, &fork); err != nil {
				t.Fatal(err)
			}
			if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.SourceRunStatus != "paused" {
				t.Fatalf("fork lost its paused original: %+v", fork)
			}
			copied, err := proof.CountRunNamedEvent(t.Context(), fork.ForkRunID, "work.first")
			if err != nil {
				t.Fatal(err)
			}
			original, found, err := proof.ReadCanonicalEvent(t.Context(), point.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if copied != 0 || !found || original.ID() != point.EventID || original.RunID() != seed.RunID {
				t.Fatalf("fixed child cut copied post-cut input: copied=%d original_found=%t original_id=%s original_run=%s", copied, found, original.ID(), original.RunID())
			}
			requireServedOKJSONRPC(t, proof.Endpoint, "run.pause", map[string]any{"run_id": fork.ForkRunID, "idempotency_key": "post-cut-child-pause"})
			requireServedOKJSONRPC(t, proof.Endpoint, "run.continue", map[string]any{"run_id": seed.RunID, "idempotency_key": "post-cut-original-continue"})
			waitServedRunDeliveryQuiescence(t, proof.ReadRunDeliveries, seed.RunID)
			requirePendingInputStateCount(t, proof.ListWorkflowInstances, seed.RunID, "done", 2)
			requirePendingInputPublicStates(t, proof.Endpoint, seed.RunID, 2)
		})
	}
}

func (r *competingForkContinueReader) LoadRunLiveScope(ctx context.Context, runID string) (runlifecycle.LiveRunScope, error) {
	r.reads++
	if r.reads == 2 {
		r.outcome, r.err = r.Store.ContinueRunControlOutcome(ctx, runcontrol.TransitionRequest{
			RunID: r.runID, Reason: "competing_fork_continue", ControlledBy: "test",
		})
	}
	return r.LiveRunReader.LoadRunLiveScope(ctx, runID)
}

// Source artifact admission is private setup, not proof of public publication.
// Publication, pause, fork and readback below use the assembled HTTP surface.
func TestSelectedForkPendingInputBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"unchanged", "compatible", "incompatible", "missing_input", "ambiguous_artifact", "explicit_child", "ordinary_child", "ordinary_root", "reference", "pause_resume", "fork_wait_publication", "competing_continue"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				var competitor *competingForkContinueReader
				if mode == "competing_continue" {
					previous := buildSelectedAPICapabilities
					buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
						caps, err := previous(owner, req)
						if err != nil {
							return caps, err
						}
						executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
						if !ok {
							t.Fatal("selected fork executor is required")
						}
						control, ok := executor.LiveRuns.(runcontrol.Store)
						if !ok {
							t.Fatal("selected run-control owner is required")
						}
						landing, ok := executor.LiveRuns.(runlifecycle.LandingRunReader)
						if !ok {
							t.Fatal("selected landing-run reader is required")
						}
						competitor = &competingForkContinueReader{LiveRunReader: executor.LiveRuns, LandingRunReader: landing, Store: control}
						executor.LiveRuns = competitor
						caps.RunFork = executor
						return caps, nil
					}
					t.Cleanup(func() { buildSelectedAPICapabilities = previous })
				}
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				variant := canonicalrouting.PendingInputOriginal
				if mode == "ordinary_root" {
					variant = canonicalrouting.PendingInputOrdinaryRoot
				}
				root := canonicalrouting.CopySelectedForkPendingInput(t, variant)
				publication := newExactOriginPublicationProbe(t)
				rt := startServedLiveFlowProofRuntimeFromSource(t, backend, root)
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "input-seed",
				})
				if competitor != nil {
					competitor.runID = seed.RunID
				}
				waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 2)
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "input-pause"})
				var releaseFork func()
				if mode == "fork_wait_publication" {
					owner, ok := selected.RuntimeDeps().PipelineObligations.(pipelineobligation.ParentTransitionOwner)
					if !ok {
						t.Fatal("selected store has no native source fork transition")
					}
					transition, err := owner.BeginSourceForkTransition(t.Context(), seed.RunID)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(transition.Done)
					workDone, err := rt.Runtime.WorkOccurrence().BeginRunTransition(t.Context(), seed.RunID)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(workDone)
					releaseFork = func() { workDone(); transition.Done() }
				}
				params := map[string]any{"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "input-pending"}
				wantDone := 2
				if mode == "reference" {
					params["source_event_id"] = seed.EventID
				}
				if mode == "ordinary_root" {
					wantDone = 1
				}
				if mode == "ordinary_child" {
					params["event_name"] = "child/work.first"
					wantDone = 1
				}
				if mode == "explicit_child" {
					var entity string
					if err := rt.DB.QueryRow(`SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path='child'`, seed.RunID).Scan(&entity); err != nil {
						t.Fatal(err)
					}
					params["target"] = map[string]any{"flow_instance": "child", "entity_id": entity}
					wantDone = 1
				}
				if mode == "ordinary_child" || mode == "ordinary_root" {
					if err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkReceiverApplication(t, rt)
					response := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", params)
					if response.Data["code"] != apiv1.EventNotDeclaredCode {
						t.Fatalf("private/non-input publication must fail admission: %+v", response)
					}
					if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
						t.Fatal("rejected publication changed application state")
					}
					requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 2)
					return
				}
				point := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, point.EventID)
				if mode == "explicit_child" && len(point.Deliveries) != 1 {
					t.Fatalf("explicit connected receiver restriction admitted %d deliveries, want one child", len(point.Deliveries))
				}
				requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 2)
				if mode == "ambiguous_artifact" {
					// Duplicate endpoint identity is rejected by canonical artifact
					// compilation, before a selectable artifact or fork can exist.
					if err := waitExactOriginPublications(t.Context(), publication, seed.EventID, point.EventID); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkReceiverApplication(t, rt)
					candidate := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputDuplicateEndpoint)
					_, err := contracts.LoadWorkflowContractBundleWithOverrides(repoRootForTest(), candidate, filepath.Join(repoRootForTest(), defaultPlatformSpecPath))
					if err == nil || !strings.Contains(err.Error(), "declared more than once") {
						t.Fatalf("ambiguous input artifact: %v", err)
					}
					if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
						t.Fatal("invalid artifact changed application state")
					}
					return
				}
				if mode == "pause_resume" {
					requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{"run_id": seed.RunID, "idempotency_key": "input-resume"})
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
					requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "done", 2)
					requirePendingInputPublicStates(t, rt.Endpoint, seed.RunID, 2)
					return
				}
				if mode == "fork_wait_publication" {
					replayed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					if point.RunID != seed.RunID || point.NewRunCreated || replayed.EventID != point.EventID || replayed.RunID != point.RunID {
						t.Fatalf("fenced publication lost exact queued identity: first=%+v replay=%+v", point, replayed)
					}
					releaseFork()
					requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{"run_id": seed.RunID, "idempotency_key": "input-resume"})
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
					requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "done", 2)
					requirePendingInputPublicStates(t, rt.Endpoint, seed.RunID, 2)
					return
				}
				forkParams := map[string]any{"source_run_id": seed.RunID, "fork_event_id": point.EventID, "idempotency_key": "input-fork"}
				if mode == "compatible" || mode == "incompatible" || mode == "missing_input" {
					variant := canonicalrouting.PendingInputCompatible
					if mode == "incompatible" {
						variant = canonicalrouting.PendingInputIncompatible
					}
					if mode == "missing_input" {
						variant = canonicalrouting.PendingInputMissingEndpoint
					}
					bundle := loadWorkflowValidationBundleAt(t, canonicalrouting.CopySelectedForkPendingInput(t, variant))
					fact, err := prepareServeSourceArtifact(servedControlProofAuthorActivityContext(t, rt), selected.SourceArtifactWriter(), bundle)
					if err != nil {
						t.Fatal(err)
					}
					if fact.BundleHash() == rt.BundleHash {
						t.Fatal("selected artifact did not change")
					}
					forkParams["bundle_hash"] = fact.BundleHash()
				}
				before := requireSelectedGlobalRunCount(t, rt.ReceiverStateReader)
				response := requestServedJSONRPC(t, rt.Endpoint, "run.fork", forkParams)
				if competitor != nil {
					if competitor.reads < 2 {
						t.Fatalf("competing Continue did not reach the paused fork cut: reads=%d", competitor.reads)
					}
					if competitor.outcome.Acknowledged {
						if response.Error == nil {
							t.Fatal("fork materialized after competing Continue committed")
						}
						after := requireSelectedGlobalRunCount(t, rt.ReceiverStateReader)
						if after != before {
							t.Fatalf("competing Continue minted a fork child: before=%d after=%d", before, after)
						}
						status, err := storetest.ReadRunStatusStorage(t.Context(), rt.ReceiverStateReader, seed.RunID)
						if err != nil || status != "live" {
							t.Fatalf("committed Continue lost its original run: status=%s err=%v", status, err)
						}
						waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
						requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "done", 2)
						requirePendingInputPublicStates(t, rt.Endpoint, seed.RunID, 2)
						return
					}
					if competitor.err == nil {
						t.Fatal("competing Continue neither committed nor refused")
					}
				}
				if mode == "incompatible" || mode == "missing_input" {
					if response.Error == nil {
						t.Fatal("invalid selected input was accepted")
					}
					after := requireSelectedGlobalRunCount(t, rt.ReceiverStateReader)
					status, err := storetest.ReadRunStatusStorage(t.Context(), rt.ReceiverStateReader, seed.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if after != before || status != "paused" {
						t.Fatalf("invalid preparation mutated fork/source: runs %d -> %d, status %s", before, after, status)
					}
					requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 2)
					return
				}
				if response.Error != nil {
					t.Fatalf("run.fork: %+v", response.Error)
				}
				var fork apiv1.RunForkExecutionResult
				if err := json.Unmarshal(response.Result, &fork); err != nil {
					t.Fatal(err)
				}
				waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, fork.ForkRunID, point.EventID)
				requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, fork.ForkRunID, "done", wantDone)
				requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, fork.ForkRunID, "ready", 2-wantDone)
				requirePendingInputPublicStates(t, rt.Endpoint, fork.ForkRunID, wantDone)
				copied, err := storetest.CountRunNamedEventStorage(t.Context(), rt.ReceiverStateReader, fork.ForkRunID, "work.seeded")
				if err != nil {
					t.Fatal(err)
				}
				if copied != 0 {
					t.Fatal("completed seed input was executed again")
				}
			})
		}
	}
}

func TestExplicitTargetRestrictsConnectedSiblingsBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputConnectedSiblings)
			publication := newExactOriginPublicationProbe(t)
			rt := startServedLiveFlowProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "target-seed",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 3)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "target-pause"})
			var childEntity, siblingEntity string
			if err := rt.DB.QueryRow(`SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path='child'`, seed.RunID).Scan(&childEntity); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path='sibling'`, seed.RunID).Scan(&siblingEntity); err != nil {
				t.Fatal(err)
			}
			params := map[string]any{
				"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "selected"},
				"target": map[string]any{"flow_instance": "child", "entity_id": childEntity}, "idempotency_key": "target-child",
			}
			point := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if len(point.Deliveries) != 1 {
				t.Fatalf("targeted publication deliveries = %+v, want one child", point.Deliveries)
			}
			assertNoUnselectedTargetActivation(t, rt, seed.RunID)
			var readback operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": point.EventID}, &readback)
			if len(readback.Deliveries) != 1 || readback.Deliveries[0].Target.FlowInstance != "child" || readback.Deliveries[0].Target.EntityID != childEntity {
				t.Fatalf("targeted event readback = %+v, want exact child only", readback.Deliveries)
			}
			replayed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if replayed.EventID != point.EventID || !reflect.DeepEqual(replayed.Deliveries, point.Deliveries) {
				t.Fatalf("idempotent replay = %+v, want %+v", replayed, point)
			}
			if err := waitExactOriginPublications(t.Context(), publication, seed.EventID, point.EventID); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkReceiverApplication(t, rt)
			invalid := map[string]any{
				"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "invalid"},
				"target": map[string]any{"flow_instance": "child", "entity_id": siblingEntity}, "idempotency_key": "target-invalid",
			}
			response := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", invalid)
			if response.Data["code"] != apiv1.EventNotDeclaredCode || !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
				t.Fatalf("invalid target changed application state: %+v", response)
			}
			requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{"run_id": seed.RunID, "idempotency_key": "target-resume"})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			_, child := requireReceiverTargetState(t, rt.ReceiverStateReader, seed.RunID, "child", "child", childEntity)
			_, sibling := requireReceiverTargetState(t, rt.ReceiverStateReader, seed.RunID, "sibling", "sibling", siblingEntity)
			if child.CurrentState != "done" || sibling.CurrentState != "ready" {
				t.Fatalf("unselected sibling executed: child=%s sibling=%s", child.CurrentState, sibling.CurrentState)
			}
			assertNoUnselectedTargetActivation(t, rt, seed.RunID)
			requirePendingInputStateCount(t, rt.ReceiverStateReader.ListWorkflowInstances, seed.RunID, "ready", 2)
		})
	}
}

func assertNoUnselectedTargetActivation(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	var count int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='on_demand'`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("targeted publication activated %d unselected template instances", count)
	}
}

func requireSelectedGlobalRunCount(t *testing.T, reader receiverProofStateReader) int {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(t.Context(), reader)
	if err != nil {
		t.Fatal(err)
	}
	runs, present := snapshot["runs"]
	if !present {
		t.Fatal("selected application snapshot has no global runs table")
	}
	return len(runs.Rows)
}

func requirePendingInputPublicStates(t *testing.T, endpoint, runID string, wantDone int) {
	t.Helper()
	var report struct {
		Run       operatorread.RunHeader                      `json:"run"`
		Instances []operatorread.RunInstanceOperationalStatus `json:"instances"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &report)
	if report.Run.RunID != runID || len(report.Instances) != 2 {
		t.Fatalf("public instance census: %+v", report)
	}
	done := 0
	for _, instance := range report.Instances {
		if instance.FlowInstance != runID && instance.FlowInstance != "child" {
			t.Fatalf("public input result acquired another instance: %+v", instance)
		}
		if instance.Stage == "done" {
			done++
		} else if instance.Stage != "ready" {
			t.Fatalf("unexpected public state: %+v", instance)
		}
	}
	if done != wantDone {
		t.Fatalf("public done count=%d want=%d", done, wantDone)
	}
}

func requirePendingInputStateCount(t *testing.T, list func(context.Context, string) ([]pipeline.WorkflowInstance, error), runID, state string, want int) {
	t.Helper()
	instances, err := list(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, instance := range instances {
		if instance.CurrentState == state {
			count++
		}
	}
	if count != want {
		t.Fatalf("run %s state %s count=%d, want %d", runID, state, count, want)
	}
}
