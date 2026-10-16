package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func proveSelectedQ1SessionSiblingReforkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"sibling_then_refork", "refork_without_sibling", "frozen_source_advanced_cut"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				ctx := runForkTestContext(t)
				var selected startupownership.Store
				var owner SelectedContractExecutionOwner
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, owner = s, selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
					selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
				}
				var providerCalls, lookupCalls atomic.Int32
				lookup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					lookupCalls.Add(1)
					_, _ = w.Write([]byte(`{"status":"ready"}`))
				}))
				defer lookup.Close()
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["model"] != "request-A" {
						t.Errorf("fork provider request: model=%v err=%v", request["model"], err)
					}
					response := `{"model":"reported-Q1","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"lookup-1","type":"function","function":{"name":"lookup_data","arguments":"{\"query\":\"status\"}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`
					if providerCalls.Add(1)%2 == 0 {
						response = `{"model":"reported-Q1","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":13,"completion_tokens":4,"total_tokens":17}}`
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(response))
				}))
				defer provider.Close()
				repo := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: selectedForkFrameContracts(t, repo, lookup.URL), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := credentialStore.Set(ctx, selection.OpenAICompatibleCredentialEnv, "test-key"); err != nil {
					t.Fatal(err)
				}
				runID, eventID := uuid.NewString(), uuid.NewString()
				at := time.Unix(1700002203, 0).UTC()
				bundle, found := semanticview.Bundle(loaded.Source)
				if !found || bundle.SourceArtifact == nil {
					t.Fatal("missing admitted source artifact")
				}
				storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
				root := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
				input := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", []byte(`{"entity_id":"`+runID+`"}`), 0, runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), eventtest.RootRoutingSource(runID), at, executionmode.Live)
				route := selectedExecutionTestAgentRoute(t, runID, "test-agent", runID)
				route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: root.TemplateID, FlowInstance: root.InstancePath, EntityID: root.EntityID})
				storetest.CommitSemanticEventWithRoutes(t, ctx, selected, input, []events.DeliveryRoute{route}, "subscribed")
				sourceCtx := correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact)
				command := selectedExecutionSourceFlowCommand(t, sourceCtx, loaded, input, root)
				if committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(sourceCtx, command); err != nil || !committed.Acknowledged {
					t.Fatalf("source flow construction: %+v err=%v", committed, err)
				}
				storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
				firstCut := eventID
				if variant == "frozen_source_advanced_cut" {
					firstCut = uuid.NewString()
					later := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(firstCut, "task.finalized", "source-runtime", "", []byte(`{"entity_id":"`+runID+`"}`), 0, runID, input.Envelope(), input.RoutingSource(), at.Add(time.Second), executionmode.Live)
					storetest.CommitSemanticEvent(t, ctx, selected, later)
					storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
				}
				reads := selected.(operatorread.ConversationReader)
				headers := selected.(interface {
					LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				})
				sessionIDs, turnIDs := map[string]bool{}, map[string]bool{}
				requests := []SelectedContractExecutionRequest{}
				operations := []runfork.ForkOperationRecord{}
				childIDs, inputIDs, operationIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
				readTurns := func(id string) operatorread.OperatorConversationTurnListResult {
					t.Helper()
					list, err := reads.ListOperatorConversations(ctx, operatorread.OperatorConversationListOptions{RunID: id, AgentID: "test-agent"})
					if err != nil || len(list.Conversations) != 1 || list.Conversations[0].RunID != id {
						t.Fatalf("fork-local conversation: run=%s list=%+v err=%v", id, list, err)
					}
					page, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: list.Conversations[0].SessionID})
					if err != nil || len(page.Turns) != 2 {
						t.Fatalf("fork-local turns: run=%s page=%+v err=%v", id, page, err)
					}
					return page
				}
				execute := func(sourceID, point, sourceInput string) selectedQ1ExecutedFork {
					t.Helper()
					callsBefore, lookupBefore := providerCalls.Load(), lookupCalls.Load()
					operation := runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "bearer:q1-operator", IdempotencyKey: uuid.NewString(), TransportHash: "sha256:q1-" + sourceID + ":" + point,
						SourceRunID: sourceID, ForkEventID: point, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
						ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					}
					request := SelectedContractExecutionRequest{
						SourceRunID: sourceID, At: point, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
						ForkOperation:     &operation,
						ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
						AgentRuntime: SelectedContractAgentRuntimeOptions{Config: selectedForkAPIProviderConfig(selection.BackendOpenAICompatible, "request-A", provider.URL), ProviderCredentials: credentialStore,
							ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected), QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout},
					}
					result, err := executeLiveSelectedContractRunFork(ctx, request)
					if err != nil {
						t.Fatalf("selected session fork: source=%s point=%s provider_delta=%d lookup_delta=%d err=%v", sourceID, point, providerCalls.Load()-callsBefore, lookupCalls.Load()-lookupBefore, err)
					}
					if !result.Activation.Activated || result.Materialization.SourceRunID != sourceID || result.Materialization.ForkRunID == sourceID {
						t.Fatalf("selected session fork: source=%s point=%s result=%+v err=%v", sourceID, point, result, err)
					}
					inputID := waitSelectedQ1CompletedInput(t, ctx, selected, result, sourceInput)
					recorded, found, err := selected.(liveWinnerOperationReader).LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
					if err != nil || !found || recorded.Validate() != nil || recorded.Request.OperationID != operation.OperationID || recorded.Result == nil || recorded.Result.ForkRunID != result.Materialization.ForkRunID || recorded.Result.SourceFrozen != result.Activation.SourceFrozen || recorded.Result.SourceRunStatus != result.Activation.SourceRunStatus {
						t.Fatalf("lost exact durable activation ACK: %+v found=%t err=%v", recorded, found, err)
					}
					requests, operations = append(requests, request), append(operations, recorded)
					if childIDs[recorded.ForkRunID] || inputIDs[inputID] || operationIDs[recorded.Request.OperationID] {
						t.Fatal("independent selected forks shared a run, publication or durable operation identity")
					}
					childIDs[recorded.ForkRunID], inputIDs[inputID], operationIDs[recorded.Request.OperationID] = true, true, true
					if providerCalls.Load()-callsBefore != 2 || lookupCalls.Load()-lookupBefore != 1 {
						t.Fatalf("fork repeated or omitted work: provider=%d lookup=%d", providerCalls.Load()-callsBefore, lookupCalls.Load()-lookupBefore)
					}
					page := readTurns(result.Materialization.ForkRunID)
					if page.Conversation.SessionID == "" || sessionIDs[page.Conversation.SessionID] {
						t.Fatalf("fork reused a source/sibling session: %+v", page.Conversation)
					}
					sessionIDs[page.Conversation.SessionID] = true
					for _, turn := range page.Turns {
						if turn.TurnID == "" || turnIDs[turn.TurnID] || turn.TriggerEventID != inputID || turn.CompletionEvidence.Request.RequestedModel != "request-A" || turn.ResolvedModel != "reported-Q1" || !turn.ParseOK || turn.Failure != nil {
							t.Fatalf("fork reused or misattributed completion: %+v", turn)
						}
						turnIDs[turn.TurnID] = true
					}
					return selectedQ1ExecutedFork{ACK: result, InputID: inputID}
				}
				first := execute(runID, firstCut, eventID)
				firstID, firstPoint := first.ACK.Materialization.ForkRunID, first.InputID
				frozenSource, err := headers.LoadRunHeader(ctx, runID)
				if err != nil || frozenSource.Status != "forked" || frozenSource.ContinuedAsRunID != firstID || frozenSource.EndedAt == nil {
					t.Fatalf("first child did not establish exact source freeze: %+v %v", frozenSource, err)
				}
				sourceDomain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, runID)
				if err != nil {
					t.Fatal(err)
				}
				firstHeader, err := headers.LoadRunHeader(ctx, firstID)
				if err != nil {
					t.Fatal(err)
				}
				firstDomain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, firstID)
				if err != nil {
					t.Fatal(err)
				}
				before := readTurns(firstID)
				plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: firstID, At: firstPoint})
				if err != nil {
					t.Fatal(err)
				}
				var pending *runfork.RunForkPendingWork
				for i := range plan.PendingWork {
					if plan.PendingWork[i].EventID == firstPoint && plan.PendingWork[i].SubscriberType == "agent" {
						pending = &plan.PendingWork[i]
					}
				}
				if pending == nil || pending.Classification != runfork.RunForkPendingClassificationPending {
					t.Fatalf("past publication cut used current session/claim state: %+v", plan.PendingWork)
				}
				current, err := selected.(deliverylifecycle.Store).Snapshot(ctx, pending.DeliveryID)
				if err != nil || current.Status != deliverylifecycle.StatusDelivered || current.ClaimVersion == 0 {
					t.Fatalf("refork source did not actually execute/settle its session input: %+v err=%v", current, err)
				}
				children := int32(2)
				var siblingID string
				if variant != "refork_without_sibling" {
					sibling := execute(runID, eventID, eventID)
					siblingID = sibling.ACK.Materialization.ForkRunID
					if !sibling.ACK.Activation.SourceFrozen || sibling.ACK.Activation.SourceRunStatus != "forked" || sibling.ACK.Activation.BranchDivergence != nil {
						t.Fatalf("historical source was misreported as a new freeze/divergence: %+v", sibling.ACK.Activation)
					}
					if sibling.ACK.Activation.SourceAdvancedAfterFork != (variant == "frozen_source_advanced_cut") {
						t.Fatalf("historical admission invented or omitted real source advancement: %+v", sibling.ACK.Activation)
					}
					children++
				}
				grandchild := execute(firstID, firstPoint, firstPoint)
				if siblingID == firstID || grandchild.ACK.Materialization.ForkRunID == firstID || grandchild.ACK.Materialization.ForkRunID == siblingID {
					t.Fatal("independent sibling/refork operations shared a child run")
				}
				after := readTurns(firstID)
				if !reflect.DeepEqual(before.Turns, after.Turns) || before.Conversation.SessionID != after.Conversation.SessionID {
					t.Fatalf("refork changed retained source conversation: before=%+v after=%+v", before, after)
				}
				again, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: firstID, At: firstPoint})
				if err != nil || !reflect.DeepEqual(plan.PendingWork, again.PendingWork) || !reflect.DeepEqual(plan.ReplayResumeAdmission, again.ReplayResumeAdmission) {
					t.Fatalf("sibling/refork changed older-cut decisions: before=%+v after=%+v err=%v", plan, again, err)
				}
				for _, conservation := range []struct {
					id     string
					header operatorread.RunHeader
					domain map[string]storetest.SelectedForkStorageTableSnapshot
				}{{runID, frozenSource, sourceDomain}, {firstID, firstHeader, firstDomain}} {
					header, err := headers.LoadRunHeader(ctx, conservation.id)
					if err != nil || !reflect.DeepEqual(header, conservation.header) {
						t.Fatalf("sibling/refork rewrote source lifecycle: before=%+v after=%+v err=%v", conservation.header, header, err)
					}
					domain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, conservation.id)
					if err != nil || !reflect.DeepEqual(domain, conservation.domain) {
						t.Fatalf("sibling/refork changed source state/delivery domain: run=%s err=%v", conservation.id, err)
					}
				}
				beforeReplay, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				for i, request := range requests {
					retry := *request.ForkOperation
					retry.OperationID = uuid.NewString()
					request.ForkOperation = &retry
					replayed, err := executeLiveSelectedContractRunFork(ctx, request)
					if err != nil || !reflect.DeepEqual(replayed, selectedForkRecordedOutcome(operations[i])) {
						t.Fatalf("same key did not replay exact original ACK after completion: %+v err=%v", replayed, err)
					}
					retry.TransportHash += ":changed-meaning"
					var conflict *runfork.ForkOperationKeyConflictError
					if _, err := executeLiveSelectedContractRunFork(ctx, request); !errors.As(err, &conflict) {
						t.Fatalf("changed same-key meaning was not refused: %v", err)
					}
				}
				afterReplay, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(beforeReplay, afterReplay) {
					t.Fatalf("exact/hostile replay changed durable application evidence: %v", err)
				}
				if providerCalls.Load() != 2*children || lookupCalls.Load() != children || len(sessionIDs) != int(children) || len(turnIDs) != 2*int(children) {
					t.Fatalf("whole lineage accounting: provider=%d lookup=%d sessions=%d turns=%d", providerCalls.Load(), lookupCalls.Load(), len(sessionIDs), len(turnIDs))
				}
			})
		}
	}
}

type selectedQ1ExecutedFork struct {
	ACK     SelectedContractExecutionResult
	InputID string
}

// Activation acknowledges ownership. Completion is observed through the same
// native readers as retained execution, never written back into that ACK.
func waitSelectedQ1CompletedInput(t *testing.T, ctx context.Context, selected startupownership.Store, ack SelectedContractExecutionResult, sourceEventID string) string {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, selectedForkCapabilityProofQuiescenceTimeout)
	defer cancel()
	child := ack.Materialization.ForkRunID
	reader := selected.(operatorread.ObservabilityReader)
	headers := selected.(interface {
		LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	})
	for {
		header, err := headers.LoadRunHeader(wait, child)
		if err != nil || header.Failure != nil {
			logs, logErr := reader.ListOperatorRuntimeLogs(wait, operatorread.OperatorRuntimeLogListOptions{RunID: child, Order: "desc", Limit: 10})
			t.Fatalf("selected child failed: header=%+v failure=%+v err=%v logs=%+v log_error=%v", header, header.Failure, err, logs, logErr)
		}
		inputs, err := reader.ListOperatorEvents(wait, operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: child, EventName: "task.assigned"},
			Limit:  2, Order: "asc",
		})
		if err != nil || inputs.NextCursor != "" || len(inputs.Events) > 1 {
			t.Fatalf("selected input cardinality: %+v err=%v", inputs, err)
		}
		storage, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
		if err != nil {
			t.Fatal(err)
		}
		delivery, err := storetest.ReadManagedDeliveryStorage(wait, selected, child, "test-agent")
		if err != nil || delivery.AgentDeliveries > 1 || len(delivery.Failures) != 0 {
			t.Fatalf("selected input delivery: %+v err=%v", delivery, err)
		}
		if header.Status == "completed" && storage.State == "closed" && len(inputs.Events) == 1 && delivery.AgentDeliveries == 1 && delivery.Delivered == 1 {
			input := inputs.Events[0]
			prepared, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(wait, input.EventID)
			if err != nil || !found || input.RunID != child {
				t.Fatalf("selected input missing canonical publication: %+v found=%t err=%v", input, found, err)
			}
			lineage, bound := prepared.Event.Event().SelectedForkLineage()
			if !bound || lineage.SourceEventID() != sourceEventID {
				t.Fatalf("selected input lost immediate-parent lineage: %+v", lineage)
			}
			agents := 0
			for _, row := range input.Deliveries {
				if row.SubscriberType == "agent" && row.SubscriberID == "test-agent" {
					agents++
					snapshot, err := selected.(deliverylifecycle.Store).Snapshot(wait, row.DeliveryID)
					if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered || snapshot.ClaimVersion == 0 {
						t.Fatalf("selected input was not actually claimed and delivered: %+v err=%v", snapshot, err)
					}
				}
			}
			if agents != 1 {
				t.Fatalf("selected input has %d agent deliveries, want exactly one", agents)
			}
			return input.EventID
		}
		select {
		case <-wait.Done():
			t.Fatalf("selected child completion not observed: header=%+v storage=%+v inputs=%+v delivery=%+v cause=%v", header, storage, inputs, delivery, context.Cause(wait))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
