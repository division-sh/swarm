package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Ordinary materialization retains its exact blockers. Selected preparation
// supplies the admitted historical evidence for the original success oracles.
func TestServedJoinWriterCompletedHistoryRefusalBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, false, false)
}

func TestServedJoinWriterCompletedHistoryRefusalSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, true, false)
}

func TestServedJoinWriterForkRetainedGenerationsBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, false, true)
}

func TestServedJoinWriterForkRetainedGenerationsSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, true, true)
}

func testServedJoinWriterCompletedHistoryRefusal(t *testing.T, separateCheckpoint, success bool) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkLoopRetainedJoin(t)
			if separateCheckpoint {
				root = canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint(t)
			}
			bundle := loadWorkflowValidationBundleAt(t, root)
			carriage, err := semanticview.CompileOriginalLoopCarriage(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "member-one"}, "idempotency_key": "retained-join-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			rows, err := rt.DB.Query(`SELECT CAST(d.failure AS TEXT) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1`, started.RunID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var failure string
				if err := rows.Scan(&failure); err != nil {
					t.Fatal(err)
				}
				t.Errorf("ordinary join writer dead-lettered before fork: %s", failure)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if t.Failed() {
				t.FailNow()
			}
			waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			first, joins := readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if len(joins) != 1 || first.Attempt != 1 {
				t.Fatalf("first ordinary join: loop=%+v joins=%+v", first, joins)
			}
			firstRef := joins[0].JoinRef()
			nativeEvents, hasNativeEvents := rt.ReceiverStateReader.(runtimebus.EventStore)
			if !hasNativeEvents || nativeEvents == nil {
				t.Fatal("retained join outcome requires its original selected event reader")
			}
			nativeHandoff := servedWorkspaceProofRuntime{Events: nativeEvents}
			firstCtx, cancelFirst := context.WithTimeout(t.Context(), servedProofPollDeadline)
			firstWaitErr := waitServedRetainedJoinOutcome(firstCtx, started.RunID, firstRef, func() (loopruntime.Activation, []joinruntime.Activation) {
				return readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			})
			cancelFirst()
			if firstWaitErr != nil {
				t.Fatalf("first exact retained join outcome: %v", firstWaitErr)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)
			first, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
				"payload": map[string]any{"revision_id": first.RevisionID, "token": "member-one"}, "idempotency_key": "retained-join-repeat",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			current, sourceJoins := readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {
				t.Fatalf("ordinary repeat lost retained joins: loop=%+v joins=%+v", current, sourceJoins)
			}
			var currentRef timeridentity.JoinRef
			currentMatches := 0
			for _, join := range sourceJoins {
				if join.Generation() == current.Generation() {
					currentRef = join.JoinRef()
					currentMatches++
				}
			}
			if currentMatches != 1 {
				t.Fatalf("current retained join reference is not unique: %d", currentMatches)
			}
			currentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)
			currentWaitErr := waitServedRetainedJoinOutcome(currentCtx, started.RunID, currentRef, func() (loopruntime.Activation, []joinruntime.Activation) {
				return readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			})
			cancelCurrent()
			if currentWaitErr != nil {
				t.Fatalf("current exact retained join outcome: %v", currentWaitErr)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)
			current, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			for _, join := range sourceJoins {
				if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete || len(join.Outputs) != 1 || !join.TimerCancelled || join.OutcomePending || !join.OutcomeFired {
					t.Fatalf("ordinary join did not complete truthfully: %+v", join)
				}
			}
			if !separateCheckpoint {
				rows, err := rt.DB.Query(`SELECT payload FROM events WHERE run_id=$1 AND event_name='join.observed'`, started.RunID)
				if err != nil {
					t.Fatal(err)
				}
				observed := map[string]int{}
				for rows.Next() {
					var raw []byte
					if err := rows.Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var payload struct {
						RevisionID string `json:"revision_id"`
						Completed  int    `json:"completed"`
					}
					if err := json.Unmarshal(raw, &payload); err != nil || payload.Completed != 1 {
						t.Fatalf("join outcome lost typed completion: %s %v", raw, err)
					}
					observed[payload.RevisionID]++
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(observed, map[string]int{first.RevisionID: 1, current.RevisionID: 1}) {
					t.Fatalf("join outcomes substituted captured generation: %#v", observed)
				}
			}
			if separateCheckpoint {
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "checkpoint.requested", "run_id": started.RunID, "source_event_id": started.EventID,
					"payload": map[string]any{"revision_id": current.RevisionID}, "idempotency_key": "retained-join-checkpoint",
				})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
				waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			}
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='join.observed' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			var frontierRevision int64
			if err := rt.DB.QueryRow(`SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND present`, started.RunID, frontier).Scan(&frontierRevision); err != nil || frontierRevision <= 0 {
				t.Fatalf("missing fixed-revision frontier: revision=%d err=%v", frontierRevision, err)
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: started.RunID, At: frontier, OriginalLoopCarriage: carriage}
			if success {
				requireServedRetainedJoinForkSuccess(t, rt, selected, request, current, sourceJoins)
				return
			}
			// Delivery settlement does not join the lifecycle candidate executor. Stop
			// the real source writers before measuring the store-only refusal.
			if err := rt.Runtime.Shutdown(); err != nil {
				t.Fatalf("join source runtime before refusal snapshot: %v", err)
			}
			before := snapshotForkReceiverApplication(t, rt)
			owner, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork owner")
			}
			const refusal = "fork materialization requires execution-ready plan; blockers: timer_history_unproven"
			for attempt := 0; attempt < 2; attempt++ {
				child, err := owner.Materialize(servedControlProofAuthorActivityContext(t, rt), request)
				if err == nil || err.Error() != refusal {
					t.Fatalf("completed ordinary join history must retain its exact policy blocker: result=%+v err=%v", child, err)
				}
				if child.ForkRunID != "" || child.MaterializedEntityCount != 0 {
					t.Fatalf("refused materialization returned child work: %+v", child)
				}
				if child.SourceRunID != started.RunID || child.ForkPoint.EventID != frontier || child.ForkPoint.Revision != frontierRevision || child.ExecutionReady || !child.DeliveryResumeBlocked {
					t.Fatalf("refusal lost exact selected-revision admission: %+v", child)
				}
				var codes []string
				for _, blocker := range child.UnsupportedBlockers {
					codes = append(codes, blocker.Code)
				}
				if !reflect.DeepEqual(codes, []string{runfork.RunForkBlockerTimerHistoryUnproven}) {
					t.Fatalf("refusal lost canonical blocker evidence: %+v", child.UnsupportedBlockers)
				}
				if after := snapshotForkReceiverApplication(t, rt); !reflect.DeepEqual(before, after) {
					for table, rows := range after {
						if !reflect.DeepEqual(before[table], rows) {
							t.Errorf("refused join-history materialization changed table %s", table)
						}
					}
					t.Fatal("refused join-history materialization changed application state")
				}
			}
		})
	}
}

func requireServedRetainedJoinForkSuccess(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, request runfork.RunForkMaterializeRequest,
	current loopruntime.Activation, sourceJoins []joinruntime.Activation) {
	t.Helper()
	owner, ok := selected.RunFork()
	if !ok {
		t.Fatal("retained join success requires the original selected store owner")
	}
	ctx := servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash)
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: request.SourceRunID, At: request.At, ExpectedBundleHash: rt.BundleHash,
		ContractSelection: selection,
		ForkOperation: &runfork.ForkOperationRequest{
			OperationID: uuid.NewString(), Actor: "retained-join-proof", IdempotencyKey: "retained-join-materialization",
			TransportHash: "retained-join-proof", SourceRunID: request.SourceRunID, ForkEventID: request.At,
			TargetBundleHash: rt.BundleHash, ContractSelection: selection,
		},
		SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
			RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
		},
		AgentRuntime: rt.ForkRuntime,
	})
	if err != nil {
		t.Fatalf("prepare the original completed join success: %v", err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Errorf("join retained join preparation: %v", err)
		}
	})
	admitted, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	materializer, ok := owner.Availability().(runforkexecution.SelectedContractForkLifecycle)
	if !ok {
		t.Fatal("retained join success requires the canonical selected materialization owner")
	}
	sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, request.SourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceBefore) != 7 {
		t.Fatalf("retained join source witness lost table families: %+v", sourceBefore)
	}
	before := snapshotForkReceiverApplication(t, rt)
	child, err := materializer.MaterializeRunForkForSelectedContractExecution(ctx, admitted)
	if err != nil {
		if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
			t.Fatal("failed join-history materialization changed application state")
		}
		t.Fatalf("materialize the original completed join success: %v", err)
	}
	childLoop, childJoins := readNativeRetainedRootJoins(t, rt.ReceiverStateReader, child.ForkRunID)
	wantLoop, err := loopruntime.Fork(current, child.ForkRunID, child.ForkRunID)
	if err != nil || !reflect.DeepEqual(childLoop, wantLoop) || childLoop.Attempt != 2 || len(childJoins) != len(sourceJoins) {
		t.Fatalf("materialized join/loop ownership: loop=%+v joins=%+v error=%v", childLoop, childJoins, err)
	}
	for _, source := range sourceJoins {
		want, err := loopruntime.ForkGeneration(source.JoinRef().Generation(), child.ForkRunID, child.ForkRunID)
		if err != nil {
			t.Fatal(err)
		}
		matches := 0
		for _, projected := range childJoins {
			if projected.JoinRef().Generation() != want {
				continue
			}
			matches++
			if projected.JoinRef().Node() != source.JoinRef().Node() || projected.Key() == source.Key() ||
				!reflect.DeepEqual(projected.Members, source.Members) || !reflect.DeepEqual(projected.Outputs, source.Outputs) ||
				projected.Status != source.Status || projected.CloseReason != source.CloseReason || projected.TimerCancelled != source.TimerCancelled ||
				projected.OutcomePending != source.OutcomePending || projected.OutcomeFired != source.OutcomeFired ||
				projected.TimerEventType() != source.TimerEventType() || projected.JoinRef().Declaration() != source.JoinRef().Declaration() ||
				!reflect.DeepEqual(projected.MemberCount, source.MemberCount) || !projected.DeadlineAt.Equal(source.DeadlineAt) ||
				!projected.ArmedAt.Equal(source.ArmedAt) || !projected.FireAt.Equal(source.FireAt) {
				t.Fatalf("retained join evidence changed: source=%+v child=%+v", source, projected)
			}
		}
		if matches != 1 {
			t.Fatalf("expected exactly one historical child generation %+v, got%d", want, matches)
		}
	}
	sourceAfterFirst, sourceFirstErr := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, request.SourceRunID)
	if sourceFirstErr != nil {
		t.Fatalf("read source after first retained join materialization: %v", sourceFirstErr)
	}
	operation, semanticHash, err := admitted.ForkOperation.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	ack, found, err := materializer.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if err != nil || !found || ack.Validate() != nil || ack.Status != runfork.ForkOperationMaterialized || ack.Result != nil ||
		ack.ForkRunID != child.ForkRunID || child.SelectedContractBinding == nil || ack.BindingID != child.SelectedContractBinding.BindingID ||
		ack.SemanticHash != semanticHash || !reflect.DeepEqual(ack.Request, operation) {
		t.Fatalf("retained join lacks its exact materialized operation ACK: %+v err=%v", ack, err)
	}
	childBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, ack.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	_, freshErr := materializer.MaterializeRunForkForSelectedContractExecution(ctx, admitted)
	var replay *runfork.ForkOperationReplayRequiredError
	if freshErr == nil {
		t.Fatal("fresh materialization silently reused a durable operation")
	} else if !errors.As(freshErr, &replay) || !failures.OnlyBranches(freshErr, func(branch error) bool { return branch == replay }) ||
		replay.OperationID != operation.OperationID || replay.ForkRunID != ack.ForkRunID {
		t.Fatalf("fresh materialization lost its exact operation replay refusal: %T %v", freshErr, freshErr)
	}
	readback := admitted
	readback.ForkOperation = nil
	again, err := materializer.RequireMaterializedRunForkForSelectedContractExecution(ctx, ack.ForkRunID, readback)
	_, repeated := readNativeRetainedRootJoins(t, rt.ReceiverStateReader, child.ForkRunID)
	sourceAfter, sourceErr := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, request.SourceRunID)
	if err != nil {
		t.Errorf("repeated retained join materialization: %v", err)
	}
	if again.ForkRunID != child.ForkRunID {
		t.Errorf("repeated materialization changed child identity: first=%s repeated=%s", child.ForkRunID, again.ForkRunID)
	}
	if again.ForkRunStatus != runfork.RunForkMaterializedStatus {
		t.Errorf("materialization readback activated the child: %+v", again)
	}
	if err := compareRetainedJoinActivations(childJoins, repeated); err != nil {
		t.Errorf("repeated materialization changed child join rows: %v; first=%+v repeated=%+v", err, childJoins, repeated)
	}
	if sourceErr != nil {
		t.Fatalf("read source after repeated retained join materialization: %v", sourceErr)
	}
	if !reflect.DeepEqual(sourceBefore, sourceAfter) {
		for table, original := range sourceBefore {
			current, present := sourceAfter[table]
			if !present || !reflect.DeepEqual(original, current) {
				t.Errorf("retained join source table changed: table=%s present=%t columns=%q -> %q rows=%q -> %q; after-first=%+v",
					table, present, original.Columns, current.Columns, original.Rows, current.Rows, sourceAfterFirst[table])
			}
		}
		for table := range sourceAfter {
			if _, present := sourceBefore[table]; !present {
				t.Errorf("retained join source gained table family: %s %+v", table, sourceAfter[table])
			}
		}
	}
	stableACK, found, ackErr := materializer.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if ackErr != nil || !found || !reflect.DeepEqual(ack, stableACK) {
		t.Errorf("materialization replay changed its immutable ACK: %+v -> %+v err=%v", ack, stableACK, ackErr)
	}
	childAfter, childErr := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, ack.ForkRunID)
	if childErr != nil || !reflect.DeepEqual(childBefore, childAfter) {
		t.Errorf("materialization-only readback executed child work: %+v -> %+v err=%v", childBefore, childAfter, childErr)
	}
	if err := prepared.Close(); err != nil {
		t.Fatalf("join retained join preparation: %v", err)
	}
}

func compareRetainedJoinActivations(first, repeated []joinruntime.Activation) error {
	if len(first) != len(repeated) {
		return fmt.Errorf("join cardinality changed: %d -> %d", len(first), len(repeated))
	}
	index := func(activations []joinruntime.Activation) (map[string]joinruntime.Activation, error) {
		byKey := make(map[string]joinruntime.Activation, len(activations))
		for _, activation := range activations {
			key := activation.Key()
			if key == "" {
				return nil, errors.New("retained join has an empty activation key")
			}
			if _, duplicate := byKey[key]; duplicate {
				return nil, fmt.Errorf("duplicate retained join activation key %q", key)
			}
			byKey[key] = activation
		}
		return byKey, nil
	}
	before, err := index(first)
	if err != nil {
		return err
	}
	after, err := index(repeated)
	if err != nil {
		return err
	}
	for key, activation := range before {
		reloaded, found := after[key]
		if !found || !reflect.DeepEqual(activation, reloaded) {
			return fmt.Errorf("retained join activation %q changed (present=%t)", key, found)
		}
	}
	return nil
}

func TestRetainedJoinComparisonRequiresExactUniqueActivations(t *testing.T) {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	makeActivation := func(id string) joinruntime.Activation {
		ref, err := timeridentity.NewJoinRef(identitytest.RootNode(t, "join"), "item.done", "review", id)
		if err != nil {
			t.Fatal(err)
		}
		ref, err = ref.BindStageEntry(timeridentity.StageEntryRef{RunID: "run", FlowScope: ".", InstanceID: "one", InstancePath: ".", EntityID: "entity", Stage: "review", Cause: "construction"}, loopruntime.Activation{}.Generation())
		if err != nil {
			t.Fatal(err)
		}
		activation, err := joinruntime.NewActivation(ref, []string{"member"}, nil, at, at.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return activation
	}
	first, second, extra := makeActivation("first"), makeActivation("second"), makeActivation("extra")
	changed := first
	changed.DeadlineAt = changed.DeadlineAt.Add(time.Microsecond)
	for _, test := range []struct {
		name          string
		before, after []joinruntime.Activation
		wantError     bool
	}{
		{"permutation", []joinruntime.Activation{first, second}, []joinruntime.Activation{second, first}, false},
		{"duplicate_before", []joinruntime.Activation{first, first}, []joinruntime.Activation{first, second}, true},
		{"duplicate_after", []joinruntime.Activation{first, second}, []joinruntime.Activation{first, first}, true},
		{"missing", []joinruntime.Activation{first, second}, []joinruntime.Activation{first}, true},
		{"extra", []joinruntime.Activation{first}, []joinruntime.Activation{first, second}, true},
		{"replaced_key", []joinruntime.Activation{first, second}, []joinruntime.Activation{first, extra}, true},
		{"changed_value_same_key", []joinruntime.Activation{first, second}, []joinruntime.Activation{changed, second}, true},
		{"empty_key_before", []joinruntime.Activation{{}}, []joinruntime.Activation{first}, true},
		{"empty_key_after", []joinruntime.Activation{first}, []joinruntime.Activation{{}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := compareRetainedJoinActivations(test.before, test.after); (err != nil) != test.wantError {
				t.Fatalf("exact retained join comparison error=%v, want error=%t", err, test.wantError)
			}
		})
	}
}

func waitRetainedJoinPipelineHandoff(t *testing.T, reader receiverProofStateReader, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var incomplete int
	for time.Now().Before(deadline) {
		var err error
		incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(context.Background(), reader, runID)
		if err != nil {
			t.Fatal(err)
		}
		if incomplete == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("source pipeline completion did not finish: run=%s incomplete=%d", runID, incomplete)
}

func readRetainedRootJoins(t *testing.T, reader receiverProofStateReader, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	_, root := requireReceiverTargetState(t, reader, runID, ".", runID, runID)
	carrier, err := engine.StateCarrierFromPersisted(root.Fields, root.Bookkeeping, root.Gates, root.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
	if err != nil || !found {
		t.Fatalf("root loop missing: found=%v err=%v", found, err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return loop, joins
}
