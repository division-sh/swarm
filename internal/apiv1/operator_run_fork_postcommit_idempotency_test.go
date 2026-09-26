package apiv1

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestOperatorRunForkActivatedPostCommitErrorReplaysWithoutReexecution(t *testing.T) {
	cleanupErr := errors.New("selected fork postcommit cleanup failed")
	selectedCalls := 0
	selected := SelectedContractRunForkExecutor{
		ExecuteSelectedContractRunFork: func(_ context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
			selectedCalls++
			if req.SourceRunID != runForkTestSourceRunID || req.At != runForkTestEventID || req.ExpectedBundleHash != runForkTestBundleHash || !req.AllowSourceFreeze {
				t.Fatalf("selected executor request = %#v", req)
			}
			return runforkexecution.SelectedContractExecutionResult{
				Owner: runfork.RunForkSelectedContractExecutionOwner,
				Materialization: runfork.RunForkMaterialization{
					SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
					ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: runForkTestEventID, Revision: 1},
				},
				Activation: runfork.RunForkActivation{
					SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
					ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: runForkTestEventID, Revision: 1},
					Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus,
					SourceRunStatus: runfork.RunForkSourceFrozenStatus, SourceFrozen: true,
				},
				ExecutedEventCount: 2,
			}, cleanupErr
		},
	}
	operations := newRecordingRunForkOperations()
	executor := &recordingRunForkExecutor{execute: selected.ExecuteRunFork, operations: operations, persistOnError: true}
	availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}
	opts := RunForkHandlerOptions{Availability: availability, Operations: operations, Executor: executor}
	request := runForkOperationTestRequest("activated-fork-postcommit", "fork-postcommit-request-hash")
	first, err := invokeRunForkOperation(opts, request)
	if err != nil {
		t.Fatalf("acknowledged cleanup failure returned error: %v", err)
	}
	if len(operations.byID) != 1 || selectedCalls != 1 || availability.calls != 1 {
		t.Fatalf("durable activation was not the completion owner: records=%d execution=%d availability=%d", len(operations.byID), selectedCalls, availability.calls)
	}
	availability.err = runbundle.ErrRunNotFound
	replay, err := invokeRunForkOperation(opts, request)
	if err != nil || !reflect.DeepEqual(first, replay) || selectedCalls != 1 || availability.calls != 1 {
		t.Fatalf("durable replay: first=%#v replay=%#v err=%v execution=%d availability=%d", first, replay, err, selectedCalls, availability.calls)
	}
	result, ok := replay.(RunForkExecutionResult)
	if !ok || result.ForkRunID != runForkTestForkRunID || result.ExecutedEventCount != 2 || !result.activationAcknowledged {
		t.Fatalf("durable replay lost acknowledged activation: %#v", replay)
	}
}

func TestSelectedForkEventlessRevisionAcknowledgementRequiresExactPoint(t *testing.T) {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 4}
	executor := SelectedContractRunForkExecutor{ExecuteSelectedContractRunFork: func(_ context.Context, _ runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
		return runforkexecution.SelectedContractExecutionResult{
			Owner: runfork.RunForkSelectedContractExecutionOwner,
			Materialization: runfork.RunForkMaterialization{
				SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: point,
			},
			Activation: runfork.RunForkActivation{
				SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: point,
				Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus,
			},
		}, nil
	}}
	request := RunForkExecutionRequest{SourceRunID: runForkTestSourceRunID, BundleHash: runForkTestBundleHash}
	result, err := executor.ExecuteRunFork(context.Background(), request)
	if err != nil || !result.activationAcknowledged || result.ForkPointKind != string(point.Kind) || result.ForkRevision != point.Revision || result.ForkEventID != "" {
		t.Fatalf("exact eventless revision was not acknowledged: result=%+v err=%v", result, err)
	}
	executor.ExecuteSelectedContractRunFork = func(_ context.Context, _ runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
		changed := point
		changed.Revision++
		return runforkexecution.SelectedContractExecutionResult{
			Owner: runfork.RunForkSelectedContractExecutionOwner,
			Materialization: runfork.RunForkMaterialization{
				SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: point,
			},
			Activation: runfork.RunForkActivation{
				SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: changed,
				Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus,
			},
		}, nil
	}
	if _, err := executor.ExecuteRunFork(context.Background(), request); err == nil {
		t.Fatal("changed deployment revision was acknowledged as the original fork point")
	}
}

func TestOperatorRunForkKeyedLookupPrecedesMutableAvailabilityAndConflictsWithoutMutation(t *testing.T) {
	operations := newRecordingRunForkOperations()
	executor := &recordingRunForkExecutor{operations: operations, result: runForkActivatedTestResult()}
	availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}
	opts := RunForkHandlerOptions{Availability: availability, Operations: operations, Executor: executor}
	request := runForkOperationTestRequest("same-key", "original-transport-hash")
	if _, err := invokeRunForkOperation(opts, request); err != nil {
		t.Fatal(err)
	}
	availability.err = runbundle.ErrRunNotFound
	if _, err := invokeRunForkOperation(opts, request); err != nil {
		t.Fatalf("same-key replay depended on mutable availability: %v", err)
	}
	conflicting := request
	conflicting.RequestHash = "conflicting-transport-hash"
	_, err := invokeRunForkOperation(opts, conflicting)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) || appErr.Code != IdempotencyConflictCode {
		t.Fatalf("conflicting durable request error = %v, want %s", err, IdempotencyConflictCode)
	}
	if availability.calls != 1 || executor.calls != 1 || len(operations.byID) != 1 || operations.keyedReads != 3 {
		t.Fatalf("conflict mutated fork or consulted availability: availability=%d execution=%d records=%d keyed_reads=%d", availability.calls, executor.calls, len(operations.byID), operations.keyedReads)
	}
}

func TestOperatorRunForkMaterializedOperationResumesBeforeAvailability(t *testing.T) {
	operations := newRecordingRunForkOperations()
	executor := &recordingRunForkExecutor{operations: operations, result: runForkActivatedTestResult()}
	availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}
	opts := RunForkHandlerOptions{Availability: availability, Operations: operations, Executor: executor}
	request := runForkOperationTestRequest("materialized-resume", "materialized-transport-hash")
	if _, err := invokeRunForkOperation(opts, request); err != nil {
		t.Fatal(err)
	}
	operationID := executor.last.ForkOperation.OperationID
	record := operations.byID[operationID]
	record.Status = runfork.ForkOperationMaterialized
	record.Result = nil
	if err := record.Validate(); err != nil {
		t.Fatalf("materialized operation fixture: %v", err)
	}
	operations.byID[operationID] = record
	availability.err = runbundle.ErrRunNotFound
	value, err := invokeRunForkOperation(opts, request)
	result, ok := value.(RunForkExecutionResult)
	if err != nil || !ok || result.ForkRunID != runForkTestForkRunID || !result.activationAcknowledged {
		t.Fatalf("materialized operation resume: result=%#v err=%v", value, err)
	}
	if availability.calls != 1 || executor.calls != 2 || executor.last.ForkOperation.OperationID != operationID || operations.byID[operationID].Status != runfork.ForkOperationActivated {
		t.Fatalf("resume borrowed mutable state or changed operation: availability=%d execution=%d record=%#v", availability.calls, executor.calls, operations.byID[operationID])
	}
}

func TestOperatorRunForkRequiresDurableActivatedRecord(t *testing.T) {
	for _, status := range []string{"absent", "materialized"} {
		t.Run(status, func(t *testing.T) {
			operations := newRecordingRunForkOperations()
			executor := &recordingRunForkExecutor{result: runForkActivatedTestResult()}
			if status == "materialized" {
				executor.execute = func(_ context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
					canonical, hash, err := req.ForkOperation.Canonical()
					if err != nil {
						return RunForkExecutionResult{}, err
					}
					operations.byID[canonical.OperationID] = runfork.ForkOperationRecord{
						Request: canonical, SemanticHash: hash, ForkRunID: runForkTestForkRunID,
						BindingID: "00000000-0000-0000-0000-000000000704", Status: runfork.ForkOperationMaterialized,
					}
					return runForkActivatedTestResult(), nil
				}
			}
			opts := RunForkHandlerOptions{
				Availability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
					runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
				}},
				Operations: operations, Executor: executor,
			}
			value, err := invokeRunForkOperation(opts, runForkOperationTestRequest("", status))
			if err == nil || value != nil || operations.idReads != 1 {
				t.Fatalf("%s operation reported executor-only success: value=%#v err=%v id_reads=%d", status, value, err, operations.idReads)
			}
		})
	}
}

func TestOperatorRunForkKeylessReadsExactOperationIDAfterCleanup(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		name := "healthy"
		if cleanupFailure {
			name = "cleanup_failure"
		}
		t.Run(name, func(t *testing.T) {
			operations := newRecordingRunForkOperations()
			executor := &recordingRunForkExecutor{operations: operations, result: runForkActivatedTestResult()}
			if cleanupFailure {
				executor.err = errors.New("post-activation cleanup failed")
				executor.persistOnError = true
			}
			opts := RunForkHandlerOptions{
				Availability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
					runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
				}},
				Operations: operations, Executor: executor,
			}
			value, err := invokeRunForkOperation(opts, runForkOperationTestRequest("", name))
			result, ok := value.(RunForkExecutionResult)
			if err != nil || !ok || result.ForkRunID != runForkTestForkRunID || !result.activationAcknowledged {
				t.Fatalf("keyless exact-operation result = %#v, err=%v", value, err)
			}
			if operations.keyedReads != 0 || operations.idReads != 1 || len(operations.byKey) != 0 || len(operations.byID) != 1 {
				t.Fatalf("keyless fork borrowed keyed authority: keyed_reads=%d id_reads=%d keys=%d records=%d", operations.keyedReads, operations.idReads, len(operations.byKey), len(operations.byID))
			}
			if _, ok := operations.byID[executor.last.ForkOperation.OperationID]; !ok {
				t.Fatal("keyless result did not use the issued operation ID")
			}
		})
	}
}

func runForkOperationTestRequest(key, hash string) Request {
	params := map[string]any{
		"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID,
		"allow_source_freeze": true,
	}
	if key != "" {
		params["idempotency_key"] = key
	}
	return Request{
		Method: "run.fork", ActorTokenID: "fork-operation-test", RequestHash: hash, Params: params,
	}
}

func invokeRunForkOperation(opts RunForkHandlerOptions, request Request) (any, error) {
	return OperatorRunForkHandlers(opts)["run.fork"](context.Background(), request)
}

func runForkActivatedTestResult() RunForkExecutionResult {
	return RunForkExecutionResult{
		Owner:       runfork.RunForkSelectedContractExecutionOwner,
		SourceRunID: runForkTestSourceRunID, SourceRunStatus: runfork.RunForkSourceFrozenStatus,
		SourceFrozen: true, ForkRunID: runForkTestForkRunID,
		ForkPointKind: string(runfork.RunForkPointEvent), ForkRevision: 1, ForkEventID: runForkTestEventID,
		ForkRunStatus: runfork.RunForkActivatedStatus, BundleHash: runForkTestBundleHash,
		ExecutedEventCount: 1,
	}
}
