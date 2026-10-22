package apiv1

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestRunForkStartParamsRequireExplicitExclusiveBoolean(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]any
		valid  bool
		start  bool
		event  string
	}{
		{"omitted", nil, true, false, ""},
		{"false", map[string]any{"at_start": false}, true, false, ""},
		{"start", map[string]any{"at_start": true}, true, true, ""},
		{"event", map[string]any{"fork_event_id": runForkTestEventID}, true, false, runForkTestEventID},
		{"false_with_event", map[string]any{"at_start": false, "fork_event_id": runForkTestEventID}, true, false, runForkTestEventID},
		{"empty_event", map[string]any{"fork_event_id": ""}, true, false, ""},
		{"mixed", map[string]any{"at_start": true, "fork_event_id": runForkTestEventID}, false, false, ""},
		{"mixed_empty", map[string]any{"at_start": true, "fork_event_id": ""}, false, false, ""},
		{"mixed_null", map[string]any{"at_start": true, "fork_event_id": nil}, false, false, ""},
		{"string", map[string]any{"at_start": "true"}, false, false, ""},
		{"empty_string", map[string]any{"at_start": ""}, false, false, ""},
		{"number", map[string]any{"at_start": 1}, false, false, ""},
		{"null", map[string]any{"at_start": nil}, false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{"source_run_id": runForkTestSourceRunID}
			for key, value := range tc.params {
				params[key] = value
			}
			got, err := runForkParamsFromRequest(params)
			if tc.valid {
				if err != nil || got.AtStart != tc.start || got.ForkEventID != tc.event {
					t.Fatalf("selector = %+v, error %v", got, err)
				}
				return
			}
			var invalid *InvalidParamsError
			if !errors.As(err, &invalid) {
				t.Fatalf("invalid start selector = %v", err)
			}
		})
	}
}

func TestSelectedForkRunStartSelectorAndPermanentPoint(t *testing.T) {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7}
	request := RunForkExecutionRequest{
		SourceRunID: runForkTestSourceRunID, AtStart: true, BundleHash: runForkTestBundleHash,
		ForkOperation: &runfork.ForkOperationRequest{AtStart: true, ResolvedPoint: &point},
	}
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		for _, revision := range []int64{7, 8} {
			t.Run(fmt.Sprintf("%s/%d", kind, revision), func(t *testing.T) {
				returned := runfork.RunForkPoint{Kind: kind, Revision: revision}
				if kind == runfork.RunForkPointEvent {
					returned.EventID = runForkTestEventID
				}
				calls := 0
				executor := SelectedContractRunForkExecutor{ExecuteSelectedContractRunFork: func(_ context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
					calls++
					if !req.AtStart || req.At != "" || req.ForkOperation != request.ForkOperation || *req.ForkOperation.ResolvedPoint != point {
						t.Fatalf("start request lost its permanent point: %+v", req)
					}
					return runforkexecution.SelectedContractExecutionResult{
						Owner:           runfork.RunForkSelectedContractExecutionOwner,
						Materialization: runfork.RunForkMaterialization{SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: returned},
						Activation: runfork.RunForkActivation{
							SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: returned,
							Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus, SourceRunStatus: "running",
						},
					}, nil
				}}
				result, err := executor.ExecuteRunFork(context.Background(), request)
				valid := kind == runfork.RunForkPointRunStart && revision == point.Revision
				if calls != 1 || (err == nil) != valid || result.activationAcknowledged != valid {
					t.Fatalf("start acknowledgement result=%+v error=%v calls=%d", result, err, calls)
				}
				if valid && (result.ForkEventID != "" || result.ForkPointKind != string(point.Kind) || result.ForkRevision != point.Revision) {
					t.Fatalf("start readback invented event or changed revision: %+v", result)
				}
				if valid {
					mixed := request
					mixed.ForkEventID = runForkTestEventID
					if _, err := executor.ExecuteRunFork(context.Background(), mixed); err == nil || calls != 1 {
						t.Fatal("mixed start/event request reached selected execution")
					}
					implicit := request
					implicit.AtStart = false
					executor.ExecuteSelectedContractRunFork = func(context.Context, runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
						return runforkexecution.SelectedContractExecutionResult{
							Owner:           runfork.RunForkSelectedContractExecutionOwner,
							Materialization: runfork.RunForkMaterialization{SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: point},
							Activation:      runfork.RunForkActivation{SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID, ForkPoint: point, Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus},
						}, nil
					}
					if _, err := executor.ExecuteRunFork(context.Background(), implicit); err == nil {
						t.Fatal("omitted selector was silently reinterpreted as run start")
					}
				}
			})
		}
	}
}

func TestOperatorRunForkStartReadbackAndMaterializedRetry(t *testing.T) {
	operations := newRecordingRunForkOperations()
	result := runForkActivatedTestResult()
	result.ForkPointKind, result.ForkRevision, result.ForkEventID = string(runfork.RunForkPointRunStart), 7, ""
	executor := &recordingRunForkExecutor{operations: operations, result: result}
	availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}
	opts := RunForkHandlerOptions{Availability: availability, Operations: operations, Executor: executor}
	request := runForkOperationTestRequest("start-retry", "start-transport-hash")
	delete(request.Params, "fork_event_id")
	request.Params["at_start"] = true
	first, err := invokeRunForkOperation(opts, request)
	if err != nil || !executor.last.AtStart || executor.last.ForkEventID != "" || !executor.last.ForkOperation.AtStart {
		t.Fatalf("start request = %+v, error %v", executor.last, err)
	}
	operationID := executor.last.ForkOperation.OperationID
	point := *operations.byID[operationID].Request.ResolvedPoint
	availability.err = runbundle.ErrRunNotFound
	replay, err := invokeRunForkOperation(opts, request)
	if err != nil || !reflect.DeepEqual(first, replay) || executor.calls != 1 || availability.calls != 1 {
		t.Fatalf("start replay borrowed mutable availability: first=%+v replay=%+v error=%v", first, replay, err)
	}
	record := operations.byID[operationID]
	record.Status, record.Result = runfork.ForkOperationMaterialized, nil
	operations.byID[operationID] = record
	resumed, err := invokeRunForkOperation(opts, request)
	if err != nil || !reflect.DeepEqual(first, resumed) || executor.calls != 2 || availability.calls != 1 || *executor.last.ForkOperation.ResolvedPoint != point {
		t.Fatalf("materialized start retry lost permanent point: request=%+v result=%+v error=%v", executor.last, resumed, err)
	}
	// Even a reader returning the same transport identity cannot alias selectors.
	latest := request
	latest.Params = map[string]any{"source_run_id": runForkTestSourceRunID, "allow_source_freeze": true, "idempotency_key": "start-retry"}
	if _, err := invokeRunForkOperation(opts, latest); err == nil || executor.calls != 2 || availability.calls != 1 {
		t.Fatal("durable run-start operation aliased the latest selector")
	}
}

func TestOperatorRunForkStartHTTPReadback(t *testing.T) {
	executor := &recordingRunForkExecutor{result: runForkActivatedTestResult()}
	executor.result.ForkPointKind, executor.result.ForkRevision, executor.result.ForkEventID = string(runfork.RunForkPointRunStart), 7, ""
	handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}, executor)
	request := fmt.Sprintf(`{"jsonrpc":"2.0","id":"start","method":"run.fork","params":{"source_run_id":%q,"at_start":true,"allow_source_freeze":true,"idempotency_key":"start-http"}}`, runForkTestSourceRunID)
	response := rpcCall(t, handler, request)
	if response.Error != nil {
		t.Fatalf("start selector HTTP response: %#v", response.Error)
	}
	result := asMap(t, response.Result)
	if result["fork_point_kind"] != string(runfork.RunForkPointRunStart) || result["fork_revision"] != float64(7) || !executor.last.AtStart {
		t.Fatalf("HTTP response lost typed start: %#v", result)
	}
	if _, exists := result["fork_event_id"]; exists {
		t.Fatal("HTTP run-start readback invented an event coordinate")
	}
	if replay := rpcCall(t, handler, request); replay.Error != nil || !reflect.DeepEqual(replay.Result, response.Result) || executor.calls != 1 {
		t.Fatalf("HTTP start retry changed result: %#v", replay)
	}
}

func TestValidateRunForkExecutionResultKeepsRunStartEventless(t *testing.T) {
	result := RunForkExecutionResult{SourceRunStatus: "running", ForkPointKind: string(runfork.RunForkPointRunStart), ForkRevision: 7}
	if err := validateRunForkExecutionResult(result); err != nil {
		t.Fatal(err)
	}
	result.ForkEventID = runForkTestEventID
	if err := validateRunForkExecutionResult(result); err == nil {
		t.Fatal("run start readback accepted an event coordinate")
	}
	result.ForkEventID, result.ForkRevision = "", 0
	if err := validateRunForkExecutionResult(result); err == nil {
		t.Fatal("run start readback accepted a missing revision")
	}
}
