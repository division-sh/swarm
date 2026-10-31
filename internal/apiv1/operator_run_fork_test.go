package apiv1

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const (
	runForkTestSourceRunID = "00000000-0000-0000-0000-000000000701"
	runForkTestForkRunID   = "00000000-0000-0000-0000-000000000702"
	runForkTestEventID     = "00000000-0000-0000-0000-000000000703"
)

var runForkTestArtifact = mustAPITestSourceArtifactNamed("run-fork")
var runForkTestBundleHash = runForkTestArtifact.BundleHash()

func TestValidateRunForkExecutionResultRejectsContradictorySourceOutcome(t *testing.T) {
	for _, result := range []RunForkExecutionResult{
		{SourceRunStatus: "", SourceFrozen: false},
		{SourceRunStatus: "running", SourceFrozen: true},
		{SourceRunStatus: "forked", SourceFrozen: false},
	} {
		if err := validateRunForkExecutionResult(result); err == nil {
			t.Fatalf("validateRunForkExecutionResult(%#v) error = nil", result)
		}
	}
}

func TestValidateRunForkExecutionResultKeepsDeploymentPointEventless(t *testing.T) {
	result := RunForkExecutionResult{
		SourceRunStatus: "running", ForkPointKind: string(runfork.RunForkPointDeploymentRevision), ForkRevision: 4,
	}
	if err := validateRunForkExecutionResult(result); err != nil {
		t.Fatalf("eventless deployment revision was refused: %v", err)
	}
	result.ForkEventID = runForkTestEventID
	if err := validateRunForkExecutionResult(result); err == nil {
		t.Fatal("deployment revision invented an event identity")
	}
	result.ForkEventID = ""
	result.ForkRevision = 0
	if err := validateRunForkExecutionResult(result); err == nil {
		t.Fatal("deployment revision without durable revision was accepted")
	}
}

func TestOperatorRunForkEventlessDeploymentPointReadback(t *testing.T) {
	executor := &recordingRunForkExecutor{result: RunForkExecutionResult{
		Owner:       runfork.RunForkSelectedContractExecutionOwner,
		SourceRunID: runForkTestSourceRunID, SourceRunStatus: "paused", SourceFrozen: false,
		ForkRunID: runForkTestForkRunID, ForkPointKind: string(runfork.RunForkPointDeploymentRevision),
		ForkRevision: 4, ForkRunStatus: runfork.RunForkActivatedStatus,
	}}
	availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact)),
	}}
	handler := runForkTestHandler(t, availability, executor)
	request := fmt.Sprintf(`{"jsonrpc":"2.0","id":"deployment","method":"run.fork","params":{"source_run_id":%q,"idempotency_key":"deployment-point"}}`, runForkTestSourceRunID)
	response := rpcCall(t, handler, request)
	if response.Error != nil {
		t.Fatalf("eventless deployment fork: %#v", response.Error)
	}
	if executor.last.ForkOperation.TargetRootReference != executor.selectedSource.Admission().Reference() ||
		executor.last.ForkOperation.TargetRootReference == availability.rows[runForkTestSourceRunID].RootReference {
		t.Fatal("fork operation did not retain its independently selected target root")
	}
	result := asMap(t, response.Result)
	if result["fork_point_kind"] != string(runfork.RunForkPointDeploymentRevision) || result["fork_revision"] != float64(4) {
		t.Fatalf("deployment fork lost typed revision: %#v", result)
	}
	if _, hasEvent := result["fork_event_id"]; hasEvent {
		t.Fatalf("eventless deployment fork invented an event ID: %#v", result)
	}
	if replay := rpcCall(t, handler, request); replay.Error != nil || !reflect.DeepEqual(replay.Result, response.Result) || executor.calls != 1 {
		t.Fatalf("deployment operation did not replay exact typed result: first=%#v replay=%#v calls=%d", response, replay, executor.calls)
	}
}

func TestOperatorRunForkRejectsRetiredConsentBeforeAvailability(t *testing.T) {
	for _, consent := range []struct{ key, value string }{
		{"confirm_source_freeze", "true"},
		{"confirm_source_freeze", "false"},
		{"allow_source_freeze", "true"},
		{"allow_source_freeze", "false"},
	} {
		t.Run(consent.key+"/"+consent.value, func(t *testing.T) {
			availability := &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
				runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact)),
			}}
			executor := &recordingRunForkExecutor{}
			handler := runForkTestHandler(t, availability, executor)
			resp := rpcCall(t, handler, fmt.Sprintf(
				`{"jsonrpc":"2.0","id":"retired-consent","method":"run.fork","params":{"source_run_id":%q,%q:%s}}`, runForkTestSourceRunID, consent.key, consent.value,
			))
			if resp.Error == nil || resp.Error.Code != codeInvalidParams {
				t.Fatalf("retired consent error = %#v, want invalid params", resp.Error)
			}
			details := asMap(t, asMap(t, resp.Error.Data)["details"])
			if details["field"] != consent.key || details["reason"] != "unknown parameter" {
				t.Fatalf("retired consent details = %#v", details)
			}
			if availability.calls != 0 || executor.calls != 0 {
				t.Fatalf("retired consent reached availability=%d execution=%d", availability.calls, executor.calls)
			}
		})
	}
}

func TestOperatorRunForkHandlersFailClosedOnBundleAvailability(t *testing.T) {
	tests := []struct {
		name         string
		availability runbundle.Availability
		params       string
		wantCode     string
	}{
		{
			name:         "deleted source unavailable",
			availability: runForkUnavailable(runForkTestSourceRunID, runForkTestBundleHash),
			params:       fmt.Sprintf(`{"source_run_id":%q,"fork_event_id":%q}`, runForkTestSourceRunID, runForkTestEventID),
			wantCode:     BundleUnavailableCode,
		},
		{
			name:         "persisted missing bundle row",
			availability: runForkDataIntegrity(runForkTestSourceRunID, runForkTestBundleHash),
			params:       fmt.Sprintf(`{"source_run_id":%q,"fork_event_id":%q}`, runForkTestSourceRunID, runForkTestEventID),
			wantCode:     BundleDataIntegrityErrorCode,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := &recordingRunForkExecutor{}
			handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: tc.availability}}, executor)
			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":`+tc.params+`}`)
			if resp.Error == nil {
				t.Fatalf("run.fork error = nil, want %s", tc.wantCode)
			}
			if data := asMap(t, resp.Error.Data); data["code"] != tc.wantCode {
				t.Fatalf("run.fork data = %#v, want code %s", data, tc.wantCode)
			}
			if executor.calls != 0 {
				t.Fatalf("executor calls = %d, want 0", executor.calls)
			}
		})
	}
}

func TestOperatorRunForkHandlersMapSourceAndEventErrors(t *testing.T) {
	t.Run("existing fork is a typed refusal", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: fmt.Errorf("materialization: %w", &runfork.RunForkMaterializationConflictError{ForkRunID: runForkTestForkRunID})}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
			runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact)),
		}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"existing-fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q}}`, runForkTestSourceRunID, runForkTestEventID))
		if resp.Error == nil || resp.Error.Code != codeInvalidParams || executor.calls != 1 {
			t.Fatalf("existing fork returned an internal error: error=%#v calls=%d", resp.Error, executor.calls)
		}
		details := asMap(t, asMap(t, resp.Error.Data)["details"])
		if details["fork_run_id"] != runForkTestForkRunID || !strings.Contains(fmt.Sprint(details["reason"]), runForkTestForkRunID) {
			t.Fatalf("typed refusal lost its existing fork identity: %#v", details)
		}
	})

	t.Run("source run missing", func(t *testing.T) {
		handler := runForkTestHandler(t, &recordingRunForkAvailability{err: fmt.Errorf("run %s not found: %w", runForkTestSourceRunID, runbundle.ErrRunNotFound)}, &recordingRunForkExecutor{})
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q}}`, runForkTestSourceRunID))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != RunNotFoundCode {
			t.Fatalf("run.fork missing source error = %#v, want %s", resp.Error, RunNotFoundCode)
		}
	})

	t.Run("fork event missing", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New("fork point event " + runForkTestEventID + " not found in source run " + runForkTestSourceRunID)}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact))}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"idempotency_key":"event-missing"}}`,
			runForkTestSourceRunID,
			runForkTestEventID,
		))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != EventNotFoundCode {
			t.Fatalf("run.fork missing event error = %#v, want %s", resp.Error, EventNotFoundCode)
		}
		details := asMap(t, asMap(t, resp.Error.Data)["details"])
		if details["event_id"] != runForkTestEventID {
			t.Fatalf("run.fork missing event details = %#v, want event_id %s", details, runForkTestEventID)
		}
	})

	t.Run("default fork point missing", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New("no source-run event exists for fork source run " + runForkTestSourceRunID)}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact))}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"idempotency_key":"default-event-missing"}}`,
			runForkTestSourceRunID,
		))
		if resp.Error == nil || resp.Error.Code != codeInvalidParams {
			t.Fatalf("run.fork default point error = %#v, want invalid params", resp.Error)
		}
		details := asMap(t, asMap(t, resp.Error.Data)["details"])
		if details["field"] != "fork_event_id" || details["source_run_id"] != runForkTestSourceRunID {
			t.Fatalf("run.fork default point details = %#v", details)
		}
	})

	t.Run("executor bundle data integrity", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New(runbundle.CodeBundleDataIntegrityError + ": corrupt persisted bundle catalog bytes")}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact))}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"idempotency_key":"integrity-error"}}`,
			runForkTestSourceRunID,
			runForkTestEventID,
		))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != BundleDataIntegrityErrorCode {
			t.Fatalf("run.fork integrity error = %#v, want %s", resp.Error, BundleDataIntegrityErrorCode)
		}
	})

	t.Run("executor source hash mismatch", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New(runbundle.CodeBundleDataIntegrityError + ": selected_contracts source hash mismatch: request bundle-v2:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc source " + runForkTestBundleHash)}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, apiArtifactRoot(t, runForkTestArtifact))}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"idempotency_key":"hash-mismatch"}}`,
			runForkTestSourceRunID,
			runForkTestEventID,
		))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != BundleDataIntegrityErrorCode {
			t.Fatalf("run.fork hash mismatch error = %#v, want %s", resp.Error, BundleDataIntegrityErrorCode)
		}
	})
}

func runForkTestHandler(t *testing.T, availability RunForkAvailabilityStore, executor RunForkExecutor) *Handler {
	t.Helper()
	operations := newRecordingRunForkOperations()
	if recording, ok := executor.(*recordingRunForkExecutor); ok {
		recording.operations = operations
		recording.selectedSource, _ = sourceartifactfixture.Rooted(t, "selected-target", runForkTestArtifact)
	}
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: availability,
			RunForkOperations:   operations,
			RunFork:             executor,
		}),
	})
}

func runForkAvailable(runID string, root sourceartifact.RootAdmission) runbundle.Availability {
	return runbundle.Availability{
		RunID:                 strings.TrimSpace(runID),
		Status:                "running",
		BundleHash:            root.BundleHash(),
		RootReference:         root.Reference(),
		RootAdmissionPresent:  true,
		SourceArtifactPresent: true,
	}
}

func apiArtifactRoot(t testing.TB, artifact *sourceartifact.AdmittedSourceArtifact) sourceartifact.RootAdmission {
	t.Helper()
	rooted, _ := sourceartifactfixture.Rooted(t, "review", artifact)
	return rooted.Admission()
}

func runForkUnavailable(runID, bundleHash string) runbundle.Availability {
	availability := runbundle.Availability{RunID: strings.TrimSpace(runID), Status: "running", BundleHash: strings.TrimSpace(bundleHash)}
	availability.SourceArtifactPresent = false
	availability.ErrorCode = runbundle.CodeBundleUnavailable
	availability.Cause = "source_artifact_unavailable"
	return availability
}

func runForkDataIntegrity(runID, bundleHash string) runbundle.Availability {
	availability := runbundle.Availability{RunID: strings.TrimSpace(runID), Status: "running", BundleHash: strings.TrimSpace(bundleHash)}
	availability.SourceArtifactPresent = false
	availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
	availability.Cause = "missing_source_artifact"
	return availability
}

type recordingRunForkAvailability struct {
	rows  map[string]runbundle.Availability
	err   error
	calls int
}

func (s *recordingRunForkAvailability) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	s.calls++
	if s.err != nil {
		return runbundle.Availability{}, s.err
	}
	availability, ok := s.rows[strings.TrimSpace(runID)]
	if !ok {
		return runbundle.Availability{}, fmt.Errorf("run %s not found: %w", strings.TrimSpace(runID), runbundle.ErrRunNotFound)
	}
	return availability, nil
}

type recordingRunForkExecutor struct {
	selectedSource sourceartifact.RootedSource
	calls          int
	last           RunForkExecutionRequest
	result         RunForkExecutionResult
	err            error
	execute        func(context.Context, RunForkExecutionRequest) (RunForkExecutionResult, error)
	operations     *recordingRunForkOperations
	persistOnError bool
}

func (e *recordingRunForkExecutor) InspectRunForkSelectedContractSourceForRequest(_ context.Context, req runforkexecution.SelectedContractSourceLoadRequest) (runforkexecution.LoadedSelectedContractSource, error) {
	if err := e.selectedSource.Validate(); err != nil {
		return runforkexecution.LoadedSelectedContractSource{}, err
	}
	if e.selectedSource.Admission().BundleHash() != req.BundleHash {
		return runforkexecution.LoadedSelectedContractSource{}, fmt.Errorf("fixture selected source differs from requested target")
	}
	return runforkexecution.LoadedSelectedContractSource{RootedSource: e.selectedSource, Selection: req.Selection}, nil
}

func (e *recordingRunForkExecutor) ExecuteRunFork(ctx context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	e.calls++
	e.last = req
	result, err := e.result, e.err
	if e.execute != nil {
		result, err = e.execute(ctx, req)
	}
	if result.BundleHash == "" {
		result.BundleHash = req.BundleHash
	}
	if e.operations != nil && (err == nil || e.persistOnError) {
		if req.ForkOperation == nil {
			return RunForkExecutionResult{}, fmt.Errorf("mock fork executor requires a durable operation request")
		}
		if recordErr := e.operations.activate(*req.ForkOperation, result); recordErr != nil {
			return RunForkExecutionResult{}, recordErr
		}
	}
	return result, err
}

type recordingRunForkOperations struct {
	byID       map[string]runfork.ForkOperationRecord
	byKey      map[string]string
	keyedReads int
	idReads    int
}

func newRecordingRunForkOperations() *recordingRunForkOperations {
	return &recordingRunForkOperations{byID: map[string]runfork.ForkOperationRecord{}, byKey: map[string]string{}}
}

func (o *recordingRunForkOperations) LoadForkOperation(_ context.Context, actor, key, transportHash string) (runfork.ForkOperationRecord, bool, error) {
	o.keyedReads++
	id, ok := o.byKey[actor+"\x00"+key]
	if !ok {
		return runfork.ForkOperationRecord{}, false, nil
	}
	record := o.byID[id]
	if record.Request.TransportHash != transportHash {
		return runfork.ForkOperationRecord{}, false, &apiidempotency.ConflictError{
			OriginalRequestHash: record.Request.TransportHash, ConflictingRequestHash: transportHash,
			Method: "run.fork", ResourceID: id,
		}
	}
	return record, true, nil
}

func (o *recordingRunForkOperations) LoadForkOperationByID(_ context.Context, id string) (runfork.ForkOperationRecord, bool, error) {
	o.idReads++
	record, ok := o.byID[id]
	return record, ok, nil
}

func (o *recordingRunForkOperations) activate(request runfork.ForkOperationRequest, result RunForkExecutionResult) error {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointKind(result.ForkPointKind), Input: request.ForkEventID, Revision: result.ForkRevision, EventID: result.ForkEventID}
	request.ResolvedPoint = &point
	canonical, hash, err := request.Canonical()
	if err != nil {
		return err
	}
	record := runfork.ForkOperationRecord{
		Request: canonical, SemanticHash: hash, ForkRunID: result.ForkRunID,
		BindingID: "00000000-0000-0000-0000-000000000704", Status: runfork.ForkOperationActivated,
		Result: &runfork.ForkOperationResult{
			SourceRunID: result.SourceRunID, SourceRunStatus: result.SourceRunStatus, SourceFrozen: result.SourceFrozen,
			ForkRunID: result.ForkRunID, ForkEventID: result.ForkEventID, ForkPoint: point, ForkRunStatus: result.ForkRunStatus,
			BundleHash: result.BundleHash, ExecutedEventCount: result.ExecutedEventCount, DataPins: result.DataPins,
		},
	}
	if err := record.Validate(); err != nil {
		return err
	}
	o.byID[canonical.OperationID] = record
	if canonical.IdempotencyKey != "" {
		o.byKey[canonical.Actor+"\x00"+canonical.IdempotencyKey] = canonical.OperationID
	}
	return nil
}
