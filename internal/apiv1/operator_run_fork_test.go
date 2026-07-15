package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

const (
	runForkTestSourceRunID = "00000000-0000-0000-0000-000000000701"
	runForkTestForkRunID   = "00000000-0000-0000-0000-000000000702"
	runForkTestEventID     = "00000000-0000-0000-0000-000000000703"
	runForkTestBundleHash  = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestOperatorRunForkHandlersUseAvailabilityAndSelectedExecutor(t *testing.T) {
	availability := runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash)
	executor := &recordingRunForkExecutor{
		result: RunForkExecutionResult{
			Owner:              "runtime.run_fork.selected_contract_execution",
			SourceRunID:        runForkTestSourceRunID,
			ForkRunID:          runForkTestForkRunID,
			ForkEventID:        runForkTestEventID,
			ForkRunStatus:      "running",
			ExecutedEventCount: 1,
		},
	}
	handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: availability}}, executor)

	resp := rpcCall(t, handler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"idem-fork"}}`,
		runForkTestSourceRunID,
		runForkTestEventID,
	))
	if resp.Error != nil {
		t.Fatalf("run.fork error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["fork_run_id"] != runForkTestForkRunID || result["source_run_id"] != runForkTestSourceRunID {
		t.Fatalf("run.fork result = %#v", result)
	}
	if result["bundle_hash"] != runForkTestBundleHash {
		t.Fatalf("run.fork bundle_hash = %#v, want %q", result["bundle_hash"], runForkTestBundleHash)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.last.SourceRunID != runForkTestSourceRunID || executor.last.ForkEventID != runForkTestEventID || executor.last.BundleHash != runForkTestBundleHash || !executor.last.ConfirmSourceFreeze {
		t.Fatalf("executor request = %#v", executor.last)
	}

	replay := rpcCall(t, handler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork-replay","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"idem-fork"}}`,
		runForkTestSourceRunID,
		runForkTestEventID,
	))
	if replay.Error != nil {
		t.Fatalf("run.fork replay error = %#v", replay.Error)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls after replay = %d, want 1", executor.calls)
	}
}

func TestOperatorRunForkHandlersRequireActiveSourceFreezeConfirmation(t *testing.T) {
	executor := &recordingRunForkExecutor{}
	handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
		runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
	}}, executor)

	resp := rpcCall(t, handler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q}}`,
		runForkTestSourceRunID,
		runForkTestEventID,
	))
	if resp.Error == nil || resp.Error.Code != codeInvalidParams {
		t.Fatalf("run.fork confirmation error = %#v, want invalid params", resp.Error)
	}
	details := asMap(t, asMap(t, resp.Error.Data)["details"])
	if details["field"] != "confirm_source_freeze" {
		t.Fatalf("run.fork confirmation details = %#v", details)
	}
	if executor.calls != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.calls)
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
			name:         "legacy source unavailable",
			availability: runForkUnavailable(runForkTestSourceRunID, runForkTestBundleHash, storerunlifecycle.BundleSourceLegacy),
			params:       fmt.Sprintf(`{"source_run_id":%q,"fork_event_id":%q}`, runForkTestSourceRunID, runForkTestEventID),
			wantCode:     BundleUnavailableCode,
		},
		{
			name:         "persisted missing bundle row",
			availability: runForkDataIntegrity(runForkTestSourceRunID, runForkTestBundleHash),
			params:       fmt.Sprintf(`{"source_run_id":%q,"fork_event_id":%q}`, runForkTestSourceRunID, runForkTestEventID),
			wantCode:     BundleDataIntegrityErrorCode,
		},
		{
			name:         "different bundle hash",
			availability: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
			params: fmt.Sprintf(
				`{"source_run_id":%q,"fork_event_id":%q,"bundle_hash":"bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","confirm_source_freeze":true}`,
				runForkTestSourceRunID,
				runForkTestEventID,
			),
			wantCode: BundleUnavailableCode,
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
	t.Run("source run missing", func(t *testing.T) {
		handler := runForkTestHandler(t, &recordingRunForkAvailability{err: errors.New("run " + runForkTestSourceRunID + " not found")}, &recordingRunForkExecutor{})
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q}}`, runForkTestSourceRunID))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != RunNotFoundCode {
			t.Fatalf("run.fork missing source error = %#v, want %s", resp.Error, RunNotFoundCode)
		}
	})

	t.Run("fork event missing", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New("fork point event " + runForkTestEventID + " not found in source run " + runForkTestSourceRunID)}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash)}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"event-missing"}}`,
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
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash)}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"confirm_source_freeze":true,"idempotency_key":"default-event-missing"}}`,
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
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash)}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"integrity-error"}}`,
			runForkTestSourceRunID,
			runForkTestEventID,
		))
		if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != BundleDataIntegrityErrorCode {
			t.Fatalf("run.fork integrity error = %#v, want %s", resp.Error, BundleDataIntegrityErrorCode)
		}
	})

	t.Run("executor source hash mismatch", func(t *testing.T) {
		executor := &recordingRunForkExecutor{err: errors.New(runbundle.CodeBundleDataIntegrityError + ": selected_contracts source hash mismatch: request bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc source " + runForkTestBundleHash)}
		handler := runForkTestHandler(t, &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash)}}, executor)
		resp := rpcCall(t, handler, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"hash-mismatch"}}`,
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
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: availability,
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
		}),
	})
}

func runForkAvailable(runID, bundleHash string) runbundle.Availability {
	return runbundle.Availability{
		RunID:            strings.TrimSpace(runID),
		Status:           "running",
		BundleHash:       strings.TrimSpace(bundleHash),
		BundleSource:     storerunlifecycle.BundleSourcePersisted,
		BundleRowPresent: true,
	}
}

func runForkUnavailable(runID, bundleHash, cause string) runbundle.Availability {
	availability := runForkAvailable(runID, bundleHash)
	availability.BundleSource = strings.TrimSpace(cause)
	availability.BundleRowPresent = false
	availability.ErrorCode = runbundle.CodeBundleUnavailable
	availability.Cause = strings.TrimSpace(cause)
	return availability
}

func runForkDataIntegrity(runID, bundleHash string) runbundle.Availability {
	availability := runForkAvailable(runID, bundleHash)
	availability.BundleRowPresent = false
	availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
	availability.Cause = "persisted_missing_bundle_row"
	return availability
}

type recordingRunForkAvailability struct {
	rows map[string]runbundle.Availability
	err  error
}

func (s *recordingRunForkAvailability) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	if s.err != nil {
		return runbundle.Availability{}, s.err
	}
	availability, ok := s.rows[strings.TrimSpace(runID)]
	if !ok {
		return runbundle.Availability{}, fmt.Errorf("run %s not found: %w", strings.TrimSpace(runID), store.ErrRunNotFound)
	}
	return availability, nil
}

type recordingRunForkExecutor struct {
	calls  int
	last   RunForkExecutionRequest
	result RunForkExecutionResult
	err    error
}

func (e *recordingRunForkExecutor) ExecuteRunFork(_ context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	e.calls++
	e.last = req
	if e.err != nil {
		return RunForkExecutionResult{}, e.err
	}
	result := e.result
	if result.BundleHash == "" {
		result.BundleHash = req.BundleHash
	}
	return result, nil
}
