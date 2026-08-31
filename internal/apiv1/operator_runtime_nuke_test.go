package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/google/uuid"
)

func TestOperatorRuntimeNukeDryRunUsesDestructiveResetOwners(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	owners := newRecordingRuntimeNukeOwners()
	fixture := newOperatorRuntimeContextFixture(t)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      newRecordingAPIIdempotencyStore(),
			RuntimeContexts:  fixture.manager,
			ResetCoordinator: owners,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"dry_run":true,"idempotency_key":"dry-run"}}`)
	if resp.Error != nil {
		t.Fatalf("runtime.nuke dry-run error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != true || result["status"] != "dry_run" || result["dry_run"] != true || result["include_source_artifacts"] != true || result["partial_failure"] != false {
		t.Fatalf("runtime.nuke dry-run result = %#v", result)
	}
	if got, want := strings.Join(owners.calls, ","), "plan,quiescence,cleanup,containers"; got != want {
		t.Fatalf("owner calls = %s, want %s", got, want)
	}
	assertRuntimeNukeContractStatus(t, result, destructivereset.ContractPublicAPIWrapper, "implemented_public_owner")
	assertRuntimeNukeContractStatus(t, result, destructivereset.ContractLegacyResetMigration, "split")
	if owners.lastPlan.DryRun != true || owners.lastQuiescence.Result.DryRun != true || owners.lastCleanup.Result.DryRun != true || owners.lastContainers.Result.DryRun != true {
		t.Fatalf("dry-run flag not propagated through owners: plan=%v quiescence=%v cleanup=%v containers=%v", owners.lastPlan.DryRun, owners.lastQuiescence.Result.DryRun, owners.lastCleanup.Result.DryRun, owners.lastContainers.Result.DryRun)
	}
	if !owners.lastPlan.IncludeSourceArtifacts || !owners.lastPlan.IncludeSourceArtifactsSet || !owners.lastQuiescence.Result.IncludeSourceArtifacts || !owners.lastCleanup.Result.IncludeSourceArtifacts || !owners.lastContainers.Result.IncludeSourceArtifacts {
		t.Fatalf("include_source_artifacts default not propagated through owners: plan=%#v quiescence=%v cleanup=%v containers=%v", owners.lastPlan, owners.lastQuiescence.Result.IncludeSourceArtifacts, owners.lastCleanup.Result.IncludeSourceArtifacts, owners.lastContainers.Result.IncludeSourceArtifacts)
	}
	if _, err := uuid.Parse(owners.lastPlan.OperationID); err != nil {
		t.Fatalf("runtime.nuke operation id = %q: %v", owners.lastPlan.OperationID, err)
	}
	for _, bundleHash := range []string{runStartTestBundleHash, runtimeContextTestBundleHashB} {
		if lookup := fixture.manager.LookupBundleHashStatus(bundleHash); !lookup.Loaded() {
			t.Fatalf("dry-run unloaded runtime context %s: %#v", bundleHash, lookup)
		}
	}
}

func TestOperatorRuntimeNukeUsesDistinctOperationIdentityPerLogicalRequest(t *testing.T) {
	owners := newRecordingRuntimeNukeOwners()
	idempotency := newRecordingAPIIdempotencyStore()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Ready: func() bool { return true }, Database: fakePinger{},
			Idempotency: idempotency, ResetCoordinator: owners,
		}),
	})

	first := `{"jsonrpc":"2.0","id":"nuke-1","method":"runtime.nuke","params":{"dry_run":true,"idempotency_key":"identity-1"}}`
	second := `{"jsonrpc":"2.0","id":"nuke-2","method":"runtime.nuke","params":{"dry_run":true,"idempotency_key":"identity-2"}}`
	for _, body := range []string{first, first, second} {
		if resp := rpcCall(t, handler, body); resp.Error != nil {
			t.Fatalf("runtime.nuke error = %#v", resp.Error)
		}
	}
	if len(owners.plans) != 2 {
		t.Fatalf("runtime.nuke executions = %d, want 2", len(owners.plans))
	}
	if owners.plans[0].OperationID == owners.plans[1].OperationID {
		t.Fatalf("separate runtime.nuke requests reused operation id %q", owners.plans[0].OperationID)
	}
	for i, req := range owners.plans {
		if _, err := uuid.Parse(req.OperationID); err != nil {
			t.Fatalf("runtime.nuke execution %d operation id = %q: %v", i, req.OperationID, err)
		}
	}
}

func TestOperatorRuntimeNukeApplyReportsPartialFailureAndIdempotency(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	owners := newRecordingRuntimeNukeOwners()
	owners.containerFailure = "docker stop denied"
	idempotency := newRecordingAPIIdempotencyStore()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      idempotency,
			ResetCoordinator: owners,
		}),
	})
	body := `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"include_source_artifacts":false,"idempotency_key":"apply"}}`

	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("runtime.nuke apply error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != false || result["status"] != "partial_failure" || result["include_source_artifacts"] != false || result["partial_failure"] != true {
		t.Fatalf("runtime.nuke partial result = %#v", result)
	}
	if owners.lastPlan.IncludeSourceArtifacts || !owners.lastPlan.IncludeSourceArtifactsSet || owners.lastCleanup.Result.IncludeSourceArtifacts || owners.lastContainers.Result.IncludeSourceArtifacts {
		t.Fatalf("include_source_artifacts=false not propagated through owners: plan=%#v cleanup=%v containers=%v", owners.lastPlan, owners.lastCleanup.Result.IncludeSourceArtifacts, owners.lastContainers.Result.IncludeSourceArtifacts)
	}
	if len(owners.calls) != 4 {
		t.Fatalf("owner call count = %d, want 4", len(owners.calls))
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("runtime.nuke replay error = %#v", replay.Error)
	}
	if len(owners.calls) != 4 {
		t.Fatalf("owner calls after replay = %d, want unchanged 4", len(owners.calls))
	}

	conflict := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"include_source_artifacts":true,"idempotency_key":"apply"}}`)
	if conflict.Error == nil {
		t.Fatal("runtime.nuke idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("runtime.nuke conflict data = %#v, want %s", data, IdempotencyConflictCode)
	}
}

func TestOperatorRuntimeNukeIncludeSourceArtifactsDeactivatesLoadedRuntimeContexts(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	fixture := newOperatorRuntimeContextFixture(t)
	owners := newRecordingRuntimeNukeOwners()
	owners.runtimeContexts = fixture.manager
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      newRecordingAPIIdempotencyStore(),
			RuntimeContexts:  fixture.manager,
			ResetCoordinator: owners,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"include_source_artifacts":true,"idempotency_key":"nuke-contexts"}}`)
	if resp.Error != nil {
		t.Fatalf("runtime.nuke error = %#v", resp.Error)
	}
	assertRuntimeContextsUnloaded(t, fixture.manager)
}

func TestOperatorRuntimeNukeIncludeSourceArtifactsPartialFailureDeactivatesLoadedRuntimeContextsAndReplay(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	idempotency := newRecordingAPIIdempotencyStore()
	fixture := newOperatorRuntimeContextFixture(t)
	owners := newRecordingRuntimeNukeOwners()
	owners.containerFailure = "docker stop denied"
	owners.runtimeContexts = fixture.manager
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      idempotency,
			RuntimeContexts:  fixture.manager,
			ResetCoordinator: owners,
		}),
	})
	body := `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"include_source_artifacts":true,"idempotency_key":"nuke-contexts-partial"}}`

	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("runtime.nuke partial error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	if result["ok"] != false || result["status"] != "partial_failure" || result["include_source_artifacts"] != true || result["partial_failure"] != true {
		t.Fatalf("runtime.nuke include_source_artifacts partial result = %#v", result)
	}
	if len(owners.calls) != 4 {
		t.Fatalf("runtime.nuke partial owner calls = %d, want 4", len(owners.calls))
	}
	assertRuntimeContextsUnloaded(t, fixture.manager)

	replayFixture := newOperatorRuntimeContextFixture(t)
	replayOwners := newRecordingRuntimeNukeOwners()
	replayHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      idempotency,
			RuntimeContexts:  replayFixture.manager,
			ResetCoordinator: replayOwners,
		}),
	})
	replay := rpcCall(t, replayHandler, body)
	if replay.Error != nil {
		t.Fatalf("runtime.nuke partial replay error = %#v", replay.Error)
	}
	if len(replayOwners.calls) != 0 {
		t.Fatalf("runtime.nuke partial replay owner calls = %d, want 0", len(replayOwners.calls))
	}
	assertRuntimeContextsLoaded(t, replayFixture.manager)
}

func assertRuntimeContextsUnloaded(t *testing.T, manager *swruntime.RuntimeContextManager) {
	t.Helper()
	for _, bundleHash := range []string{runStartTestBundleHash, runtimeContextTestBundleHashB} {
		lookup := manager.LookupBundleHashStatus(bundleHash)
		if lookup.Loaded() || lookup.Cause != swruntime.RuntimeContextCauseUnloaded {
			t.Fatalf("runtime context %s after runtime.nuke = %#v, want unloaded", bundleHash, lookup)
		}
	}
}

func assertRuntimeContextsLoaded(t *testing.T, manager *swruntime.RuntimeContextManager) {
	t.Helper()
	for _, bundleHash := range []string{runStartTestBundleHash, runtimeContextTestBundleHashB} {
		lookup := manager.LookupBundleHashStatus(bundleHash)
		if !lookup.Loaded() {
			t.Fatalf("runtime context %s after idempotent replay = %#v, want no repeated side effect", bundleHash, lookup)
		}
	}
}

func TestOperatorRuntimeNukeExcludeSourceArtifactsStillQuiescesRuntimeContexts(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	fixture := newOperatorRuntimeContextFixture(t)
	owners := newRecordingRuntimeNukeOwners()
	owners.runtimeContexts = fixture.manager
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return now },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      newRecordingAPIIdempotencyStore(),
			RuntimeContexts:  fixture.manager,
			ResetCoordinator: owners,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"include_source_artifacts":false,"idempotency_key":"nuke-no-contexts"}}`)
	if resp.Error != nil {
		t.Fatalf("runtime.nuke include_source_artifacts=false error = %#v", resp.Error)
	}
	assertRuntimeContextsUnloaded(t, fixture.manager)
}

func TestOperatorRuntimeNukeAuthFailureDoesNotTouchResetOwners(t *testing.T) {
	owners := newRecordingRuntimeNukeOwners()
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      newRecordingAPIIdempotencyStore(),
			ResetCoordinator: owners,
		}),
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{}}`))
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d, want 401", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{}}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad auth status = %d, want 401", rec.Code)
	}
	if len(owners.calls) != 0 {
		t.Fatalf("owner calls after auth failures = %v, want none", owners.calls)
	}
}

func TestOperatorRuntimeNukeOperationInProgressFailsClosed(t *testing.T) {
	owners := newRecordingRuntimeNukeOwners()
	owners.planErr = destructivereset.ErrOperationInProgress
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Idempotency:      newRecordingAPIIdempotencyStore(),
			ResetCoordinator: owners,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"nuke","method":"runtime.nuke","params":{"idempotency_key":"busy"}}`)
	if resp.Error == nil {
		t.Fatal("runtime.nuke busy error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != RuntimeNukeInProgressCode || data["retryable"] != true {
		t.Fatalf("runtime.nuke busy data = %#v, want retryable %s", data, RuntimeNukeInProgressCode)
	}
}

type recordingRuntimeNukeOwners struct {
	calls            []string
	planErr          error
	containerFailure string
	lastPlan         destructivereset.Request
	plans            []destructivereset.Request
	lastQuiescence   destructivereset.QuiescenceRequest
	lastCleanup      destructivereset.CleanupRequest
	lastContainers   destructivereset.ContainerResetRequest
	runtimeContexts  *swruntime.RuntimeContextManager
}

func newRecordingRuntimeNukeOwners() *recordingRuntimeNukeOwners {
	return &recordingRuntimeNukeOwners{}
}

func (o *recordingRuntimeNukeOwners) BuildPlan(_ context.Context, req destructivereset.Request) (destructivereset.Result, bool, error) {
	o.calls = append(o.calls, "plan")
	o.lastPlan = req
	o.plans = append(o.plans, req)
	if o.planErr != nil {
		return destructivereset.Result{}, false, o.planErr
	}
	return destructivereset.Result{
		OperationName:          destructivereset.DefaultOperationName,
		DryRun:                 req.DryRun,
		IncludeSourceArtifacts: req.IncludeSourceArtifacts,
		PlannedAt:              req.RequestedAt,
		Plan: destructivereset.Plan{
			IncludeSourceArtifacts: req.IncludeSourceArtifacts,
			ActiveRuns:             []destructivereset.RunRef{{RunID: "00000000-0000-0000-0000-000000000001", Status: "running"}},
			ManagedContainers: []destructivereset.ContainerRef{{
				Name:          "swarm-agent-1",
				Kind:          "agent",
				Action:        destructivereset.ContainerActionStop,
				ResetEligible: true,
				RunID:         "00000000-0000-0000-0000-000000000001",
			}},
			DownstreamContracts: destructivereset.DefaultDownstreamContracts(),
			ResetSeams:          destructivereset.DefaultResetSeams(),
		},
	}, false, nil
}

func (o *recordingRuntimeNukeOwners) Execute(ctx context.Context, req destructivereset.Request) (destructivereset.ExecutionResult, error) {
	result, _, err := o.BuildPlan(ctx, req)
	if err != nil {
		return destructivereset.ExecutionResult{}, err
	}
	if !req.DryRun && o.runtimeContexts != nil {
		if err := o.runtimeContexts.QuiesceAllRuntimeContexts(ctx); err != nil {
			return destructivereset.ExecutionResult{}, err
		}
	}
	quiescence, err := (recordingRuntimeNukeQuiescer{o}).Apply(ctx, destructivereset.QuiescenceRequest{Result: result, ActorTokenID: req.ActorTokenID, RequestedAt: req.RequestedAt})
	if err != nil {
		return destructivereset.ExecutionResult{}, err
	}
	cleanup, err := (recordingRuntimeNukeCleaner{o}).Apply(ctx, destructivereset.CleanupRequest{OperationID: req.OperationID, Result: result, Quiescence: quiescence, ActorTokenID: req.ActorTokenID, RequestedAt: req.RequestedAt})
	if err != nil {
		return destructivereset.ExecutionResult{}, err
	}
	containers, err := (recordingRuntimeNukeContainerStopper{o}).Apply(ctx, destructivereset.ContainerResetRequest{Result: result, Cleanup: cleanup, ActorTokenID: req.ActorTokenID, RequestedAt: req.RequestedAt})
	if err != nil {
		return destructivereset.ExecutionResult{}, err
	}
	return destructivereset.ExecutionResult{Plan: result, Quiescence: quiescence, Cleanup: cleanup, Containers: containers}, nil
}

func (o *recordingRuntimeNukeOwners) ApplyQuiescence(req destructivereset.QuiescenceRequest) destructivereset.QuiescenceResult {
	return destructivereset.QuiescenceResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
		ReasonCode:    destructivereset.QuiescenceReasonCode,
		ControlledBy:  destructivereset.QuiescenceControlledBy,
	}
}

type recordingRuntimeNukeQuiescer struct {
	owners *recordingRuntimeNukeOwners
}

func (q recordingRuntimeNukeQuiescer) Apply(ctx context.Context, req destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error) {
	o := q.owners
	o.calls = append(o.calls, "quiescence")
	o.lastQuiescence = req
	return o.ApplyQuiescence(req), nil
}

func (o *recordingRuntimeNukeOwners) ApplyCleanup(req destructivereset.CleanupRequest) destructivereset.CleanupResult {
	return destructivereset.CleanupResult{
		OperationName:          req.Result.OperationName,
		DryRun:                 req.Result.DryRun,
		IncludeSourceArtifacts: req.Result.IncludeSourceArtifacts,
		AppliedAt:              req.RequestedAt,
		RunIDs:                 []string{"00000000-0000-0000-0000-000000000001"},
	}
}

type recordingRuntimeNukeCleaner struct {
	owners *recordingRuntimeNukeOwners
}

func (c recordingRuntimeNukeCleaner) Apply(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	o := c.owners
	o.calls = append(o.calls, "cleanup")
	o.lastCleanup = req
	return o.ApplyCleanup(req), nil
}

func (o *recordingRuntimeNukeOwners) ApplyContainerReset(req destructivereset.ContainerResetRequest) destructivereset.ContainerResetResult {
	result := destructivereset.ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
		Selected:      req.Result.Plan.ManagedContainers,
	}
	if req.Result.DryRun {
		return result
	}
	if o.containerFailure != "" {
		result.Failed = []destructivereset.ContainerStopFailure{{
			Container: req.Result.Plan.ManagedContainers[0],
			Error:     o.containerFailure,
		}}
		return result
	}
	result.Stopped = req.Result.Plan.ManagedContainers
	return result
}

type recordingRuntimeNukeContainerStopper struct {
	owners *recordingRuntimeNukeOwners
}

func (s recordingRuntimeNukeContainerStopper) Apply(ctx context.Context, req destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error) {
	o := s.owners
	o.calls = append(o.calls, "containers")
	o.lastContainers = req
	return o.ApplyContainerReset(req), nil
}

type recordingAPIIdempotencyStore struct {
	records map[string]apiidempotency.Completion
	hashes  map[string]string
}

func newRecordingAPIIdempotencyStore() *recordingAPIIdempotencyStore {
	return &recordingAPIIdempotencyStore{
		records: map[string]apiidempotency.Completion{},
		hashes:  map[string]string{},
	}
}

func (s *recordingAPIIdempotencyStore) WithAPIIdempotency(
	ctx context.Context,
	req apiidempotency.Request,
	execute func(context.Context) (apiidempotency.Completion, error),
) (apiidempotency.Completion, bool, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	key := strings.Join([]string{req.Method, req.ActorTokenID, req.IdempotencyKey}, "|")
	if completion, ok := s.records[key]; ok {
		if s.hashes[key] != req.RequestHash {
			return apiidempotency.Completion{}, false, &apiidempotency.ConflictError{
				OriginalRequestHash:    s.hashes[key],
				ConflictingRequestHash: req.RequestHash,
				Method:                 req.Method,
				ResourceID:             completion.ResourceID,
			}
		}
		copied := completion
		copied.Response = append(json.RawMessage(nil), completion.Response...)
		return copied, true, nil
	}
	completion, err := execute(ctx)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	copied := completion
	copied.Response = append(json.RawMessage(nil), completion.Response...)
	s.records[key] = copied
	s.hashes[key] = req.RequestHash
	return completion, false, nil
}

func assertRuntimeNukeContractStatus(t *testing.T, result map[string]any, contractID, status string) {
	t.Helper()
	planResult := asMap(t, result["plan"])
	plan := asMap(t, planResult["plan"])
	contracts, ok := plan["downstream_contracts"].([]any)
	if !ok {
		t.Fatalf("downstream_contracts = %#v, want array", plan["downstream_contracts"])
	}
	for _, raw := range contracts {
		contract := asMap(t, raw)
		if contract["id"] == contractID {
			if contract["status"] != status {
				t.Fatalf("contract %s status = %#v, want %s", contractID, contract["status"], status)
			}
			return
		}
	}
	t.Fatalf("contract %s missing from downstream_contracts: %#v", contractID, contracts)
}
