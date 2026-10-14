package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestPublicPublicationCompletionBoundaryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, true)
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			h, hash := startChannelAnchorJourneyRuntime(t, backend, "completion-boundary-test", false)
			endpoint := h.rpcEndpoint()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan publicationAuditHTTPResult, 1)
			go func() { done <- publicationAuditHTTP(ctx, endpoint, publicationAuditParams(hash, "audit1930-held")) }()
			select {
			case <-probe.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("owned readiness cut not reached")
			}
			probe.mu.Lock()
			request, native, committed := probe.request, probe.native, probe.committed
			probe.mu.Unlock()
			if !committed.Acknowledged {
				t.Fatal("readiness cut lacks exact native COMMIT acknowledgment")
			}
			if count := probe.process.PublicCompletionCount(); count != 1 {
				t.Fatalf("held consequence released its reservation: %d", count)
			}
			stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
			if err != nil || !found {
				t.Fatalf("completion at post-ACK cut: found=%t err=%v", found, err)
			}
			result := <-done
			if result.err != nil || result.rpcError != nil || !bytes.Equal(result.result, stored.Response) {
				t.Fatalf("recorded completion did not return while readiness was held: %+v stored=%s", result, stored.Response)
			}
			cancel()
			replay := publicationAuditHTTP(t.Context(), endpoint, publicationAuditParams(hash, "audit1930-held"))
			if replay.err != nil || replay.rpcError != nil || len(replay.result) == 0 {
				t.Fatalf("same-key replay while readiness held: %+v", replay)
			}
			probe.mu.Lock()
			calls := probe.commits
			probe.mu.Unlock()
			if calls != 1 || !bytes.Equal(replay.result, stored.Response) {
				t.Fatalf("same-key replay changed completion or executed %d commits", calls)
			}
			probe.releaseOnce.Do(func() { close(probe.release) })
			waitPublicationCompletionDisposition(t, probe)
		})
	}
}

func TestPublicFeedOnlyCompletionUnloadRestartBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, false)
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnly(t)
			managers := make(chan *runtimepkg.RuntimeContextManager, 2)
			h.opts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) { managers <- manager }
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			manager := <-managers
			var identity apiv1.RuntimeIdentityResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
			if len(identity.SourceArtifacts) != 1 {
				t.Fatalf("initial source selection: %+v", identity)
			}
			hash := identity.SourceArtifacts[0].BundleHash
			params := map[string]any{
				"bundle_hash": hash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
				"data": map[string]any{"imports": []any{map[string]any{
					"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
					"expected_head": map[string]any{"state": "absent"},
					"input":         map[string]any{"format": "jsonl", "content_base64": ""},
				}}, "pins": []any{}},
			}
			first := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", params)
			if first.err != nil || first.rpcError != nil {
				t.Fatalf("initial feed-only start: error=%v rpc=%s", first.err, first.rpcError)
			}
			probe.mu.Lock()
			native, request, committed, process := probe.native, probe.request, probe.deployment, probe.process
			probe.mu.Unlock()
			stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
			if err != nil || !found || committed == nil || !committed.Acknowledged || !bytes.Equal(first.result, stored.Response) || !bytes.Equal(first.result, committed.Completion.Response) {
				t.Fatalf("initial native/store/wire completion differs: found=%t err=%v", found, err)
			}
			deadline := time.Now().Add(20 * time.Second)
			for process.PublicCompletionCount() != 0 {
				if time.Now().After(deadline) {
					t.Fatal("feed-only activation retained its public responsibility")
				}
				time.Sleep(time.Millisecond)
			}
			unloaded := manager.DeactivateBundleHash(hash, runtimepkg.RuntimeContextCauseUnloaded)
			if !unloaded.Found || !unloaded.Changed || unloaded.ShutdownErr != nil || unloaded.State != runtimepkg.RuntimeContextStateUnloaded || manager.LookupBundleHashStatus(hash).Loaded() {
				t.Fatalf("source remained executable: %+v", unloaded)
			}
			assertPureReplay := func(label string) {
				t.Helper()
				probe.mu.Lock()
				owner := probe.native
				probe.mu.Unlock()
				before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
				if err != nil {
					t.Fatal(err)
				}
				replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", params)
				if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, first.result) {
					t.Fatalf("%s exact replay failed: error=%v rpc=%s result=%s", label, replay.err, replay.rpcError, replay.result)
				}
				changed := make(map[string]any, len(params))
				for key, value := range params {
					changed[key] = value
				}
				changed["run_id"] = uuid.NewString()
				conflict := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", changed)
				var refusal struct {
					Data struct {
						Code string `json:"code"`
					} `json:"data"`
				}
				if conflict.err != nil || json.Unmarshal(conflict.rpcError, &refusal) != nil || refusal.Data.Code != apiv1.IdempotencyConflictCode {
					t.Fatalf("%s conflict lost precedence: error=%v rpc=%s", label, conflict.err, conflict.rpcError)
				}
				after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
				if err != nil || before != after {
					t.Fatalf("%s replay/conflict changed rows: before=%+v after=%+v err=%v", label, before, after, err)
				}
			}
			assertPureReplay("unloaded")
			h.stop(t)
			h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionReplacement(t)
			h.start(t)
			manager = <-managers
			if lookup := manager.LookupBundleHashStatus(hash); lookup.Loaded() {
				t.Fatalf("restart resurrected original source: %+v", lookup)
			}
			assertPureReplay("restarted-with-source-unavailable")
		})
	}
}

func TestPublicPublicationCompletionExactBytesBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, false)
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			h, hash := startChannelAnchorJourneyRuntime(t, backend, "completion-bytes-test", false)
			endpoint := h.rpcEndpoint()
			params := publicationAuditParams(hash, "audit1930-bytes")
			first := publicationAuditHTTP(t.Context(), endpoint, params)
			if first.err != nil || first.rpcError != nil || len(first.result) == 0 {
				t.Fatalf("initial publication: %+v", first)
			}
			probe.mu.Lock()
			request, native, committed := probe.request, probe.native, probe.committed
			probe.mu.Unlock()
			stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
			if err != nil || !found || !committed.Acknowledged {
				t.Fatalf("native completion: found=%t ack=%t err=%v", found, committed.Acknowledged, err)
			}
			replay := publicationAuditHTTP(t.Context(), endpoint, params)
			if replay.err != nil || replay.rpcError != nil || len(replay.result) == 0 {
				t.Fatalf("same-key publication replay: %+v", replay)
			}
			probe.mu.Lock()
			calls := probe.commits
			probe.mu.Unlock()
			if calls != 1 || !bytes.Equal(first.result, replay.result) {
				t.Fatalf("first/replay divergence or re-execution: commits=%d first=%s replay=%s", calls, first.result, replay.result)
			}
			for label, raw := range map[string][]byte{"native": committed.Completion.Response, "stored": stored.Response, "wire": first.result, "replay": replay.result} {
				t.Logf("AUDIT1930 %s bytes=%d sha256=%x", label, len(raw), sha256.Sum256(raw))
			}
			t.Logf("AUDIT1930 exact_native_stored=%t exact_stored_first_wire=%t exact_stored_replay_wire=%t commits=%d", bytes.Equal(committed.Completion.Response, stored.Response), bytes.Equal(stored.Response, first.result), bytes.Equal(stored.Response, replay.result), calls)
			if !bytes.Equal(committed.Completion.Response, stored.Response) || !bytes.Equal(stored.Response, first.result) || !bytes.Equal(stored.Response, replay.result) {
				t.Fatal("native/store/first/replay result bytes differ")
			}
			waitPublicationCompletionDisposition(t, probe)
		})
	}
}

func TestPublicPublicationRetirementRecoveryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, true)
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			h, hash := startChannelAnchorJourneyRuntime(t, backend, "completion-retirement-test", false)
			params := publicationAuditParams(hash, "completion-retirement")
			first := publicationAuditHTTP(t.Context(), h.rpcEndpoint(), params)
			if first.err != nil || first.rpcError != nil {
				t.Fatalf("first native acknowledgment: error=%v rpc=%s", first.err, first.rpcError)
			}
			select {
			case <-probe.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("accepted consequence did not reach readiness cut")
			}
			probe.mu.Lock()
			process, native, request := probe.process, probe.native, probe.request
			probe.mu.Unlock()
			stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
			if err != nil || !found || !bytes.Equal(first.result, stored.Response) || process.PublicCompletionCount() != 1 {
				t.Fatalf("held exact responsibility: found=%t err=%v count=%d", found, err, process.PublicCompletionCount())
			}
			h.stop(t)
			if count := process.PublicCompletionCount(); count != 0 {
				t.Fatalf("served shutdown released dependencies before responsibility joined: %d", count)
			}
			probe.mu.Lock()
			probe.hold = false
			probe.mu.Unlock()
			h.start(t)
			replay := publicationAuditHTTP(t.Context(), h.rpcEndpoint(), params)
			if replay.err != nil || replay.rpcError != nil || !bytes.Equal(replay.result, stored.Response) {
				t.Fatalf("recovered response: error=%v rpc=%s result=%s", replay.err, replay.rpcError, replay.result)
			}
			probe.mu.Lock()
			commits := probe.commits
			probe.mu.Unlock()
			if commits != 1 {
				t.Fatalf("restart/replay performed %d API commits", commits)
			}
			waitPublicationCompletionDisposition(t, probe)
		})
	}
}

type publicationAuditHTTPResult struct {
	result   json.RawMessage
	rpcError json.RawMessage
	err      error
}

func publicationAuditHTTP(ctx context.Context, endpoint string, params map[string]any) publicationAuditHTTPResult {
	return publicationCompletionHTTPMethod(ctx, endpoint, "event.publish", params)
}

func publicationCompletionHTTPMethod(ctx context.Context, endpoint, method string, params map[string]any) publicationAuditHTTPResult {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return publicationAuditHTTPResult{err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return publicationAuditHTTPResult{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return publicationAuditHTTPResult{err: err}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return publicationAuditHTTPResult{err: err}
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return publicationAuditHTTPResult{err: err}
	}
	return publicationAuditHTTPResult{result: envelope.Result, rpcError: envelope.Error}
}

func publicationAuditParams(hash, key string) map[string]any {
	return map[string]any{"bundle_hash": hash, "event_name": "work.requested", "idempotency_key": key,
		"payload": map[string]any{"seed": true}}
}

type publicationCompletionAudit struct {
	mu                  sync.Mutex
	native              runtimebus.APIEventPublicationCommitOwner
	workflow            pipeline.WorkflowPersistenceOwner
	obligations         pipelineobligation.Store
	plan                pipeline.DynamicFlowRuntimeReadinessPlan
	process             *worklifetime.Process
	request             apiidempotency.Request
	committed           runtimebus.CommittedAPIEventPublication
	deployment          *runtimebus.CommittedDeploymentRunCreation
	deployments         int
	heldRuns            map[string]pipeline.DynamicFlowRuntimeReadinessPlan
	commits             int
	run                 string
	eventID             string
	hold                bool
	resultFault         string
	holdRelease         bool
	entered             chan struct{}
	release             chan struct{}
	claimDisposed       chan struct{}
	claimDisposeOnce    sync.Once
	claimDisposeCount   int
	claimDisposeError   error
	enterOnce           sync.Once
	releaseOnce         sync.Once
	beforeCommit        func(context.Context)
	afterCommit         func(context.Context, bool)
	holdBeforeReadiness bool
}

func installPublicationCompletionAudit(t *testing.T, hold bool) *publicationCompletionAudit {
	p := &publicationCompletionAudit{hold: hold, entered: make(chan struct{}), release: make(chan struct{}), claimDisposed: make(chan struct{})}
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		projected := previous(owner)
		native, ok := projected.deps.EventStore.(publicationAuditStore)
		if !ok {
			panic(fmt.Sprintf("audit requires the complete native publication roles, got %T", projected.deps.EventStore))
		}
		p.native = native
		projected.deps.EventStore = &publicationAuditPublication{publicationAuditStore: native, probe: p}
		projected.deps.WorkflowPersistence = pipeline.NewWorkflowPersistence(&publicationAuditReadiness{WorkflowPersistenceOwner: native, probe: p})
		preflight, ok := projected.deps.PipelineObligations.(pipelineobligation.ResumeAdmissionPreflighter)
		if !ok {
			panic("audit requires the native pending-work preflight owner")
		}
		projected.deps.PipelineObligations = &publicationAuditPipelineRelease{Store: projected.deps.PipelineObligations, ResumeAdmissionPreflighter: preflight, probe: p}
		p.workflow = native
		p.obligations = projected.deps.PipelineObligations
		return projected
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	// Registered before serving cleanup; deferred release below is also required
	// so a failed assertion cannot leave the harness waiting on this audit cut.
	t.Cleanup(func() { p.releaseOnce.Do(func() { close(p.release) }) })
	return p
}

func (p *publicationCompletionAudit) commit(ctx context.Context, native runtimebus.APIEventPublicationCommitOwner, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	if p.beforeCommit != nil {
		p.beforeCommit(ctx)
	}
	result, err := native.CommitAPIEventPublication(ctx, command)
	p.mu.Lock()
	p.commits++
	p.request, p.committed = command.Idempotency, result
	p.process, _ = worklifetime.ProcessFromContext(ctx)
	p.run = command.Publication.Commit.Event.Event().RunID()
	p.eventID = command.Publication.Commit.Event.Event().ID()
	fault := p.resultFault
	p.mu.Unlock()
	if p.afterCommit != nil {
		p.afterCommit(ctx, result.Acknowledged)
	}
	return corruptPublicationAuditResult(result, err, fault)
}

type publicationAuditStore interface {
	runtimebus.EventStore
	runtimebus.CommitPublicationOwner
	runtimebus.APIEventPublicationCommitOwner
	runtimebus.DeploymentRunCreationCommitOwner
	runtimebus.FlowInstanceActivationCommitOwner
	pipeline.FlowInstanceIndexReader
	runtimebus.PreparedPublishEventReader
	runtimemanager.SelectedContractRouteRecoveryReader
	pipeline.WorkflowTargetPersistenceReader
	pipeline.WorkflowPersistenceOwner
	RequirePublicationRunActive(context.Context, string) error
}

type publicationAuditPublication struct {
	publicationAuditStore
	probe *publicationCompletionAudit
}

func (s *publicationAuditPublication) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	return s.probe.commit(ctx, s.publicationAuditStore, command)
}

func (s *publicationAuditPublication) CommitDeploymentRunCreation(ctx context.Context, command runtimebus.DeploymentRunCreationCommand) (runtimebus.CommittedDeploymentRunCreation, error) {
	if s.probe.beforeCommit != nil {
		s.probe.beforeCommit(ctx)
	}
	result, err := s.publicationAuditStore.CommitDeploymentRunCreation(ctx, command)
	s.probe.observeDeployment(ctx, command, result)
	if s.probe.afterCommit != nil {
		s.probe.afterCommit(ctx, result.Acknowledged)
	}
	return s.probe.corruptDeployment(result, err)
}

func (p *publicationCompletionAudit) observeDeployment(ctx context.Context, command runtimebus.DeploymentRunCreationCommand, result runtimebus.CommittedDeploymentRunCreation) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deployment = &result
	p.deployments++
	p.request = command.Idempotency
	p.process, _ = worklifetime.ProcessFromContext(ctx)
	p.run = command.RunCreation.RunID
	if p.heldRuns != nil && result.Acknowledged && !result.Replay {
		p.heldRuns[p.run] = pipeline.DynamicFlowRuntimeReadinessPlan{}
	}
}

type publicationAuditReadiness struct {
	pipeline.WorkflowPersistenceOwner
	probe *publicationCompletionAudit
}

func (s *publicationAuditReadiness) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	s.probe.mu.Lock()
	acknowledged := s.probe.committed.Acknowledged || s.probe.deployment != nil && s.probe.deployment.Acknowledged
	hold := s.probe.holdBeforeReadiness && acknowledged && runID == s.probe.run
	s.probe.mu.Unlock()
	if hold {
		s.probe.enterOnce.Do(func() { close(s.probe.entered) })
		select {
		case <-s.probe.release:
		case <-ctx.Done():
		}
	}
	return s.WorkflowPersistenceOwner.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func (s *publicationAuditReadiness) BeginDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	s.probe.mu.Lock()
	acknowledged := s.probe.committed.Acknowledged || s.probe.deployment != nil && s.probe.deployment.Acknowledged
	hold := s.probe.hold && acknowledged && request.Plan().RunID == s.probe.run
	if s.probe.heldRuns != nil {
		_, admitted := s.probe.heldRuns[request.Plan().RunID]
		hold = s.probe.hold && admitted
		if admitted {
			s.probe.heldRuns[request.Plan().RunID] = request.Plan()
		}
	}
	if acknowledged && request.Plan().RunID == s.probe.run && s.probe.plan.RunID == "" {
		s.probe.plan = request.Plan()
	}
	s.probe.mu.Unlock()
	if hold {
		s.probe.enterOnce.Do(func() { close(s.probe.entered) })
		select {
		case <-s.probe.release:
		case <-ctx.Done():
		}
	}
	return s.WorkflowPersistenceOwner.BeginDynamicFlowRuntimeActivation(ctx, request)
}

func waitPublicationCompletionDisposition(t *testing.T, p *publicationCompletionAudit) {
	t.Helper()
	p.mu.Lock()
	native, committed, workflow, process := p.native, p.committed, p.workflow, p.process
	runID := p.run
	p.mu.Unlock()
	deadline := time.Now().Add(20 * time.Second)
	for {
		p.mu.Lock()
		plan := p.plan
		p.mu.Unlock()
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, runID, committed.Completion.ResourceID)
		ready := false
		if plan.RunID != "" {
			readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), runID, plan.Identity.Route())
			if err != nil || !found {
				t.Fatalf("exact readiness readback: found=%t err=%v", found, err)
			}
			ready = readiness.Phase == pipeline.FlowAttachmentReady
		}
		if evidence.PipelineReceiptCount == 1 && evidence.PipelineReceiptOutcome == "success" && evidence.PipelineReceiptFailure == nil && ready && process != nil && process.PublicCompletionCount() == 0 {
			if !evidence.RecordFound || evidence.CommittedScope == "" {
				t.Fatal("settlement lost the exact committed publication")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("accepted publication did not settle after readiness release: %+v", evidence)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
