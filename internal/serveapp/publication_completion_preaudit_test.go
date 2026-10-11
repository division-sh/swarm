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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestPublicPublicationCompletionBoundaryBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, true)
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			endpoint, _, hash := startPublicInputRollbackRuntime(t, backend)
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

func TestPublicPublicationCompletionExactBytesBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, false)
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			endpoint, _, hash := startPublicInputRollbackRuntime(t, backend)
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

type publicationAuditHTTPResult struct {
	result   json.RawMessage
	rpcError json.RawMessage
	err      error
}

func publicationAuditHTTP(ctx context.Context, endpoint string, params map[string]any) publicationAuditHTTPResult {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "event.publish", "params": params})
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
	return map[string]any{"bundle_hash": hash, "event_name": "inbound.telegram.text_message", "idempotency_key": key,
		"payload": map[string]any{"conversation_reference": "42", "conversation_scope": "direct", "external_account_reference": "42", "provider_message_reference": 1, "text": "completion boundary audit"}}
}

type publicationCompletionAudit struct {
	mu          sync.Mutex
	native      runtimebus.APIEventPublicationCommitOwner
	workflow    pipeline.WorkflowPersistenceOwner
	plan        pipeline.DynamicFlowRuntimeReadinessPlan
	process     *worklifetime.Process
	request     apiidempotency.Request
	committed   runtimebus.CommittedAPIEventPublication
	commits     int
	run         string
	hold        bool
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func installPublicationCompletionAudit(t *testing.T, hold bool) *publicationCompletionAudit {
	p := &publicationCompletionAudit{hold: hold, entered: make(chan struct{}), release: make(chan struct{})}
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		projected := previous(owner)
		var workflows pipeline.WorkflowPersistenceOwner
		switch native := projected.deps.EventStore.(type) {
		case *store.SQLiteRuntimeStore:
			p.native, workflows = native, native
			projected.deps.EventStore = &publicationAuditSQLite{SQLiteRuntimeStore: native, probe: p}
		case *store.PostgresStore:
			p.native, workflows = native, native
			projected.deps.EventStore = &publicationAuditPostgres{PostgresStore: native, probe: p}
		default:
			panic(fmt.Sprintf("audit requires native selected owner, got %T", native))
		}
		projected.deps.WorkflowPersistence = pipeline.NewWorkflowPersistence(&publicationAuditReadiness{WorkflowPersistenceOwner: workflows, probe: p})
		p.workflow = workflows
		return projected
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	// Registered before serving cleanup; deferred release below is also required
	// so a failed assertion cannot leave the harness waiting on this audit cut.
	t.Cleanup(func() { p.releaseOnce.Do(func() { close(p.release) }) })
	return p
}

func (p *publicationCompletionAudit) commit(ctx context.Context, native runtimebus.APIEventPublicationCommitOwner, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	result, err := native.CommitAPIEventPublication(ctx, command)
	p.mu.Lock()
	p.commits++
	p.request, p.committed = command.Idempotency, result
	p.process, _ = worklifetime.ProcessFromContext(ctx)
	p.run = command.Publication.Commit.Event.Event().RunID()
	p.mu.Unlock()
	return result, err
}

type publicationAuditSQLite struct {
	*store.SQLiteRuntimeStore
	probe *publicationCompletionAudit
}

func (s *publicationAuditSQLite) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	return s.probe.commit(ctx, s.SQLiteRuntimeStore, command)
}

type publicationAuditPostgres struct {
	*store.PostgresStore
	probe *publicationCompletionAudit
}

func (s *publicationAuditPostgres) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	return s.probe.commit(ctx, s.PostgresStore, command)
}

type publicationAuditReadiness struct {
	pipeline.WorkflowPersistenceOwner
	probe *publicationCompletionAudit
}

func (s *publicationAuditReadiness) BeginDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	s.probe.mu.Lock()
	hold := s.probe.hold && s.probe.committed.Acknowledged && request.Plan().RunID == s.probe.run
	if s.probe.committed.Acknowledged && request.Plan().RunID == s.probe.run && s.probe.plan.RunID == "" {
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
	native, committed, workflow, plan, process := p.native, p.committed, p.workflow, p.plan, p.process
	runID := p.run
	p.mu.Unlock()
	deadline := time.Now().Add(20 * time.Second)
	for {
		evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, runID, committed.Completion.ResourceID)
		ready := false
		if plan.RunID != "" {
			readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), runID, plan.Identity.Route())
			if err != nil || !found {
				t.Fatalf("exact readiness readback: found=%t err=%v", found, err)
			}
			ready = readiness.Phase == pipeline.FlowAttachmentReady
		}
		if evidence.PipelineReceiptCount == 1 && ready && process != nil && process.PublicCompletionCount() == 0 {
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
