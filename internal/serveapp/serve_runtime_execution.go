package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// Reset withdraws executable readiness, not the already-booted control API.
// Operators must still be able to read state and replay the reset outcome.
type serveSupervisorReadiness struct {
	serveReadiness
	supervisor *processLifecycleSupervisor
}

func (r serveSupervisorReadiness) RuntimeLoad() bool {
	return runtimeAdmissionReady(r.serveReadiness)
}

func (r serveSupervisorReadiness) ControlReady() bool {
	r.supervisor.mu.RLock()
	defer r.supervisor.mu.RUnlock()
	return r.supervisor.apiReady
}

// These handlers retain execution objects. Store-only and process-control
// handlers are composed separately and do not acquire a runtime occurrence.
func buildServeRuntimeExecution(stores *storeselected.Owner, req selectedAPICapabilityRequest, primary serveRuntimeBundleContext, binding toolgateway.Binding) (map[string]apiv1.MethodHandler, error) {
	rt := primary.runtime
	req.Workspaces, req.Source = primary.workspaces, primary.loaded.source
	req.LoadedBundle, req.ExecutionPosture = primary.loaded, rt.ExecutionPosture
	caps, err := buildSelectedAPICapabilities(stores, req)
	if err != nil {
		return nil, err
	}
	forkChatLLM, err := buildForkChatSandboxLLMRuntimes(req.Config, primary.workspaces, binding, req.ProviderCredentials, stores.Effects(), stores.Completion(), stores.CompletionHeartbeat(), rt.Budget)
	if err != nil {
		return nil, err
	}
	idempotency, deps := stores.Idempotency(), stores.RuntimeDeps()
	publication := apiv1.EventPublicationOptions{
		ExecutionPosture: rt.ExecutionPosture,
		Idempotency:      idempotency, Events: rt.Bus, Acknowledged: rt.Bus, RecipientPlans: rt.Bus, SourceArtifact: rt.Bus,
		Runs: caps.Runs, Entities: caps.Entities, Observability: caps.Observability,
		RunBundleContext: caps.RunBundleContext, RuntimeContexts: caps.RuntimeContexts,
		Source: req.Source, Bundle: primary.bootIdentity, ScenarioExecutionProfiles: stores.ScenarioExecutionProfiles(),
	}
	handlers := apiv1.MergeOperatorHandlers(
		apiv1.OperatorAgentFrameHandlers(apiv1.AgentFrameHandlerOptions{Effective: rt.Manager}),
		apiv1.OperatorConversationForkHandlers(apiv1.ConversationForkHandlerOptions{Reads: caps.ConversationForks, Lifecycle: caps.ConversationForkLifecycle, Chat: cliapp.NewWorkspaceAdmittedForkChatExecutor(apiv1.NewLLMForkChatExecutor(forkChatLLM), forkChatLLM, primary.workspaceBackend), Idempotency: idempotency, ExecutionPosture: rt.ExecutionPosture}),
		apiv1.OperatorDecisionCardHandlers(apiv1.DecisionCardHandlerOptions{Cards: deps.DecisionCards, ProposedEffects: deps.ProposedEffects, Mailbox: stores.MailboxAPI(), NoticeAcknowledgment: stores.MailboxNoticeAcknowledgment(), Authority: rt.Pipeline, SourceArtifact: rt.Bus, Idempotency: idempotency, RuntimeContexts: caps.RuntimeContexts}),
		apiv1.OperatorRunStartHandlers(apiv1.RunStartHandlerOptions{Publication: publication}),
		apiv1.OperatorEventPublishHandlers(apiv1.EventPublishHandlerOptions{Publication: publication}),
		apiv1.OperatorEventReplayHandlers(apiv1.EventReplayHandlerOptions{ExecutionPosture: rt.ExecutionPosture, Idempotency: idempotency, Events: rt.Bus, Observability: caps.Observability, AgentIdentities: caps.Agents, RuntimeContexts: caps.RuntimeContexts}),
		apiv1.OperatorTestSetupHandlers(apiv1.TestSetupHandlerOptions{Setup: caps.TestSetup, Idempotency: idempotency, RunBundleContext: caps.RunBundleContext, RuntimeContexts: caps.RuntimeContexts, SourceArtifact: rt.Bus, Source: req.Source, ScenarioExecutionProfiles: stores.ScenarioExecutionProfiles()}),
		apiv1.OperatorRunForkHandlers(apiv1.RunForkHandlerOptions{Availability: caps.RunForkAvailability, Executor: caps.RunFork, Selector: caps.RunForkSelector, Idempotency: idempotency, RuntimeContexts: caps.RuntimeContexts}),
		apiv1.OperatorRunControlHandlers(apiv1.RunControlHandlerOptions{Controller: rt.RunControl, Idempotency: idempotency, RuntimeContexts: caps.RuntimeContexts}),
		apiv1.OperatorRuntimeControlHandlers(apiv1.RuntimeControlHandlerOptions{Ingress: rt.RuntimeIngress, Idempotency: idempotency, RuntimeContexts: caps.RuntimeContexts}),
	)
	for method := range handlers {
		if !slices.Contains(serveRuntimeExecutionMethods, method) {
			return nil, fmt.Errorf("runtime execution method %q lacks unloaded reset registration", method)
		}
	}
	// These three consumers use the retained primary manager, fork workspace,
	// or source-admission bus directly. The other execution methods select and
	// own their target occurrence through RuntimeContextManager themselves.
	for _, method := range []string{"agent.frame", "conversation.fork_chat", "mailbox.acknowledge"} {
		if handler := handlers[method]; handler != nil {
			handlers[method] = req.RuntimeSupervisor.bindPrimaryExecution(rt, handler)
		}
	}
	return handlers, nil
}

func (s *processLifecycleSupervisor) executionDispatch() map[string]apiv1.MethodHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	handlers := make(map[string]apiv1.MethodHandler, len(s.execution))
	for method := range s.execution {
		handlers[method] = nil
	}
	// The control API survives a source-clearing reset, including process death
	// before the final receipt. Registration must not require a live predecessor.
	if s.currentRT == nil {
		for _, method := range serveRuntimeExecutionMethods {
			handlers[method] = nil
		}
	}
	for method := range handlers {
		handlers[method] = func(ctx context.Context, req apiv1.Request) (any, error) {
			s.mu.RLock()
			handler := s.execution[method]
			if s.resetting || handler == nil {
				s.mu.RUnlock()
				return nil, apiv1.NewApplicationError(apiv1.BundleUnavailableCode, false, map[string]any{"cause": runtime.RuntimeContextCauseReset})
			}
			s.mu.RUnlock()
			return handler(ctx, req)
		}
	}
	return handlers
}

// Kept exhaustive against the real composed handlers by the served reset proof.
var serveRuntimeExecutionMethods = []string{
	"agent.frame", "agent.replay", "conversation.fork", "conversation.fork_chat",
	"conversation.fork_delete", "conversation.fork_list", "conversation.fork_view",
	"event.publish", "event.replay", "mailbox.acknowledge", "mailbox.begin_input",
	"mailbox.cancel_input", "mailbox.decide", "mailbox.defer", "mailbox.get", "mailbox.list",
	"run.continue", "run.fork", "run.pause", "run.start", "run.stop",
	"runtime.pause", "runtime.resume", "test.setup_entities",
}

func (s *processLifecycleSupervisor) bindPrimaryExecution(expected *runtime.Runtime, handler apiv1.MethodHandler) apiv1.MethodHandler {
	return func(ctx context.Context, req apiv1.Request) (any, error) {
		s.mu.RLock()
		if s.resetting || s.currentRT != expected || s.runtimeContexts == nil {
			s.mu.RUnlock()
			return nil, apiv1.NewApplicationError(apiv1.BundleUnavailableCode, false, map[string]any{"cause": runtime.RuntimeContextCauseReset})
		}
		use, _, err := s.runtimeContexts.AcquireBundleHash(ctx, s.currentSourceArtifactFact.BundleHash())
		s.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		if use == nil {
			return nil, apiv1.NewApplicationError(apiv1.BundleUnavailableCode, false, map[string]any{"cause": "runtime_context_unavailable"})
		}
		defer use.Done()
		if use.Runtime() != expected {
			return nil, errors.New("selected runtime execution changed")
		}
		return handler(use.WorkContext(), req)
	}
}

func (s *processLifecycleSupervisor) serveMCP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	// All candidates may need MCP preflight before the complete set is
	// executable. Their runtime/grant owner admits only exact startup tokens.
	for _, candidate := range s.resetContexts {
		handler, lease, err := candidate.runtime.AcquireStartupMCPRequest(r)
		if err != nil {
			s.mu.RUnlock()
			http.Error(w, "runtime startup is unavailable", http.StatusServiceUnavailable)
			return
		}
		if handler != nil {
			s.mu.RUnlock()
			defer lease.Done()
			handler.ServeHTTP(w, r.WithContext(lease.Context()))
			return
		}
	}
	if s.resetting || s.currentRT == nil || s.currentRT.ToolGateway == nil || s.runtimeContexts == nil {
		s.mu.RUnlock()
		http.Error(w, "runtime execution is unavailable", http.StatusServiceUnavailable)
		return
	}
	handler := s.currentRT.ToolGateway.Handler()
	use, _, err := s.runtimeContexts.AcquireBundleHash(r.Context(), s.currentSourceArtifactFact.BundleHash())
	s.mu.RUnlock()
	if err != nil || use == nil {
		http.Error(w, "runtime execution is unavailable", http.StatusServiceUnavailable)
		return
	}
	defer use.Done()
	handler.ServeHTTP(w, r.WithContext(use.WorkContext()))
}
