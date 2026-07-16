package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func (r servedEventPublishBlockingLLMRuntime) StartSession(ctx context.Context, agentID string, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.PlatformDefault()
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID:             uuid.NewString(),
		AgentID:        agentID,
		SystemPrompt:   systemPrompt,
		Tools:          append([]runtimellm.ToolDefinition(nil), tools...),
		Memory:         memory,
		MemoryIdentity: execution.Identity,
	}, nil
}

func (r servedEventPublishBlockingLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	return &runtimellm.Response{
		Message:   runtimellm.Message{Role: "assistant", Content: "acknowledged"},
		SessionID: sessionID,
	}, nil
}

func (r servedSessionCleanupProofLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	if !ok {
		return nil, errors.New("served session cleanup proof requires canonical memory execution")
	}
	lease, err := r.store.Acquire(ctx, execution.Identity, "served-cleanup-start")
	if err != nil {
		return nil, err
	}
	if err := r.store.Release(ctx, lease); err != nil {
		return nil, err
	}
	return &runtimellm.Session{
		ID: lease.SessionID, AgentID: agentID, SystemPrompt: systemPrompt,
		Tools: append([]runtimellm.ToolDefinition(nil), tools...), Memory: execution.Plan, MemoryIdentity: execution.Identity,
	}, nil
}

func (r servedSessionCleanupProofLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, _ runtimellm.Message) (*runtimellm.Response, error) {
	if r.store == nil || session == nil {
		return nil, errors.New("served session cleanup proof requires store and session")
	}
	lease, err := r.store.Acquire(ctx, session.MemoryIdentity, "served-cleanup-proof")
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.store.Release(context.Background(), lease) }()
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("served session cleanup proof requires managed capability surface")
	}
	runID := session.MemoryIdentity.RunID
	err = r.store.AppendAgentTurn(ctx, runtimellm.AgentTurnRecord{
		AgentID: session.AgentID, Memory: session.Memory, SessionID: lease.SessionID,
		FlowInstance: session.MemoryIdentity.FlowInstance, RunID: runID, CapabilitySurface: &surface,
		ResponseRaw: []byte(`{"proof":"in-flight"}`), ParseOK: true,
	})
	if err != nil {
		return nil, err
	}
	select {
	case r.started <- lease.SessionID:
	default:
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "released"}, SessionID: lease.SessionID, CapabilitySurface: &surface}, nil
}

func (f *servedDirectivePersistenceFaults) setRecordResultFault(afterCommit bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterCommit = afterCommit
	f.remaining = 1
}

func (f *servedDirectivePersistenceFaults) takeRecordResultFault() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.remaining == 0 {
		return false, false
	}
	f.remaining--
	return f.afterCommit, true
}

func (s *servedPostgresDirectiveFaultStore) RecordDirectiveExecuted(ctx context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	afterCommit, inject := s.faults.takeRecordResultFault()
	if inject && !afterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errors.New("injected served directive result persistence rollback")
	}
	op, err := s.PostgresStore.RecordDirectiveExecuted(ctx, operationID, ownerID, response, now)
	if err == nil && inject && afterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errors.New("injected served directive result acknowledgment loss")
	}
	return op, err
}

func (s *servedSQLiteDirectiveFaultStore) RecordDirectiveExecuted(ctx context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	afterCommit, inject := s.faults.takeRecordResultFault()
	if inject && !afterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errors.New("injected served directive result persistence rollback")
	}
	op, err := s.SQLiteRuntimeStore.RecordDirectiveExecuted(ctx, operationID, ownerID, response, now)
	if err == nil && inject && afterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errors.New("injected served directive result acknowledgment loss")
	}
	return op, err
}

func (servedLiveAgentProofLLMRuntime) StartSession(ctx context.Context, agentID string, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	execution, ok := agentmemory.FromContext(ctx)
	memory := agentmemory.PlatformDefault()
	if ok {
		memory = execution.Plan
	}
	return &runtimellm.Session{
		ID:             uuid.NewString(),
		AgentID:        agentID,
		SystemPrompt:   systemPrompt,
		Tools:          append([]runtimellm.ToolDefinition(nil), tools...),
		Memory:         memory,
		MemoryIdentity: execution.Identity,
	}, nil
}

func (r servedLiveAgentProofLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	if r.calls != nil {
		r.calls.Add(1)
	}
	if r.directiveFailures {
		switch {
		case strings.Contains(message.Content, "untyped directive failure"):
			return nil, errors.New("raw provider failure must not survive")
		case strings.Contains(message.Content, "typed directive failure"):
			return nil, runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "served-llm", "continue_session", map[string]any{
				"auth_kind": "provider_credential",
				"provider":  "served-proof",
			})
		}
	}
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("served live agent proof requires managed capability surface")
	}
	observed, err := runtimellm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	return &runtimellm.Response{
		Message:   runtimellm.Message{Role: "tool", Content: `[{"ok":true,"result":"handled live agent event"}]`},
		SessionID: sessionID, CapabilitySurface: &observed,
	}, nil
}

func (recordingSchemaBootstrapper) BootstrapSchema(context.Context, store.SchemaBootstrapRequest) error {
	return nil
}

func (recordingSchemaBootstrapper) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return store.StoreSchemaCapabilities{}, nil
}

func (c *capturingSchemaBootstrapper) BootstrapSchema(_ context.Context, request store.SchemaBootstrapRequest) error {
	c.plans = append([]store.SchemaTableDDL{}, request.PlatformPlans...)
	c.plans = append(c.plans, request.StatePlans...)
	return nil
}

func (c *capturingSchemaBootstrapper) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return store.StoreSchemaCapabilities{}, nil
}
