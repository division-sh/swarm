package runtimepersistence_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type destructiveResetBlockingDirectiveAgent struct {
	id      string
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (a *destructiveResetBlockingDirectiveAgent) ID() string { return a.id }
func (*destructiveResetBlockingDirectiveAgent) Type() string { return "test" }
func (*destructiveResetBlockingDirectiveAgent) Subscriptions() []events.EventType {
	return nil
}
func (*destructiveResetBlockingDirectiveAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *destructiveResetBlockingDirectiveAgent) BoardStep(context.Context, runtimeagentcontrol.BoardDirective) (string, error) {
	a.calls.Add(1)
	select {
	case a.started <- struct{}{}:
	default:
	}
	<-a.release
	return "accepted", nil
}

func TestDestructiveResetFailsClosedWhileDirectiveBoardStepIsRunning(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := storeTestWorkContext(t, testAuthorActivityContext())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
	bus, err := newStoreTestEventBus(t, pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	agent := &destructiveResetBlockingDirectiveAgent{
		id:      "directive-reset-agent",
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, func(runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live,
		WorkOwner:        storeTestWorkOwner(t),
		PersistenceRoles: runtimemanager.PersistenceRoles{
			EventExistence:      pg,
			DirectiveOperations: pg,
			DirectiveTargets:    pg,
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg))
	identity := agentidentitytest.RootRuntime(t, agent.id, "destructive-reset-integration")
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "test#agents.directive-reset-agent.intent", "Exercise destructive reset coordination.")
	if err != nil {
		t.Fatalf("resolve test agent intent: %v", err)
	}
	rec := runtimemanager.PersistedAgent{
		Config:    runtimeactors.AgentConfig{ExecutionMode: "live", ResolvedLLMBackend: "anthropic", ID: agent.id, Identity: identity, Role: "test", Model: "regular", Intent: intent, SystemPrompt: intent.Content},
		Status:    "active",
		HiredBy:   "destructive-reset-test",
		StartedAt: time.Now().UTC(),
	}
	if err := pg.UpsertAgent(ctx, rec); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := manager.RegisterEphemeralAgentForExecution(ctx, rec); err != nil {
		t.Fatalf("RegisterEphemeralAgentForExecution: %v", err)
	}
	request := runtimeagentcontrol.SendDirectiveRequest{
		AgentID:        agent.id,
		Directive:      "continue",
		RunID:          runID,
		Source:         runtimeagentcontrol.DirectiveSourceV1RPC,
		ActorTokenID:   "operator-token",
		IdempotencyKey: "nuke-vs-board-step",
		RequestHash:    "nuke-vs-board-step-hash",
	}
	resultCh := make(chan runtimeagentcontrol.SendDirectiveResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := manager.SendDirective(ctx, request)
		resultCh <- result
		errCh <- err
	}()
	select {
	case <-agent.started:
	case err := <-errCh:
		t.Fatalf("SendDirective failed before BoardStep: %v", err)
	case <-time.After(time.Second):
		t.Fatal("BoardStep did not start")
	}

	now := time.Now().UTC()
	_, err = pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
		ActorTokenID: "operator-token",
		RequestedAt:  now,
		Result: destructivereset.Result{
			OperationName: destructivereset.DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
			Plan: destructivereset.Plan{
				CleanupRunSetKnown: true,
				CleanupRuns:        []destructivereset.RunRef{{RunID: runID}},
			},
		},
		Quiescence: destructivereset.QuiescenceResult{
			OperationName: destructivereset.DefaultOperationName,
			AppliedAt:     now.Add(-30 * time.Second),
		},
	})
	if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "state=executing") {
		t.Fatalf("runtime.nuke error = %v, want executing directive refusal", err)
	}
	if _, err := manager.SendDirective(ctx, request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
		t.Fatalf("same-key directive during BoardStep error = %v, want in progress", err)
	}
	if got := agent.calls.Load(); got != 1 {
		t.Fatalf("BoardStep calls while cleanup refused = %d, want 1", got)
	}

	close(agent.release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("first SendDirective: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first SendDirective did not finish")
	}
	if result := <-resultCh; !result.OK || result.OperationID == "" {
		t.Fatalf("first SendDirective result = %#v", result)
	}
}
