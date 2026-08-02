package engine

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type emitSurfaceContextKey struct{}

type EmitSurface string

const (
	EmitSurfaceDeclarative EmitSurface = "declarative"
	EmitSurfaceAction      EmitSurface = "action"
)

func WithEmitSurface(ctx context.Context, surface EmitSurface) context.Context {
	return context.WithValue(ctx, emitSurfaceContextKey{}, surface)
}

func EmitSurfaceFromContext(ctx context.Context) EmitSurface {
	if ctx == nil {
		return EmitSurfaceDeclarative
	}
	if surface, ok := ctx.Value(emitSurfaceContextKey{}).(EmitSurface); ok && surface != "" {
		return surface
	}
	return EmitSurfaceDeclarative
}

type SemanticSourceProvider interface {
	SemanticSource() semanticview.Source
}

type StateAddress struct {
	FlowID   identity.FlowID
	Route    runtimeflowidentity.Route
	EntityID identity.EntityID
}

type StateRepository interface {
	LoadState(ctx context.Context, address StateAddress) (StateSnapshot, bool, error)
	SaveState(ctx context.Context, address StateAddress, mutation StateMutation) error
}

type EmitPersistenceFieldPrerequisite struct {
	Field       string
	Expected    any
	HasExpected bool
}

type EmitPersistencePrerequisites struct {
	Fields []EmitPersistenceFieldPrerequisite
}

type EmitPersistenceVerifier interface {
	VerifyEmitPersistence(ctx context.Context, address StateAddress, prerequisites EmitPersistencePrerequisites) error
}

type Tx interface {
	Context() context.Context
}

type TransactionRunner interface {
	Run(ctx context.Context, fn func(Tx) error) error
}

type EntityLocker interface {
	WithEntityLock(ctx context.Context, entityID identity.EntityID, fn func(context.Context) error) error
}

type OutboxWriter interface {
	WriteOutbox(ctx context.Context, intents []EmitIntent) error
}

type WorkflowLifecycleEffectOwner interface {
	AcceptedEventEffect(entityID identity.EntityID, event events.Event, fromState, toState string) (runtimeworkflowlifecycle.Effect, error)
	ApplyWorkflowLifecycleEffects(ctx context.Context, effects []runtimeworkflowlifecycle.Effect) error
}

type PostCommitDispatcher interface {
	DispatchPostCommit(ctx context.Context, intents []EmitIntent) error
}

type ActivityIntentWriter interface {
	WriteActivityIntents(ctx context.Context, intents []ActivityIntent) error
}

type ActivityDispatcher interface {
	DispatchActivities(ctx context.Context, intents []ActivityIntent) error
}

type GuardRegistry interface {
	HasGuard(id identity.GuardKey) bool
	IsExecutable(id identity.GuardKey) bool
	Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool)
}

type GuardRunner interface {
	EvaluateGuard(ctx context.Context, id identity.GuardKey, entry runtimeregistry.GuardInstruction, execCtx ExecutionContext) (bool, bool, error)
}

type ActionRegistry interface {
	HasAction(id identity.ActionKey) bool
	IsExecutable(id identity.ActionKey) bool
	Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool)
}

type ActionExecution struct {
	Handled     bool
	EmitIntents []EmitIntent
}

type ActionRunner interface {
	ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimeregistry.ActionInstruction, execCtx ExecutionContext) (ActionExecution, error)
}

type PayloadShaper interface {
	ShapeEmitPayload(ctx context.Context, req ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error)
}

type TransitionValidator interface {
	ValidateTransition(currentState, nextState string) error
}

type RuntimeDependencies struct {
	Source              semanticview.Source
	StateRepo           StateRepository
	EmitVerifier        EmitPersistenceVerifier
	TxRunner            TransactionRunner
	Locker              EntityLocker
	Outbox              OutboxWriter
	WorkflowLifecycle   WorkflowLifecycleEffectOwner
	Dispatcher          PostCommitDispatcher
	ActivityIntents     ActivityIntentWriter
	ActivityDispatcher  ActivityDispatcher
	GuardRegistry       GuardRegistry
	GuardRunner         GuardRunner
	ActionRegistry      ActionRegistry
	ActionRunner        ActionRunner
	PayloadShaper       PayloadShaper
	TransitionValidator TransitionValidator
	EmitNow             func() time.Time
	MaxChainDepth       int
}
