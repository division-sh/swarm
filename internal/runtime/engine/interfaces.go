package engine

import (
	"context"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	runtimepinrouting "swarm/internal/runtime/core/pinrouting"
	runtimeregistry "swarm/internal/runtime/core/registry"
	"swarm/internal/runtime/semanticview"
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

type StateRepository interface {
	LoadState(ctx context.Context, entityID identity.EntityID) (StateSnapshot, bool, error)
	SaveState(ctx context.Context, entityID identity.EntityID, mutation StateMutation) error
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
	VerifyEmitPersistence(ctx context.Context, entityID identity.EntityID, prerequisites EmitPersistencePrerequisites) error
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

type TimerApplier interface {
	ApplyTimerIntents(ctx context.Context, entityID identity.EntityID, intents []TimerIntent) error
}

type PostCommitDispatcher interface {
	DispatchPostCommit(ctx context.Context, intents []EmitIntent) error
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

type ActionRunner interface {
	ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimeregistry.ActionInstruction, execCtx ExecutionContext) (bool, error)
}

type PayloadShaper interface {
	ShapeEmitPayload(ctx context.Context, req ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error)
}

type TargetDescriptorLoader func(context.Context) ([]runtimepinrouting.Descriptor, error)

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
	TimerApplier        TimerApplier
	Dispatcher          PostCommitDispatcher
	GuardRegistry       GuardRegistry
	GuardRunner         GuardRunner
	ActionRegistry      ActionRegistry
	ActionRunner        ActionRunner
	PayloadShaper       PayloadShaper
	TargetDescriptors   TargetDescriptorLoader
	TransitionValidator TransitionValidator
	MaxChainDepth       int
}
