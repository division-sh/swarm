package engine

import (
	"context"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeregistry "empireai/internal/runtime/core/registry"
	"empireai/internal/runtime/semanticview"
)

type SemanticSourceProvider interface {
	SemanticSource() semanticview.Source
}

type StateRepository interface {
	LoadState(ctx context.Context, entityID identity.EntityID) (StateSnapshot, bool, error)
	SaveState(ctx context.Context, entityID identity.EntityID, mutation StateMutation) error
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

type RuntimeDependencies struct {
	Source         semanticview.Source
	StateRepo      StateRepository
	TxRunner       TransactionRunner
	Locker         EntityLocker
	Outbox         OutboxWriter
	TimerApplier   TimerApplier
	Dispatcher     PostCommitDispatcher
	GuardRegistry  GuardRegistry
	GuardRunner    GuardRunner
	ActionRegistry ActionRegistry
	ActionRunner   ActionRunner
	PayloadShaper  PayloadShaper
	MaxChainDepth  int
}
