package engine

import (
	"context"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/identity"
	"empireai/internal/runtime/semanticview"
)

type SemanticSourceProvider interface {
	SemanticSource() semanticview.Source
}

type StateRepository interface {
	LoadState(ctx context.Context, entityID identity.EntityID) (StateSnapshot, bool, error)
	SaveState(ctx context.Context, entityID identity.EntityID, mutation StateMutation) error
}

type InstanceRepository interface {
	LoadInstance(ctx context.Context, instanceID string) (StateSnapshot, bool, error)
	UpsertInstance(ctx context.Context, snapshot StateSnapshot) error
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
	HasGuard(id string) bool
	IsExecutable(id string) bool
	Guard(id string) (runtimecontracts.GuardActionEntry, bool)
}

type GuardRunner interface {
	EvaluateGuard(ctx context.Context, id string, entry runtimecontracts.GuardActionEntry, execCtx ExecutionContext) (bool, bool, error)
}

type ActionRegistry interface {
	HasAction(id string) bool
	IsExecutable(id string) bool
	Action(id string) (runtimecontracts.GuardActionEntry, bool)
}

type ActionRunner interface {
	ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimecontracts.GuardActionEntry, execCtx ExecutionContext) (bool, error)
}

type PayloadShaper interface {
	ShapeEmitPayload(ctx context.Context, req ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error)
}

type RuntimeDependencies struct {
	Source            semanticview.Source
	StateRepo         StateRepository
	InstanceRepo      InstanceRepository
	TxRunner          TransactionRunner
	Locker            EntityLocker
	Outbox            OutboxWriter
	TimerApplier      TimerApplier
	Dispatcher        PostCommitDispatcher
	GuardRegistry     GuardRegistry
	GuardRunner       GuardRunner
	ActionRegistry    ActionRegistry
	ActionRunner      ActionRunner
	PayloadShaper     PayloadShaper
	MaxChainDepth     int
}
