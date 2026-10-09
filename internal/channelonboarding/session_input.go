package channelonboarding

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type SessionInputReference struct {
	ConnectionID string
	OccurrenceID string
	Conversation string
	EventID      string
	Kind         string
}

type SessionInputScope string

const (
	SessionInputOnboarding SessionInputScope = "onboarding"
	SessionInputBusiness   SessionInputScope = "business"
)

// NativeSessionInput is data returned by the owned provider capture reader.
// Only SessionInputOwner can turn it into input admission after currentness.
type NativeSessionInput struct {
	Scope              SessionInputScope
	Account            operatorchannel.SessionAccountAdmission
	OperationID        string
	OperationRevision  int64
	ActivationRevision int64
	TargetSelector     string
	PrincipalID        string
	Source             ChannelDurableContextIdentity
	BindingRevision    int64
	Body               []byte
	ReceivedAt         time.Time
	Context            context.Context
	Release            func()
}

type NativeSessionInputReader interface {
	ReadAuthenticatedSessionInput(context.Context, SessionInputReference) (NativeSessionInput, error)
}

// SessionInputOwner joins native account/capture authentication with the
// existing selected-store responsibility. There is no DTO-to-grant constructor.
type SessionInputOwner struct {
	store      Store
	native     NativeSessionInputReader
	coordinate ChannelRuntimeContextCoordinate
	operation  string
	generation triggergeneration.Generation
}

func NewSessionInputOwner(store Store, native NativeSessionInputReader, coordinate ChannelRuntimeContextCoordinate,
	operationID string, generation triggergeneration.Generation,
) (*SessionInputOwner, error) {
	if store == nil || native == nil || operationID == "" || coordinate.ValidateContext() != nil || !generation.Valid() {
		return nil, fmt.Errorf("session input requires native capture, selected responsibility, exact context and verified catalog generation")
	}
	return &SessionInputOwner{store: store, native: native, coordinate: coordinate, operation: operationID, generation: generation}, nil
}

type admittedSessionInput struct {
	owner          *SessionInputOwner
	responsibility AdmissionResponsibility
	provider       string
	body           []byte
	receivedAt     time.Time
	ctx            context.Context
	release        func()
	once           sync.Once
}

// SessionInputAdmission is closed, process-local and non-serializable. Copies
// share one lease; closing any copy invalidates every copy.
type SessionInputAdmission struct{ value *admittedSessionInput }

func (o *SessionInputOwner) Admit(ctx context.Context, reference SessionInputReference) (SessionInputAdmission, error) {
	if o == nil || ctx == nil || ctx.Err() != nil {
		return SessionInputAdmission{}, fmt.Errorf("current session input owner is required")
	}
	input, err := o.native.ReadAuthenticatedSessionInput(ctx, reference)
	if err != nil {
		return SessionInputAdmission{}, err
	}
	if input.Release == nil {
		return SessionInputAdmission{}, fmt.Errorf("native session input must retain its owned lease")
	}
	retained := false
	defer func() {
		if !retained {
			input.Release()
		}
	}()
	if input.Context == nil || input.Context.Err() != nil || ctx.Err() != nil || len(input.Body) == 0 ||
		input.OperationID != o.operation || input.Account.ConnectionID != reference.ConnectionID ||
		input.ReceivedAt.IsZero() || !input.ReceivedAt.Equal(input.ReceivedAt.Truncate(time.Microsecond)) {
		return SessionInputAdmission{}, fmt.Errorf("native session input belongs to a different responsibility")
	}
	op, err := o.store.GetChannelOnboarding(ctx, o.operation)
	if err != nil {
		return SessionInputAdmission{}, err
	}
	if !input.matchesOriginalOperation(op, o.coordinate) {
		return SessionInputAdmission{}, fmt.Errorf("native session input is outside its original selected-store responsibility")
	}
	responsibility := AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: input.OperationRevision,
		ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
		Provider: op.Provider, Credentials: append([]CredentialAdmission(nil), op.CredentialAdmissions...), SessionAccount: op.SessionAccount}
	current, err := AdmissionResponsibilityCurrent(ctx, o.store, responsibility, true)
	if err != nil {
		return SessionInputAdmission{}, err
	}
	if !current || input.Context.Err() != nil || ctx.Err() != nil {
		return SessionInputAdmission{}, fmt.Errorf("session input responsibility is no longer current")
	}
	retained = true
	ownedContext, cancel := context.WithCancel(input.Context)
	return SessionInputAdmission{value: &admittedSessionInput{owner: o, responsibility: responsibility,
		provider: op.Provider, body: bytes.Clone(input.Body), receivedAt: input.ReceivedAt, ctx: ownedContext, release: func() { cancel(); input.Release() }}}, nil
}

func (input NativeSessionInput) matchesOriginalOperation(op Operation, coordinate ChannelRuntimeContextCoordinate) bool {
	return op.OperationID == input.OperationID && op.Posture == ActivationSessionConnection &&
		op.Phase != PhaseFailed && op.Phase != PhaseRetired && op.Coordinate.MatchesDeclaration(coordinate) &&
		op.SessionAccount == input.Account && op.ActivationRevision == input.ActivationRevision &&
		op.TargetSelector == input.TargetSelector && op.PrincipalID == input.PrincipalID &&
		op.Coordinate.DurableIdentity().Matches(input.Source) && op.BindingRevision == input.BindingRevision &&
		op.ValidateSessionAccount() == nil && (input.Scope == SessionInputOnboarding || input.Scope == SessionInputBusiness) &&
		(input.Scope == SessionInputBusiness) == op.Phase.RequiresExecutableTarget(op.Posture)
}

func (a SessionInputAdmission) ReceivedAt() time.Time {
	if a.value == nil {
		return time.Time{}
	}
	return a.value.receivedAt
}

func (a SessionInputAdmission) Validate(ctx context.Context, provider string, generation triggergeneration.Generation) error {
	if a.value == nil || a.value.owner == nil || ctx == nil || ctx.Err() != nil || a.value.ctx.Err() != nil ||
		provider != a.value.provider || !generation.Equal(a.value.owner.generation) {
		return fmt.Errorf("exact current session input admission is required")
	}
	current, err := AdmissionResponsibilityCurrent(ctx, a.value.owner.store, a.value.responsibility, true)
	if err != nil {
		return err
	}
	if !current || a.value.ctx.Err() != nil || ctx.Err() != nil {
		return fmt.Errorf("session input admission has been retired")
	}
	return nil
}

func (a SessionInputAdmission) Body() []byte {
	if a.value == nil {
		return nil
	}
	return bytes.Clone(a.value.body)
}

func (a SessionInputAdmission) Coordinate() ChannelRuntimeContextCoordinate {
	if a.value == nil {
		return ChannelRuntimeContextCoordinate{}
	}
	return a.value.responsibility.Coordinate
}

func (a SessionInputAdmission) TargetSelector() string {
	if a.value == nil {
		return ""
	}
	return a.value.responsibility.TargetSelector
}

func (a SessionInputAdmission) Context() context.Context {
	if a.value == nil {
		return nil
	}
	return a.value.ctx
}

func (a SessionInputAdmission) Close() {
	if a.value != nil {
		a.value.once.Do(a.value.release)
	}
}
