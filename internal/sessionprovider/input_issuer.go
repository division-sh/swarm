package sessionprovider

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/inputfact"
)

type SessionInputReference struct {
	ConnectionID string
	OccurrenceID string
	Conversation string
	EventID      string
	Kind         string
}

type sessionInputOwner struct {
	store     channelonboarding.Store
	native    *sessionInputReader
	operation string
}

func newSessionInputOwner(store channelonboarding.Store, native *sessionInputReader, operation string) (*sessionInputOwner, error) {
	if store == nil || native == nil || operation == "" {
		return nil, fmt.Errorf("native input requires its owned SDK/capture and exact declared responsibility")
	}
	return &sessionInputOwner{store: store, native: native, operation: operation}, nil
}

func (o *sessionInputOwner) admit(ctx context.Context, reference SessionInputReference) (input.Admission, error) {
	if o == nil || ctx == nil || ctx.Err() != nil {
		return input.Admission{}, fmt.Errorf("current native input owner is required")
	}
	native, err := o.native.readOwnedInput(ctx, reference)
	if err != nil {
		return input.Admission{}, err
	}
	if native.Release == nil {
		return input.Admission{}, fmt.Errorf("native capture must retain its SDK lifetime")
	}
	retained := false
	defer func() {
		if !retained {
			native.Release()
		}
	}()
	if native.Context == nil || native.Context.Err() != nil || ctx.Err() != nil || len(native.Body) == 0 ||
		native.OperationID != o.operation || native.Account.ConnectionID != reference.ConnectionID ||
		native.ReceivedAt.IsZero() || !native.ReceivedAt.Equal(native.ReceivedAt.Truncate(time.Microsecond)) {
		return input.Admission{}, fmt.Errorf("native capture is outside its original occurrence")
	}
	op, err := o.store.GetChannelOnboarding(ctx, o.operation)
	if err != nil {
		return input.Admission{}, err
	}
	if !native.matchesOriginalOperation(op, native.SourceContext.Coordinate) {
		return input.Admission{}, fmt.Errorf("native capture is outside its original responsibility")
	}
	responsibility := channelonboarding.AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: native.OperationRevision,
		ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
		Provider: op.Provider, Credentials: append([]channelonboarding.CredentialAdmission(nil), op.CredentialAdmissions...), SessionAccount: op.SessionAccount}
	current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, o.store, responsibility, true)
	if err != nil {
		return input.Admission{}, err
	}
	if !current || native.Context.Err() != nil || ctx.Err() != nil {
		return input.Admission{}, fmt.Errorf("native capture responsibility is no longer current")
	}
	ownedContext, cancel := context.WithCancel(native.Context)
	retained = true
	return inputfact.SealOwnedCapture(inputfact.Capture{Store: o.store, Responsibility: responsibility,
		Scope: native.Scope, BindingRevision: native.BindingRevision, PublicationRunID: native.PublicationBinding.RunID,
		Body: native.Body, ReceivedAt: native.ReceivedAt, Generation: native.SourceContext.CatalogGeneration, Context: ownedContext,
		Release: func() { cancel(); native.Release() }}), nil
}
