//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
)

type nativeSessionInput struct {
	Scope              channelonboarding.SessionInputScope
	Account            operatorchannel.SessionAccountAdmission
	OperationID        string
	OperationRevision  int64
	ActivationRevision int64
	TargetSelector     string
	PrincipalID        string
	Source             channelonboarding.ChannelDurableContextIdentity
	PublicationBinding runtimeinbound.BindingGeneration
	BindingRevision    int64
	Body               []byte
	ReceivedAt         time.Time
	Context            context.Context
	Release            func()
	SourceContext      captureSource
	OriginalCapture    capturedEvent
	OwnedSource        correlation.SourceArtifactFact
	NativeCurrent      func() bool
}

func (input nativeSessionInput) matchesOriginalOperation(op channelonboarding.Operation, coordinate channelonboarding.ChannelRuntimeContextCoordinate) bool {
	return op.OperationID == input.OperationID && op.Posture == channelonboarding.ActivationSessionConnection &&
		op.Phase != channelonboarding.PhaseFailed && op.Phase != channelonboarding.PhaseRetired && op.Coordinate.MatchesDeclaration(coordinate) &&
		op.SessionAccount == input.Account && op.ActivationRevision == input.ActivationRevision &&
		op.TargetSelector == input.TargetSelector && op.PrincipalID == input.PrincipalID &&
		op.Coordinate.DurableIdentity().Matches(input.Source) && op.BindingRevision == input.BindingRevision &&
		op.ValidateSessionAccount() == nil && (input.Scope == channelonboarding.SessionInputOnboarding || input.Scope == channelonboarding.SessionInputBusiness)
}

// The reader owns actual provider state and the private capture handoff. It
// returns verified bytes, never caller-supplied normalized payload or authority.
type sessionInputReader struct {
	state *sessionState
	spool *captureStore
}

func newSessionInputReader(state *sessionState, spool *captureStore) (*sessionInputReader, error) {
	if state == nil || spool == nil || state.directory == nil || state.directory.connectionID != spool.connectionID {
		return nil, fmt.Errorf("WhatsApp session input requires its same owned provider state and capture database")
	}
	return &sessionInputReader{state: state, spool: spool}, nil
}

func (r *sessionInputReader) readOwnedInput(ctx context.Context, reference SessionInputReference) (nativeSessionInput, error) {
	var result nativeSessionInput
	if r == nil || ctx == nil || ctx.Err() != nil || reference.ConnectionID != r.spool.connectionID ||
		uuid.Validate(reference.OccurrenceID) != nil || reference.Conversation == "" || reference.EventID == "" {
		return result, errCaptureScopeChanged
	}
	occurrence := r.state.currentOccurrence()
	if occurrence == nil || occurrence.occurrenceID != reference.OccurrenceID {
		return result, errClientOccurrenceFenced
	}
	workContext, release, err := occurrence.acquire(ctx)
	if err != nil {
		return result, err
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	r.state.mu.Lock()
	current := !r.state.closed && !r.state.retiring && r.state.occurrence == occurrence
	r.state.mu.Unlock()
	if !current {
		return result, errClientOccurrenceFenced
	}
	// The occurrence lease holds database possession through this read. A
	// callback must not wait for the lifecycle mutex held by shutdown joining.
	device, err := r.state.device(workContext)
	if err != nil {
		return result, err
	}
	if device.ID == nil || device.ID.User == "" {
		return result, errSessionAccount
	}
	account := device.ID.ToNonAD().String()
	ownedSource, _ := correlation.SourceArtifactFactFromContext(occurrence.ctx)
	events, err := r.spool.pending(workContext)
	if err != nil {
		return result, err
	}
	for _, event := range events {
		if event.Conversation != reference.Conversation || event.EventID != reference.EventID || event.Kind != reference.Kind {
			continue
		}
		if event.OccurrenceID != reference.OccurrenceID || event.Scope.Session.ConnectionID != reference.ConnectionID ||
			event.Scope.Session.AccountRef != account || workContext.Err() != nil {
			return result, errCaptureScopeChanged
		}
		retained = true
		nativeCurrent := func() bool {
			return r.state.ownsConnectedOccurrence(workContext, occurrence)
		}
		return nativeSessionInput{Scope: event.Scope.Kind, Account: event.Scope.Session, OperationID: event.Scope.OnboardingOperation,
			OperationRevision: event.Scope.OperationRevision, ActivationRevision: event.Scope.ActivationRevision, TargetSelector: event.Scope.TargetSelector,
			PrincipalID: event.Scope.PrincipalID, Source: event.Scope.Source, BindingRevision: event.Scope.BindingRevision,
			PublicationBinding: event.Scope.PublicationBinding,
			SourceContext:      event.Source,
			OriginalCapture:    event,
			OwnedSource:        ownedSource,
			NativeCurrent:      nativeCurrent,
			Body:               event.Body, ReceivedAt: event.ReceivedAt, Context: workContext, Release: release}, nil
	}
	return result, fmt.Errorf("WhatsApp authenticated input requires its verified retained capture")
}
