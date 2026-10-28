//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type nativeSessionInput struct {
	DeploymentRegistration     bool
	Scope                      channelonboarding.SessionInputScope
	Account                    operatorchannel.SessionAccountAdmission
	OperationID                string
	OperationRevision          int64
	ActivationID               string
	ActivationRevision         int64
	TargetSelector             string
	PrincipalID                string
	Source                     channelonboarding.ChannelDurableContextIdentity
	PublicationBinding         runtimeinbound.BindingGeneration
	BindingRevision            int64
	AccountOnly                bool
	Body                       []byte
	ReceivedAt                 time.Time
	Context                    context.Context
	Release                    func()
	SourceContext              captureSource
	OriginalCapture            capturedEvent
	OriginalPublicationRequest []byte
	OwnedSource                correlation.SourceArtifactFact
	OwnedRoot                  sourceartifact.RootAdmission
	NativeCurrent              func() bool
}

func (input nativeSessionInput) matchesOriginalOperation(op channelonboarding.Operation, coordinate channelonboarding.ChannelRuntimeContextCoordinate) bool {
	if input.DeploymentRegistration != op.DeploymentRegistration {
		return false
	}
	awaitingIdentity := slices.Contains([]channelonboarding.Phase{channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation}, op.Phase)
	if op.DeploymentRegistration {
		return input.Scope == channelonboarding.SessionInputOnboarding && input.OperationID == op.OperationID &&
			op.AccountConfirmed && op.SessionAccount == input.Account && op.PrincipalID == input.PrincipalID &&
			op.Revision == input.OperationRevision && awaitingIdentity &&
			coordinate == (channelonboarding.ChannelRuntimeContextCoordinate{}) && input.ActivationRevision == 0 && input.TargetSelector == ""
	}
	return input.matchesRuntimeOperation(op, coordinate, awaitingIdentity)
}

func (input nativeSessionInput) matchesRuntimeOperation(op channelonboarding.Operation, coordinate channelonboarding.ChannelRuntimeContextCoordinate, awaitingIdentity bool) bool {
	activationCurrent := op.ActivationRevision == input.ActivationRevision
	if input.Scope == channelonboarding.SessionInputOnboarding && input.ActivationRevision == 0 && op.AccountConnection() &&
		op.IdentityOperationID != "" && awaitingIdentity {
		activationCurrent = true
	}
	return op.OperationID == input.OperationID && op.Posture == channelonboarding.ActivationSessionConnection &&
		(input.Scope != channelonboarding.SessionInputBusiness || input.AccountOnly == op.AccountConnection()) &&
		!slices.Contains([]channelonboarding.Phase{channelonboarding.PhaseFailed, channelonboarding.PhaseRetired}, op.Phase) && op.Coordinate.MatchesDeclaration(coordinate) &&
		op.SessionAccount == input.Account && activationCurrent &&
		op.TargetSelector == input.TargetSelector && op.PrincipalID == input.PrincipalID &&
		op.Coordinate.DurableIdentity().Matches(input.Source) && op.BindingRevision == input.BindingRevision &&
		op.ValidateSessionAccount() == nil && slices.Contains([]channelonboarding.SessionInputScope{channelonboarding.SessionInputOnboarding, channelonboarding.SessionInputBusiness}, input.Scope)
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
	return r.readRetainedInput(ctx, reference, "")
}

func (r *sessionInputReader) readPendingBusinessInput(ctx context.Context, reference SessionInputReference) (nativeSessionInput, error) {
	return r.readRetainedInput(ctx, reference, channelonboarding.SessionInputBusiness)
}

func (r *sessionInputReader) readPendingClaimInput(ctx context.Context, reference SessionInputReference) (nativeSessionInput, error) {
	return r.readRetainedInput(ctx, reference, channelonboarding.SessionInputOnboarding)
}

func (r *sessionInputReader) readRetainedInput(ctx context.Context, reference SessionInputReference, recoveryScope channelonboarding.SessionInputScope) (nativeSessionInput, error) {
	var result nativeSessionInput
	if r == nil || ctx == nil || ctx.Err() != nil || reference.ConnectionID != r.spool.connectionID ||
		uuid.Validate(reference.OccurrenceID) != nil || reference.Conversation == "" || reference.EventID == "" {
		return result, errCaptureScopeChanged
	}
	occurrence := r.state.currentOccurrence()
	if occurrence == nil || (recoveryScope == "" && occurrence.occurrenceID != reference.OccurrenceID) {
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
	ownedRoot, _, err := correlation.RootAdmissionFromContext(occurrence.ctx)
	if err != nil {
		return result, err
	}
	rows, err := r.spool.pendingPublications(workContext)
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		event := row.event
		if event.Conversation != reference.Conversation || event.EventID != reference.EventID || event.Kind != reference.Kind {
			continue
		}
		if workContext.Err() != nil || (recoveryScope != "" && event.Scope.Kind != recoveryScope) ||
			!nativeCaptureMatchesReference(event, reference, account, ownedSource, recoveryScope == channelonboarding.SessionInputBusiness) {
			return result, errCaptureScopeChanged
		}
		retained = true
		nativeCurrent := func() bool {
			return r.state.ownsConnectedOccurrence(workContext, occurrence)
		}
		return nativeSessionInput{Scope: event.Scope.Kind, Account: event.Scope.Session, OperationID: event.Scope.OnboardingOperation,
			DeploymentRegistration: event.Scope.DeploymentRegistration,
			AccountOnly:            event.Scope.AccountOnly,
			OperationRevision:      event.Scope.OperationRevision, ActivationID: event.Scope.ActivationID, ActivationRevision: event.Scope.ActivationRevision, TargetSelector: event.Scope.TargetSelector,
			PrincipalID: event.Scope.PrincipalID, Source: event.Scope.Source, BindingRevision: event.Scope.BindingRevision,
			PublicationBinding:         event.Scope.PublicationBinding,
			SourceContext:              event.Source,
			OriginalCapture:            event,
			OriginalPublicationRequest: row.requestBytes,
			OwnedSource:                ownedSource,
			OwnedRoot:                  ownedRoot,
			NativeCurrent:              nativeCurrent,
			Body:                       event.Body, ReceivedAt: event.ReceivedAt, Context: workContext, Release: release}, nil
	}
	return result, fmt.Errorf("WhatsApp authenticated input requires its verified retained capture")
}

func nativeCaptureMatchesReference(event capturedEvent, reference SessionInputReference, account string, source correlation.SourceArtifactFact, recovery bool) bool {
	if event.OccurrenceID != reference.OccurrenceID || event.Scope.Session.ConnectionID != reference.ConnectionID || event.Scope.Session.AccountRef != account {
		return false
	}
	if recovery && event.Scope.Kind != channelonboarding.SessionInputBusiness {
		return false
	}
	return event.Scope.Kind != channelonboarding.SessionInputBusiness ||
		(source.Validate() == nil && source.BundleHash() == event.Source.Coordinate.BundleHash)
}
