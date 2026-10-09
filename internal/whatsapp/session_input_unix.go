//go:build linux || darwin

package whatsapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

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

func (r *sessionInputReader) ReadAuthenticatedSessionInput(ctx context.Context, reference channelonboarding.SessionInputReference) (channelonboarding.NativeSessionInput, error) {
	var result channelonboarding.NativeSessionInput
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
		return channelonboarding.NativeSessionInput{Scope: event.Scope.Kind, Account: event.Scope.Session, OperationID: event.Scope.OnboardingOperation,
			OperationRevision: event.Scope.OperationRevision, ActivationRevision: event.Scope.ActivationRevision, TargetSelector: event.Scope.TargetSelector,
			PrincipalID: event.Scope.PrincipalID, Source: event.Scope.Source, BindingRevision: event.Scope.BindingRevision,
			Body: event.Body, ReceivedAt: event.ReceivedAt, Context: workContext, Release: release}, nil
	}
	return result, fmt.Errorf("WhatsApp authenticated input requires its verified retained capture")
}
