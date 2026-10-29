//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type runtimeIncomingStore interface {
	channelonboarding.Store
	sessionBusinessStore
	ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	ListOperatorChannelOperations(context.Context, string) ([]operatorchannel.Operation, error)
	claimReceiptReader
	SettleSessionChannelClaim(context.Context, authority.Claim) (operatorchannel.ClaimSettlement, error)
}

type runtimeIncoming struct {
	mu         sync.Mutex
	connection *RuntimeConnection
	store      runtimeIncomingStore
	handoff    *sessionBusinessHandoff
	registered *RegisteredBusinessBinding
}

func validateRuntimeIncoming(ctx context.Context, opts *RuntimeIncomingOptions, store channelonboarding.Store, op channelonboarding.Operation) error {
	if opts == nil {
		return nil
	}
	_, owned := store.(runtimeIncomingStore)
	if !owned || opts.Bus == nil || !opts.Posture.Valid() || contracts.ValidateIngressAlias(opts.Alias) != nil ||
		!opts.Trigger.Valid() || opts.Trigger.Transport() != packs.ChannelTransportSession || opts.Trigger.Provider() != op.Provider {
		return fmt.Errorf("WhatsApp incoming installation requires its compiled session trigger, runtime bus and selected owners")
	}
	_, err := opts.Bus.AdmitSourceArtifactFact(ctx)
	return err
}

func newRuntimeIncoming(c *RuntimeConnection, opts RuntimeIncomingOptions) (*runtimeIncoming, error) {
	reader, err := newSessionInputReader(c.state, c.captures)
	if err != nil {
		return nil, err
	}
	input, err := newSessionInputOwner(c.store, c.operations, reader, c.operation.OperationID)
	if err != nil {
		return nil, err
	}
	store := c.store.(runtimeIncomingStore) // checked before opening provider state
	return &runtimeIncoming{connection: c, store: store, handoff: &sessionBusinessHandoff{
		input: input, trigger: opts.Trigger, alias: opts.Alias, bus: opts.Bus, store: store, posture: opts.Posture, channel: c.plan}}, nil
}

func (c *RuntimeConnection) receive(ctx context.Context, raw any) (err error) {
	message, ok := raw.(*events.Message)
	if !ok {
		return nil
	}
	if c.incoming == nil {
		if c.operation.DeploymentRegistration {
			return errIncomingAdmissionPending
		}
		return errRuntimeIngressUnavailable
	}
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	account, err := c.AdmitSessionAccount(work.Context(), c.sessionAccount())
	if err != nil {
		return err
	}
	defer account.Close()
	if message.Info.IsFromMe {
		device, err := c.state.device(work.Context())
		if err != nil {
			return err
		}
		return validateLinkedDeviceEcho(message, device.ID, device.LID)
	}
	incoming := c.incoming
	if c.operation.DeploymentRegistration {
		op, err := c.currentOperation(work.Context())
		if err != nil {
			return err
		}
		if !op.AccountConfirmed {
			return errIncomingAdmissionPending
		}
		claim, err := incoming.pendingOperatorClaim(work.Context(), op, message)
		if err != nil {
			return err
		}
		if !claim && c.registeredBusiness != nil {
			incoming, err = c.registeredCustomerIncoming(work.Context(), incoming, op, message)
			if err != nil || incoming == nil {
				return err
			}
			defer func() { err = errors.Join(err, incoming.registered.Release()) }()
			ctx = incoming.registered.Context
		}
	}
	if !c.operation.DeploymentRegistration || incoming.registered == nil {
		ctx = work.Context()
	}
	scope, source, err := incoming.receiveScope(ctx, message)
	if err != nil {
		return err
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return errClientOccurrenceFenced
	}
	event, err := captureSDKMessage(scope, source, occurrence.occurrenceID, message, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		return err
	}
	if err := c.captures.capture(ctx, event); err != nil {
		return err
	}
	// Local capture, selected-store publication and the SDK receipt remain
	// distinct. Failure preserves capture and returns non-success to the SDK.
	return incoming.drain(ctx)
}

// The SDK owns linked-device echoes, not customer ingress.
func validateLinkedDeviceEcho(message *events.Message, id *types.JID, lid types.JID) error {
	sender := message.Info.Sender.ToNonAD()
	if id == nil || sender.IsEmpty() || message.Message == nil || !exactMessageID(message.Info.ID) ||
		message.Info.Timestamp.IsZero() || message.Info.Timestamp.UnixMilli() < 1 ||
		sender != id.ToNonAD() && sender != lid.ToNonAD() {
		return errIncomingMalformed
	}
	return nil
}

func (c *RuntimeConnection) registeredCustomerIncoming(ctx context.Context, incoming *runtimeIncoming, op channelonboarding.Operation, message *events.Message) (*runtimeIncoming, error) {
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return nil, errClientOccurrenceFenced
	}
	unclaimed, err := captureSDKMessage(captureScope{DeploymentRegistration: true, Kind: channelonboarding.SessionInputOnboarding,
		Session: op.SessionAccount, OnboardingOperation: op.OperationID, OperationRevision: op.Revision, PrincipalID: op.PrincipalID},
		captureSource{DeploymentRegistration: true, CatalogGeneration: incoming.handoff.trigger.Generation()}, occurrence.occurrenceID, message, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		return nil, err
	}
	if terminal, err := c.captures.isUnclaimed(ctx, unclaimed); err != nil || terminal {
		return nil, err
	}
	binding, err := c.registeredBusiness(ctx, op)
	if errors.Is(err, ErrAccountUnclaimed) {
		return nil, c.captures.retainUnclaimed(ctx, unclaimed)
	}
	if err != nil {
		return nil, err
	}
	if binding.Release == nil || binding.Current == nil || binding.Context == nil || binding.Coordinate.Validate() != nil || binding.Publication.Validate() != nil {
		if binding.Release != nil {
			_ = binding.Release()
		}
		return nil, errCaptureScopeChanged
	}
	if err := binding.Current(binding.Context); err != nil {
		return nil, errors.Join(err, binding.Release())
	}
	incoming, err = newRuntimeIncoming(c, binding.Incoming)
	if err != nil {
		return nil, errors.Join(err, binding.Release())
	}
	incoming.registered, incoming.handoff.input.registered = &binding, &binding
	return incoming, nil
}

func (i *runtimeIncoming) receiveScope(ctx context.Context, message *events.Message) (captureScope, captureSource, error) {
	op, err := i.connection.currentOperation(ctx)
	if err != nil {
		return captureScope{}, captureSource{}, err
	}
	if op.DeploymentRegistration {
		if i.registered != nil {
			binding := i.registered
			scope := captureScope{DeploymentRegistration: true, RegisteredBusiness: true, AccountOnly: true,
				Kind: channelonboarding.SessionInputBusiness, Session: op.SessionAccount,
				PublicationBinding: binding.Publication, Source: binding.Coordinate.DurableIdentity(),
				OnboardingOperation: op.OperationID, OperationRevision: op.Revision,
				TargetSelector: binding.TargetSelector, PrincipalID: op.PrincipalID}
			return scope, captureSource{DeploymentRegistration: true, Coordinate: binding.Coordinate,
				CatalogGeneration: binding.Incoming.Trigger.Generation()}, scope.Validate()
		}
		claim, err := i.pendingOperatorClaim(ctx, op, message)
		if err != nil {
			return captureScope{}, captureSource{}, err
		}
		if !op.AccountConfirmed || !claim {
			return captureScope{}, captureSource{}, errIncomingAdmissionPending
		}
		scope := captureScope{DeploymentRegistration: true, Kind: channelonboarding.SessionInputOnboarding,
			Session: op.SessionAccount, OnboardingOperation: op.OperationID, OperationRevision: op.Revision, PrincipalID: op.PrincipalID}
		return scope, captureSource{DeploymentRegistration: true, CatalogGeneration: i.handoff.trigger.Generation()}, scope.Validate()
	}
	// Native account admission precedes this check. Pending execution is not
	// a capture failure, and cannot acknowledge or publish the incoming input.
	switch op.Phase {
	case channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation:
		if op.ActivationRevision == 0 && op.IdentityOperationID == "" {
			return captureScope{}, captureSource{}, errIncomingAdmissionPending
		}
	case channelonboarding.PhasePublishingActivation, channelonboarding.PhasePublishingProcessActivation,
		channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation:
		if op.AccountConnection() {
			return captureScope{}, captureSource{}, errIncomingAdmissionPending
		}
	}
	if (op.Phase != channelonboarding.PhaseAwaitingExternalIdentity && op.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation) || op.IdentityOperationID == "" {
		return i.businessScope(ctx)
	}
	if op.ActivationRevision > 0 {
		claim, err := i.pendingOperatorClaim(ctx, op, message)
		if err != nil {
			return captureScope{}, captureSource{}, err
		}
		if !claim {
			return i.businessScope(ctx)
		}
	}
	scope := captureScope{Kind: channelonboarding.SessionInputOnboarding, Session: op.SessionAccount,
		Source: op.Coordinate.DurableIdentity(), OnboardingOperation: op.OperationID, OperationRevision: op.Revision,
		TargetSelector: op.TargetSelector, PrincipalID: op.PrincipalID, BindingRevision: op.BindingRevision}
	return scope, captureSource{Coordinate: op.Coordinate, CatalogGeneration: i.handoff.trigger.Generation()}, scope.Validate()
}

func (i *runtimeIncoming) pendingOperatorClaim(ctx context.Context, op channelonboarding.Operation, message *events.Message) (bool, error) {
	if message == nil || message.Message == nil || message.Info.IsFromMe {
		return false, nil
	}
	kind, text, _, _, err := incomingContent(message)
	if err != nil || kind != "message" || text == nil {
		return false, err
	}
	challenge, shaped := operatorchannel.ChallengeFromText(*text)
	if !shaped {
		return false, nil
	}
	operations, err := i.store.ListOperatorChannelOperations(ctx, op.PrincipalID)
	if err != nil {
		return false, err
	}
	parentScope := authority.RuntimeParent
	if op.DeploymentRegistration {
		parentScope = authority.DeploymentParent
	}
	for _, identity := range operations {
		if identity.OperationID == op.IdentityOperationID && identity.OnboardingOperationID == op.OperationID &&
			identity.ProviderAuthority.Scope == parentScope &&
			identity.PrincipalID == op.PrincipalID && identity.Interface.Normalized() == op.Interface.Normalized() &&
			identity.ProviderAuthority.Kind == operatorchannel.ProviderAuthoritySession && identity.ProviderAuthority.Session == op.SessionAccount &&
			(identity.State == operatorchannel.StateAwaitingClaim || identity.State == operatorchannel.StateAwaitingConfirmation) &&
			identity.ExpiresAt.After(time.Now().UTC()) && identity.Challenge == challenge {
			return true, nil
		}
	}
	return false, nil
}

func (i *runtimeIncoming) businessScope(ctx context.Context) (captureScope, captureSource, error) {
	var scope captureScope
	var source captureSource
	op, err := i.connection.currentOperation(ctx)
	if err != nil {
		return scope, source, err
	}
	activation, err := i.store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return scope, source, err
	}
	responsibility := channelonboarding.AdmissionResponsibility{OperationID: op.OperationID,
		OperationRevision: activation.OperationRevision, ActivationRevision: op.ActivationRevision,
		Coordinate: op.Coordinate, TargetSelector: op.TargetSelector, Provider: op.Provider,
		Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
	bindings, err := i.store.ListOperatorChannelBindings(ctx, op.PrincipalID)
	if err != nil {
		return scope, source, err
	}
	matched := 0
	if responsibility.MatchesAccountBusiness(op, activation) {
		matched = 1
	}
	for _, binding := range bindings {
		if responsibility.MatchesBusinessBinding(op, activation, binding, op.BindingRevision) {
			matched++
		}
	}
	if matched != 1 {
		return scope, source, errCaptureScopeChanged
	}
	target, err := packs.ParseChannelRegistrationTarget(op.TargetSelector)
	if err != nil || target.Provider != op.Provider {
		return scope, source, errCaptureScopeChanged
	}
	ctx, err = i.handoff.bus.AdmitSourceArtifactFact(ctx)
	if err != nil {
		return scope, source, err
	}
	ownedSource, _ := correlation.SourceArtifactFactFromContext(ctx)
	landing, found, err := i.store.LoadLandingRun(ctx, runlifecycle.LiveRunScope{FlowPath: target.FlowPath})
	if err != nil {
		return scope, source, err
	}
	if !found || ctx.Err() != nil || !landing.Source.Matches(ownedSource) ||
		(landing.State != runlifecycle.StateLive && landing.State != runlifecycle.StatePaused) {
		return scope, source, errCaptureScopeChanged
	}
	scope = captureScope{Kind: channelonboarding.SessionInputBusiness, Session: op.SessionAccount,
		AccountOnly:        op.AccountConnection(),
		PublicationBinding: inbound.BindingGeneration{RunID: landing.RunID},
		Source:             op.Coordinate.DurableIdentity(), OnboardingOperation: op.OperationID,
		OperationRevision: responsibility.OperationRevision, ActivationID: activation.ActivationID,
		ActivationRevision: responsibility.ActivationRevision, TargetSelector: op.TargetSelector,
		PrincipalID: op.PrincipalID, BindingRevision: op.BindingRevision}
	source = captureSource{Coordinate: op.Coordinate, CatalogGeneration: i.handoff.trigger.Generation()}
	return scope, source, scope.Validate()
}

// ReconcileIncoming resumes only retained incoming work. It does not reconnect
// the SDK, replay an outgoing effect or adopt a replacement account/source.
func (c *RuntimeConnection) ReconcileIncoming(ctx context.Context) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	if c.incoming == nil {
		return errRuntimeIngressUnavailable
	}
	if _, err := c.currentOperation(work.Context()); err != nil {
		return err
	}
	return c.incoming.drain(work.Context())
}
