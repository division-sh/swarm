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
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"go.mau.fi/whatsmeow/types/events"
)

// RuntimeIncomingOptions supplies existing runtime owners, not raw capture,
// a connected flag or a caller-defined authority issuer.
type RuntimeIncomingOptions struct {
	Alias   string
	Trigger providertriggers.InboundAdmissionPlan
	Bus     *bus.EventBus
	Posture executionposture.Posture
}

type runtimeIncomingStore interface {
	channelonboarding.Store
	sessionBusinessStore
	ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	claimReceiptReader
	SettleSessionChannelClaim(context.Context, authority.Claim) (operatorchannel.ClaimSettlement, error)
}

type runtimeIncoming struct {
	mu         sync.Mutex
	connection *RuntimeConnection
	store      runtimeIncomingStore
	handoff    *sessionBusinessHandoff
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
	input, err := newSessionInputOwner(c.store, reader, c.operation.OperationID)
	if err != nil {
		return nil, err
	}
	store := c.store.(runtimeIncomingStore) // checked before opening provider state
	return &runtimeIncoming{connection: c, store: store, handoff: &sessionBusinessHandoff{
		input: input, trigger: opts.Trigger, alias: opts.Alias, bus: opts.Bus, store: store, posture: opts.Posture}}, nil
}

func (c *RuntimeConnection) receive(ctx context.Context, raw any) (err error) {
	message, ok := raw.(*events.Message)
	if !ok {
		return nil
	}
	if c.incoming == nil {
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
	scope, source, err := c.incoming.receiveScope(work.Context())
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
	if err := c.captures.capture(work.Context(), event); err != nil {
		return err
	}
	// Local capture, selected-store publication and the SDK receipt remain
	// distinct. Failure preserves capture and returns non-success to the SDK.
	return c.incoming.drain(work.Context())
}

func (i *runtimeIncoming) receiveScope(ctx context.Context) (captureScope, captureSource, error) {
	op, err := i.connection.currentOperation(ctx)
	if err != nil {
		return captureScope{}, captureSource{}, err
	}
	if op.Phase != channelonboarding.PhaseAwaitingExternalIdentity || op.IdentityOperationID == "" || op.ActivationRevision != 0 {
		return i.businessScope(ctx)
	}
	scope := captureScope{Kind: channelonboarding.SessionInputOnboarding, Session: op.SessionAccount,
		Source: op.Coordinate.DurableIdentity(), OnboardingOperation: op.OperationID, OperationRevision: op.Revision,
		TargetSelector: op.TargetSelector, PrincipalID: op.PrincipalID, BindingRevision: op.BindingRevision}
	return scope, captureSource{Coordinate: op.Coordinate, CatalogGeneration: i.handoff.trigger.Generation()}, scope.Validate()
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
	ownedSource, _ := correlation.SourceArtifactFactFromContext(ctx)
	standing, found, err := i.store.LoadReconciledStandingService(ctx, pipeline.StandingServiceCandidate{
		ServiceID: flowidentity.StandingServiceID(target.FlowPath), FlowPath: target.FlowPath, BindingEnabled: true, Source: ownedSource})
	if err != nil {
		return scope, source, err
	}
	if !found || ctx.Err() != nil || !standing.BindingEnabled || standing.EffectiveState != "active" ||
		!standing.RestartDisposition.Executable() || standing.Generation < 1 || standing.PublicationSequence < 1 ||
		uint64(standing.Generation) != op.Coordinate.TargetGeneration {
		return scope, source, errCaptureScopeChanged
	}
	scope = captureScope{Kind: channelonboarding.SessionInputBusiness, Session: op.SessionAccount,
		PublicationBinding: inbound.BindingGeneration{ServiceID: standing.ServiceID, RunID: standing.RunID, Generation: standing.Generation},
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
