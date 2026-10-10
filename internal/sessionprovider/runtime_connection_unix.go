//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	effects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/execution"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/executionfact"
	"github.com/google/uuid"
)

var errRuntimeConnection = errors.New("WhatsApp connection requires its retained operation and live runtime source")
var errRuntimeIngressUnavailable = errors.New("WhatsApp runtime incoming publication is not installed")

type RuntimeConnectionOptions struct {
	Directory   string
	OperationID string
	Store       channelonboarding.Store
	Credentials operatorchannel.CredentialCurrentness
	Plan        packs.SatisfactionPlan
	Incoming    *RuntimeIncomingOptions
}

// RuntimeConnection owns the actual SDK state and socket, not a registration
// promise. Neither its zero value nor its readback can manufacture execution.
type RuntimeConnection struct {
	lifecycle   chan struct{}
	parent      *worklifetime.RuntimeOccurrence
	work        *worklifetime.Lease
	ctx         context.Context
	cancel      context.CancelFunc
	closed      chan struct{}
	closeErr    error
	state       *sessionState
	captures    *captureStore
	directory   string
	store       channelonboarding.Store
	operation   channelonboarding.Operation
	account     atomic.Pointer[operatorchannel.SessionAccountAdmission]
	incoming    *runtimeIncoming
	plan        packs.SatisfactionPlan
	credentials operatorchannel.CredentialCurrentness
}

// OpenRuntimeConnection resumes paired state only after its original selected
// operation and runtime source are verified. Unpaired bootstrap has a separate
// reservation/QR path; this constructor cannot adopt a new provider account.
func OpenRuntimeConnection(ctx context.Context, opts RuntimeConnectionOptions) (*RuntimeConnection, error) {
	return openRuntimeConnection(ctx, opts, false)
}

// OpenRuntimeBootstrap opens only the existing reserved pairing responsibility.
// It cannot resume an admitted account or manufacture an enabled business run.
func OpenRuntimeBootstrap(ctx context.Context, opts RuntimeConnectionOptions) (*RuntimeConnection, error) {
	return openRuntimeConnection(ctx, opts, true)
}

func openRuntimeConnection(ctx context.Context, opts RuntimeConnectionOptions, bootstrap bool) (*RuntimeConnection, error) {
	parent, op, err := runtimeConnectionReservation(ctx, opts)
	if err != nil {
		return nil, err
	}
	if bootstrap {
		if op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
			return nil, errRuntimeConnection
		}
	} else if op.SessionAccount.Validate() != nil {
		return nil, errRuntimeConnection
	}
	// This is a non-transient process-control lease, not a standing service or
	// enabled business binding. Runtime retirement still joins the whole owner.
	work, err := parent.BeginStanding(ctx)
	if err != nil {
		return nil, err
	}
	owned, cancel := context.WithCancel(work.Context())
	c := &RuntimeConnection{parent: parent, work: work, ctx: owned, cancel: cancel, closed: make(chan struct{}), lifecycle: make(chan struct{}, 1),
		store: opts.Store, operation: op, plan: opts.Plan, credentials: opts.Credentials, directory: opts.Directory}
	c.account.Store(&op.SessionAccount)
	go c.joinRetirement()
	if err := c.open(opts.Incoming); err != nil {
		return c, errors.Join(err, c.Close(context.WithoutCancel(ctx)))
	}
	return c, nil
}

func runtimeConnectionReservation(ctx context.Context, opts RuntimeConnectionOptions) (*worklifetime.RuntimeOccurrence, channelonboarding.Operation, error) {
	var empty channelonboarding.Operation
	if ctx == nil || ctx.Err() != nil || opts.Store == nil || opts.Directory == "" || uuid.Validate(opts.OperationID) != nil {
		return nil, empty, errRuntimeConnection
	}
	parent, ok := worklifetime.RuntimeOccurrenceFromContext(ctx)
	source, sourceOK := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || !sourceOK || source.Validate() != nil {
		return nil, empty, errRuntimeConnection
	}
	op, err := opts.Store.GetChannelOnboarding(ctx, opts.OperationID)
	if err != nil {
		return nil, empty, err
	}
	identity := parent.Identity()
	if ctx.Err() != nil || op.OperationID != opts.OperationID || op.Provider != "whatsapp" ||
		op.Posture != channelonboarding.ActivationSessionConnection || op.Revision < 1 ||
		op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired ||
		op.ValidateSessionAccount() != nil ||
		op.Coordinate.ValidateContext() != nil || op.Interface.Validate() != nil || uuid.Validate(op.PrincipalID) != nil ||
		op.Coordinate.RuntimeInstanceID != identity.RuntimeInstanceID || op.Coordinate.BundleHash != identity.BundleHash ||
		source.BundleHash() != identity.BundleHash {
		return nil, empty, errRuntimeConnection
	}
	if err := validateRuntimeConnectionPlan(opts.Plan, op); err != nil {
		return nil, empty, err
	}
	if err := validateRuntimeIncoming(ctx, opts.Incoming, opts.Store, op); err != nil {
		return nil, empty, err
	}
	return parent, op, nil
}

func validateRuntimeConnectionPlan(plan packs.SatisfactionPlan, op channelonboarding.Operation) error {
	generation, err := plan.Generation()
	if err != nil {
		return err
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		return err
	}
	if plan.Transport() != packs.ChannelTransportSession || plan.Provider() != op.Provider ||
		!generation.Equal(op.Coordinate.PlanGeneration) || identity.Normalized() != op.Interface.Normalized() {
		return errRuntimeConnection
	}
	return QualifyBootstrapPlan(plan)
}

// QualifyBootstrapPlan checks the concrete shipped SDK implementation for
// pairing only. It grants no native account, output seal or business readiness.
func QualifyBootstrapPlan(plan packs.SatisfactionPlan) error {
	if plan.Transport() != packs.ChannelTransportSession || plan.Provider() != "whatsapp" || len(plan.OperationNames()) == 0 {
		return errRuntimeConnection
	}
	for _, name := range plan.OperationNames() {
		id, tool, err := plan.ConnectorOperation(name)
		if err != nil {
			return err
		}
		target, native := tool.InProcess()
		if !native || target != contracts.ToolInProcessWhatsAppSendText || tool.ValidateDeclarationName(id) != nil {
			return fmt.Errorf("WhatsApp runtime operation %q has no owned native implementation", name)
		}
	}
	return nil
}

func (c *RuntimeConnection) open(incoming *RuntimeIncomingOptions) error {
	if err := c.lockLifecycle(c.ctx); err != nil {
		return err
	}
	defer func() { <-c.lifecycle }()
	if c.ctx.Err() != nil {
		return errRuntimeConnection
	}
	var err error
	c.state, err = openSessionState(c.ctx, c.directory, c.operation.SessionConnectionID, c.operation.SessionAccount.AccountRef)
	if err != nil {
		return err
	}
	c.captures, err = newCaptureStore(c.ctx, c.state.database, c.operation.SessionConnectionID)
	if err != nil {
		return err
	}
	if incoming != nil {
		c.incoming, err = newRuntimeIncoming(c, *incoming)
		if err != nil {
			return err
		}
	}
	occurrence, err := c.state.newOccurrence(c.ctx, uuid.NewString())
	if err != nil {
		return err
	}
	if c.operation.SessionAccount == (operatorchannel.SessionAccountAdmission{}) && occurrence.client.Store.ID == nil {
		if _, err := occurrence.bindPairing(pairingQRScope{PrincipalID: c.operation.PrincipalID,
			OperationID: c.operation.OperationID, ConnectionID: c.operation.SessionConnectionID,
			OccurrenceID: occurrence.occurrenceID, Coordinate: c.operation.Coordinate}); err != nil {
			return err
		}
	}
	_, err = occurrence.bindCallbacks(c.receive, c.captures.recordFailure)
	return err
}

func (c *RuntimeConnection) begin(ctx context.Context) (*worklifetime.Lease, error) {
	if c == nil || c.parent == nil || c.ctx == nil || c.ctx.Err() != nil || ctx == nil || ctx.Err() != nil {
		return nil, errRuntimeConnection
	}
	return c.parent.Begin(ctx)
}

func (c *RuntimeConnection) currentOperation(ctx context.Context) (channelonboarding.Operation, error) {
	op, err := c.currentOperationScope(ctx)
	if err != nil {
		return op, err
	}
	if op.SessionAccount != c.sessionAccount() {
		return op, errRuntimeConnection
	}
	return op, nil
}

func (c *RuntimeConnection) sessionAccount() operatorchannel.SessionAccountAdmission {
	if account := c.account.Load(); account != nil {
		return *account
	}
	return operatorchannel.SessionAccountAdmission{}
}

func (c *RuntimeConnection) currentOperationScope(ctx context.Context) (channelonboarding.Operation, error) {
	op, err := c.store.GetChannelOnboarding(ctx, c.operation.OperationID)
	if err != nil {
		return op, err
	}
	if ctx.Err() != nil || c.ctx.Err() != nil || op.OperationID != c.operation.OperationID ||
		op.Revision < c.operation.Revision ||
		op.Provider != c.operation.Provider || op.Posture != c.operation.Posture ||
		op.PrincipalID != c.operation.PrincipalID || op.Interface.Normalized() != c.operation.Interface.Normalized() ||
		op.TargetSelector != c.operation.TargetSelector ||
		op.SessionConnectionID != c.operation.SessionConnectionID ||
		!op.Coordinate.MatchesDeclaration(c.operation.Coordinate) ||
		op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired || op.ValidateSessionAccount() != nil {
		return op, errRuntimeConnection
	}
	return op, nil
}

func (c *RuntimeConnection) Connect(ctx context.Context) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			err = errors.Join(err, work.Done())
		}
	}()
	if _, err := c.currentOperation(work.Context()); err != nil {
		return err
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return errClientOccurrenceFenced
	}
	// Cancellation may stop the caller's wait before the SDK's handshake wait
	// exits. The attempt retains this transient lease through its complete
	// occurrence join; no dial or cleanup is abandoned on caller return.
	done := make(chan error, 1)
	handedOff = true
	go func() {
		err := occurrence.connect(work.Context())
		done <- errors.Join(err, work.Done())
	}()
	select {
	case err := <-done:
		return errors.Join(err, context.Cause(ctx))
	case <-ctx.Done():
		occurrence.fence()
		return context.Cause(ctx)
	case <-c.ctx.Done():
		occurrence.fence()
		return context.Cause(c.ctx)
	}
}

func (c *RuntimeConnection) CurrentValueMatchesSeal(ctx context.Context, expected credentials.ValueEvidence) (bool, error) {
	if c == nil || c.credentials == nil {
		return false, fmt.Errorf("native connection has no credential snapshot owner")
	}
	return c.credentials.CurrentValueMatchesSeal(ctx, expected)
}

func (c *RuntimeConnection) ConnectionID() string {
	if c == nil {
		return ""
	}
	return c.operation.SessionConnectionID
}

// CheckBootstrap proves this original attempt remains connected. A retained
// pointer, completed Close or failed Connect is never successful bootstrap.
func (c *RuntimeConnection) CheckBootstrap(ctx context.Context) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	if err := c.lockLifecycle(work.Context()); err != nil {
		return err
	}
	defer func() { <-c.lifecycle }()
	if _, err := c.currentOperation(work.Context()); err != nil {
		return err
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return errClientOccurrenceFenced
	}
	occurrence.mu.Lock()
	current := occurrence.started && !occurrence.fenced && occurrence.ctx.Err() == nil && occurrence.client.IsConnected()
	occurrence.mu.Unlock()
	if !current {
		return errClientOccurrenceFenced
	}
	return nil
}

func (c *RuntimeConnection) AdmitSessionAccount(ctx context.Context, expected operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if c == nil || c.ctx == nil || c.ctx.Err() != nil || ctx == nil || ctx.Err() != nil {
		return authority.Admission{}, errRuntimeConnection
	}
	op, err := c.currentOperation(ctx)
	if err != nil {
		return authority.Admission{}, err
	}
	owner, err := newSessionAuthorityOwner(c.state, c.store, op, c.credentials)
	if err != nil {
		return authority.Admission{}, err
	}
	return owner.AdmitSessionAccount(ctx, expected)
}

func (c *RuntimeConnection) ChannelExecution(ctx context.Context) (execution.Channel, error) {
	if c == nil {
		return execution.Channel{}, errRuntimeConnection
	}
	admitted, err := c.AdmitSessionAccount(ctx, c.sessionAccount())
	if err != nil {
		return execution.Channel{}, err
	}
	defer admitted.Close()
	return executionfact.SealOwnedChannel(c.operation.OperationID, c), nil
}

// ObserveSession returns held admission plus this SDK occurrence's observation.
// The readiness owner must still join source, activation, principal and receipts.
func (c *RuntimeConnection) ObserveSession(ctx context.Context) (operatorchannel.ProviderAuthority, operatorchannel.SessionConnectionObservation, error) {
	var empty operatorchannel.SessionConnectionObservation
	if c == nil {
		return operatorchannel.ProviderAuthority{}, empty, errRuntimeConnection
	}
	account := c.sessionAccount()
	provider := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}
	admitted, current, err := provider.AdmitExecution(ctx, c)
	if err != nil || !current {
		return operatorchannel.ProviderAuthority{}, empty, errors.Join(errRuntimeConnection, err)
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil || !c.state.ownsConnectedOccurrence(ctx, occurrence) || admitted.RequireExecutable() != nil {
		admitted.CloseExecution()
		return operatorchannel.ProviderAuthority{}, empty, errClientOccurrenceFenced
	}
	return admitted, operatorchannel.SessionConnectionObservation{Admission: account,
		OccurrenceID: occurrence.occurrenceID, Connected: true, ObservedAt: time.Now().UTC()}, nil
}

func (c *RuntimeConnection) ExecuteChannelWrite(ctx context.Context, operation, toolID string, tool contracts.ToolSchemaEntry, input map[string]any, lineage map[string]string, kind effects.AuthorityKind) (result registration.DeliveryResult, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return result, err
	}
	owner, err := newSessionAuthorityOwner(c.state, c.store, op, c.credentials)
	if err != nil {
		return result, err
	}
	return (sessionChannelExecutor{owner: owner, plan: c.plan}).ExecuteChannelWrite(work.Context(), operation, toolID, tool, input, lineage, kind)
}

func (c *RuntimeConnection) joinRetirement() {
	select {
	case <-c.ctx.Done():
		_ = c.Close(context.Background()) // Close retains the error and unreleased responsibility.
	case <-c.closed:
	}
}

func (c *RuntimeConnection) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil || c.closed == nil || c.cancel == nil || c.work == nil || c.lifecycle == nil {
		return errRuntimeConnection
	}
	// Request retirement independently of this caller's bounded join wait. The
	// existing counted owner retains possession until SDK cleanup completes.
	c.cancel()
	if err := c.lockLifecycle(ctx); err != nil {
		return err
	}
	defer func() { <-c.lifecycle }()
	select {
	case <-c.closed:
		return c.closeErr
	default:
	}
	if c.state != nil {
		if err := c.state.close(ctx); err != nil {
			c.closeErr = err
			return err
		}
	}
	if err := c.work.Done(); err != nil {
		c.closeErr = err
		return err
	}
	c.closeErr = nil
	close(c.closed)
	return nil
}

// The cleanup owner retains resources while joining. Other callers only wait
// for serialization and may cancel that wait without canceling its cleanup.
func (c *RuntimeConnection) lockLifecycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	select {
	case c.lifecycle <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-c.lifecycle
			return context.Cause(ctx)
		}
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

var _ operatorchannel.SessionAdmissionOwner = (*RuntimeConnection)(nil)
var _ operatorchannel.CredentialCurrentness = (*RuntimeConnection)(nil)
