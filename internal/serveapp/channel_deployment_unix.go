//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/store/selected"
)

type deploymentChannelSession struct {
	opts       cliapp.ChannelRegistrationOptions
	connection *sessionprovider.RuntimeConnection
	offset     int64
	business   sessionprovider.RegisteredBusinessSelector
	opened     func(channelonboarding.Operation, *sessionprovider.RuntimeConnection) error
}

func deploymentChannelHandlers(repo, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, webhook func(context.Context, string, string) bool, runtimeOptions deploymentRuntimeOptions) (apiv1.MethodHandler, publicingress.PendingDeploymentClaim, func(context.Context) error, func() error) {
	root, rootErr := cliapp.NewInvocationRoot(repo)
	factory := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		opts.CurrentWebhook = webhook
		value, err := OpenChannelRegistration(ctx, opts)
		if err == nil {
			configureDeploymentRuntimeSession(value, runtimeOptions)
		}
		return value, err
	}
	var mu sync.Mutex
	clients := map[string]*cliapp.DeploymentChannelControl{}
	handler := func(ctx context.Context, req apiv1.Request) (any, error) {
		if rootErr != nil {
			return nil, rootErr
		}
		if err := requireDeploymentHolder(ctx, capability, req.Params["holder"]); err != nil {
			return nil, err
		}
		var err error
		provider, _ := req.Params["provider"].(string)
		id, _ := req.Params["operation_id"].(string)
		account, _ := req.Params["account"].(string)
		if req.Method == "channel.list" && account != "" && runtimeOptions.manager != nil {
			ref := channelonboarding.AccountReference{Provider: provider, Account: account}
			return observeDeploymentAccountStatus(ctx, ref, runtimeOptions, stores.DeploymentChannels(), clients, &mu)
		}
		key := provider + ":" + id
		mu.Lock()
		client, found := clients[key]
		if !found {
			client, err = cliapp.UseDeploymentChannelControl(ctx, root, config, directory, provider, stores, process, factory)
		}
		mu.Unlock()
		if err != nil {
			return nil, err
		}
		var result json.RawMessage
		err = client.Dispatch(ctx, req.Method, req.Params, &result)
		if err == nil && req.Method == "channel.list" && runtimeOptions.manager != nil {
			result, err = observeDeploymentAccountList(ctx, result, runtimeOptions, clients, &mu)
		}
		head, decodeErr := deploymentControlResult(result, err)
		if decodeErr != nil {
			return nil, decodeErr
		}
		rememberDeploymentControl(clients, &mu, provider, head.Operation, client)
		if !found && head.Operation.OperationID == "" {
			err = errors.Join(err, client.Close(context.WithoutCancel(ctx)))
		}
		if err == nil && !slices.Contains([]string{"channel.list", "channel.onboarding_get", "channel.onboarding_select"}, req.Method) && runtimeOptions.refresh != nil {
			err = runtimeOptions.refresh(ctx)
		}
		return result, err
	}
	close := func() error {
		mu.Lock()
		defer mu.Unlock()
		return closeDeploymentControls(clients)
	}
	claim := deploymentPendingClaimHandler(root, config, directory, stores, process, capability, factory)
	restore := func(ctx context.Context) error {
		operations, err := stores.DeploymentChannels().ListChannelOnboardingOperations(ctx)
		if err != nil || rootErr != nil {
			return errors.Join(err, rootErr)
		}
		return restoreDeploymentAccountControls(ctx, operations, root, config, directory, stores, process, factory, clients, &mu)
	}
	return handler, claim, restore, close
}

func deploymentPendingClaimHandler(root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, factory cliapp.ChannelRegistrationFactory) publicingress.PendingDeploymentClaim {
	return func(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
		work, err := process.Begin(ctx)
		if err != nil {
			return false, err
		}
		defer func() { _ = work.Done() }()
		if err := capability.ProveCurrent(work.Context()); err != nil {
			return false, err
		}
		operations, err := stores.DeploymentChannels().ListChannelOnboardingOperations(work.Context())
		switch {
		case err != nil:
			return false, err
		case !slices.ContainsFunc(operations, func(op channelonboarding.Operation) bool {
			return op.PendingDeploymentClaim(account)
		}):
			return false, nil
		}
		client, err := cliapp.UseDeploymentChannelControl(work.Context(), root, config, directory, account.Provider, stores, process, factory)
		if err != nil {
			return false, err
		}
		defer func() { _ = client.Close(context.WithoutCancel(ctx)) }()
		return client.ConsumeWebhookClaim(work.Context(), account, credential, admit, current)
	}
}

func deploymentControlResult(result json.RawMessage, dispatchErr error) (channelonboarding.Result, error) {
	var head channelonboarding.Result
	if dispatchErr == nil {
		if err := json.Unmarshal(result, &head); err != nil {
			return head, err
		}
	}
	var required *channelonboarding.CredentialRequiredError
	if errors.As(dispatchErr, &required) {
		head.Operation.OperationID = required.OperationID
	}
	return head, nil
}

func rememberDeploymentControl(clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex, provider string, op channelonboarding.Operation, client *cliapp.DeploymentChannelControl) {
	mu.Lock()
	defer mu.Unlock()
	if op.OperationID != "" {
		clients[provider+":"+op.OperationID] = client
	}
	if op.IdentityOperationID != "" {
		clients[provider+":"+op.IdentityOperationID] = client
	}
}

func restoreDeploymentAccountControls(ctx context.Context, operations []channelonboarding.Operation, root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, factory cliapp.ChannelRegistrationFactory, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) error {
	for _, op := range channelonboarding.RetainedConfirmedAccounts(operations, "whatsapp") {
		ref := channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}
		client, err := cliapp.UseDeploymentChannelControl(ctx, root, config, directory, op.Provider, stores, process, factory)
		if err != nil {
			return err
		}
		if err := client.ResumeRegisteredAccount(ctx, ref); err != nil {
			return errors.Join(err, client.Close(context.WithoutCancel(ctx)))
		}
		mu.Lock()
		clients[op.Provider+":"+op.OperationID] = client
		mu.Unlock()
	}
	return nil
}

func configureDeploymentRuntimeSession(value cliapp.ChannelRegistrationSession, runtimeOptions deploymentRuntimeOptions) {
	if runtimeOptions.manager == nil {
		return
	}
	session := value.(*deploymentChannelSession)
	session.business = serveRegisteredBusinessSelector(runtimeOptions.manager)
	if owner, ok := runtimeOptions.sessions.(*serveSessionBootstrap); ok {
		session.opened = owner.registerDeploymentConnection
	}
}

func closeDeploymentControls(clients map[string]*cliapp.DeploymentChannelControl) error {
	seen := map[*cliapp.DeploymentChannelControl]bool{}
	var err error
	for _, client := range clients {
		if !seen[client] {
			err = errors.Join(err, client.Close(context.Background()))
			seen[client] = true
		}
	}
	return err
}

func requireDeploymentHolder(ctx context.Context, capability startupownership.ProcessCapability, supplied any) error {
	var expected startupownership.Authority
	raw, err := json.Marshal(supplied)
	if err != nil || json.Unmarshal(raw, &expected) != nil || expected.Validate() != nil {
		return channelonboarding.ErrInvalidRequest
	}
	actual, err := capability.Evidence()
	if err != nil || actual != expected {
		return errors.Join(channelonboarding.ErrConflict, err)
	}
	return capability.ProveCurrent(ctx)
}

// OpenChannelRegistration composes an invocation, not Run or a runtime target.
// Keeping native wiring here follows the existing CLI executable-runner seam.
func OpenChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	if ctx == nil || ctx.Err() != nil || opts.Process == nil || opts.Operations == nil || opts.Store == nil ||
		opts.Credentials == nil || opts.Principal.Validate() != nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	return &deploymentChannelSession{opts: opts}, nil
}

func (c *deploymentChannelSession) QualifySessionPlan(candidate channelonboarding.Candidate) error {
	return sessionprovider.QualifyBootstrapPlan(candidate.Plan)
}
func (c *deploymentChannelSession) BootstrapSession(ctx context.Context, op channelonboarding.Operation, _ channelonboarding.Candidate) error {
	if op.Provider == "telegram" {
		return nil
	}
	if c.connection != nil {
		return c.connection.CheckBootstrap(ctx)
	}
	var err error
	c.connection, err = sessionprovider.OpenDeploymentConnection(ctx, sessionprovider.DeploymentConnectionOptions{Directory: c.opts.Directory,
		OperationID: op.OperationID, Principal: c.opts.Principal, Process: c.opts.Process, Operations: c.opts.Operations,
		Store: c.opts.Store, Credentials: c.opts.Credentials, Plan: c.opts.Plan, Trigger: c.opts.Trigger, RegisteredBusiness: c.business})
	if err != nil {
		return err
	}
	if c.opened != nil {
		if err := c.opened(op, c.connection); err != nil {
			return errors.Join(err, c.connection.Close(context.WithoutCancel(ctx)))
		}
	}
	return c.connection.Connect(ctx)
}
func (c *deploymentChannelSession) ResumeSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if c.connection != nil {
		reusable, err := c.connection.CheckSessionReuse(ctx, op)
		if err != nil || reusable {
			return err
		}
		if err := c.connection.Close(ctx); err != nil {
			return err
		}
		c.connection = nil
	}
	return c.BootstrapSession(ctx, op, candidate)
}
func (c *deploymentChannelSession) AwaitSessionBootstrap(ctx context.Context, op channelonboarding.Operation) error {
	if op.Provider == "telegram" {
		return nil // The pairing checkpoint performs the authenticated getMe.
	}
	return c.connection.CheckBootstrap(ctx)
}
func (c *deploymentChannelSession) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	if op.Provider == "telegram" {
		identity, err := c.telegramRead(ctx, op)
		if err != nil {
			return op, false, err
		}
		ref, err := channelonboarding.CanonicalAccount(op.Provider, identity)
		if err != nil || op.ExpectedAccount != "" && op.ExpectedAccount != ref.Account {
			return op, false, errors.Join(channelonboarding.ErrConflict, err)
		}
		next, err := c.opts.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
			ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseAwaitingExternalIdentity, AccountReference: ref.Account, Now: time.Now().UTC()})
		return next, err == nil, err
	}
	return c.connection.CheckpointPairing(ctx, op.Revision)
}

func (c *deploymentChannelSession) telegramRead(ctx context.Context, op channelonboarding.Operation) (string, error) {
	profile, ok := c.opts.Plan.Registration()
	if !ok || !op.DeploymentRegistration || op.Provider != "telegram" || op.PrincipalID != c.opts.Principal.ID {
		return "", channelonboarding.ErrInvalidRequest
	}
	plan, err := profile.Operation(packs.RegistrationOperationIdentify)
	if err != nil {
		return "", err
	}
	values := map[string]any{}
	for _, admission := range op.CredentialAdmissions {
		snapshot, current, err := c.opts.Credentials.ObserveValueMatchingSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
		if err != nil || !current {
			return "", errors.Join(operatorchannel.ErrCredentialStale, err)
		}
		values[admission.Role] = snapshot.CredentialValue()
	}
	input, err := plan.Prepare(map[string]any{})
	if err != nil {
		return "", err
	}
	executor := registration.HTTPExecutor{Client: c.opts.HTTP, Preflight: func(ctx context.Context) error {
		current, err := c.opts.Operations.GetChannelOnboarding(ctx, op.OperationID)
		if err != nil || current.Revision != op.Revision || current.PrincipalID != op.PrincipalID {
			return errors.Join(channelonboarding.ErrRevisionConflict, err)
		}
		for _, admission := range op.CredentialAdmissions {
			matches, err := c.CurrentValueMatchesSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
			if err != nil || !matches {
				return errors.Join(operatorchannel.ErrCredentialStale, err)
			}
		}
		return nil
	}}
	output, err := executor.Read(ctx, plan, input, values)
	if err != nil {
		return "", err
	}
	projected, err := plan.Project(output)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(projected["resource_id"]), nil
}
func (c *deploymentChannelSession) ReadSessionPairing(ctx context.Context, _ channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if c.connection == nil {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	return c.connection.PairingReadback(ctx, principal)
}
func (c *deploymentChannelSession) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view := channelonboarding.SessionConnectionReadback{State: "disconnected", Linked: op.AccountReference != "", AccountPresentation: op.AccountReference}
	if op.Provider == "telegram" && op.AccountReference != "" && !op.Phase.Terminal() {
		if _, err := c.telegramRead(ctx, op); err != nil {
			return view, err
		}
		view.State = "connected"
		return view, nil
	}
	if c.connection == nil || op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired {
		return view, nil
	}
	view.Reason = c.connection.DisconnectionReason()
	count, err := c.connection.UnclaimedCount(ctx)
	if err != nil {
		return view, err
	}
	view.Unclaimed = &count
	if op.SessionAccount.Validate() == nil {
		var err error
		view.AccountPresentation, view.AccountName, err = c.connection.AccountPresentation(ctx)
		if err != nil {
			return view, err
		}
	}
	if view.Reason == "" {
		view.State = "connecting"
		if view.Linked && c.connection.CheckBootstrap(ctx) == nil {
			view.State = "connected"
		}
	}
	return view, nil
}
func (c *deploymentChannelSession) CurrentValueMatchesSeal(ctx context.Context, value credentials.ValueEvidence) (bool, error) {
	return c.opts.Credentials.CurrentValueMatchesSeal(ctx, value)
}
func (c *deploymentChannelSession) AdmitSessionAccount(ctx context.Context, account operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if c.connection == nil {
		return authority.Admission{}, &operatorchannel.SessionProviderUnavailableError{Provider: account.Provider}
	}
	return c.connection.AdmitSessionAccount(ctx, account)
}
func (c *deploymentChannelSession) Close(ctx context.Context) error {
	if c.connection == nil {
		return nil
	}
	if err := errors.Join(c.connection.Close(ctx), ctx.Err()); err != nil {
		return err
	}
	c.connection = nil
	return nil
}

func (c *deploymentChannelSession) PollOperatorClaim(ctx context.Context, op channelonboarding.Operation) error {
	var err error
	c.offset, err = sessionprovider.PollDeploymentClaim(ctx, c.opts.HTTP, c.opts.Operations, c.opts.Identities, c.opts.Credentials,
		c.opts.Plan, c.opts.Trigger, op, c.offset)
	var active providertriggers.PollingWebhookActive
	if errors.As(err, &active) && c.opts.CurrentWebhook != nil && c.opts.CurrentWebhook(ctx, active.URL, op.AccountReference) {
		return nil
	}
	return err
}

func (c *deploymentChannelSession) ConsumeWebhookClaim(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
	return sessionprovider.ConsumeDeploymentWebhookClaim(ctx, sessionprovider.DeploymentConnectionOptions{Operations: c.opts.Operations, Principal: c.opts.Principal, Plan: c.opts.Plan}, c.opts.Identities, account, credential, admit, current)
}

func (c *deploymentChannelSession) VerifyRegistration(ctx context.Context, op channelonboarding.Operation) (bool, error) {
	if op.Provider == "telegram" {
		identity, err := c.telegramRead(ctx, op)
		if err != nil {
			return false, err
		}
		if identity != op.AccountReference {
			return false, channelonboarding.ErrConflict
		}
		if op.IdentityOperationID != "" {
			return true, c.PollOperatorClaim(ctx, op)
		}
		return true, nil
	}
	if c.connection == nil {
		return false, channelonboarding.ErrInvalidRequest
	}
	return c.connection.VerifyDeploymentRegistration(ctx, op)
}

func (c *deploymentChannelSession) LogoutRegistration(ctx context.Context, op channelonboarding.Operation) error {
	if op.Provider == "telegram" {
		for _, state := range []string{"account_logout_launched", "account_logout_unlinked"} {
			var err error
			op, err = c.opts.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: op.Phase, FailureCode: state, Now: time.Now().UTC()})
			if err != nil {
				return err
			}
		}
		return nil
	}
	if c.connection == nil {
		if op.FailureCode != "account_logout_pending" {
			return fmt.Errorf("%w: logout outcome is unconfirmed; no unlink was repeated", channelonboarding.ErrConflict)
		}
		if err := c.BootstrapSession(ctx, op, channelonboarding.Candidate{}); err != nil {
			return err
		}
	}
	if op.FailureCode == "account_logout_pending" {
		verified, err := c.VerifyRegistration(ctx, op)
		if err != nil || !verified {
			return errors.Join(channelonboarding.ErrConflict, err)
		}
	}
	if err := c.connection.LogoutDeploymentRegistration(ctx, op); err != nil {
		return err
	}
	return c.CloseRegistration(ctx, op)
}

func (c *deploymentChannelSession) CloseRegistration(ctx context.Context, op channelonboarding.Operation) error {
	if op.Provider != "whatsapp" || c.connection == nil || op.PrincipalID != c.opts.Principal.ID || c.connection.ConnectionID() != op.SessionConnectionID {
		return channelonboarding.ErrRevisionConflict
	}
	return c.Close(ctx)
}
