//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
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
	connection serveSessionConnection
	business   sessionprovider.RegisteredBusinessSelector
	opened     func(channelonboarding.Operation, serveSessionConnection) error
}

func (c *deploymentChannelSession) AcknowledgeRegisteredOperator(ctx context.Context, op channelonboarding.Operation, child operatorchannel.Operation, binding operatorchannel.Binding) error {
	if c == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if c.connection == nil {
		return channelonboarding.ErrInvalidRequest
	}
	return c.connection.AcknowledgeRegisteredOperator(ctx, c.opts.Effects, op, child, binding)
}

func deploymentChannelHandlers(repo, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, webhook func(context.Context, string, string) bool, runtimeOptions deploymentRuntimeOptions) (apiv1.MethodHandler, publicingress.PendingDeploymentClaim, func(context.Context) error, func() error) {
	root, rootErr := cliapp.NewInvocationRoot(repo)
	factory := channelRegistrations{
		configure: func(opts cliapp.ChannelRegistrationOptions) cliapp.ChannelRegistrationOptions {
			opts.CurrentWebhook = webhook
			return opts
		},
		opened: func(value cliapp.ChannelRegistrationSession) {
			configureDeploymentRuntimeSession(value, runtimeOptions)
		},
	}
	var mu sync.Mutex
	clients := map[string]*cliapp.DeploymentChannelControl{}
	if runtimeOptions.deployment != nil {
		control := func(ctx context.Context, op channelonboarding.Operation) (*cliapp.DeploymentChannelControl, error) {
			if rootErr != nil {
				return nil, rootErr
			}
			return deploymentTransportControl(ctx, op, root, config, directory, stores, process, factory, clients, &mu)
		}
		runtimeOptions.deployment.resume = func(ctx context.Context, op channelonboarding.Operation) error {
			client, err := control(ctx, op)
			if err != nil {
				return err
			}
			return client.ResumeRegisteredAccount(ctx, channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}, op)
		}
		runtimeOptions.deployment.registrations = func(ctx context.Context) ([]serveDeploymentRegistration, error) {
			if rootErr != nil {
				return nil, rootErr
			}
			return readDeploymentTransportRegistrations(ctx, root, config, stores, control)
		}
		runtimeOptions.deployment.read = func(ctx context.Context, parent channelnative.TransportParent) (channelonboarding.Result, packs.OutboundBindingPlan, error) {
			op, err := stores.DeploymentChannels().GetChannelOnboarding(ctx, parent.ID)
			if err != nil {
				return channelonboarding.Result{}, packs.OutboundBindingPlan{}, err
			}
			client, err := control(ctx, op)
			if err != nil {
				return channelonboarding.Result{}, packs.OutboundBindingPlan{}, err
			}
			return client.DeploymentTransport(ctx, parent)
		}
	}
	handler := func(ctx context.Context, req apiv1.Request) (any, error) {
		if rootErr != nil {
			return nil, rootErr
		}
		return dispatchDeploymentControl(ctx, req, root, config, directory, stores, process, capability, factory, runtimeOptions, clients, &mu)
	}
	close := func() error {
		mu.Lock()
		defer mu.Unlock()
		return closeDeploymentControls(clients)
	}
	claim := deploymentPendingClaimHandler(root, config, directory, stores, process, capability, factory, runtimeOptions)
	restore := func(ctx context.Context) error {
		operations, err := stores.DeploymentChannels().ListChannelOnboardingOperations(ctx)
		if err != nil || rootErr != nil {
			return errors.Join(err, rootErr)
		}
		return restoreDeploymentAccountControls(ctx, operations, root, config, directory, stores, process, factory, clients, &mu, runtimeOptions.output, runtimeOptions.sessions)
	}
	return handler, claim, restore, close
}

func readDeploymentTransportRegistrations(ctx context.Context, root cliapp.InvocationRoot, config string, stores *selected.Owner, control func(context.Context, channelonboarding.Operation) (*cliapp.DeploymentChannelControl, error)) ([]serveDeploymentRegistration, error) {
	loaded, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: root.Path(), ExplicitPath: config, ChannelRegistration: ChannelRegistrations()})
	if err != nil {
		return nil, err
	}
	if loaded.Config.Channels.HITL == nil {
		return nil, nil
	}
	facts, _ := installedChannelFactsFor(loaded.Config.Channels.HITL.Provider)
	op, err := channelonboarding.RegisteredAccount(ctx, stores.DeploymentChannels(), *loaded.Config.Channels.HITL, facts)
	if errors.Is(err, channelonboarding.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	client, err := control(ctx, op)
	if err != nil {
		return nil, err
	}
	result, plan, err := client.DeploymentTransport(ctx, channelnative.TransportParent{})
	if errors.Is(err, channelonboarding.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []serveDeploymentRegistration{{result: result, plan: plan, trigger: client.DeploymentTrigger()}}, nil
}

func dispatchDeploymentControl(ctx context.Context, req apiv1.Request, root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, factory cliapp.ChannelRegistrationFactory, runtimeOptions deploymentRuntimeOptions, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) (any, error) {
	if err := capability.ProveCurrent(ctx); err != nil {
		return nil, err
	}
	var err error
	provider, _ := req.Params["provider"].(string)
	id, _ := req.Params["operation_id"].(string)
	account, _ := req.Params["account"].(string)
	if req.Method == "channel.list" && account != "" && runtimeOptions.manager != nil {
		ref := channelonboarding.AccountReference{Provider: provider, Account: account}
		return observeDeploymentAccountStatus(ctx, ref, runtimeOptions, stores.DeploymentChannels(), clients, mu)
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
		result, err = observeDeploymentAccountList(ctx, result, runtimeOptions, stores.DeploymentChannels(), clients, mu)
	}
	head, decodeErr := deploymentControlResult(result, err)
	if decodeErr != nil {
		return nil, decodeErr
	}
	if err == nil && req.Method == "channel.confirm" {
		head.Operation, err = confirmedDeploymentControlParent(ctx, stores, result, provider)
		if err != nil {
			return nil, err
		}
	}
	rememberDeploymentControl(clients, mu, provider, head.Operation, client)
	if !found && head.Operation.OperationID == "" {
		err = errors.Join(err, client.Close(context.WithoutCancel(ctx)))
	}
	if err == nil && !slices.Contains([]string{"channel.list", "channel.onboarding_get", "channel.onboarding_select"}, req.Method) && runtimeOptions.refresh != nil {
		err = runtimeOptions.refresh(ctx, channelonboarding.AccountReference{Provider: head.Operation.Provider, Account: head.Operation.AccountReference})
	}
	return result, err
}

func confirmedDeploymentControlParent(ctx context.Context, stores *selected.Owner, result json.RawMessage, provider string) (channelonboarding.Operation, error) {
	var confirmed struct {
		Operation operatorchannel.Operation `json:"operation"`
	}
	if err := json.Unmarshal(result, &confirmed); err != nil {
		return channelonboarding.Operation{}, err
	}
	parent, err := stores.DeploymentChannels().GetChannelOnboarding(ctx, confirmed.Operation.OnboardingOperationID)
	if err != nil || parent.Provider != provider || parent.IdentityOperationID != confirmed.Operation.OperationID ||
		parent.PrincipalID != confirmed.Operation.PrincipalID {
		return channelonboarding.Operation{}, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	return parent, nil
}

func deploymentTransportControl(ctx context.Context, op channelonboarding.Operation, root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, factory cliapp.ChannelRegistrationFactory, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) (*cliapp.DeploymentChannelControl, error) {
	key := op.Provider + ":" + op.OperationID
	mu.Lock()
	client := clients[key]
	mu.Unlock()
	if client != nil {
		return client, nil
	}
	if isInstalledSession(op.Provider) {
		return nil, fmt.Errorf("deployment operator connection has no exact restored control")
	}
	client, err := cliapp.UseDeploymentChannelControl(ctx, root, config, directory, op.Provider, stores, process, factory)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	owned := clients[key]
	if owned == nil {
		clients[key] = client
	}
	mu.Unlock()
	if owned != nil {
		return owned, client.Close(context.WithoutCancel(ctx))
	}
	return client, nil
}

func deploymentPendingClaimHandler(root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, factory cliapp.ChannelRegistrationFactory, runtimeOptions deploymentRuntimeOptions) publicingress.PendingDeploymentClaim {
	return func(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), claim func(providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error), operator func(providertriggers.InboundAdmissionPlan) (providertriggers.AdmittedRequest, error), current func() bool) (bool, error) {
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
			return op.PendingDeploymentClaim(account) || op.DeploymentRegistration && op.Provider == account.Provider &&
				op.AccountReference == account.Account && op.AccountConfirmed && op.IdentityOperationID != "" &&
				op.Phase == channelonboarding.PhaseSucceeded && !op.LogoutCompleted
		}):
			return false, nil
		}
		client, err := cliapp.UseDeploymentChannelControl(work.Context(), root, config, directory, account.Provider, stores, process, factory)
		if err != nil {
			return false, err
		}
		defer func() { _ = client.Close(context.WithoutCancel(ctx)) }()
		return client.ConsumeWebhookClaim(work.Context(), account, credential, admit, claim, operator, current)
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

func restoreDeploymentAccountControls(ctx context.Context, operations []channelonboarding.Operation, root cliapp.InvocationRoot, config, directory string, stores *selected.Owner, process *worklifetime.Process, factory cliapp.ChannelRegistrationFactory, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex, output io.Writer, sessions serveSessionBootstrapOwner) error {
	for _, binding := range installedChannelFacts() {
		if !binding.Session() {
			continue
		}
		for _, op := range channelonboarding.RetainedConfirmedAccounts(operations, binding.Provider()) {
			ref := channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}
			client, err := cliapp.UseDeploymentChannelControl(ctx, root, config, directory, op.Provider, stores, process, factory)
			if err != nil {
				return err
			}
			if err := client.ResumeRegisteredAccount(ctx, ref, op); err != nil {
				if channelonboarding.SessionUnavailableOnly(err) && errors.Is(err, channelonboarding.ErrSessionNotLinked) {
					if err := client.Close(context.WithoutCancel(ctx)); err != nil {
						return err
					}
					observer, ok := sessions.(interface {
						recordDeploymentNotLinked(context.Context, channelonboarding.Operation, error) error
					})
					if !ok {
						return channelonboarding.ErrInvalidRequest
					}
					if observeErr := observer.recordDeploymentNotLinked(ctx, op, err); observeErr != nil {
						return observeErr
					}
					if output != nil {
						fmt.Fprintln(output, channelonboarding.ErrSessionNotLinked)
					}
					continue
				}
				if !channelonboarding.SessionUnavailableOnly(err) {
					return errors.Join(err, client.Close(context.WithoutCancel(ctx)))
				}
			}
			mu.Lock()
			clients[op.Provider+":"+op.OperationID] = client
			mu.Unlock()
		}
	}
	return nil
}

func configureDeploymentRuntimeSession(value cliapp.ChannelRegistrationSession, runtimeOptions deploymentRuntimeOptions) {
	if runtimeOptions.manager == nil {
		return
	}
	session, native := value.(*deploymentChannelSession)
	if !native {
		return
	}
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

// OpenChannelRegistration composes an invocation, not Run or a runtime target.
// Keeping native wiring here follows the existing CLI executable-runner seam.
func OpenChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	binding, found := installedChannelProvider(opts.Plan.Provider())
	if !found {
		return nil, channelonboarding.ErrInvalidRequest
	}
	return openChannelRegistration(ctx, opts, binding)
}

func openChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions, binding installedChannelBinding) (cliapp.ChannelRegistrationSession, error) {
	if ctx == nil || ctx.Err() != nil || opts.Process == nil || opts.Operations == nil || opts.Store == nil ||
		opts.Credentials == nil || opts.Principal.Validate() != nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	if !binding.facts.MatchesProvider(opts.Plan.Provider()) || binding.registration == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	opts.Plan = opts.Plan.WithProviderFacts(binding.facts)
	return binding.registration(ctx, opts)
}

func (c *deploymentChannelSession) QualifySessionPlan(candidate channelonboarding.Candidate) error {
	return sessionprovider.QualifyBootstrapPlan(candidate.Plan)
}
func (c *deploymentChannelSession) BootstrapSession(ctx context.Context, op channelonboarding.Operation, _ channelonboarding.Candidate) error {
	if c.connection != nil {
		return c.connection.CheckBootstrap(ctx)
	}
	binding, installed := installedChannelProvider(c.opts.Plan.Provider())
	if !installed || binding.deployment == nil {
		return channelonboarding.ErrInvalidRequest
	}
	connection, err := binding.deployment(ctx, sessionprovider.DeploymentConnectionOptions{Directory: c.opts.Directory,
		OperationID: op.OperationID, Principal: c.opts.Principal, Process: c.opts.Process, Operations: c.opts.Operations,
		Store: c.opts.Store, Credentials: c.opts.Credentials, Plan: c.opts.Plan, Trigger: c.opts.Trigger, RegisteredBusiness: c.business,
		OperatorAccount: c.opts.OperatorAccount, OperatorBinding: c.opts.OperatorBinding})
	if connection != nil {
		c.connection = connection
	}
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
		if err != nil {
			return err
		}
		if !reusable {
			if err := c.connection.Close(ctx); err != nil {
				return err
			}
			c.connection = nil
		}
	}
	if c.connection == nil {
		if err := c.BootstrapSession(ctx, op, candidate); err != nil {
			return err
		}
	}
	if op.AccountConfirmed {
		// Wait for login while preserving observed revocation for re-verification.
		_, err := c.connection.VerifyDeploymentRegistration(ctx, op)
		return c.connection.RecoveryFailure(ctx, op, err)
	}
	return nil
}
func (c *deploymentChannelSession) AwaitSessionBootstrap(ctx context.Context, op channelonboarding.Operation) error {
	return c.connection.CheckBootstrap(ctx)
}
func (c *deploymentChannelSession) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	return c.connection.CheckpointPairing(ctx, op.Revision)
}
func (c *telegramDeploymentSession) telegramIdentity(ctx context.Context, op channelonboarding.Operation) (string, string, error) {
	profile, ok := c.opts.Plan.Registration()
	if !ok || !op.DeploymentRegistration || op.Provider != "telegram" || op.PrincipalID != c.opts.Principal.ID {
		return "", "", channelonboarding.ErrInvalidRequest
	}
	plan, err := profile.Operation(packs.RegistrationOperationIdentify)
	if err != nil {
		return "", "", err
	}
	values := map[string]any{}
	for _, admission := range op.CredentialAdmissions {
		snapshot, current, err := c.opts.Credentials.ObserveValueMatchingSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
		if err != nil || !current {
			return "", "", errors.Join(operatorchannel.ErrCredentialStale, err)
		}
		values[admission.Role] = snapshot.CredentialValue()
	}
	input, err := plan.Prepare(map[string]any{})
	if err != nil {
		return "", "", err
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
		return "", "", err
	}
	projected, err := plan.Project(output)
	if err != nil {
		return "", "", err
	}
	identity := fmt.Sprint(projected["resource_id"])
	object, ok := output.(map[string]any)
	if !ok || op.AccountReference != "" && identity != op.AccountReference {
		return "", "", channelonboarding.ErrConflict
	}
	metadata, _ := object["profile"].(map[string]any)
	name, _ := metadata["username"].(string)
	return identity, name, nil
}

func (c *telegramDeploymentSession) telegramRead(ctx context.Context, op channelonboarding.Operation) (string, error) {
	identity, _, err := c.telegramIdentity(ctx, op)
	return identity, err
}
func (c *deploymentChannelSession) ReadSessionPairing(ctx context.Context, _ channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if c.connection == nil {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	return c.connection.PairingReadback(ctx, principal)
}
func (c *deploymentChannelSession) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view := channelonboarding.SessionConnectionReadback{State: "disconnected", Linked: op.AccountReference != "", AccountPresentation: op.AccountReference}
	if c.connection == nil || op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired {
		return view, nil
	}
	view.Reason, view.Cause = c.connection.FailureReport()
	notice, err := c.connection.ReadClaimCodeNotice(ctx, op)
	if err != nil {
		return view, err
	}
	view.ClaimCodeNotice = notice
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
		connected := false
		if view.Linked {
			if op.DeploymentRegistration && !op.AccountConfirmed {
				connected, err = c.connection.SessionPreviewConnected(ctx, op)
				if err != nil {
					return view, err
				}
			} else {
				connected = c.connection.CheckBootstrap(ctx) == nil
			}
		}
		if connected {
			view.State = "connected"
		}
	}
	// The fenced retained-preview wait is a separately held control boundary.
	retainedPreview := view.State == "connecting" && op.DeploymentRegistration && !op.AccountConfirmed && op.Phase == channelonboarding.PhaseAwaitingExternalIdentity
	if view.Cause != "" && !retainedPreview {
		view.State = "error"
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

func (c *deploymentChannelSession) PollOperatorClaim(context.Context, channelonboarding.Operation) error {
	return channelonboarding.ErrInvalidRequest
}

func (c *deploymentChannelSession) ConsumeWebhookClaim(context.Context, channelonboarding.AccountReference, func(channelonboarding.CredentialAdmission) bool, func(providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error), func() bool) (bool, error) {
	return false, channelonboarding.ErrInvalidRequest
}

func (c *deploymentChannelSession) VerifyRegistration(ctx context.Context, op channelonboarding.Operation) (bool, error) {
	if c.connection == nil {
		return false, channelonboarding.ErrInvalidRequest
	}
	return c.connection.VerifyDeploymentRegistration(ctx, op)
}

func (c *deploymentChannelSession) LogoutRegistration(ctx context.Context, op channelonboarding.Operation) error {
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
	if !isInstalledSession(op.Provider) || op.Provider != c.opts.Plan.Provider() || c.connection == nil ||
		op.PrincipalID != c.opts.Principal.ID || c.connection.ConnectionID() != op.SessionConnectionID {
		return channelonboarding.ErrRevisionConflict
	}
	return c.Close(ctx)
}
