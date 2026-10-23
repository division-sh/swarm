//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
}

func deploymentChannelHandlers(repo, config, directory string, stores *selected.Owner, process *worklifetime.Process, capability startupownership.ProcessCapability, webhook func(context.Context, string, string) bool) (apiv1.MethodHandler, publicingress.PendingDeploymentClaim, func() error) {
	root, rootErr := cliapp.NewInvocationRoot(repo)
	factory := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		opts.CurrentWebhook = webhook
		return OpenChannelRegistration(ctx, opts)
	}
	type control interface {
		Dispatch(context.Context, string, map[string]any, any) error
		Close(context.Context) error
	}
	var mu sync.Mutex
	clients := map[string]control{}
	handler := func(ctx context.Context, req apiv1.Request) (any, error) {
		if rootErr != nil {
			return nil, rootErr
		}
		var expected startupownership.Authority
		raw, err := json.Marshal(req.Params["holder"])
		if err != nil || json.Unmarshal(raw, &expected) != nil || expected.Validate() != nil {
			return nil, channelonboarding.ErrInvalidRequest
		}
		actual, err := capability.Evidence()
		if err != nil || actual != expected {
			return nil, errors.Join(channelonboarding.ErrConflict, err)
		}
		if err := capability.ProveCurrent(ctx); err != nil {
			return nil, err
		}
		provider, _ := req.Params["provider"].(string)
		id, _ := req.Params["operation_id"].(string)
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
		var head struct {
			Operation struct {
				OperationID         string `json:"operation_id"`
				IdentityOperationID string `json:"identity_operation_id"`
			} `json:"operation"`
		}
		if err == nil && len(result) != 0 {
			if decodeErr := json.Unmarshal(result, &head); decodeErr != nil {
				return nil, decodeErr
			}
		}
		var required *channelonboarding.CredentialRequiredError
		if errors.As(err, &required) {
			head.Operation.OperationID = required.OperationID
		}
		mu.Lock()
		if head.Operation.OperationID != "" {
			clients[provider+":"+head.Operation.OperationID] = client
		}
		if head.Operation.IdentityOperationID != "" {
			clients[provider+":"+head.Operation.IdentityOperationID] = client
		}
		mu.Unlock()
		if !found && head.Operation.OperationID == "" {
			err = errors.Join(err, client.Close(context.WithoutCancel(ctx)))
		}
		return result, err
	}
	close := func() error {
		mu.Lock()
		defer mu.Unlock()
		seen := map[control]bool{}
		var err error
		for _, client := range clients {
			if !seen[client] {
				err = errors.Join(err, client.Close(context.Background()))
				seen[client] = true
			}
		}
		return err
	}
	claim := func(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
		work, err := process.Begin(ctx)
		if err != nil {
			return false, err
		}
		defer func() { _ = work.Done() }()
		if err := capability.ProveCurrent(work.Context()); err != nil {
			return false, err
		}
		client, err := cliapp.UseDeploymentChannelControl(work.Context(), root, config, directory, account.Provider, stores, process, factory)
		if err != nil {
			return false, err
		}
		defer func() { _ = client.Close(context.WithoutCancel(ctx)) }()
		return client.ConsumeWebhookClaim(work.Context(), account, credential, admit, current)
	}
	return handler, claim, close
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
		Store: c.opts.Store, Credentials: c.opts.Credentials, Plan: c.opts.Plan, Trigger: c.opts.Trigger})
	if err != nil {
		return err
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
		_, err := c.telegramRead(ctx, op, packs.RegistrationOperationIdentify)
		return err
	}
	return c.connection.CheckBootstrap(ctx)
}
func (c *deploymentChannelSession) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	if op.Provider == "telegram" {
		identity, err := c.telegramRead(ctx, op, packs.RegistrationOperationIdentify)
		if err != nil {
			return op, false, err
		}
		ref, err := channelonboarding.CanonicalAccount(op.Provider, identity["resource_id"])
		if err != nil || op.ExpectedAccount != "" && op.ExpectedAccount != ref.Account {
			return op, false, errors.Join(channelonboarding.ErrConflict, err)
		}
		next, err := c.opts.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
			ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseAwaitingExternalIdentity, AccountReference: ref.Account, Now: time.Now().UTC()})
		return next, err == nil, err
	}
	return c.connection.CheckpointPairing(ctx, op.Revision)
}

func (c *deploymentChannelSession) telegramRead(ctx context.Context, op channelonboarding.Operation, name string) (map[string]string, error) {
	profile, ok := c.opts.Plan.Registration()
	if !ok || !op.DeploymentRegistration || op.Provider != "telegram" || op.PrincipalID != c.opts.Principal.ID {
		return nil, channelonboarding.ErrInvalidRequest
	}
	plan, err := profile.Operation(name)
	if err != nil {
		return nil, err
	}
	values := map[string]any{}
	for _, admission := range op.CredentialAdmissions {
		snapshot, current, err := c.opts.Credentials.ObserveValueMatchingSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
		if err != nil || !current {
			return nil, errors.Join(operatorchannel.ErrCredentialStale, err)
		}
		values[admission.Role] = snapshot.CredentialValue()
	}
	input, err := plan.Prepare(map[string]any{})
	if err != nil {
		return nil, err
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
		return nil, err
	}
	projected, err := plan.Project(output)
	if name == packs.RegistrationOperationReadback {
		if fields, ok := output.(map[string]any); ok {
			if callback, ok := fields["callback_url"].(string); ok {
				return map[string]string{"callback_url": callback}, nil
			}
		}
	}
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(projected))
	for key, value := range projected {
		result[key] = fmt.Sprint(value)
	}
	return result, nil
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
		if _, err := c.telegramRead(ctx, op, packs.RegistrationOperationIdentify); err != nil {
			return view, err
		}
		view.State = "connected"
		return view, nil
	}
	if c.connection == nil || op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired {
		return view, nil
	}
	view.Reason = c.connection.DisconnectionReason()
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
	return errors.Join(c.connection.Close(ctx), ctx.Err())
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
		identity, err := c.telegramRead(ctx, op, packs.RegistrationOperationIdentify)
		if err != nil {
			return false, err
		}
		if identity["resource_id"] != op.AccountReference {
			return false, channelonboarding.ErrConflict
		}
		return true, nil
	}
	if c.connection == nil {
		return false, channelonboarding.ErrInvalidRequest
	}
	return c.connection.VerifyDeploymentRegistration(ctx, op)
}

func (c *deploymentChannelSession) LogoutRegistration(ctx context.Context, original, retired channelonboarding.Operation) error {
	if original.Provider == "telegram" {
		return nil
	}
	if c.connection == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if err := c.connection.LogoutDeploymentRegistration(ctx, original, retired); err != nil {
		return err
	}
	if err := c.connection.Close(ctx); err != nil {
		return err
	}
	c.connection = nil
	return nil
}

func (c *deploymentChannelSession) CloseRegistration(ctx context.Context, op channelonboarding.Operation) error {
	if op.Provider != "whatsapp" || c.connection == nil || op.PrincipalID != c.opts.Principal.ID || c.connection.ConnectionID() != op.SessionConnectionID {
		return channelonboarding.ErrRevisionConflict
	}
	if err := c.connection.Close(ctx); err != nil {
		return err
	}
	c.connection = nil
	return nil
}
