package cliapp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected"
)

// DeploymentChannelControl owns one attempt. The durable operation owner holds
// registration; this composition retains only its current SDK possession.
type DeploymentChannelControl struct {
	mu              sync.Mutex
	store           *selected.Owner
	service         *channelonboarding.Service
	process         *worklifetime.Process
	session         ChannelRegistrationSession
	expectedAccount string
	provider        string
	borrowed        bool
	remote          *cliAPIClient
	holder          *startupownership.Authority
}

type registrationHolder struct {
	store   *selected.Owner
	process *worklifetime.Process
}

func UseDeploymentChannelControl(ctx context.Context, root InvocationRoot, config, directory, provider string, owner *selected.Owner, process *worklifetime.Process, factory ChannelRegistrationFactory) (*DeploymentChannelControl, error) {
	if owner == nil || process == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	opts := defaultRootCommandOptions()
	opts.invocationRoot, opts.channelRegistration = root, factory
	opts.rootFlags = &rootCommandFlagState{configPath: config, configPathSet: true, swarmDir: directory, swarmDirSet: true}
	return openDeploymentChannelClient(ctx, provider, opts, registrationHolder{owner, process})
}

func openDeploymentChannelClient(ctx context.Context, provider string, opts rootCommandOptions, existing ...registrationHolder) (_ *DeploymentChannelControl, err error) {
	root := opts.invocationRoot
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root.Path(), ExplicitPath: rootConfigPath(opts)})
	if err != nil {
		return nil, err
	}
	if !loaded.DeploymentConfigured() {
		return nil, errors.New("No deployment config here: run from the deployment directory or pass --config")
	}
	base, err := LoadConfiguredPlatformPackBase(root.Path(), loaded)
	if err != nil {
		return nil, err
	}
	inventory, err := packartifact.NewEffectivePackInventory(base)
	if err != nil {
		return nil, err
	}
	specPath, err := EmbeddedPlatformSpecPath()
	if err != nil {
		return nil, err
	}
	spec, err := loadChannelPlatformSpecDocument(specPath)
	if err != nil {
		return nil, err
	}
	projection, err := packadmission.Admit(inventory, spec)
	if err != nil {
		return nil, err
	}
	var candidate channelonboarding.Candidate
	for _, plan := range projection.ChannelPlans {
		if plan.Provider() != provider {
			continue
		}
		if candidate.Provider != "" {
			return nil, fmt.Errorf("more than one admitted channel implements %s", provider)
		}
		identity, err := plan.InterfaceIdentity()
		if err != nil {
			return nil, err
		}
		profile, ok := plan.OnboardingProfile()
		if !ok {
			return nil, channelonboarding.ErrInvalidRequest
		}
		candidate = channelonboarding.Candidate{DeploymentRegistration: true, Provider: provider, Interface: identity,
			Plan: plan, Posture: channelonboarding.ActivationPosture(profile.ActivationPosture()),
			Ceremony: channelonboarding.CeremonyAccountLink, ProviderCredentialRole: profile.ProviderCredential(),
			SigningCredentialRole: profile.SigningCredential()}
	}
	if candidate.Provider == "" || candidate.Validate() != nil {
		return nil, fmt.Errorf("no admitted deployment channel for %s", provider)
	}
	directory, err := resolveCLISwarmDirFromConfig(root, opts.swarmDirResolutionOptions(), loaded.cli)
	if err != nil {
		return nil, err
	}
	c, err := openDeploymentChannelStore(ctx, provider, opts, loaded, existing...)
	if err != nil {
		return nil, err
	}
	if c.remote != nil {
		return c, nil
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Close(context.WithoutCancel(ctx)))
		}
	}()
	if !c.borrowed {
		if err := c.bootstrapRegistrationSchema(ctx, spec); err != nil {
			return nil, err
		}
	}
	return c, c.initializeRegistration(ctx, directory.Path, loaded.Config.Channels.HITL, candidate, projection.ProviderTriggers, opts)
}

func (c *DeploymentChannelControl) bootstrapRegistrationSchema(ctx context.Context, spec runtimecontracts.PlatformSpecDocument) error {
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return err
	}
	schema, err := SchemaBootstrapRequest(spec, plans, nil)
	if err != nil {
		return err
	}
	return c.store.Schema().BootstrapSchema(ctx, schema)
}

func openDeploymentChannelStore(ctx context.Context, provider string, opts rootCommandOptions, loaded RuntimeConfigLoadResult, existing ...registrationHolder) (*DeploymentChannelControl, error) {
	root := opts.invocationRoot
	selection, err := ResolveRuntimeStoreSelection(root.Path(), "", false, loaded)
	if err != nil {
		return nil, err
	}
	request, err := selectedAuthorityRequest(ctx, selection, loaded.Config)
	if err != nil {
		return nil, err
	}
	if selection.Backend == storebackend.BackendSQLite {
		if err := os.MkdirAll(filepath.Dir(selection.SQLitePath), 0o700); err != nil {
			return nil, err
		}
	}
	c := &DeploymentChannelControl{process: worklifetime.NewProcess(), provider: provider}
	if len(existing) == 1 {
		c.store, c.process, c.borrowed = existing[0].store, existing[0].process, true
		return c, nil
	}
	_, statErr := os.Stat(selection.SQLitePath)
	if selection.Backend == storebackend.BackendSQLite && statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if selection.Backend != storebackend.BackendSQLite || statErr == nil {
		inspection, err := selected.OpenAuthorityInspection(ctx, request)
		if err != nil {
			return nil, err
		}
		observed, readErr := inspection.InspectAuthority(ctx)
		if err := errors.Join(readErr, inspection.Close()); err != nil {
			return nil, err
		}
		if observed.Possession == nil {
			return nil, channelonboarding.ErrConflict
		}
		if !observed.Possession.Available {
			if observed.Authority == nil {
				return nil, errors.New("deployment is busy; no authenticated serve holder is available")
			}
			remote, err := newCLIAPIClient(opts)
			if err != nil {
				return nil, err
			}
			return &DeploymentChannelControl{remote: remote, holder: observed.Authority, provider: provider}, nil
		}
	}
	c.store, err = selected.OpenRuntime(ctx, selected.RuntimeRequest{Selection: selection, PostgresDSN: request.PostgresDSN})
	return c, err
}

func (c *DeploymentChannelControl) initializeRegistration(ctx context.Context, directory string, hitl *channelonboarding.AccountReference, candidate channelonboarding.Candidate, triggers *providertriggers.CatalogSnapshot, opts rootCommandOptions) error {
	owner, provider := c.store, c.provider
	credentialStore, err := CredentialFileStore()
	if err != nil {
		return err
	}
	currentness, err := credentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		return err
	}
	proofs, err := operatorchannel.NewFileProofStore(directory)
	if err != nil {
		return err
	}
	principal, err := owner.OperatorChannels().EnsureOperatorPrincipal(ctx, time.Now().UTC())
	if err != nil {
		return err
	}
	var trigger providertriggers.InboundAdmissionPlan
	switch provider {
	case "whatsapp":
		trigger, err = triggers.CompileAdmission(providertriggers.CompileAdmissionRequest{Provider: provider, Alias: provider})
	case "telegram":
		trigger, err = triggers.CompileRegistrationPolling(provider)
	}
	if err != nil {
		return err
	}
	if opts.channelRegistration == nil {
		return errors.New("channel registration is not installed in this executable")
	}
	c.session, err = opts.channelRegistration(ctx, ChannelRegistrationOptions{Directory: directory, Principal: principal,
		Process: c.process, Store: owner.ChannelOnboarding(), Operations: owner.DeploymentChannels(), Credentials: currentness,
		Identities: owner.OperatorChannels(), HTTP: opts.httpClient, Plan: candidate.Plan, Trigger: trigger})
	if err != nil {
		return err
	}
	identities, err := operatorchannel.NewService(owner.OperatorChannels(), proofs, c.session, []operatorchannel.InterfaceIdentity{candidate.Interface}, principal.ID)
	if err != nil {
		return err
	}
	if _, err = identities.PreparePrincipal(ctx, time.Now().UTC()); err != nil {
		return err
	}
	writer, err := channelonboarding.NewCredentialWriter(credentialStore)
	if err != nil {
		return err
	}
	c.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{Deployment: true, OperatorAccount: hitl,
		Store: owner.ChannelOnboarding(), Operations: owner.DeploymentChannels(), Identities: identities, Credentials: writer, Sessions: c.session,
		Catalog: func() (*channelonboarding.CandidateCatalog, error) {
			return channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
		}})
	return err
}

func (c *DeploymentChannelControl) call(ctx context.Context, method string, params map[string]any, target any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remote != nil {
		params["scope"], params["holder"], params["provider"] = "deployment", c.holder, c.provider
		if method == "channel.onboarding_start" && c.expectedAccount != "" {
			params["account"] = c.expectedAccount
		}
		return c.remote.call(ctx, method, params, target)
	}
	var result any
	var err error
	switch method {
	case "channel.onboarding_select", "channel.onboarding_start":
		expected := c.expectedAccount
		if channelSelectionParam(params, "account") != "" {
			expected = channelSelectionParam(params, "account")
		}
		save, _ := params["save_proof"].(bool)
		input := channelonboarding.StartInput{Verb: channelonboarding.Verb(channelSelectionParam(params, "verb")), SaveProof: save,
			Selection: channelonboarding.CandidateSelection{Provider: channelSelectionParam(params, "provider")}, ExpectedAccount: expected,
			IdempotencyKey: channelSelectionParam(params, "idempotency_key"), ProviderCredential: channelSelectionParam(params, "provider_credential"), ExpectedSelection: channelSelectionParam(params, "expected_selection")}
		if method == "channel.onboarding_select" {
			result, err = c.service.Select(ctx, input)
		} else {
			result, err = c.service.Start(ctx, input)
		}
	case "channel.onboarding_get":
		current, readErr := c.service.Get(ctx, channelSelectionParam(params, "operation_id"))
		if readErr != nil {
			return readErr
		}
		if current.Operation.Provider == "telegram" && current.IdentityOperation != nil && current.IdentityOperation.State == operatorchannel.StateAwaitingClaim {
			if err := c.session.PollOperatorClaim(ctx, current.Operation); err != nil {
				return err
			}
		}
		result, err = c.service.Get(ctx, channelSelectionParam(params, "operation_id"))
	case "channel.onboarding_retry":
		if reverify, _ := params["reverify"].(bool); reverify {
			result, err = c.service.ReverifyDeployment(ctx, channelSelectionParam(params, "operation_id"))
			break
		}
		checkpoint, _ := params["checkpoint_only"].(bool)
		result, err = c.service.Retry(ctx, channelonboarding.RetryInput{OperationID: channelSelectionParam(params, "operation_id"), CheckpointOnly: checkpoint, ProviderCredential: channelSelectionParam(params, "provider_credential")})
	case "channel.confirm":
		revision, _ := params["expected_revision"].(int64)
		approve, _ := params["approve"].(bool)
		op, binding, failure := c.service.ConfirmIdentity(ctx, channelSelectionParam(params, "operation_id"), revision, approve, time.Now().UTC())
		result, err = channelOperationEnvelope{Operation: op, Binding: &binding}, failure
	case "channel.logout":
		revision, _ := params["expected_revision"].(int64)
		result, err = c.service.LogoutDeployment(ctx, channelSelectionParam(params, "operation_id"), revision)
	case "channel.list":
		if account, _ := params["account"].(string); account != "" {
			ref, failure := channelonboarding.CanonicalAccount(c.provider, account)
			if failure != nil {
				return failure
			}
			result, err = c.service.DeploymentAccountStatus(ctx, ref)
			break
		}
		rows, failure := c.service.DeploymentAccounts(ctx, c.provider)
		result, err = map[string]any{"accounts": rows}, failure
	default:
		return channelonboarding.ErrInvalidRequest
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func (c *DeploymentChannelControl) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remote != nil {
		return nil
	}
	var err error
	if c.session != nil {
		err = c.session.Close(ctx)
	}
	if c.borrowed {
		return err
	}
	c.process.Retire()
	_, joined := c.process.Join(ctx)
	if joined != nil {
		return errors.Join(err, joined)
	}
	return errors.Join(err, c.store.CloseUnactivated())
}

func (c *DeploymentChannelControl) Dispatch(ctx context.Context, method string, params map[string]any, target any) error {
	return c.call(ctx, method, params, target)
}

// ResumeRegisteredAccount restores only the retained transport. It neither
// advances registration nor grants any source/flow execution authority.
func (c *DeploymentChannelControl) ResumeRegisteredAccount(ctx context.Context, ref channelonboarding.AccountReference) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remote != nil || c.store == nil || ref.Provider != c.provider {
		return channelonboarding.ErrInvalidRequest
	}
	op, err := channelonboarding.RegisteredAccount(ctx, c.store.DeploymentChannels(), ref)
	if err != nil {
		return err
	}
	return c.session.ResumeSession(ctx, op, channelonboarding.Candidate{})
}

func (c *DeploymentChannelControl) AccountStatus(ctx context.Context, ref channelonboarding.AccountReference) (channelonboarding.AccountStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.service == nil || ref.Provider != c.provider {
		return channelonboarding.AccountStatus{}, channelonboarding.ErrInvalidRequest
	}
	return c.service.DeploymentAccountStatus(ctx, ref)
}

func (c *DeploymentChannelControl) ConsumeWebhookClaim(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool, plans ...packs.SatisfactionPlan) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if consumed, err := c.registeredOperatorInput(ctx, account, credential, admit, current, plans); consumed || err != nil {
		return consumed, err
	}
	return c.session.ConsumeWebhookClaim(ctx, account, credential, admit, current, plans...)
}

func (c *DeploymentChannelControl) registeredOperatorInput(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool, plans []packs.SatisfactionPlan) (bool, error) {
	selected, err := c.service.DeploymentOperatorBinding(ctx, account)
	if errors.Is(err, channelonboarding.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(plans) == 0 {
		return false, channelonboarding.ErrRevisionConflict
	}
	provider, err := channelonboarding.ProviderCredentialAdmission(selected.Operation, selected.Candidate.ProviderCredentialRole)
	if err != nil {
		return false, err
	}
	if credential == nil || admit == nil || current == nil || !current() || !credential(provider) {
		return false, channelonboarding.ErrRevisionConflict
	}
	delivery, err := admit()
	if err != nil {
		return false, err
	}
	reserved := false
	for _, output := range delivery.Events {
		for _, plan := range plans {
			if plan.Provider() != account.Provider {
				continue
			}
			text, matched, err := plan.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
			if err != nil {
				return false, err
			}
			if matched && text.ConversationRef == selected.Binding.ConversationRef && text.ConversationScope == selected.Binding.ConversationScope {
				reserved = true
			}
			action, matched, err := plan.ProjectActionFact(string(output.Name), output.Authorization, output.Payload)
			if err != nil {
				return false, err
			}
			if matched && action.ConversationRef == selected.Binding.ConversationRef && action.ConversationScope == selected.Binding.ConversationScope {
				reserved = true
			}
		}
	}
	latest, err := c.service.DeploymentOperatorBinding(ctx, account)
	if err != nil || latest.Operation.OperationID != selected.Operation.OperationID || latest.Operation.Revision != selected.Operation.Revision || latest.Binding == nil || *latest.Binding != *selected.Binding || !current() || !credential(provider) {
		return false, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	return reserved, nil
}

func (c *DeploymentChannelControl) accounts(ctx context.Context) ([]channelonboarding.Result, error) {
	result := map[string][]channelonboarding.Result{}
	err := c.call(ctx, "channel.list", map[string]any{}, &result)
	return result["accounts"], err
}

func (c *DeploymentChannelControl) connectAccount(ctx context.Context, out, errOut io.Writer, opts channelConnectOptions) (begun channelOnboardingResult, proceed bool, err error) {
	accounts, err := c.accounts(ctx)
	if err != nil {
		return begun, false, returnCLIValidationError(errOut, err)
	}
	if opts.account != "" {
		ref, err := channelonboarding.CanonicalAccount(c.provider, opts.account)
		if err != nil {
			return begun, false, returnCLIValidationError(errOut, err)
		}
		c.expectedAccount = ref.Account
		operation, err := selectDeploymentAccount(accounts, ref.Account, opts.apiOptions.input, errOut, false)
		if err != nil {
			return begun, false, returnCLIValidationError(errOut, err)
		}
		if err := c.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": operation.OperationID, "reverify": true}, &begun); err != nil {
			return begun, false, returnCLIValidationError(errOut, err)
		}
		if begun.Operation.Phase == channelonboarding.PhaseRetired && begun.Operation.FailureCode == "native_account_revoked" {
			fmt.Fprintln(out, "Account was logged out from the phone. Link it again:")
			begun = channelOnboardingResult{}
		}
	} else if len(accounts) > 0 && !opts.yes {
		for _, account := range accounts {
			fmt.Fprintf(out, "Account: %s.\n", account.Operation.DeploymentAccount())
		}
		approved, err := confirmChannelAccount(opts.apiOptions.input, errOut, "Link another? [y/N] ")
		if err != nil || !approved {
			return begun, false, err
		}
	}
	return begun, true, nil
}

func (opts channelConnectOptions) requireDeploymentSelection() error {
	if opts.sourceSet || opts.target != "" || opts.interfaceRef != "" {
		return errors.New("connect registers a deployment account; do not select a flow, target or interface")
	}
	return nil
}

func runDeploymentChannelList(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, output cliOutputOptions) error {
	accounts := []channelonboarding.Result{}
	for _, provider := range []string{"telegram", "whatsapp"} {
		client, err := openDeploymentChannelClient(ctx, provider, opts)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		rows, err := client.accounts(ctx)
		err = errors.Join(err, client.Close(context.WithoutCancel(ctx)))
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		accounts = append(accounts, rows...)
	}
	return renderCLIOutput(out, errOut, output, map[string]any{"accounts": accounts}, func(w io.Writer) {
		for _, row := range accounts {
			if row.Status != nil {
				writeDeploymentAccountStatus(w, *row.Status)
				continue
			}
			state := "pending"
			if row.Operation.AccountConfirmed && row.Operation.Phase == channelonboarding.PhaseSucceeded {
				state = "registered"
			}
			fmt.Fprintf(w, "%s %s · %s\n", row.Operation.Provider, row.Operation.DeploymentAccount(), state)
		}
		if len(accounts) == 0 {
			fmt.Fprintln(w, "No registered accounts. Run swarm channel connect <provider>.")
		}
	}, func() ([]string, error) {
		values := make([]string, 0, len(accounts))
		for _, row := range accounts {
			values = append(values, row.Operation.DeploymentAccount())
		}
		return values, nil
	})
}

func runDeploymentChannelStatus(ctx context.Context, out, errOut io.Writer, provider, account string, opts rootCommandOptions, output cliOutputOptions) (resultErr error) {
	if err := output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if account != "" {
		ref, err := channelonboarding.CanonicalAccount(provider, account)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		account = ref.Account
	}
	client, err := openDeploymentChannelClient(ctx, provider, opts)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	defer func() { resultErr = errors.Join(resultErr, client.Close(context.WithoutCancel(ctx))) }()
	if account == "" {
		rows, err := client.accounts(ctx)
		if err != nil {
			return returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		op, err := selectDeploymentAccount(rows, account, opts.input, errOut, false)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		account = op.DeploymentAccount()
	}
	var status channelonboarding.AccountStatus
	if err := client.call(ctx, "channel.list", map[string]any{"account": account}, &status); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	return renderCLIOutput(out, errOut, output, status, func(w io.Writer) {
		writeDeploymentAccountStatus(w, status)
	}, func() ([]string, error) { return []string{status.Account}, nil })
}

func writeDeploymentAccountStatus(w io.Writer, status channelonboarding.AccountStatus) {
	role := "idle"
	if len(status.Holders) == 1 {
		role = "held by " + status.Holders[0].Flow
	} else if len(status.Holders) == 0 {
		if status.Linked {
			role += " (set operator_account in a flow's policy.yaml; Telegram and WhatsApp receive live)"
		} else {
			role += " (run swarm channel connect " + cliSanitizeOneLineValue(status.Provider) + ")"
		}
	}
	fmt.Fprintf(w, "%s %s · %s · %s · %s", status.Provider, status.Account, status.State, role, status.Health)
	if status.Reason != "" {
		fmt.Fprintf(w, ": %s", status.Reason)
	}
	if status.Unclaimed != nil {
		fmt.Fprintf(w, " · account_unclaimed=%d", *status.Unclaimed)
	}
	fmt.Fprintln(w)
}

func runDeploymentChannelLogout(ctx context.Context, out, errOut io.Writer, provider, account string, opts rootCommandOptions, output cliOutputOptions) (resultErr error) {
	if err := output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if account != "" {
		ref, err := channelonboarding.CanonicalAccount(provider, account)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		account = ref.Account
	}
	client, err := openDeploymentChannelClient(ctx, provider, opts)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	defer func() { resultErr = errors.Join(resultErr, client.Close(context.WithoutCancel(ctx))) }()
	rows, err := client.accounts(ctx)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if len(rows) == 0 && provider == "whatsapp" && account == "" {
		if err := client.Close(context.WithoutCancel(ctx)); err != nil {
			return returnCLIValidationError(errOut, err)
		}
		return runChannelLogout(ctx, out, errOut, provider, "", opts, output)
	}
	selected, err := selectDeploymentAccount(rows, account, opts.input, errOut, !output.asJSON && !output.asYAML && !output.quiet && controlStdinIsTerminal(opts))
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if selected.OperationID == "" {
		return nil
	}
	var result channelonboarding.Result
	if err := client.call(ctx, "channel.logout", map[string]any{"operation_id": selected.OperationID, "expected_revision": selected.Revision}, &result); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if !result.Operation.LogoutCompleted {
		return returnCLIValidationError(errOut, errors.New("logout has not completed"))
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		fmt.Fprintf(w, "Logged out %s account %s.\n", provider, selected.AccountReference)
	}, func() ([]string, error) { return []string{selected.AccountReference}, nil })
}

func selectDeploymentAccount(rows []channelonboarding.Result, account string, input io.Reader, out io.Writer, interactive bool) (selected channelonboarding.Operation, resultErr error) {
	if account == "" && len(rows) > 1 {
		if !interactive {
			return selected, errors.New("more than one account matches; use swarm channel logout <provider> <account>")
		}
		for _, row := range rows {
			fmt.Fprintf(out, "%s %s\n", row.Operation.Provider, row.Operation.DeploymentAccount())
		}
		if input != nil {
			input = bufio.NewReader(input)
		}
		for _, row := range rows {
			approved, err := confirmChannelAccount(input, out, "Log out "+row.Operation.DeploymentAccount()+"? [y/N] ")
			if err != nil {
				return selected, err
			}
			if approved {
				account = row.Operation.DeploymentAccount()
				break
			}
		}
		if account == "" {
			return selected, nil
		}
	}
	for _, row := range rows {
		if account != "" && row.Operation.DeploymentAccount() != account {
			continue
		}
		if selected.OperationID != "" {
			return selected, errors.New("account registration is ambiguous")
		}
		selected = row.Operation
	}
	if selected.OperationID == "" {
		return selected, errors.New("account is not registered; run swarm channel connect <provider> to link it")
	}
	return selected, nil
}
